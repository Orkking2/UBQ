"""A deployment is one study on one cluster: its plan, its state, and `advance`.

`advance` is the only thing that moves a deployment forward. It is idempotent
and runs under the deployment lock, and it is called from everywhere: the
build job and every task call it as they finish, a one-core watchdog job calls
it after each array ends (covering tasks that crashed without a word), and the
local CLI calls it on every `status`. Any caller may die at any point; the
next one continues from the saved state. No daemon runs on a login node.

Phases: building -> measuring (stage 1) -> deep (stage 2) -> done, or failed.
A deployment is superseded when a newer study is launched on its cluster,
and cancelled on request; either way its jobs are cancelled.
"""
from __future__ import annotations

import math
import os
import time
import uuid
from collections import Counter, defaultdict
from pathlib import Path

from . import analysis
from .collectors import APX_RECIPES
from .common import digest_text, locked, read_json, read_json_or, write_json
from .scheduler import JOB_PREFIX, TERMINAL, SubmitError, scheduler_for
from .spec import SpecError, cells, collectors_for

MAX_ATTEMPTS = 2
FINAL = {"done", "failed"}
ENDED = {"done", "failed", "superseded", "cancelled"}
# Largest task array submitted at once, unless the cluster's `array_limit`
# says otherwise. Every element counts against the QOS submit quota (366 per
# user on Grace and MN5), so the clusters set this below their quota and the
# remainder is submitted as earlier arrays drain.
ARRAY_LIMIT = 500


def build_key(arm):
    """Cached-binary name: the arm's source identity plus its feature set."""
    features = ",".join(sorted(arm["features"]))
    return f"{arm['source'][:24]}-{digest_text(features)[:8] if features else 'default'}"


def task_minutes(spec, arms, kind, collector=None, counter_groups=1):
    """A generous Slurm time limit; tasks normally finish far sooner."""
    throughput = spec["throughput"]
    if kind == "throughput":
        seconds = arms * (90 + throughput["pairs"] * (throughput["max_run_seconds"] + 20)) + 120
    elif kind == "memory":
        seconds = arms * len(spec["memory"]["depths"]) * 120 + 120
    else:
        run = spec["deep"]["run_seconds"] * throughput["max_run_seconds"] / throughput["run_seconds"]
        if collector in APX_RECIPES:
            per_arm = run * 4 + 1500
        elif collector == "counters":
            per_arm = (counter_groups + 1) * (run * 2 + 40)
        else:
            per_arm = run * 5 + 900
        seconds = arms * per_arm + 300
    return math.ceil(seconds / 60)


def make_plan(study, cluster, config):
    spec = study["spec"]
    found, skipped = cells(spec, config["cores"])
    collectors_for(spec, cluster, config)  # Reject unknown collectors before anything runs.
    arms = len(study["arms"])
    tasks = []
    for cell in found:
        for replicate in range(spec["throughput"]["replicates"]):
            tasks.append({"id": f"s1-{cell['cell']}-r{replicate}", "stage": 1, **cell,
                          "replicate": replicate, "minutes": task_minutes(spec, arms, cell["kind"])})
    return {"tasks": tasks, "skipped_scenarios": skipped, "cores": config["cores"]}


class Deployment:
    def __init__(self, path):
        self.path = Path(path).resolve()
        self.study_dir = self.path.parent
        self.cluster = self.path.name
        self.study = read_json(self.study_dir / "study.json")
        deploy = read_json(self.path / "deploy.json")
        self.config = deploy["config"]
        self.root = Path(deploy["root"])
        self.prefix = f"{JOB_PREFIX}{self.study['id'].rsplit('-', 1)[1]}"

    @classmethod
    def create(cls, study_dir, cluster, root):
        path = Path(study_dir).resolve() / cluster
        if (path / "deploy.json").exists():
            return cls(path)
        study = read_json(Path(study_dir) / "study.json")
        if cluster not in study["config"]["clusters"]:
            raise SpecError(f"unknown cluster {cluster}; configure it in lab/config.json")
        config = dict(study["config"]["clusters"][cluster])
        if config.get("scheduler") == "local":
            config["cores"] = os.cpu_count()
        plan = make_plan(study, cluster, config)
        write_json(path / "plan.json", plan)
        write_json(path / "state.json", {
            "phase": "building", "created": time.time(), "selected": False, "pending": None,
            "build": {"job": None, "status": "new", "attempts": 0}, "jobs": [], "notes": [],
            "tasks": {task["id"]: {"status": "new", "attempts": 0} for task in plan["tasks"]}})
        (path / "logs").mkdir(exist_ok=True)
        # Written last: its presence means the deployment is complete.
        write_json(path / "deploy.json", {"cluster": cluster, "config": config, "root": str(Path(root).resolve()),
                                          "created": time.time()})
        return cls(path)

    def plan(self):
        return read_json(self.path / "plan.json")

    def load_state(self):
        return read_json(self.path / "state.json")

    def save_state(self, state):
        write_json(self.path / "state.json", state)

    def attempt_dir(self, task_id, attempt):
        return self.path / "tasks" / task_id / f"a{attempt}"

    def build_targets(self):
        return {build_key(arm): arm for arm in self.study["arms"]}

    def binary(self, arm):
        return self.root / "builds" / self.cluster / build_key(arm) / "bench_hardware"

    def missing_builds(self):
        return [key for key in self.build_targets()
                if not (self.root / "builds" / self.cluster / key / "build.json").exists()]

    def marker(self):
        for kind in ("superseded", "cancelled"):
            found = read_json_or(self.path / f"{kind}.json", None)
            if found:
                return {**found, "kind": kind}
        return None

    def status(self, state):
        plan = {task["id"]: task for task in self.plan()["tasks"]}
        counts = {}
        for task_id, entry in state["tasks"].items():
            counts.setdefault(f"stage{plan[task_id]['stage']}", Counter())[entry["status"]] += 1
        failed = [{"task": task_id, "error": entry.get("last_error", "")}
                  for task_id, entry in state["tasks"].items() if entry["status"] == "failed"]
        return {"study": self.study["id"], "cluster": self.cluster, "phase": state["phase"],
                "counts": {stage: dict(c) for stage, c in sorted(counts.items())},
                "build": state["build"], "notes": state["notes"][-6:], "failed": failed[:12],
                "marker": self.marker(), "summary": read_json_or(self.path / "summary.json", {}),
                "path": str(self.path)}


def note(state, message):
    state["notes"] = (state["notes"] + [f"{time.strftime('%m-%d %H:%M')} {message}"])[-40:]


def advance(dep, scheduler=None):
    """Move `dep` as far forward as its jobs allow, then return its status."""
    scheduler = scheduler or scheduler_for(dep)
    with locked(dep.path / "lock"):
        state = dep.load_state()
        marker = dep.marker()
        if marker:
            if not state.get("jobs_cancelled"):
                scheduler.cancel(state["jobs"])
                state["jobs_cancelled"] = True
            state["phase"] = marker["kind"]
        elif state["phase"] not in ENDED:
            _recover_pending(dep, state, scheduler)
            _refresh(dep, state, scheduler)
            # Selection and summaries read task outcomes from disk.
            dep.save_state(state)
            _progress(dep, state, scheduler)
            dep.save_state(state)
            try:
                write_json(dep.path / "summary.json", analysis.summary(dep.study, dep.path))
            except Exception as exc:  # A summary must never stall the pipeline.
                note(state, f"summary failed: {exc!r}")
        dep.save_state(state)
        return dep.status(state)


def _retry_or_fail(entry, reason):
    entry["last_error"] = reason
    entry["status"] = "new" if entry["attempts"] < MAX_ATTEMPTS else "failed"


def _refresh(dep, state, scheduler):
    """Fold each live job's outcome into the state.

    A task's own status file is authoritative: every task writes it before
    calling advance, while its job still shows as RUNNING. Only a job that
    ended without writing one (killed, out of memory, node failure) is judged
    by the scheduler.
    """
    build = state["build"]
    live = [entry["job"] for entry in state["tasks"].values() if entry["status"] in ("pending", "running")]
    if build["status"] in ("pending", "running"):
        live.append(build["job"])
    states = scheduler.states(live) if live else {}
    for task_id, entry in state["tasks"].items():
        if entry["status"] not in ("pending", "running"):
            continue
        sidecar = read_json_or(dep.attempt_dir(task_id, entry["attempts"]) / "status.json", {})
        slurm = states.get(entry["job"], "UNKNOWN")
        if sidecar.get("status") == "done":
            entry["status"] = "done"
        elif sidecar.get("status") == "failed":
            _retry_or_fail(entry, sidecar.get("error", "failed"))
        elif slurm in TERMINAL:
            _retry_or_fail(entry, f"job {entry['job']} ended {slurm} without a result")
        else:
            entry["status"] = "running" if slurm == "RUNNING" or sidecar.get("status") == "running" else "pending"
    if build["status"] in ("pending", "running"):
        slurm = states.get(build["job"], "UNKNOWN")
        build["status"] = "ended" if slurm in TERMINAL else "running" if slurm == "RUNNING" else "pending"


def _progress(dep, state, scheduler):
    missing = dep.missing_builds()
    if missing:
        state["phase"] = "building"
        build = state["build"]
        if build["status"] in ("pending", "running"):
            return
        if build["attempts"] >= MAX_ATTEMPTS:
            state["phase"] = "failed"
            note(state, f"build of {', '.join(missing)} failed; see {dep.path / 'logs'}/build-*")
            return
        _submit(dep, state, scheduler, "build", None, ["build", str(dep.path)],
                minutes=dep.config.get("build_minutes", 60), kind="build")
        return
    plan = {task["id"]: task for task in dep.plan()["tasks"]}
    _submit_ready(dep, state, scheduler, plan)
    first = [entry for task_id, entry in state["tasks"].items() if plan[task_id]["stage"] == 1]
    if not state["selected"] and all(entry["status"] in FINAL for entry in first):
        selection = analysis.select(dep.study, analysis.effects(dep.study, dep.path))
        write_json(dep.path / "selection.json", {"cells": selection, "time": time.time()})
        state["selected"] = True
        if dep.study["spec"]["deep"]["auto"]:
            add_deep(dep, state, selection)
        else:
            note(state, "stage 1 done; deep.auto is false, so request cells with `./ab deep`")
        _submit_ready(dep, state, scheduler, {task["id"]: task for task in dep.plan()["tasks"]})
    if state["selected"] and all(entry["status"] in FINAL for entry in state["tasks"].values()):
        state["phase"] = "done"
    else:
        state["phase"] = "deep" if state["selected"] else "measuring"


def add_deep(dep, state, selection, collectors=None):
    """Append deep tasks for the selected cells; existing tasks are kept."""
    spec = dep.study["spec"]
    collectors = collectors or collectors_for(spec, dep.cluster, dep.config)
    groups = len(dep.config.get("counter_groups", {})) or 1
    plan = dep.plan()
    existing = {task["id"] for task in plan["tasks"]}
    # A cell requested again is captured again: its new tasks continue the
    # replicate numbering, so a capture that failed under a collector can be
    # repeated without touching the earlier results.
    done_before = defaultdict(int)
    for task in plan["tasks"]:
        if task["kind"] == "deep":
            done_before[(task["cell"], task["collector"])] += 1
    added = 0
    for entry in selection:
        schedule = analysis.cell_schedule(dep.path, entry["cell"])
        if schedule is None:
            note(state, f"{entry['cell']}: no completed stage-1 schedule to replay; skipped")
            continue
        # Replay stage 1's round size; scale its round count to the deep run length.
        rounds = max(3, round(schedule["rounds"] * spec["deep"]["run_seconds"] / spec["throughput"]["run_seconds"]))
        for collector in collectors:
            first = done_before[(entry["cell"], collector)] if entry.get("again") else 0
            for replicate in range(first, first + spec["deep"]["replicates"]):
                task_id = f"s2-{entry['cell']}-{collector}-r{replicate}"
                if task_id in existing:
                    continue
                plan["tasks"].append({
                    "id": task_id, "stage": 2, "kind": "deep", "cell": entry["cell"],
                    "scenario": entry["scenario"], "producer_batch": entry["producer_batch"],
                    "consumer_batch": entry["consumer_batch"], "collector": collector,
                    "arms": entry["arms"], "replicate": replicate, "reasons": entry.get("reasons", []),
                    "schedule": {"round_items": schedule["round_items"], "rounds": rounds, "warmup_rounds": 2},
                    "minutes": task_minutes(spec, len(entry["arms"]), "deep", collector, groups)})
                state["tasks"][task_id] = {"status": "new", "attempts": 0}
                added += 1
    write_json(dep.path / "plan.json", plan)
    note(state, f"deep stage: {added} tasks for {', '.join(e['cell'] for e in selection) or 'no cells'}")


def _submit_ready(dep, state, scheduler, plan):
    groups = {}
    for task_id, entry in state["tasks"].items():
        if entry["status"] == "new":
            task = plan[task_id]
            groups.setdefault((task["stage"], task["minutes"]), []).append(task_id)
    limit = dep.config.get("array_limit") or ARRAY_LIMIT
    for (stage, minutes), task_ids in sorted(groups.items()):
        for start in range(0, len(task_ids), limit):
            if not _submit(dep, state, scheduler, f"s{stage}", task_ids[start:start + limit],
                           None, minutes=minutes, kind="task"):
                return


def _submit(dep, state, scheduler, label, task_ids, args, *, minutes, kind):
    """Submit one job (or array), recording intent first so a crash cannot duplicate it."""
    token = uuid.uuid4().hex[:6]
    name = f"{dep.prefix}.{label}.{token}"
    if task_ids is not None:
        mapping = dep.path / "arrays" / f"{token}.json"
        write_json(mapping, [{"id": task_id, "attempt": state["tasks"][task_id]["attempts"] + 1}
                             for task_id in task_ids])
        args = ["task", str(dep.path), str(mapping)]
    state["pending"] = {"name": name, "tasks": task_ids}
    dep.save_state(state)
    try:
        job = scheduler.submit(name, args, minutes=minutes, log_dir=dep.path / "logs", kind=kind,
                               array=len(task_ids) if task_ids is not None else None)
    except SubmitError as exc:
        state["pending"] = None
        note(state, str(exc))
        return False
    state["pending"] = None
    if job is None:
        note(state, "the QOS submit quota is full; the next advance retries")
        return False
    _adopt(state, task_ids, job)
    dep.save_state(state)
    if kind == "task":
        # Covers the one case no task can report: all of them crashing.
        try:
            watchdog = scheduler.submit(f"{dep.prefix}.adv.{token}", ["advance", str(dep.path)], minutes=10,
                                        log_dir=dep.path / "logs", kind="control", dependency=job)
            if watchdog:
                state["jobs"].append(watchdog)
        except SubmitError as exc:
            note(state, f"watchdog not submitted ({exc}); `./ab status` still advances")
    return True


def _adopt(state, task_ids, job):
    if task_ids is None:
        build = state["build"]
        build.update(job=job, status="pending", attempts=build["attempts"] + 1)
    else:
        for index, task_id in enumerate(task_ids):
            entry = state["tasks"][task_id]
            entry.update(status="pending", job=f"{job}_{index}", attempts=entry["attempts"] + 1)
    state["jobs"].append(job)


def _recover_pending(dep, state, scheduler):
    """Reconcile a submission interrupted between sbatch and saving its job id."""
    pending = state.get("pending")
    if not pending:
        return
    found = scheduler.find(pending["name"])
    if found:
        _adopt(state, pending["tasks"], found[0])
        scheduler.cancel(found[1:])
    state["pending"] = None


def supersede_others(dep, scheduler):
    """Cancel every earlier study still running on this cluster."""
    cancelled = []
    for other in sorted((dep.root / "studies").glob(f"*/{dep.cluster}/deploy.json")):
        path = other.parent
        if path == dep.path or read_json_or(path / "state.json", {}).get("phase") in ENDED:
            continue
        with locked(path / "lock"):
            write_json(path / "superseded.json", {"by": dep.study["id"], "time": time.time()})
            state = read_json(path / "state.json")
            scheduler.cancel(state.get("jobs", []))
            state.update(jobs_cancelled=True, phase="superseded")
            write_json(path / "state.json", state)
        cancelled.append(path.parent.name)
    # Lab jobs whose deployment directory is gone, or was never recorded.
    strays = [(job, name) for job, name in scheduler.ours() if not name.startswith(dep.prefix + ".")]
    scheduler.cancel([job for job, _ in strays])
    return cancelled + [name for _, name in strays]


def deploy(study_dir, cluster, keep_running=False, root=None):
    """Create (or reuse) the deployment, cancel superseded studies, and advance."""
    study_dir = Path(study_dir).resolve()
    # Remotely a study lives at <root>/studies/<id>; locally the root is separate.
    dep = Deployment.create(study_dir, cluster, root=root or study_dir.parent.parent)
    scheduler = scheduler_for(dep)
    superseded = [] if keep_running else supersede_others(dep, scheduler)
    status = advance(dep, scheduler)
    status["superseded"] = superseded
    return status


def cancel(dep, reason="requested", scheduler=None):
    write_json(dep.path / "cancelled.json", {"reason": reason, "time": time.time()})
    return advance(dep, scheduler)


def request_deep(dep, cells_requested, collectors=None, scheduler=None):
    """Add deep tasks for chosen cells, reopening a finished deployment."""
    known = {task["cell"]: task for task in dep.plan()["tasks"] if task["kind"] == "throughput"}
    unknown = [cell for cell in cells_requested if cell not in known]
    if unknown:
        raise SpecError(f"no stage-1 cells named {unknown}; known: {sorted(known)}")
    if collectors:
        available = dep.config.get("collectors", [])
        if set(collectors) - set(available):
            raise SpecError(f"{dep.cluster} collectors are {available}")
    everyone = [arm["name"] for arm in dep.study["arms"]]
    selection = [{"cell": cell, "scenario": known[cell]["scenario"], "producer_batch": known[cell]["producer_batch"],
                  "consumer_batch": known[cell]["consumer_batch"], "arms": everyone, "again": True,
                  "reasons": ["requested with ./ab deep"]} for cell in cells_requested]
    with locked(dep.path / "lock"):
        state = dep.load_state()
        if dep.marker():
            raise SpecError("this deployment was superseded or cancelled")
        if not state["selected"]:
            raise SpecError("stage 1 has not finished; deep cells are added after it")
        add_deep(dep, state, selection, collectors)
        state["phase"] = "deep"
        dep.save_state(state)
    return advance(dep, scheduler)
