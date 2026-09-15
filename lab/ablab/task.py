"""What runs inside an allocation: the build, the measurement tasks, and `advance`.

A throughput task measures one cell (scenario x batch size) once per replicate:
every arm first passes an exact-once check at that shape, a short pilot sizes
one fixed schedule (round size and count) for all arms, and then the arms run
that identical work in interleaved pairs, rotating which arm goes first.
"""
from __future__ import annotations

import math
import os
import platform
import shutil
import sys
import time
import traceback
from pathlib import Path

from . import collectors
from .common import (Interrupted, install_signal_handlers, locked, read_json, run, sha256_file,
                     tail, write_json)
from .deploy import Deployment, advance

VERIFY_ITEMS = 262144
VERIFY_ROUNDS = 8
PILOT_ITEMS = 1 << 20
PILOT_SECONDS = 1.5
# A round is one empty-to-empty handoff burst. ~100 ms rounds keep the
# harness's ~100 us rendezvous well under 1% of a round for all but the
# fastest configurations, which the size cap then bounds.
ROUND_TARGET_SECONDS = 0.1
MIN_ROUND_ITEMS = 1 << 16
MAX_ROUND_ITEMS = 1 << 24
MIN_ROUNDS = 3
FAILURES = (RuntimeError, TimeoutError, OSError, KeyError, ValueError)


class RunError(RuntimeError):
    pass


def rotate(items, shift):
    items = list(items)
    if not items:
        return items
    shift %= len(items)
    return items[shift:] + items[:shift]


def validate(result, config):
    """Reject a result that did not do exactly the work `config` asked for."""
    if not isinstance(result, dict):
        raise RunError("the workload printed no result")
    if result.get("config") != config:
        raise RunError("the workload echoed a different configuration")
    if not result.get("affinity_ok") and not config["allow_unpinned"]:
        raise RunError("worker pinning failed")
    if config["mode"] == "memory":
        if result["checkpoints"][-1]["phase"] != "destroyed":
            raise RunError("incomplete memory lifecycle")
        return result
    align = int(config["scenario"].split("p")[0]) * (config["producer_batch"] or 1)
    expected = config["round_items"] // align * align
    if result.get("round_items") != expected or result.get("items") != expected * result.get("rounds", -1):
        raise RunError("fixed-work item count mismatch")
    if config["rounds"] is not None and result.get("rounds") != config["rounds"]:
        raise RunError("round count mismatch")
    if result.get("elapsed_ns", 0) <= 0:
        raise RunError("non-positive elapsed time")
    if config["mode"] == "verify" and result.get("exact_once_checked") is not True:
        raise RunError("the exact-once check did not run")
    return result


class Workload:
    """Runs one arm's bench_hardware and checks it did exactly the work asked."""

    def __init__(self, binary, env, limit_seconds):
        self.binary = binary
        self.env = env
        self.limit = limit_seconds

    def command(self, run_dir, config):
        run_dir.mkdir(parents=True, exist_ok=True)
        write_json(run_dir / "workload.json", config)
        return [str(self.binary), "--config", str(run_dir / "workload.json")]

    def timeout(self, config, slowdown=1):
        if config["mode"] == "memory":
            return slowdown * 300
        if config["rounds"] is None or config["mode"] == "verify":
            return slowdown * 180
        return slowdown * (2 * self.limit + 90)

    def run(self, run_dir, config, prefix=()):
        command = self.command(run_dir, config)
        completed = run([*prefix, *command], cwd=run_dir, env=self.env,
                        timeout=self.timeout(config, 3 if prefix else 1),
                        stdout=run_dir / "stdout.log", stderr=run_dir / "stderr.log")
        if completed.returncode:
            raise RunError(f"exit {completed.returncode}: {tail(completed.stderr, 800)}")
        return self.accept(collectors.find_result(completed.stdout), config, run_dir)

    def accept(self, result, config, run_dir=None):
        validate(result, config)
        if run_dir is not None:
            write_json(run_dir / "benchmark.json", result)
        return result


def summarize(result):
    keys = ("ops_per_sec", "elapsed_ns", "rounds", "round_items", "items", "warmup_rounds",
            "barrier_overhead_proxy_fraction", "empty_round_median_ns")
    return {key: result.get(key) for key in keys}


class Context:
    def __init__(self, dep, task, attempt):
        self.dep = dep
        self.study = dep.study
        self.spec = dep.study["spec"]
        self.task = task
        self.dir = dep.attempt_dir(task["id"], attempt)
        self.local = dep.config.get("scheduler") == "local"
        self.env = dict(os.environ, RUST_BACKTRACE="1")
        limit = self.spec["throughput"]["max_run_seconds"]
        if task["kind"] == "deep":
            limit *= self.spec["deep"]["run_seconds"] / self.spec["throughput"]["run_seconds"]
        self.arms = {arm["name"]: arm for arm in self.study["arms"]}
        self.workloads = {}
        for name, arm in self.arms.items():
            binary = dep.binary(arm)
            built = read_json(binary.parent / "build.json")
            if sha256_file(binary) != built["sha256"]:
                raise RunError(f"{binary} does not match its build record")
            self.workloads[name] = Workload(binary, self.env, limit)

    def config(self, arm, **overrides):
        config = {"queue": None, "scenario": self.task["scenario"],
                  "producer_batch": self.task["producer_batch"], "consumer_batch": self.task["consumer_batch"],
                  "backoff": "crossbeam", "mode": "handoff", "round_items": PILOT_ITEMS, "rounds": None,
                  "duration_secs": 60.0, "warmup_rounds": 1, "memory_depth": 65536, "idle_secs": [1],
                  # A runaway guard, not a budget: an exclusive Grace or MN5 node has
                  # hundreds of GB, and a wide batched shape legitimately holds several.
                  "rss_limit_mib": 32768, "allow_unpinned": self.local, "block_size": 256,
                  "capacity": 1048576, "segment_size": 256}
        config.update(self.arms[arm]["workload"])
        config.update(overrides)
        return config


def topology(cores):
    """One CPU per physical core of the allocation, ordered by NUMA node, socket and core."""
    listing = run(["lscpu", "-p=CPU,CORE,SOCKET,NODE"])
    if listing.returncode:
        raise RunError("lscpu cannot describe the node")
    allowed = os.sched_getaffinity(0)
    chosen = {}
    for line in listing.stdout.splitlines():
        if line.startswith("#") or not line.strip():
            continue
        cpu, core, socket, node = (int(field or 0) for field in line.split(","))
        if cpu in allowed:
            chosen.setdefault((socket, core), (node, socket, core, cpu))
    ordered = sorted(chosen.values())
    if len(ordered) != cores:
        raise RunError(f"expected {cores} physical cores in the allocation, found {len(ordered)}")
    return [entry[-1] for entry in ordered]


def prepare_node(ctx):
    provenance = {"node": platform.node(), "platform": platform.platform(), "python": sys.version,
                  "slurm": {k: v for k, v in os.environ.items() if k.startswith("SLURM_")}}
    if not ctx.local:
        cpus = topology(ctx.dep.config["cores"])
        ctx.env["UBQ_BENCH_CORE_IDS"] = ",".join(map(str, cpus))
        provenance["cpus"] = cpus
        for name, command in {"lscpu": ["lscpu"], "numa": ["numactl", "--hardware"],
                              "perf": ["perf", "--version"]}.items():
            try:
                provenance[name] = run(command).stdout
            except (OSError, TimeoutError) as exc:
                provenance[name] = f"unavailable: {exc}"
        for file in ("/proc/sys/kernel/perf_event_paranoid",
                     "/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor"):
            if Path(file).exists():
                provenance[file] = Path(file).read_text().strip()
    write_json(ctx.dir / "provenance.json", provenance)


def throughput_task(ctx, record, save):
    spec = ctx.spec["throughput"]
    replicate = ctx.task["replicate"]
    names = list(ctx.arms)
    record["arms"] = {name: {"runs": []} for name in names}
    # A round is cut to a multiple of producers x producer batch, so it must
    # hold at least one batch per producer or the harness has no work to do.
    align = int(ctx.task["scenario"].split("p")[0]) * (ctx.task["producer_batch"] or 1)
    passing = []
    for name in rotate(names, replicate):
        config = ctx.config(name, mode="verify", round_items=max(VERIFY_ITEMS, align), rounds=VERIFY_ROUNDS)
        try:
            ctx.workloads[name].run(ctx.dir / f"verify-{name}", config)
            record["arms"][name]["verify"] = {"ok": True}
            passing.append(name)
        except FAILURES as exc:
            record["arms"][name]["verify"] = {"ok": False, "error": str(exc)}
        save()
    pilots = {}
    for name in passing:
        config = ctx.config(name, duration_secs=PILOT_SECONDS, round_items=max(PILOT_ITEMS, align))
        try:
            pilots[name] = ctx.workloads[name].run(ctx.dir / f"pilot-{name}", config)["ops_per_sec"]
        except FAILURES as exc:
            record["arms"][name]["pilot_error"] = str(exc)
    record["pilot_ops_per_sec"] = pilots
    if not pilots:
        return
    # One schedule for every arm: rounds sized for ~100 ms of the fastest arm,
    # as many as the fastest needs for run_seconds, but no more than lets the
    # slowest finish within max_run_seconds.
    fastest, slowest = max(pilots.values()), min(pilots.values())
    round_items = 1 << int(math.log2(max(1.0, fastest * ROUND_TARGET_SECONDS)))
    round_items = max(min(MAX_ROUND_ITEMS, max(MIN_ROUND_ITEMS, round_items)), align)
    rounds = math.ceil(spec["run_seconds"] * fastest / round_items)
    rounds = max(MIN_ROUNDS, min(rounds, math.floor(spec["max_run_seconds"] * slowest / round_items)))
    schedule = {"round_items": round_items, "rounds": rounds, "warmup_rounds": 2}
    record["schedule"] = schedule
    save()
    measured = [name for name in names if name in pilots]
    for pair in range(spec["pairs"]):
        for slot, name in enumerate(rotate(measured, pair + replicate)):
            entry = {"pair": pair, "slot": slot}
            try:
                result = ctx.workloads[name].run(ctx.dir / f"p{pair}-{name}", ctx.config(name, **schedule))
                entry.update(ops_per_sec=result["ops_per_sec"], elapsed_ns=result["elapsed_ns"],
                             rounds=result["rounds"], round_items=result["round_items"],
                             barrier_fraction=result.get("barrier_overhead_proxy_fraction"),
                             cpu_ns=(result.get("os") or {}).get("user_cpu_ns"),
                             rss_bytes=collectors.rss_bytes(result))
            except FAILURES as exc:
                entry["error"] = str(exc)
            record["arms"][name]["runs"].append(entry)
            save()


def memory_task(ctx, record, save):
    record["arms"] = {}
    for name in rotate(list(ctx.arms), ctx.task["replicate"]):
        depths = record["arms"].setdefault(name, {"depths": {}})["depths"]
        for depth in ctx.spec["memory"]["depths"]:
            config = ctx.config(name, mode="memory", memory_depth=depth)
            try:
                result = ctx.workloads[name].run(ctx.dir / f"{name}-depth{depth}", config)
                depths[str(depth)] = {
                    "rss": {c["phase"]: c["memory"].get("Rss_bytes") for c in result["checkpoints"]},
                    "phase_elapsed_ns": result.get("phase_elapsed_ns")}
            except FAILURES as exc:
                depths[str(depth)] = {"error": str(exc)}
            save()


def deep_task(ctx, record, save):
    task = ctx.task
    record.update(collector=task["collector"], schedule=task["schedule"], arms={})
    for name in rotate(task["arms"], task["replicate"]):
        entry = record["arms"][name] = {}
        config = ctx.config(name, **task["schedule"])
        # Every capture gets an unprofiled twin on the same node and work, so
        # the collector's own slowdown is visible.
        try:
            entry["shadow"] = summarize(ctx.workloads[name].run(ctx.dir / f"{name}-shadow", config))
        except FAILURES as exc:
            entry["shadow_error"] = str(exc)
        save()
        run_name = ".".join([ctx.study["id"], ctx.dep.cluster, task["cell"], name, task["collector"],
                             f"r{task['replicate']}"])
        try:
            captured = collectors.collect(
                task["collector"], ctx.workloads[name], ctx.dir / f"{name}-{task['collector']}", config,
                cluster_config=ctx.dep.config, source_dir=ctx.dep.root / "sources" / ctx.arms[name]["source"] / "src",
                run_name=run_name)
            entry["capture"] = summarize(captured["result"]) if captured["result"] else None
            entry["metrics"] = captured["metrics"]
        except FAILURES + (collectors.CollectorError,) as exc:
            entry["error"] = str(exc)
        save()


def task_job(dep, mapping, index):
    entry = read_json(mapping)[index]
    task = next(task for task in dep.plan()["tasks"] if task["id"] == entry["id"])
    directory = dep.attempt_dir(task["id"], entry["attempt"])
    if directory.exists():  # Slurm requeued this allocation; start the attempt afresh.
        shutil.rmtree(directory)
    directory.mkdir(parents=True)
    status = directory / "status.json"
    write_json(status, {"status": "running", "started": time.time(), "node": platform.node(),
                        "job": os.environ.get("SLURM_JOB_ID")})
    record = {"task": task["id"], "node": platform.node(), "started": time.time()}

    def save():
        write_json(directory / "result.json", record)

    code = 0
    try:
        install_signal_handlers()
        ctx = Context(dep, task, entry["attempt"])
        prepare_node(ctx)
        {"throughput": throughput_task, "memory": memory_task, "deep": deep_task}[task["kind"]](ctx, record, save)
        record["finished"] = time.time()
        save()
        write_json(status, {"status": "done", "finished": time.time()})
    except (Interrupted, Exception) as exc:
        save()
        write_json(status, {"status": "failed", "error": f"{type(exc).__name__}: {exc}",
                            "traceback": traceback.format_exc()[-4000:], "finished": time.time()})
        code = 1
    finally:
        finish(dep)
    return code


def finish(dep):
    """Report completion by advancing the deployment (Slurm only)."""
    if os.environ.get("AB_LOCAL"):
        return
    try:
        advance(dep)
    except Exception as exc:
        print(f"advance after job failed: {exc!r}", file=sys.stderr, flush=True)


def build_one(dep, key, arm, output):
    source = dep.root / "sources" / arm["source"]
    logs = dep.path / "logs"
    env = dict(os.environ, CARGO_TARGET_DIR=str(dep.root / "cache" / dep.cluster / "target"),
               CARGO_PROFILE_RELEASE_DEBUG="2", CARGO_PROFILE_RELEASE_STRIP="none",
               RUSTFLAGS="-C force-frame-pointers=yes -C target-cpu=native",
               CXXFLAGS="-fno-omit-frame-pointer -g")
    features = ["--features", ",".join(arm["features"])] if arm["features"] else []
    tests = dep.study["spec"]["build"]["tests"]
    steps = [["cargo", "build", "--offline", "--locked", "--release", "--bin", "bench_hardware", *features]]
    if tests:
        steps.append(["cargo", "test", "--offline", "--locked", "--release", "--lib", *features, "--", *tests])
    for number, step in enumerate(steps):
        log = logs / f"build-{key}-{number}"
        completed = run(step, cwd=source, env=env, timeout=5400, stdout=f"{log}.out", stderr=f"{log}.err")
        if completed.returncode:
            raise RunError(f"`{' '.join(step)}` failed; see {log}.err")
    temporary = output.with_name(output.name + f".tmp-{os.getpid()}")
    shutil.rmtree(temporary, ignore_errors=True)
    temporary.mkdir(parents=True)
    binary = temporary / "bench_hardware"
    shutil.copy2(dep.root / "cache" / dep.cluster / "target" / "release" / "bench_hardware", binary)
    write_json(temporary / "build.json", {
        "key": key, "source": arm["source"], "features": arm["features"], "sha256": sha256_file(binary),
        "rustc": run(["rustc", "-Vv"], env=env).stdout, "host": platform.node(), "time": time.time(),
        "flags": {k: env[k] for k in ("RUSTFLAGS", "CXXFLAGS", "CARGO_PROFILE_RELEASE_DEBUG")}})
    temporary.rename(output)


def build_job(dep):
    failures = []
    try:
        install_signal_handlers()
        builds = dep.root / "builds" / dep.cluster
        for key, arm in dep.build_targets().items():
            output = builds / key
            if (output / "build.json").exists():
                continue
            # Builds share one Cargo target directory per cluster, so vendored
            # dependencies compile once; the lock also covers the copy-out.
            with locked(builds / ".lock", timeout=4 * 3600):
                if not (output / "build.json").exists():
                    try:
                        build_one(dep, key, arm, output)
                    except (RuntimeError, OSError, TimeoutError) as exc:
                        failures.append(f"{key}: {exc}")
        for failure in failures:
            print(failure, file=sys.stderr, flush=True)
    finally:
        finish(dep)
    return 1 if failures else 0


def job_main(argv):
    """`ab.py job build <dep>`, `job task <dep> <mapping>`, or `job advance <dep>`."""
    kind, dep = argv[0], Deployment(argv[1])
    if kind == "build":
        return build_job(dep)
    if kind == "task":
        return task_job(dep, Path(argv[2]), int(os.environ["SLURM_ARRAY_TASK_ID"]))
    if kind == "advance":
        advance(dep)
        return 0
    raise SystemExit(f"unknown job kind {kind}")
