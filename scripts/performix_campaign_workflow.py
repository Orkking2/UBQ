#!/usr/bin/env python3
"""Drive a revision-safe throughput-guided Performix campaign on Slurm.

The workflow is intentionally split at the evidence boundary:

``auto`` submits the complete dependency graph. The lower-level commands are:
``start`` for the build and frontier, ``select`` for evidence-driven plans,
and ``submit`` for one frozen plan. Because selection depends on results which
do not exist initially, a lightweight dependent controller extends the graph
with the selected APX arrays after the frontier completes.

All generated state lives below ``profiling_results/workflows/<machine-label>``.
The machine label is also the raw-throughput run identity, so using a fresh
label prevents results from another source revision being resumed silently.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
from collections import Counter
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


SCENARIO_CONTROLS = {
    "grace": ("64p1c", "143p1c", "1p64c", "1p143c", "64p64c", "72p72c"),
    "mn5": ("64p1c", "72p1c", "1p64c", "1p72c", "32p32c"),
}
DEFAULT_STATIC_CONTROL_QUEUES = ("ubq", "lubq")
DEFAULT_PROFILE_QOS = {"grace": "ngp_bsccs", "mn5": "gp_bsccs"}
FINGERPRINT_FILES = ("Cargo.toml", "Cargo.lock", "build.rs", ".cargo/config.toml")
FINGERPRINT_DIRS = ("src", "scripts", "slurm", "profiling", "manifests", "third_party")
LABEL_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")


class WorkflowError(RuntimeError):
    """A user-actionable workflow failure."""


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def resolve_repo_root(value: Path | None) -> Path:
    root = value or Path(os.environ.get("UBQ", Path(__file__).resolve().parents[1]))
    root = root.expanduser().resolve()
    required = (
        root / "Cargo.toml",
        root / "slurm" / "submit_build.sh",
        root / "profiling" / "campaign.contention-performix.json",
    )
    missing = [str(path) for path in required if not path.is_file()]
    if missing:
        raise WorkflowError(
            f"{root} is not a deployed UBQ repository; missing: {', '.join(missing)}"
        )
    return root


def validate_identity(cluster: str, machine_label: str) -> None:
    if cluster not in SCENARIO_CONTROLS:
        raise WorkflowError(f"unsupported cluster: {cluster}")
    if not LABEL_PATTERN.fullmatch(machine_label):
        raise WorkflowError(
            "machine label must start with an alphanumeric character and contain only "
            "letters, digits, '.', '_', or '-'"
        )


def fingerprint_paths(root: Path) -> Iterable[Path]:
    for name in FINGERPRINT_FILES:
        path = root / name
        if path.is_file():
            yield path
    for name in FINGERPRINT_DIRS:
        directory = root / name
        if not directory.is_dir():
            continue
        for path in sorted(directory.rglob("*")):
            if not path.is_file():
                continue
            if "__pycache__" in path.parts or path.suffix == ".pyc":
                continue
            yield path


def source_fingerprint(root: Path) -> str:
    digest = hashlib.sha256()
    for path in sorted(set(fingerprint_paths(root))):
        relative = path.relative_to(root).as_posix().encode("utf-8")
        digest.update(len(relative).to_bytes(4, "big"))
        digest.update(relative)
        raw = path.read_bytes()
        digest.update(len(raw).to_bytes(8, "big"))
        digest.update(raw)
    return digest.hexdigest()


def workflow_dir(root: Path, machine_label: str) -> Path:
    return root / "profiling_results" / "workflows" / machine_label


def state_path(root: Path, machine_label: str) -> Path:
    return workflow_dir(root, machine_label) / "workflow.json"


def write_json(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    temporary.replace(path)


def load_state(root: Path, cluster: str, machine_label: str) -> dict[str, Any]:
    path = state_path(root, machine_label)
    if not path.is_file():
        raise WorkflowError(
            f"workflow state does not exist: {path}; run the start command first"
        )
    try:
        state = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise WorkflowError(f"cannot read workflow state {path}: {exc}") from exc
    if state.get("cluster") != cluster or state.get("machine_label") != machine_label:
        raise WorkflowError(f"workflow identity in {path} does not match the command")
    return state


def save_state(root: Path, state: dict[str, Any]) -> None:
    state["updated_at"] = utc_now()
    write_json(state_path(root, str(state["machine_label"])), state)


def assert_source_unchanged(root: Path, state: Mapping[str, Any]) -> str:
    current = source_fingerprint(root)
    expected = str(state.get("source_sha256") or "")
    if current != expected:
        raise WorkflowError(
            "the deployed benchmark/profiling source changed after this workflow was started\n"
            f"  recorded: {expected}\n"
            f"  current:  {current}\n"
            "Use a fresh machine label so throughput and APX data cannot be mixed across builds."
        )
    return current


def command_environment(root: Path) -> dict[str, str]:
    environment = dict(os.environ)
    environment["UBQ"] = str(root)
    return environment


def run_command(
    command: Sequence[str],
    *,
    root: Path,
    echo_stderr: bool = False,
) -> subprocess.CompletedProcess[str]:
    completed = subprocess.run(
        list(command),
        cwd=root,
        env=command_environment(root),
        text=True,
        capture_output=True,
        check=False,
    )
    if echo_stderr and completed.stderr:
        sys.stderr.write(completed.stderr)
    if completed.returncode != 0:
        detail = completed.stderr.strip() or completed.stdout.strip() or "no diagnostic output"
        raise WorkflowError(f"command failed ({' '.join(command)}):\n{detail}")
    return completed


def submit_command(command: Sequence[str], *, root: Path) -> str:
    completed = run_command(command, root=root, echo_stderr=True)
    output = completed.stdout.strip()
    match = re.fullmatch(r"([0-9]+)(?:;[^\n]+)?", output)
    if match is None:
        raise WorkflowError(
            f"submission did not return one parsable Slurm job ID: {output!r}"
        )
    return match.group(1)


def resolved_campaign(
    root: Path, machine_label: str, fingerprint: str, destination: Path
) -> str:
    source = root / "profiling" / "campaign.contention-performix.json"
    config = json.loads(source.read_text(encoding="utf-8"))
    base_campaign = str(config.get("campaign") or "queue-contention-performix-v1")
    campaign = f"{base_campaign}-{machine_label}"
    config["campaign"] = campaign
    config["source_sha256"] = fingerprint
    config["frontier_machine_label"] = machine_label
    description = str(config.get("description") or "").rstrip()
    config["description"] = (
        f"{description} Resolved for raw frontier {machine_label} and source "
        f"sha256 {fingerprint}."
    ).strip()
    for variant in config.get("variants", []):
        if isinstance(variant, dict):
            factors = variant.setdefault("factors", {})
            if isinstance(factors, dict):
                factors["source_sha256"] = fingerprint
                factors["frontier_machine_label"] = machine_label
    write_json(destination, config)
    return campaign


def plan_count(path: Path) -> int:
    with path.open(encoding="utf-8") as stream:
        return sum(1 for line in stream if line.strip())


def plan_sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def case_is_reusable(case: Mapping[str, Any], results_root: Path) -> bool:
    case_dir = results_root / str(case.get("campaign") or "") / str(case.get("case_id") or "")
    status_path = case_dir / "status.json"
    recorded_case_path = case_dir / "case.json"
    if not status_path.is_file():
        return False
    try:
        status = json.loads(status_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return False
    if status.get("status") != "completed":
        return False
    if not recorded_case_path.is_file():
        raise WorkflowError(f"completed case is missing its identity sidecar: {case_dir}")
    recorded = json.loads(recorded_case_path.read_text(encoding="utf-8"))
    if not isinstance(recorded, Mapping):
        raise WorkflowError(f"completed case has an invalid identity sidecar: {case_dir}")
    mismatched = [key for key, value in case.items() if recorded.get(key) != value]
    if mismatched:
        raise WorkflowError(
            f"completed case identity differs from the frozen plan at {case_dir}: "
            f"{', '.join(sorted(mismatched))}"
        )
    return True


def read_plan(path: Path) -> list[dict[str, Any]]:
    cases = []
    with path.open(encoding="utf-8") as stream:
        for line_number, raw in enumerate(stream, 1):
            if not raw.strip():
                raise WorkflowError(f"{path}:{line_number}: blank plan line")
            value = json.loads(raw)
            if not isinstance(value, dict) or not value.get("case_id"):
                raise WorkflowError(f"{path}:{line_number}: invalid plan case")
            cases.append(value)
    return cases


def write_plan(path: Path, cases: Sequence[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        "".join(canonical_json(case) + "\n" for case in cases), encoding="utf-8"
    )
    temporary.replace(path)


def merge_plans(paths: Sequence[Path], output: Path, maximum: int) -> dict[str, int]:
    # Earlier plans have priority. Thus tuning metadata replaces generic
    # winner metadata for the same deterministic workload case.
    by_id: dict[str, dict[str, Any]] = {}
    source_counts: dict[str, int] = {}
    for path in paths:
        if not path.is_file():
            continue
        cases = read_plan(path)
        source_counts[path.name] = len(cases)
        for case in cases:
            by_id.setdefault(str(case["case_id"]), case)
    if not by_id:
        raise WorkflowError("the component selections produced no profiling cases")
    if len(by_id) > maximum:
        raise WorkflowError(
            f"merged hotspot plan has {len(by_id)} cases, exceeding --max-cases={maximum}; "
            "reduce --cliffs-per-shape or restrict --tuning-queue"
        )
    write_plan(output, list(by_id.values()))
    return source_counts


def selector_command(
    root: Path,
    candidate: Path,
    frontier: Path,
    output: Path,
    *,
    policy: str,
    minimum_repeats: int,
    plan_repeats: int,
    maximum: int,
    scenarios: Sequence[str] = (),
    queues: Sequence[str] = (),
) -> list[str]:
    command = [
        sys.executable,
        str(root / "scripts" / "select_profile_variations.py"),
        str(candidate),
        str(frontier),
        "--policy",
        policy,
        "--min-evidence-repeats",
        str(minimum_repeats),
        "--plan-repeats",
        str(plan_repeats),
        "--max-cases",
        str(maximum),
        "--output",
        str(output),
    ]
    if policy == "one-factor":
        for dimension in ("batch_size", "backoff", "block_size"):
            command.extend(("--dimension", dimension))
    for scenario in scenarios:
        command.extend(("--scenario", scenario))
    for queue in queues:
        command.extend(("--queue", queue))
    return command


def job_is_active(job_id: str) -> bool:
    if not job_id or shutil.which("squeue") is None:
        return False
    completed = subprocess.run(
        ["squeue", "-h", "-j", job_id],
        text=True,
        capture_output=True,
        check=False,
    )
    return completed.returncode == 0 and bool(completed.stdout.strip())


def start_workflow(args: argparse.Namespace, root: Path) -> int:
    validate_identity(args.cluster, args.machine_label)
    directory = workflow_dir(root, args.machine_label)
    state_file = state_path(root, args.machine_label)
    fingerprint = source_fingerprint(root)
    frontier = root / "bench_results" / "runs" / args.machine_label

    if state_file.exists():
        if not args.resume:
            raise WorkflowError(
                f"workflow already exists: {state_file}; use --resume only to fill missing raw cells"
            )
        state = load_state(root, args.cluster, args.machine_label)
        assert_source_unchanged(root, state)
        if state.get("jobs", {}).get("profiles"):
            raise WorkflowError(
                "profiling was already submitted for this frozen workflow; use a fresh label "
                "instead of rebuilding its artifacts"
            )
    else:
        if frontier.exists():
            raise WorkflowError(
                f"raw frontier already exists without workflow state: {frontier}; choose a fresh label"
            )
        directory.mkdir(parents=True, exist_ok=True)
        config_path = directory / "campaign.resolved.json"
        campaign = resolved_campaign(root, args.machine_label, fingerprint, config_path)
        state = {
            "schema_version": 1,
            "created_at": utc_now(),
            "cluster": args.cluster,
            "machine_label": args.machine_label,
            "campaign": campaign,
            "source_sha256": fingerprint,
            "repo_root": str(root),
            "frontier_dir": str(frontier),
            "workflow_dir": str(directory),
            "campaign_config": str(config_path),
            "results_root": str(
                root / "profiling_results" / "contention_raw" / args.machine_label
            ),
            "jobs": {"build": [], "frontier": [], "profiles": []},
            "plans": {},
        }
        save_state(root, state)

    build_job = submit_command(
        (str(root / "slurm" / "submit_build.sh"), args.cluster), root=root
    )
    state.setdefault("jobs", {}).setdefault("build", []).append(build_job)
    save_state(root, state)

    frontier_job = submit_command(
        (
            str(root / "slurm" / "submit_contention_sweep.sh"),
            args.cluster,
            args.machine_label,
            "--after",
            build_job,
        ),
        root=root,
    )
    state["jobs"].setdefault("frontier", []).append(frontier_job)
    save_state(root, state)

    print(f"workflow:     {state_file}")
    print(f"source:       {fingerprint}")
    print(f"build job:    {build_job}")
    print(f"frontier job: {frontier_job}")
    print(f"frontier:     {frontier}")
    if not getattr(args, "automatic", False):
        print()
        print("After the frontier job leaves the queue, run:")
        print(
            f"  python3 {root / 'scripts' / 'performix_campaign_workflow.py'} "
            f"select {args.cluster} {args.machine_label}"
        )
    return 0


def automation_settings(args: argparse.Namespace) -> dict[str, Any]:
    return {
        "minimum_evidence_repeats": args.min_evidence_repeats,
        "plan_repeats": args.plan_repeats,
        "cliffs_per_shape": args.cliffs_per_shape,
        "score_queues": list(args.score_queue or []),
        "tuning_queues": list(args.tuning_queue or []),
        "static_control_queues": list(args.static_control_queue or []),
        "static_controls": not args.no_static_controls,
        "max_cases": args.max_cases,
        "max_concurrent": args.max_concurrent,
        "profile_qos": getattr(args, "profile_qos", None)
        or DEFAULT_PROFILE_QOS[args.cluster],
        "chunk_size": args.chunk_size,
        "max_submit_tasks": args.max_submit_tasks,
        "include_hardware": not args.no_hardware and args.cluster == "grace",
    }


def auto_workflow(args: argparse.Namespace, root: Path) -> int:
    """Submit the initial graph and a dependent job which grows its dynamic tail."""

    validate_identity(args.cluster, args.machine_label)
    existing_state = state_path(root, args.machine_label)
    if existing_state.is_file() and args.resume:
        state = load_state(root, args.cluster, args.machine_label)
        controllers = state.get("jobs", {}).get("controllers") or []
        if controllers and job_is_active(str(controllers[-1])):
            raise WorkflowError(
                f"an automatic continuation (controller {controllers[-1]}) is still active for "
                "this workflow; use status instead of creating a competing controller"
            )
        if controllers:
            print(
                f"previous controller {controllers[-1]} is no longer active; "
                "submitting a fresh continuation"
            )

    args.automatic = True
    start_workflow(args, root)
    state = load_state(root, args.cluster, args.machine_label)
    frontier_jobs = state.get("jobs", {}).get("frontier", [])
    if not frontier_jobs:
        raise WorkflowError("start completed without recording a frontier job")
    frontier_job = str(frontier_jobs[-1])
    state["automation"] = automation_settings(args)
    save_state(root, state)

    controller_job = submit_command(
        (
            str(root / "slurm" / "submit_performix_campaign_controller.sh"),
            args.cluster,
            args.machine_label,
            "--after",
            frontier_job,
        ),
        root=root,
    )
    state.setdefault("jobs", {}).setdefault("controllers", []).append(controller_job)
    save_state(root, state)

    hardware_tail = " → hardware arrays" if state["automation"]["include_hardware"] else ""
    print()
    print("Automatic dependency graph:")
    print(
        f"  {state['jobs']['build'][-1]} build → {frontier_job} full frontier[] → "
        f"{controller_job} select/extend → hotspot arrays{hardware_tail}"
    )
    print("No manual stage initiation is required. Use the status command at any time.")
    return 0


def select_workflow(args: argparse.Namespace, root: Path) -> int:
    validate_identity(args.cluster, args.machine_label)
    state = load_state(root, args.cluster, args.machine_label)
    fingerprint = assert_source_unchanged(root, state)
    if state.get("jobs", {}).get("profiles"):
        raise WorkflowError(
            "profiling was already submitted from this frozen selection; use a fresh machine "
            "label for a different selection"
        )
    frontier = Path(str(state["frontier_dir"]))
    frontier_jobs = state.get("jobs", {}).get("frontier", [])
    if not args.allow_partial and frontier_jobs and job_is_active(str(frontier_jobs[-1])):
        raise WorkflowError(
            f"frontier job {frontier_jobs[-1]} is still active; wait for it before selecting"
        )
    record_count = len(list(frontier.rglob("record.json"))) if frontier.is_dir() else 0
    if record_count == 0:
        raise WorkflowError(f"no raw benchmark records are available below {frontier}")
    machine_size = "144" if args.cluster == "grace" else "112"
    manifest = root / "manifests" / f"contention-{args.cluster}-{machine_size}.txt"
    with manifest.open(encoding="utf-8") as stream:
        expected_records = sum(
            1 for line in stream if line.strip() and not line.lstrip().startswith("#")
        )
    if record_count < expected_records and not args.allow_partial:
        raise WorkflowError(
            f"frontier has {record_count}/{expected_records} scenario records; wait, resume the "
            "frontier, or use --allow-partial to make the incomplete evidence explicit"
        )

    directory = workflow_dir(root, args.machine_label)
    config = Path(str(state["campaign_config"]))
    candidates = directory / "hotspots-candidates.jsonl"
    winners = directory / "hotspots-winners.jsonl"
    cliff_seed = directory / "hotspots-cliff-seed.jsonl"
    dynamic_tuning = directory / "hotspots-dynamic-tuning.jsonl"
    static_controls = directory / "hotspots-static-controls.jsonl"
    hotspots = directory / "hotspots-selected.jsonl"
    hardware_candidates = directory / "hardware-candidates.jsonl"
    hardware = directory / "hardware-selected.jsonl"

    run_command(
        (
            sys.executable,
            str(root / "scripts" / "make_profile_plan.py"),
            str(config),
            "--stage",
            "contention-apx-hotspots",
            "--cluster",
            args.cluster,
            "--output",
            str(candidates),
        ),
        root=root,
    )
    run_command(
        selector_command(
            root,
            candidates,
            frontier,
            winners,
            policy="winners",
            minimum_repeats=args.min_evidence_repeats,
            plan_repeats=args.plan_repeats,
            maximum=args.max_cases,
        ),
        root=root,
    )

    cliff_command = [
        sys.executable,
        str(root / "scripts" / "select_profile_plan.py"),
        str(candidates),
        str(frontier),
        "--cliffs-per-shape",
        str(args.cliffs_per_shape),
        "--min-evidence-repeats",
        str(args.min_evidence_repeats),
        "--plan-repeats",
        str(args.plan_repeats),
        "--representatives-per-coordinate",
        "3",
        "--max-cases",
        str(args.max_cases),
        "--output",
        str(cliff_seed),
    ]
    for queue in args.score_queue or ():
        cliff_command.extend(("--score-queue", queue))
    run_command(cliff_command, root=root)
    dynamic_scenarios = sorted({str(case["scenario"]) for case in read_plan(cliff_seed)})
    if not dynamic_scenarios:
        raise WorkflowError("cliff selection produced no study scenarios")

    run_command(
        selector_command(
            root,
            candidates,
            frontier,
            dynamic_tuning,
            policy="one-factor",
            minimum_repeats=args.min_evidence_repeats,
            plan_repeats=args.plan_repeats,
            maximum=args.max_cases,
            scenarios=dynamic_scenarios,
            queues=args.tuning_queue or (),
        ),
        root=root,
    )

    static_scenarios: list[str] = []
    static_queues: list[str] = []
    component_paths = [dynamic_tuning]
    if not args.no_static_controls:
        static_scenarios = list(SCENARIO_CONTROLS[args.cluster])
        static_queues = list(args.static_control_queue or DEFAULT_STATIC_CONTROL_QUEUES)
        run_command(
            selector_command(
                root,
                candidates,
                frontier,
                static_controls,
                policy="one-factor",
                minimum_repeats=args.min_evidence_repeats,
                plan_repeats=args.plan_repeats,
                maximum=args.max_cases,
                scenarios=static_scenarios,
                queues=static_queues,
            ),
            root=root,
        )
        component_paths.append(static_controls)
    component_paths.append(winners)
    source_counts = merge_plans(component_paths, hotspots, args.max_cases)

    study_scenarios = sorted(set(dynamic_scenarios) | set(static_scenarios))
    hardware_record = None
    if args.cluster == "grace":
        run_command(
            (
                sys.executable,
                str(root / "scripts" / "make_profile_plan.py"),
                str(config),
                "--stage",
                "contention-apx-hardware",
                "--cluster",
                args.cluster,
                "--output",
                str(hardware_candidates),
            ),
            root=root,
        )
        run_command(
            selector_command(
                root,
                hardware_candidates,
                frontier,
                hardware,
                policy="winners",
                minimum_repeats=args.min_evidence_repeats,
                plan_repeats=args.plan_repeats,
                maximum=args.max_cases,
                scenarios=study_scenarios,
            ),
            root=root,
        )
        hardware_record = {
            "path": str(hardware),
            "cases": plan_count(hardware),
            "sha256": plan_sha256(hardware),
        }

    selection_manifest = {
        "schema_version": 1,
        "selected_at": utc_now(),
        "source_sha256": fingerprint,
        "frontier_dir": str(frontier),
        "frontier_record_files": record_count,
        "expected_frontier_record_files": expected_records,
        "partial_frontier_allowed": args.allow_partial,
        "minimum_evidence_repeats": args.min_evidence_repeats,
        "plan_repeats": args.plan_repeats,
        "cliffs_per_shape": args.cliffs_per_shape,
        "score_queues": sorted(args.score_queue or []),
        "dynamic_scenarios": dynamic_scenarios,
        "static_control_scenarios": static_scenarios,
        "static_control_queues": static_queues,
        "study_scenarios": study_scenarios,
        "component_case_counts": source_counts,
        "plans": {
            "hotspots": {
                "path": str(hotspots),
                "cases": plan_count(hotspots),
                "sha256": plan_sha256(hotspots),
            },
            **({"hardware": hardware_record} if hardware_record is not None else {}),
        },
    }
    selection_path = directory / "selection-summary.json"
    write_json(selection_path, selection_manifest)
    state["selection"] = str(selection_path)
    state["selection_source_sha256"] = fingerprint
    state["plans"] = selection_manifest["plans"]
    save_state(root, state)

    print(f"frontier records:  {record_count}/{expected_records}")
    print(f"dynamic scenarios: {', '.join(dynamic_scenarios)}")
    if static_scenarios:
        print(f"fixed controls:    {', '.join(static_scenarios)} ({', '.join(static_queues)})")
    print(f"hotspot plan:      {hotspots} ({plan_count(hotspots)} cases)")
    if hardware_record is not None:
        print(f"hardware plan:     {hardware} ({plan_count(hardware)} cases)")
    else:
        print("hardware plan:     unavailable on MN5 in this campaign")
    print(f"selection summary: {selection_path}")
    print()
    print("Submit Code Hotspots first:")
    print(
        f"  python3 {root / 'scripts' / 'performix_campaign_workflow.py'} "
        f"submit {args.cluster} {args.machine_label} hotspots"
    )
    return 0


def submit_workflow(args: argparse.Namespace, root: Path) -> int:
    validate_identity(args.cluster, args.machine_label)
    state = load_state(root, args.cluster, args.machine_label)
    fingerprint = assert_source_unchanged(root, state)
    if state.get("selection_source_sha256") != fingerprint:
        raise WorkflowError("plans were not selected from this source snapshot; rerun select")
    plan_record = state.get("plans", {}).get(args.kind)
    if not isinstance(plan_record, Mapping):
        raise WorkflowError(f"no selected {args.kind} plan exists; run select first")
    plan = Path(str(plan_record.get("path") or ""))
    if not plan.is_file() or plan_sha256(plan) != plan_record.get("sha256"):
        raise WorkflowError(f"selected plan is missing or changed: {plan}; rerun select")

    profiles = state.setdefault("jobs", {}).setdefault("profiles", [])
    prior = [entry for entry in profiles if entry.get("kind") == args.kind]
    if prior and not args.resubmit:
        raise WorkflowError(
            f"{args.kind} was already submitted as job {prior[-1].get('job_id')}; "
            "use --resubmit only to resume missing cases"
        )

    after_job = args.after
    if after_job is None and args.kind == "hardware":
        hotspot_jobs = [entry for entry in profiles if entry.get("kind") == "hotspots"]
        if hotspot_jobs:
            after_job = str(hotspot_jobs[-1]["job_id"])

    plan_hash = plan_sha256(plan)
    all_cases = read_plan(plan)
    results_root = Path(str(state["results_root"]))
    pending_cases = [case for case in all_cases if not case_is_reusable(case, results_root)]
    reused_cases = len(all_cases) - len(pending_cases)
    if not pending_cases:
        state.setdefault("completed_selections", {})[args.kind] = {
            "selection_plan_sha256": plan_hash,
            "completed_at": utc_now(),
            "reused_cases": reused_cases,
        }
        save_state(root, state)
        print(f"{args.kind}: all {reused_cases} cases already completed; no Slurm array submitted")
        return 0

    pending_content = "".join(canonical_json(case) + "\n" for case in pending_cases)
    pending_hash = hashlib.sha256(pending_content.encode("utf-8")).hexdigest()
    chunks = [
        pending_cases[index : index + args.chunk_size]
        for index in range(0, len(pending_cases), args.chunk_size)
    ]
    submitted_dir = workflow_dir(root, args.machine_label) / "submitted"
    submitted_dir.mkdir(parents=True, exist_ok=True)
    if prior and args.resubmit and after_job is None:
        after_job = str(prior[-1]["job_id"])

    submitted_jobs = []
    for part, chunk in enumerate(chunks, 1):
        submitted_plan = submitted_dir / (
            f"{args.kind}-{plan_hash[:12]}-{pending_hash[:12]}-"
            f"part{part:03d}-of{len(chunks):03d}.jsonl"
        )
        expected_content = "".join(canonical_json(case) + "\n" for case in chunk)
        expected_hash = hashlib.sha256(expected_content.encode("utf-8")).hexdigest()
        if submitted_plan.is_file() and plan_sha256(submitted_plan) != expected_hash:
            raise WorkflowError(
                f"immutable submitted-plan path changed unexpectedly: {submitted_plan}"
            )
        if not submitted_plan.exists():
            write_plan(submitted_plan, chunk)

        command = [
            str(root / "slurm" / "submit_profile_plan.sh"),
            args.cluster,
            str(submitted_plan),
            "--qos",
            getattr(args, "qos", None) or DEFAULT_PROFILE_QOS[args.cluster],
            "--max-concurrent",
            str(args.max_concurrent),
            "--max-submit-tasks",
            str(args.max_submit_tasks),
            "--results-root",
            str(results_root),
            "--retry-incomplete",
        ]
        if after_job:
            command.extend(("--after", after_job))
        job_id = submit_command(command, root=root)
        record = {
            "kind": args.kind,
            "job_id": job_id,
            "submitted_at": utc_now(),
            "plan": str(submitted_plan),
            "plan_sha256": expected_hash,
            "selection_plan_sha256": plan_hash,
            "pending_plan_sha256": pending_hash,
            "part": part,
            "parts": len(chunks),
            "cases": len(chunk),
            "reused_cases": reused_cases,
            "after": after_job,
        }
        profiles.append(record)
        save_state(root, state)
        submitted_jobs.append(job_id)
        after_job = job_id

    print(f"{args.kind} jobs: {', '.join(submitted_jobs)}")
    print(
        f"cases:          {len(pending_cases)} pending in {len(chunks)} chained part(s); "
        f"{reused_cases} completed case(s) reused"
    )
    print(f"results:        {results_root}")
    if args.kind == "hotspots" and not getattr(args, "automatic", False):
        print()
        print("Inspect Code Hotspots before deciding whether to submit hardware recipes:")
        print(
            f"  python3 {root / 'scripts' / 'performix_campaign_workflow.py'} "
            f"submit {args.cluster} {args.machine_label} hardware"
        )
    return 0


def kind_fully_submitted(state: Mapping[str, Any], kind: str) -> bool:
    plan = state.get("plans", {}).get(kind)
    if not isinstance(plan, Mapping):
        return False
    selection_hash = str(plan.get("sha256") or "")
    completed = state.get("completed_selections", {}).get(kind)
    if isinstance(completed, Mapping) and completed.get("selection_plan_sha256") == selection_hash:
        return True
    entries = [
        entry
        for entry in state.get("jobs", {}).get("profiles", [])
        if entry.get("kind") == kind
        and entry.get("selection_plan_sha256") == selection_hash
    ]
    if not entries:
        return False
    expected_parts = max(int(entry.get("parts") or 0) for entry in entries)
    submitted_parts = {int(entry.get("part") or 0) for entry in entries}
    return expected_parts > 0 and submitted_parts >= set(range(1, expected_parts + 1))


def continue_workflow(args: argparse.Namespace, root: Path) -> int:
    """Run inside the dependent Slurm controller and extend the dynamic DAG."""

    validate_identity(args.cluster, args.machine_label)
    state = load_state(root, args.cluster, args.machine_label)
    settings = state.get("automation")
    if not isinstance(settings, Mapping):
        raise WorkflowError("workflow has no automatic-continuation settings; use auto first")
    assert_source_unchanged(root, state)

    if not state.get("plans"):
        select_args = argparse.Namespace(
            cluster=args.cluster,
            machine_label=args.machine_label,
            min_evidence_repeats=int(settings["minimum_evidence_repeats"]),
            plan_repeats=int(settings["plan_repeats"]),
            cliffs_per_shape=int(settings["cliffs_per_shape"]),
            score_queue=list(settings.get("score_queues") or []),
            tuning_queue=list(settings.get("tuning_queues") or []),
            static_control_queue=list(settings.get("static_control_queues") or []),
            no_static_controls=not bool(settings.get("static_controls", True)),
            max_cases=int(settings["max_cases"]),
            allow_partial=False,
        )
        select_workflow(select_args, root)
        state = load_state(root, args.cluster, args.machine_label)

    for kind in ("hotspots", "hardware"):
        if kind == "hardware" and not bool(settings.get("include_hardware", False)):
            continue
        if kind not in state.get("plans", {}):
            continue
        if kind_fully_submitted(state, kind):
            print(f"{kind}: all dependency chunks were already submitted")
            continue
        prior = [
            entry
            for entry in state.get("jobs", {}).get("profiles", [])
            if entry.get("kind") == kind
        ]
        submit_args = argparse.Namespace(
            cluster=args.cluster,
            machine_label=args.machine_label,
            kind=kind,
            after=None,
            max_concurrent=int(settings["max_concurrent"]),
            qos=str(settings.get("profile_qos") or DEFAULT_PROFILE_QOS[args.cluster]),
            max_submit_tasks=int(settings["max_submit_tasks"]),
            chunk_size=int(settings["chunk_size"]),
            resubmit=bool(prior),
            automatic=True,
        )
        submit_workflow(submit_args, root)
        state = load_state(root, args.cluster, args.machine_label)
    return 0


def result_status_counts(results_root: Path) -> Counter[str]:
    counts: Counter[str] = Counter()
    if not results_root.is_dir():
        return counts
    for path in results_root.rglob("status.json"):
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
            counts[str(value.get("status") or "unknown")] += 1
        except (OSError, json.JSONDecodeError):
            counts["invalid-status"] += 1
    return counts


def status_workflow(args: argparse.Namespace, root: Path) -> int:
    validate_identity(args.cluster, args.machine_label)
    state = load_state(root, args.cluster, args.machine_label)
    current = source_fingerprint(root)
    recorded = str(state.get("source_sha256") or "")
    frontier = Path(str(state["frontier_dir"]))
    results = Path(str(state["results_root"]))
    print(f"workflow: {state_path(root, args.machine_label)}")
    print(f"campaign: {state.get('campaign')}")
    print(f"source:   {'unchanged' if current == recorded else 'CHANGED'} ({recorded})")
    print(f"frontier: {len(list(frontier.rglob('record.json'))) if frontier.is_dir() else 0} records")
    for kind, record in sorted(state.get("plans", {}).items()):
        print(f"plan:     {kind} {record.get('cases')} cases ({record.get('path')})")
    statuses = result_status_counts(results)
    print(
        "results:  "
        + (", ".join(f"{name}={count}" for name, count in sorted(statuses.items())) or "none")
    )
    jobs = []
    job_state = state.get("jobs", {})
    jobs.extend((job, "build") for job in job_state.get("build", []))
    jobs.extend((job, "frontier") for job in job_state.get("frontier", []))
    jobs.extend((job, "controller") for job in job_state.get("controllers", []))
    jobs.extend(
        (str(entry.get("job_id")), str(entry.get("kind")))
        for entry in job_state.get("profiles", [])
    )
    if jobs:
        print("jobs:     " + ", ".join(f"{job}({kind})" for job, kind in jobs))
        if shutil.which("squeue") is not None:
            completed = subprocess.run(
                ["squeue", "-h", "-j", ",".join(job for job, _kind in jobs), "-o", "%i %T %j"],
                text=True,
                capture_output=True,
                check=False,
            )
            if completed.stdout.strip():
                print("active Slurm jobs:")
                print(completed.stdout.rstrip())
    return 0


def paths_workflow(args: argparse.Namespace, root: Path) -> int:
    validate_identity(args.cluster, args.machine_label)
    state = load_state(root, args.cluster, args.machine_label)
    directory = Path(str(state["workflow_dir"]))
    frontier = Path(str(state["frontier_dir"]))
    results = Path(str(state["results_root"]))
    relative_directory = directory.relative_to(root)
    relative_frontier = frontier.relative_to(root)
    relative_results = results.relative_to(root)
    print("Run these commands on the local machine after remote collection:")
    print()
    print(
        'mkdir -p "$LOCAL_UBQ/profiling_results/workflows" '
        '"$LOCAL_UBQ/bench_results/runs" '
        '"$LOCAL_UBQ/profiling_results/contention_raw"'
    )
    for relative in (relative_directory, relative_frontier, relative_results):
        print(f'rsync -a "<remote-host>:{root}/{relative}/" "$LOCAL_UBQ/{relative}/"')
    print()
    print('cd "$LOCAL_UBQ"')
    print(
        f"python3 scripts/aggregate_profile_results.py {relative_results} "
        f"--out-dir profiling_results/aggregate/{args.machine_label}"
    )
    print(
        f"python3 scripts/import_performix_runs.py {relative_results} "
        f"--trials-csv profiling_results/aggregate/{args.machine_label}/trials.csv"
    )
    print("# Review the manifest, then repeat the importer with --import --apx ... --source src")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--repo-root",
        type=Path,
        help="deployed UBQ root (defaults to $UBQ or the script's repository)",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    start = subparsers.add_parser("start", help="submit build and short throughput frontier")
    start.add_argument("cluster", choices=sorted(SCENARIO_CONTROLS))
    start.add_argument("machine_label")
    start.add_argument(
        "--resume",
        action="store_true",
        help="resubmit the same unchanged build/frontier to fill missing raw cells",
    )
    start.set_defaults(handler=start_workflow)

    auto = subparsers.add_parser(
        "auto", help="submit build, full frontier, selection, and APX dependency graph"
    )
    auto.add_argument("cluster", choices=sorted(SCENARIO_CONTROLS))
    auto.add_argument("machine_label")
    auto.add_argument("--resume", action="store_true")
    auto.add_argument("--min-evidence-repeats", type=int, default=3)
    auto.add_argument("--plan-repeats", type=int, default=1)
    auto.add_argument("--cliffs-per-shape", type=int, default=1)
    auto.add_argument("--score-queue", action="append")
    auto.add_argument("--tuning-queue", action="append")
    auto.add_argument("--static-control-queue", action="append")
    auto.add_argument("--no-static-controls", action="store_true")
    auto.add_argument("--max-cases", type=int, default=750)
    auto.add_argument(
        "--max-concurrent",
        type=int,
        default=32,
        help="maximum simultaneous APX nodes within each array (default: 32)",
    )
    auto.add_argument(
        "--profile-qos",
        help="profiling-array QoS (default: ngp_bsccs on Grace, gp_bsccs on MN5)",
    )
    auto.add_argument("--chunk-size", type=int, default=300)
    auto.add_argument("--max-submit-tasks", type=int, default=300)
    auto.add_argument(
        "--no-hardware",
        action="store_true",
        help="stop after automatically submitting Code Hotspots",
    )
    auto.set_defaults(handler=auto_workflow)

    select = subparsers.add_parser("select", help="select dynamic APX plans from raw throughput")
    select.add_argument("cluster", choices=sorted(SCENARIO_CONTROLS))
    select.add_argument("machine_label")
    select.add_argument("--min-evidence-repeats", type=int, default=3)
    select.add_argument("--plan-repeats", type=int, default=1)
    select.add_argument("--cliffs-per-shape", type=int, default=1)
    select.add_argument("--score-queue", action="append")
    select.add_argument(
        "--tuning-queue",
        action="append",
        help="limit dynamic one-factor expansion to this queue; repeat as needed",
    )
    select.add_argument(
        "--static-control-queue",
        action="append",
        help="fixed-panel queue (default: UBQ and LUBQ); repeat as needed",
    )
    select.add_argument("--no-static-controls", action="store_true")
    select.add_argument("--max-cases", type=int, default=750)
    select.add_argument(
        "--allow-partial",
        action="store_true",
        help="select from intentionally incomplete evidence (including an active frontier)",
    )
    select.set_defaults(handler=select_workflow)

    submit = subparsers.add_parser("submit", help="submit a frozen selected APX plan")
    submit.add_argument("cluster", choices=sorted(SCENARIO_CONTROLS))
    submit.add_argument("machine_label")
    submit.add_argument("kind", choices=("hotspots", "hardware"))
    submit.add_argument("--after", help="explicit Slurm dependency job ID")
    submit.add_argument(
        "--qos", help="profiling-array QoS (defaults to the cluster production QoS)"
    )
    submit.add_argument("--max-concurrent", type=int, default=32)
    submit.add_argument(
        "--chunk-size",
        type=int,
        default=300,
        help="maximum cases per serially chained Slurm array (default: 300)",
    )
    submit.add_argument("--max-submit-tasks", type=int, default=300)
    submit.add_argument("--resubmit", action="store_true")
    submit.set_defaults(handler=submit_workflow)

    continuation = subparsers.add_parser(
        "continue", help="resume the dynamic tail from a dependent controller job"
    )
    continuation.add_argument("cluster", choices=sorted(SCENARIO_CONTROLS))
    continuation.add_argument("machine_label")
    continuation.set_defaults(handler=continue_workflow)

    status = subparsers.add_parser("status", help="show evidence, plan, result, and job status")
    status.add_argument("cluster", choices=sorted(SCENARIO_CONTROLS))
    status.add_argument("machine_label")
    status.set_defaults(handler=status_workflow)

    paths = subparsers.add_parser("paths", help="print local rsync and import commands")
    paths.add_argument("cluster", choices=sorted(SCENARIO_CONTROLS))
    paths.add_argument("machine_label")
    paths.set_defaults(handler=paths_workflow)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        if getattr(args, "min_evidence_repeats", 1) < 1:
            raise WorkflowError("--min-evidence-repeats must be positive")
        if getattr(args, "plan_repeats", 1) < 1:
            raise WorkflowError("--plan-repeats must be positive")
        if getattr(args, "cliffs_per_shape", 1) < 0:
            raise WorkflowError("--cliffs-per-shape cannot be negative")
        if getattr(args, "max_cases", 1) < 1:
            raise WorkflowError("--max-cases must be positive")
        if getattr(args, "max_concurrent", 1) < 1:
            raise WorkflowError("--max-concurrent must be positive")
        if getattr(args, "max_submit_tasks", 1) < 1:
            raise WorkflowError("--max-submit-tasks must be positive")
        if getattr(args, "chunk_size", 1) < 1:
            raise WorkflowError("--chunk-size must be positive")
        if getattr(args, "chunk_size", 1) > getattr(args, "max_submit_tasks", 1):
            raise WorkflowError("--chunk-size cannot exceed --max-submit-tasks")
        root = resolve_repo_root(args.repo_root)
        return int(args.handler(args, root))
    except (OSError, ValueError, json.JSONDecodeError, WorkflowError) as exc:
        print(f"performix_campaign_workflow: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
