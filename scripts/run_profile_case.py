#!/usr/bin/env python3
"""Execute one JSONL profiling case inside an allocated Slurm node.

The runner deliberately performs calibration outside the collector when the
case requests it.  APX and ``perf stat`` therefore observe one exact, fixed-work
handoff rather than benchmark calibration, and every recipe executes the same
number of queue operations.  A separate unprofiled shadow result remains the
source for performance comparisons because collector overhead is recipe
dependent.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import platform
import re
import shlex
import shutil
import subprocess
import sys
import time
import zipfile
from pathlib import Path
from typing import Any, Mapping, Sequence


RUNNER_SCHEMA_VERSION = 1
RUN_ID_RE = re.compile(r"Run ID:\s*([A-Za-z0-9._-]+)")
ENV_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
RESERVED_VARIANT_ENV = {"UBQ_BENCH_CORE_IDS", "UBQ_PROFILE_BINARY_OVERRIDE"}
# `apx version` readiness-retry tuning; module-level so tests can shrink the
# delay instead of waiting on the real backoff.
APX_VERSION_ATTEMPTS = 5
APX_VERSION_RETRY_DELAY_SECS = 3


class CaseError(RuntimeError):
    """Expected case/configuration/capability failure."""


class CapabilityUnavailable(CaseError):
    """The requested collector is not installed or supported on this node."""


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def atomic_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    temporary.replace(path)


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def load_case(plan: Path, index: int) -> dict[str, Any]:
    if index < 0:
        raise CaseError("--index must be non-negative")
    selected: str | None = None
    with plan.open(encoding="utf-8") as stream:
        for line_index, raw in enumerate(stream):
            if not raw.strip():
                raise CaseError(f"{plan}:{line_index + 1}: blank plan lines are invalid")
            if line_index == index:
                selected = raw
                break
    if selected is None:
        raise CaseError(f"plan index {index} is outside {plan}")
    try:
        value = json.loads(selected)
    except json.JSONDecodeError as exc:
        raise CaseError(f"{plan}:{index + 1}: invalid JSON: {exc}") from exc
    if not isinstance(value, dict):
        raise CaseError(f"{plan}:{index + 1}: case must be a JSON object")
    for field in (
        "case_id",
        "campaign",
        "cluster",
        "backend",
        "binary",
        "queue",
        "scenario",
        "duration_secs",
        "warmup_secs",
    ):
        if field not in value:
            raise CaseError(f"case is missing {field}")
    if value.get("backend") not in {"apx", "perf"}:
        raise CaseError("backend must be apx or perf")
    return value


def run_command(
    command: Sequence[str],
    *,
    cwd: Path,
    env: Mapping[str, str],
    timeout: int,
    stdout_path: Path,
    stderr_path: Path,
) -> subprocess.CompletedProcess[str]:
    started = time.monotonic()
    try:
        completed = subprocess.run(
            list(command),
            cwd=cwd,
            env=dict(env),
            text=True,
            capture_output=True,
            timeout=timeout,
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        stdout_path.write_text(exc.stdout or "", encoding="utf-8")
        stderr_path.write_text(exc.stderr or "", encoding="utf-8")
        raise CaseError(f"command timed out after {timeout}s: {shlex.join(command)}") from exc
    stdout_path.write_text(completed.stdout, encoding="utf-8")
    stderr_path.write_text(completed.stderr, encoding="utf-8")
    timing = {
        "command": list(command),
        "elapsed_secs": time.monotonic() - started,
        "returncode": completed.returncode,
    }
    atomic_json(stdout_path.with_suffix(stdout_path.suffix + ".meta.json"), timing)
    return completed


def run_capture(command: Sequence[str], *, cwd: Path, timeout: int = 30) -> dict[str, Any]:
    try:
        result = subprocess.run(
            list(command), cwd=cwd, text=True, capture_output=True, timeout=timeout, check=False
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        return {"command": list(command), "error": str(exc)}
    return {
        "command": list(command),
        "returncode": result.returncode,
        "stdout": result.stdout,
        "stderr": result.stderr,
    }


def parse_json_object(text: str, label: str) -> dict[str, Any]:
    candidates: list[dict[str, Any]] = []
    for raw in text.splitlines():
        stripped = raw.strip()
        if not stripped.startswith("{"):
            continue
        try:
            value = json.loads(stripped)
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict) and {"queue", "scenario", "elapsed_ns", "items"} <= value.keys():
            candidates.append(value)
    if len(candidates) != 1:
        raise CaseError(f"expected exactly one workload JSON object in {label}, found {len(candidates)}")
    return candidates[0]


def parse_workload_result(path: Path) -> dict[str, Any] | None:
    """Reads the workload document the APX wrapper script teed to disk."""
    if not path.is_file():
        return None
    for raw in path.read_text(encoding="utf-8", errors="replace").splitlines():
        stripped = raw.strip()
        if not stripped.startswith("{"):
            continue
        try:
            value = json.loads(stripped)
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict) and {"queue", "scenario", "elapsed_ns", "items"} <= value.keys():
            return value
    return None


def parse_apx_workload(archive_path: Path) -> dict[str, Any] | None:
    matches: list[dict[str, Any]] = []
    with zipfile.ZipFile(archive_path) as archive:
        members = [name for name in archive.namelist() if name.endswith("/capture_log.txt")]
        for member in members:
            text = archive.read(member).decode("utf-8", errors="replace")
            for raw in text.splitlines():
                stripped = raw.strip()
                if not stripped.startswith("{"):
                    continue
                try:
                    value = json.loads(stripped)
                except json.JSONDecodeError:
                    continue
                if isinstance(value, dict) and {"queue", "scenario", "elapsed_ns", "items"} <= value.keys():
                    matches.append(value)
    if not matches:
        return None
    unique = {canonical_json(value): value for value in matches}
    if len(unique) != 1:
        raise CaseError(f"APX archive {archive_path} contains conflicting workload JSON objects")
    return next(iter(unique.values()))


def build_workload_command(
    case: Mapping[str, Any],
    binary: Path,
    *,
    items_per_producer: int | None,
    fixed: bool,
    allocator_probe: bool = False,
) -> list[str]:
    command = [
        str(binary),
        "--queue",
        str(case["queue"]),
        "--scenario",
        str(case["scenario"]),
        "--duration-secs",
        str(case["duration_secs"]),
        "--warmup-secs",
        "0" if fixed else str(case["warmup_secs"]),
        "--json",
    ]
    batch_size = case.get("batch_size")
    if batch_size is not None:
        command.extend(("--batch-size", str(batch_size)))
    ubq_label = case.get("ubq_label")
    if ubq_label:
        command.extend(("--ubq-label", str(ubq_label)))
    lubq_backoff = case.get("lubq_backoff")
    if lubq_backoff:
        command.extend(("--lubq-backoff", str(lubq_backoff)))
    fastfifo_block_size = case.get("fastfifo_block_size")
    if fastfifo_block_size is not None:
        command.extend(("--fastfifo-block-size", str(fastfifo_block_size)))
    fastfifo_capacity = case.get("fastfifo_capacity")
    if fastfifo_capacity is not None:
        command.extend(("--fastfifo-capacity", str(fastfifo_capacity)))
    lfqueue_segment_size = case.get("lfqueue_segment_size")
    if lfqueue_segment_size is not None:
        command.extend(("--lfqueue-segment-size", str(lfqueue_segment_size)))
    placement = case.get("placement")
    if isinstance(placement, Mapping):
        core_offset = placement.get("core_offset", 0)
    else:
        core_offset = case.get("core_offset", 0)
    if core_offset:
        command.extend(("--core-offset", str(core_offset)))
    if case.get("allow_unpinned", False):
        command.append("--allow-unpinned")
    if items_per_producer is not None:
        command.extend(("--items-per-producer", str(items_per_producer)))
        prewarm = case.get("prewarm_items_per_producer")
        if prewarm is not None:
            command.extend(("--prewarm-items-per-producer", str(int(prewarm))))
    if allocator_probe:
        command.append("--allocator-probe")
    variant_env = case.get("variant_env", {})
    if not isinstance(variant_env, Mapping):
        raise CaseError("variant_env must be a JSON object")
    assignments = []
    for key, value in sorted(variant_env.items()):
        if not isinstance(key, str) or not ENV_NAME_RE.fullmatch(key):
            raise CaseError(f"variant_env has invalid variable name {key!r}")
        if key in RESERVED_VARIANT_ENV:
            raise CaseError(f"variant_env may not override reserved variable {key}")
        if not isinstance(value, str) or "\n" in value or "\x00" in value:
            raise CaseError(f"variant_env.{key} must be a single-line string")
        assignments.append(f"{key}={value}")
    return ["env", *assignments, *command] if assignments else command


def resolve_items(case: Mapping[str, Any], shadow: Mapping[str, Any] | None) -> int:
    explicit = case.get("items_per_producer")
    if explicit is not None:
        value = int(explicit)
        if value <= 0:
            raise CaseError("items_per_producer must be positive")
        return value
    if shadow is None:
        raise CaseError("no fixed item count and no shadow calibration result")
    resolved = shadow.get("items_per_producer")
    if resolved is not None:
        value = int(resolved)
    else:
        producers = int(case.get("producers") or str(case["scenario"]).split("p", 1)[0])
        value = int(shadow["items"]) // producers
    if value <= 0:
        raise CaseError("shadow calibration resolved a non-positive item count")
    return value


def enriched_case(case: Mapping[str, Any]) -> dict[str, Any]:
    output = dict(case)
    output.setdefault("machine", case.get("cluster"))
    output.setdefault("collector", case.get("backend"))
    if not output.get("recipe") and case.get("backend") == "perf":
        output["recipe"] = "perf_stat"
    output.setdefault("comparison_variant", case.get("variant", "source-tree"))
    output.setdefault("workload_mode", case.get("workload", "handoff"))
    implementation = str(case.get("queue_implementation") or case.get("queue"))
    ubq_label = case.get("ubq_label")
    output.setdefault("queue_variant", f"{implementation}:{ubq_label}" if ubq_label else implementation)
    variant_factors = case.get("variant_factors")
    if isinstance(variant_factors, Mapping):
        output.setdefault("allocator", variant_factors.get("allocator", "system"))
        output.setdefault("page_distributor", variant_factors.get("page_distributor", "build-defined"))
        output.setdefault("distributor_params", dict(variant_factors))
    placement = case.get("placement")
    if isinstance(placement, Mapping):
        output.setdefault("core_placement", placement.get("name", "interleaved"))
    output.setdefault("constraint", "perfparanoid")
    return output


def provenance(
    case: Mapping[str, Any],
    binary: Path,
    allocator_binary: Path | None,
    repo_root: Path,
) -> dict[str, Any]:
    slurm_env = {key: value for key, value in sorted(os.environ.items()) if key.startswith("SLURM_")}
    perf_paranoid = ""
    paranoid_path = Path("/proc/sys/kernel/perf_event_paranoid")
    if paranoid_path.is_file():
        perf_paranoid = paranoid_path.read_text(encoding="utf-8", errors="replace").strip()
    affinity = sorted(os.sched_getaffinity(0)) if hasattr(os, "sched_getaffinity") else []
    git_revision = run_capture(("git", "rev-parse", "HEAD"), cwd=repo_root)
    git_status = run_capture(("git", "status", "--porcelain=v1"), cwd=repo_root)
    git_diff = run_capture(("git", "diff", "--no-ext-diff", "--binary"), cwd=repo_root, timeout=120)
    diff_text = str(git_diff.get("stdout", ""))
    job_id = os.environ.get("SLURM_JOB_ID", "")
    module_log_value = os.environ.get("UBQ_PROFILE_MODULE_LOG", "")
    module_log_path = Path(module_log_value) if module_log_value else None
    if module_log_path is not None and module_log_path.is_file():
        module_log_value = module_log_path.read_text(encoding="utf-8", errors="replace")
    result = {
        "schema_version": RUNNER_SCHEMA_VERSION,
        "created_at": utc_now(),
        "machine": case.get("cluster"),
        "machine_class": case.get("cluster"),
        "hostname": platform.node(),
        "node": platform.node(),
        "platform": {
            "system": platform.system(),
            "release": platform.release(),
            "machine": platform.machine(),
            "python": platform.python_version(),
            "page_size": os.sysconf("SC_PAGE_SIZE") if hasattr(os, "sysconf") else None,
            "affinity": affinity,
        },
        "scheduler": {
            "job_id": job_id,
            "array_job_id": os.environ.get("SLURM_ARRAY_JOB_ID", ""),
            "array_task_id": os.environ.get("SLURM_ARRAY_TASK_ID", ""),
            "partition": os.environ.get("SLURM_JOB_PARTITION", ""),
            "qos": os.environ.get("SLURM_JOB_QOS", ""),
            "cpus_per_task": os.environ.get("SLURM_CPUS_PER_TASK", ""),
            "constraint": "perfparanoid",
            "environment": slurm_env,
            "scontrol": run_capture(
                ("scontrol", "show", "job", "-dd", job_id), cwd=repo_root
            )
            if job_id
            else {},
        },
        "perf_event_paranoid": perf_paranoid,
        "binary": str(binary),
        "binary_sha256": sha256(binary),
        "allocator_binary": str(allocator_binary) if allocator_binary is not None else "",
        "allocator_binary_sha256": sha256(allocator_binary)
        if allocator_binary is not None
        else "",
        "git_commit": str(git_revision.get("stdout", "")).strip(),
        "git_dirty": bool(str(git_status.get("stdout", "")).strip()),
        "git_status": git_status,
        "git_diff_sha256": hashlib.sha256(diff_text.encode("utf-8")).hexdigest(),
        "lscpu": run_capture(("lscpu",), cwd=repo_root),
        "numactl_hardware": run_capture(("numactl", "--hardware"), cwd=repo_root),
        "perf_version": run_capture(("perf", "--version"), cwd=repo_root),
        "apx_version": run_capture(("apx", "version"), cwd=repo_root)
        if case.get("backend") == "apx"
        else {},
        "module_log": module_log_value,
        "apx_module_status": os.environ.get("UBQ_PROFILE_APX_MODULE_STATUS", ""),
    }
    return result


def run_shadow(
    case: Mapping[str, Any], binary: Path, case_dir: Path, repo_root: Path, env: Mapping[str, str]
) -> tuple[dict[str, Any], list[str]]:
    explicit = case.get("items_per_producer")
    calibrate = bool(case.get("calibrate_outside_collector", explicit is None))
    if explicit is None:
        if not calibrate:
            raise CaseError(
                "calibrate_outside_collector=false requires an explicit items_per_producer"
            )
        shadow_items = None
    else:
        if calibrate:
            raise CaseError(
                "calibrate_outside_collector=true conflicts with explicit items_per_producer"
            )
        shadow_items = int(explicit)
        if shadow_items <= 0:
            raise CaseError("items_per_producer must be positive")
    fixed = shadow_items is not None
    command = build_workload_command(
        case,
        binary,
        items_per_producer=shadow_items,
        fixed=fixed,
    )
    timeout = int(case.get("shadow_timeout_secs", int(case["duration_secs"]) * 4 + 300))
    completed = run_command(
        command,
        cwd=repo_root,
        env=env,
        timeout=timeout,
        stdout_path=case_dir / "shadow.stdout.log",
        stderr_path=case_dir / "shadow.stderr.log",
    )
    if completed.returncode != 0:
        raise CaseError(f"unprofiled shadow failed with exit status {completed.returncode}")
    shadow = parse_json_object(completed.stdout, "shadow stdout")
    if shadow.get("fixed_work") is not fixed:
        expected = "fixed work" if fixed else "calibration"
        raise CaseError(f"unprofiled shadow did not execute the requested {expected} path")
    atomic_json(case_dir / "shadow.json", shadow)
    return shadow, command


def run_allocator_probe(
    case: Mapping[str, Any],
    binary: Path,
    case_dir: Path,
    repo_root: Path,
    env: Mapping[str, str],
    items_per_producer: int,
) -> tuple[dict[str, Any] | None, list[str] | None]:
    if not case.get("allocator_probe", False):
        return None, None
    command = build_workload_command(
        case,
        binary,
        items_per_producer=items_per_producer,
        fixed=True,
        allocator_probe=True,
    )
    timeout = int(case.get("shadow_timeout_secs", int(case["duration_secs"]) * 4 + 300))
    completed = run_command(
        command,
        cwd=repo_root,
        env=env,
        timeout=timeout,
        stdout_path=case_dir / "allocator-probe.stdout.log",
        stderr_path=case_dir / "allocator-probe.stderr.log",
    )
    if completed.returncode != 0:
        raise CaseError(f"allocator probe failed with exit status {completed.returncode}")
    result = parse_json_object(completed.stdout, "allocator probe stdout")
    if result.get("fixed_work") is not True:
        raise CaseError("allocator probe did not execute fixed work")
    atomic_json(case_dir / "allocator-benchmark.json", result)
    return result, command


def run_perf(
    case: Mapping[str, Any], command: Sequence[str], case_dir: Path, repo_root: Path, env: Mapping[str, str]
) -> dict[str, Any]:
    if shutil.which("perf", path=env.get("PATH")) is None:
        raise CaseError("perf is not available on the compute node")
    raw_events = case.get("perf_events")
    if isinstance(raw_events, str):
        events = [event.strip() for event in raw_events.split(",") if event.strip()]
    elif isinstance(raw_events, list):
        events = [str(event) for event in raw_events]
    else:
        events = []
    if not events:
        raise CaseError("perf case has no events")
    perf_command = [
        "perf",
        "stat",
        "--no-big-num",
        "-x",
        ";",
        "-o",
        str(case_dir / "perf.csv"),
    ]
    for event in events:
        perf_command.extend(("-e", event))
    perf_command.extend(("--", *command))
    completed = run_command(
        perf_command,
        cwd=repo_root,
        env=env,
        timeout=int(case["collector_timeout_secs"]),
        stdout_path=case_dir / "collector.stdout.log",
        stderr_path=case_dir / "collector.stderr.log",
    )
    if completed.returncode != 0:
        raise CaseError(f"perf stat failed with exit status {completed.returncode}")
    benchmark = parse_json_object(completed.stdout, "perf workload stdout")
    atomic_json(case_dir / "profiled-benchmark.json", benchmark)
    return benchmark


def apx_call(
    args: Sequence[str],
    *,
    label: str,
    case_dir: Path,
    repo_root: Path,
    env: Mapping[str, str],
    timeout: int,
) -> subprocess.CompletedProcess[str]:
    return run_command(
        ("apx", *args),
        cwd=repo_root,
        env=env,
        timeout=timeout,
        stdout_path=case_dir / f"apx-{label}.stdout.log",
        stderr_path=case_dir / f"apx-{label}.stderr.log",
    )


def run_apx(
    case: Mapping[str, Any], command: Sequence[str], case_dir: Path, repo_root: Path, env: Mapping[str, str]
) -> tuple[dict[str, Any] | None, str, str, bool]:
    if shutil.which("apx", path=env.get("PATH")) is None:
        raise CapabilityUnavailable(
            "APX is unavailable; on MN5 load/stage the official Linux-x64 CLI before rerunning"
        )
    # A freshly (auto-)started apxd daemon can take a moment to bind its gRPC
    # readiness endpoint; `apx version` then fails fast rather than blocking
    # for the full subprocess timeout. Retry with backoff before treating the
    # capability as unavailable, since a bare single-shot check has been
    # observed to reject a daemon that becomes ready a few seconds later.
    version = None
    for attempt in range(APX_VERSION_ATTEMPTS):
        version = apx_call(
            ("version",), label=f"version-{attempt}", case_dir=case_dir, repo_root=repo_root,
            env=env, timeout=60,
        )
        if version.returncode == 0:
            break
        if attempt + 1 < APX_VERSION_ATTEMPTS:
            time.sleep(APX_VERSION_RETRY_DELAY_SECS)
    if version.returncode != 0:
        raise CapabilityUnavailable(
            f"apx version failed after {APX_VERSION_ATTEMPTS} attempts (daemon readiness)"
        )
    prepared = apx_call(
        ("target", "prepare", "--target", "localhost"),
        label="prepare",
        case_dir=case_dir,
        repo_root=repo_root,
        env=env,
        timeout=300,
    )
    if prepared.returncode != 0:
        raise CapabilityUnavailable("apx target prepare failed")
    recipe = str(case.get("recipe") or "")
    # The recipe's own capture_log.txt is not a reliable carrier for the
    # workload's stdout: in the v3 Grace campaign the Streamline recorder
    # dropped it for 26 of 124 cpu_microarchitecture captures (and a handful of
    # the other recipes), discarding otherwise complete multi-minute
    # collections. Tee the result document to a known path instead, through a
    # wrapper script so the redirection does not depend on APX invoking a shell.
    workload_result = case_dir / "workload-stdout.json"
    if recipe == "instruction_mix":
        # This recipe disassembles the workload executable. Passing a shell
        # script causes DISASSEMBLER_INVOCATION_FAILED even when its exec'd
        # binary is valid (same-node direct/wrapped probe, Grace 362037).
        workload = shlex.join(command)
    else:
        wrapper = case_dir / "apx-workload.sh"
        wrapper.write_text(
            "#!/bin/sh\nexec {command} > {result}\n".format(
                command=shlex.join(command), result=shlex.quote(str(workload_result))
            ),
            encoding="utf-8",
        )
        wrapper.chmod(0o755)
        workload = shlex.join([str(wrapper)])
    ready_args = (
        "recipe",
        "ready",
        recipe,
        "--workload",
        workload,
        "--working-dir",
        str(repo_root),
        "--target=localhost",
    )
    ready = apx_call(
        ready_args, label="ready", case_dir=case_dir, repo_root=repo_root, env=env, timeout=300
    )
    if ready.returncode != 0:
        raise CapabilityUnavailable(f"APX recipe {recipe!r} readiness check failed on this node")
    # APX 1.20 can report "recipe is not ready" with status zero when only
    # deployable recipe tools are missing.  The supported --deploy-tools run
    # path below is authoritative; preserve readiness output rather than
    # rejecting that recoverable state here.
    run_args = (
        "recipe",
        "run",
        recipe,
        "--workload",
        workload,
        "--working-dir",
        str(repo_root),
        "--target=localhost",
        "--timeout",
        str(case["collector_timeout_secs"]),
        "--source",
        str(repo_root / "src"),
        "--deploy-tools",
    )
    for name, value in dict(case.get("recipe_params") or {}).items():
        run_args += ("--param", f"{name}={value}")
    run = apx_call(
        run_args,
        label="run",
        case_dir=case_dir,
        repo_root=repo_root,
        env=env,
        timeout=int(case["collector_timeout_secs"]) + 600,
    )
    if run.returncode != 0:
        raise CaseError(f"APX recipe run failed with exit status {run.returncode}")
    match = RUN_ID_RE.search(f"{run.stdout}\n{run.stderr}")
    if not match:
        raise CaseError("APX completed without reporting a Run ID")
    run_id = match.group(1)
    (case_dir / "apx-run-id.txt").write_text(run_id + "\n", encoding="utf-8")
    run_name = str(case.get("apx_run_name") or case.get("case_id") or run_id)
    (case_dir / "apx-run-name.txt").write_text(run_name + "\n", encoding="utf-8")
    renamed = apx_call(
        ("run", "rename", run_id, run_name),
        label="rename",
        case_dir=case_dir,
        repo_root=repo_root,
        env=env,
        timeout=120,
    )
    # Naming is analysis metadata, not collection validity. Preserve an
    # expensive successful capture even when an older APX CLI cannot rename
    # it; the import helper retries the same stable name on the local machine.
    rename_succeeded = renamed.returncode == 0
    atomic_json(
        case_dir / "apx-identity.json",
        {
            "run_id": run_id,
            "intended_run_name": run_name,
            "rename_succeeded": rename_succeeded,
            "comparison_group_id": case.get("comparison_group_id", ""),
            "comparison_role": case.get("comparison_role", ""),
        },
    )
    info = apx_call(
        ("run", "info", run_id),
        label="run-info",
        case_dir=case_dir,
        repo_root=repo_root,
        env=env,
        timeout=120,
    )
    if info.returncode != 0:
        raise CaseError("apx run info failed")
    exported = apx_call(
        ("run", "export", run_id, str(case_dir)),
        label="export",
        case_dir=case_dir,
        repo_root=repo_root,
        env=env,
        timeout=600,
    )
    if exported.returncode != 0:
        raise CaseError("apx run export failed")
    archives = sorted(case_dir.glob("*.zip"))
    if len(archives) != 1:
        raise CaseError(f"expected one APX archive after export, found {len(archives)}")
    archive = archives[0]
    (case_dir / "apx-archive.sha256").write_text(
        f"{sha256(archive)}  {archive.name}\n", encoding="utf-8"
    )
    return (
        (parse_workload_result(workload_result) if recipe != "instruction_mix" else None)
        or parse_apx_workload(archive),
        run_id,
        run_name,
        rename_succeeded,
    )


def execute(case: dict[str, Any], args: argparse.Namespace) -> int:
    repo_root = args.repo_root.resolve()
    results_root = args.results_root.resolve()
    case = enriched_case(case)
    case_dir = results_root / str(case["campaign"]) / str(case["case_id"])
    case_dir.mkdir(parents=True, exist_ok=True)
    status_path = case_dir / "status.json"
    if status_path.is_file() and not args.force:
        previous = json.loads(status_path.read_text(encoding="utf-8"))
        if previous.get("status") == "completed":
            print(f"Reusing completed profiling case {case['case_id']}")
            return 0
        raise CaseError(f"{case_dir} contains an incomplete prior attempt; use --force to replace sidecars")
    if any(case_dir.iterdir()) and not args.force:
        raise CaseError(f"{case_dir} already exists and is non-empty; use --force")

    original_binary = Path(str(case["binary"]))
    if not original_binary.is_absolute():
        original_binary = repo_root / original_binary
    override = os.environ.get("UBQ_PROFILE_BINARY_OVERRIDE")
    binary = Path(override) if override else original_binary
    if not binary.is_file():
        raise CaseError(f"profile binary does not exist: {binary}")
    if not os.access(binary, os.X_OK):
        raise CaseError(f"profile binary is not executable: {binary}")

    allocator_binary: Path | None = None
    if case.get("allocator_probe", False):
        allocator_raw = case.get("allocator_binary")
        if not isinstance(allocator_raw, str) or not allocator_raw:
            raise CaseError("allocator_probe case has no allocator_binary")
        allocator_binary = Path(allocator_raw)
        if not allocator_binary.is_absolute():
            allocator_binary = repo_root / allocator_binary
        allocator_override = os.environ.get("UBQ_PROFILE_ALLOCATOR_BINARY_OVERRIDE")
        if allocator_override:
            allocator_binary = Path(allocator_override)
        if not allocator_binary.is_file() or not os.access(allocator_binary, os.X_OK):
            raise CaseError(f"allocator profile binary is missing or not executable: {allocator_binary}")

    atomic_json(case_dir / "case.json", case)
    prov = provenance(case, binary, allocator_binary, repo_root)
    atomic_json(case_dir / "provenance.json", prov)
    started_at = utc_now()
    atomic_json(
        status_path,
        {
            "schema_version": RUNNER_SCHEMA_VERSION,
            "status": "running",
            "started_at": started_at,
        },
    )
    env = dict(os.environ)
    placement = case.get("placement")
    if isinstance(placement, Mapping) and placement.get("core_ids"):
        env["UBQ_BENCH_CORE_IDS"] = ",".join(str(value) for value in placement["core_ids"])
    env["RUST_BACKTRACE"] = "1"

    shadow, shadow_command = run_shadow(case, binary, case_dir, repo_root, env)
    resolved_items = resolve_items(case, shadow)
    allocator_result, allocator_command = run_allocator_probe(
        case,
        allocator_binary if allocator_binary is not None else binary,
        case_dir,
        repo_root,
        env,
        resolved_items,
    )
    exact_command = build_workload_command(
        case, binary, items_per_producer=resolved_items, fixed=True
    )
    atomic_json(
        case_dir / "commands.json",
        {
            "shadow": shadow_command,
            "allocator_probe": allocator_command,
            "collected_workload": exact_command,
            "resolved_items_per_producer": resolved_items,
        },
    )
    apx_run_id = ""
    apx_run_name = ""
    apx_run_renamed = False
    if case["backend"] == "perf":
        profiled = run_perf(case, exact_command, case_dir, repo_root, env)
    else:
        profiled, apx_run_id, apx_run_name, apx_run_renamed = run_apx(
            case, exact_command, case_dir, repo_root, env
        )
    # Performance claims always use the paired unprofiled shadow. Collector
    # execution time remains diagnostic metadata because recipe overhead can
    # differ dramatically.
    benchmark = shadow
    benchmark = dict(benchmark)
    benchmark.setdefault("items_per_producer", resolved_items)
    benchmark["measurement_role"] = "unprofiled_shadow"
    if allocator_result is not None:
        allocator_metrics = [
            metric
            for metric in allocator_result.get("metrics", [])
            if isinstance(metric, Mapping)
            and (
                metric.get("source") == "allocator_probe"
                or str(metric.get("metric_id", "")).startswith("allocator.")
            )
        ]
        if not allocator_metrics:
            raise CaseError("allocator probe emitted no allocator metrics")
        benchmark["allocator_metrics"] = allocator_metrics
        benchmark["allocator_probe_elapsed_ns"] = allocator_result.get("elapsed_ns")
    if profiled is not None:
        benchmark["profiled_elapsed_ns"] = profiled.get("elapsed_ns")
        benchmark["profiled_ops_per_sec"] = profiled.get("ops_per_sec")
        shadow_rate = benchmark.get("ops_per_sec")
        profiled_rate = profiled.get("ops_per_sec")
        if shadow_rate and profiled_rate:
            benchmark["collector_overhead_ratio"] = float(shadow_rate) / float(profiled_rate)
    atomic_json(case_dir / "benchmark.json", benchmark)
    finished_at = utc_now()
    atomic_json(
        status_path,
        {
            "schema_version": RUNNER_SCHEMA_VERSION,
            "status": "completed",
            "started_at": started_at,
            "finished_at": finished_at,
            "backend": case["backend"],
            "apx_run_id": apx_run_id,
            "apx_run_name": apx_run_name,
            "apx_run_renamed": apx_run_renamed,
            "resolved_items_per_producer": resolved_items,
        },
    )
    print(f"Completed profiling case {case['case_id']} in {case_dir}")
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--plan", type=Path, required=True)
    parser.add_argument("--index", type=int, required=True, help="zero-based JSONL line index")
    parser.add_argument("--results-root", type=Path, required=True)
    parser.add_argument("--repo-root", type=Path, required=True)
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args(argv)
    case: dict[str, Any] | None = None
    case_dir: Path | None = None
    try:
        case = load_case(args.plan, args.index)
        case_dir = args.results_root.resolve() / str(case["campaign"]) / str(case["case_id"])
        return execute(case, args)
    except CapabilityUnavailable as exc:
        print(f"run_profile_case: unavailable: {exc}", file=sys.stderr)
        if case_dir is not None:
            case_dir.mkdir(parents=True, exist_ok=True)
            if case is not None and not (case_dir / "case.json").exists():
                atomic_json(case_dir / "case.json", enriched_case(case))
            atomic_json(
                case_dir / "status.json",
                {
                    "schema_version": RUNNER_SCHEMA_VERSION,
                    "status": "unavailable",
                    "finished_at": utc_now(),
                    "reason": str(exc),
                },
            )
        return 0
    except (CaseError, OSError, ValueError, zipfile.BadZipFile) as exc:
        print(f"run_profile_case: {exc}", file=sys.stderr)
        if case_dir is not None:
            case_dir.mkdir(parents=True, exist_ok=True)
            if case is not None and not (case_dir / "case.json").exists():
                atomic_json(case_dir / "case.json", enriched_case(case))
            atomic_json(
                case_dir / "status.json",
                {
                    "schema_version": RUNNER_SCHEMA_VERSION,
                    "status": "failed",
                    "finished_at": utc_now(),
                    "reason": str(exc),
                },
            )
        return 1
    finally:
        if case is not None and case.get("backend") == "apx" and shutil.which("apx"):
            subprocess.run(
                ("apx", "daemon", "stop"),
                text=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=30,
                check=False,
            )


if __name__ == "__main__":
    raise SystemExit(main())
