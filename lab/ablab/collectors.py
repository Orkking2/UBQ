"""Hardware collectors for the deep stage: perf everywhere, Arm Performix (APX) on Grace.

Each collector runs the frozen workload once under its tool and returns the
workload result plus a small `metrics` summary for the report. Raw captures
(perf.data) stay on the cluster; summaries and APX archives come home with
`./ab fetch`.
"""
from __future__ import annotations

import csv
import io
import json
import math
import platform
import re
import shlex
import shutil
import time
import zipfile
from pathlib import Path

from .common import run, tail

APX_RECIPES = ("code_hotspots", "cpu_microarchitecture", "instruction_mix", "system_utilization")
PERF_COLLECTORS = ("counters", "perf_record", "perf_mem", "perf_c2c")
MIN_SAMPLES = 100
# A counter that ran for less of the workload than this is too multiplexed to trust.
MIN_RUNNING_FRACTION = 0.90
# `perf mem` sampling period on Arm SPE. The tool default wrote 510 MB and
# 24 M samples for one six-round Grace capture; this period writes 19 MB and
# 239 k samples with no measurable change in workload throughput. PEBS on MN5
# keeps perf's default.
SPE_PERIOD = ("-c", "100003")
# perf script prints one line per SPE event flag. Bucket by instruction-pointer
# address space: across repeats the user-space count replicates to a few
# percent while the kernel count varies several-fold, so rates must not mix them.
SPE_AWK = r"""
{ space = (substr($2, 1, 4) == "ffff") ? "kernel" : "user"; c[$1 SUBSEP space]++ }
END { for (k in c) { split(k, p, SUBSEP); printf "%s\t%s\t%d\n", p[1], p[2], c[k] } }
"""
RUN_ID = re.compile(r"(?:Run ID|run[_ .-]?id)[\"':= ]+([A-Za-z0-9._-]+)", re.I)
APX_METRIC_IDS = {
    "Instructions Per Cycle": "ipc",
    "Cycles Per Instruction": "cpi",
    "Frontend Stalled Cycles": "frontend_stalled_cycles_pct",
    "Backend Stalled Cycles": "backend_stalled_cycles_pct",
    "Frontend Bound": "frontend_bound_pct",
    "Backend Bound": "backend_bound_pct",
    "Retiring": "retiring_pct",
    "Bad Speculation": "bad_speculation_pct",
    "L1D Cache MPKI": "l1d_mpki",
    "L2 Cache MPKI": "l2_mpki",
    "L3 Cache MPKI": "l3_mpki",
    "DTLB MPKI": "dtlb_mpki",
    "Branch MPKI": "branch_mpki",
    "Branch Misprediction Percentage": "branch_mispredict_pct",
    "Barrier Operations Percentage": "barrier_ops_pct",
    "Load Operations Percentage": "load_ops_pct",
    "Store Operations Percentage": "store_ops_pct",
    "Load-Exclusive Instructions Percentage": "load_exclusive_pct",
    "Store-Exclusive Instructions Percentage": "store_exclusive_pct",
    "Store-Exclusive Failures PKI": "store_exclusive_failures_pki",
    "Sample Count": "samples",
}


class CollectorError(RuntimeError):
    pass


def handoffs(result):
    """Handoffs a whole-process counter covers: measured plus warmup rounds."""
    return result["items"] + result["warmup_rounds"] * result["round_items"]


def collect(name, workload, run_dir, config, *, cluster_config, source_dir, run_name):
    """Run `config` under collector `name` in run_dir; return {"result", "metrics"}."""
    run_dir.mkdir(parents=True, exist_ok=True)
    if name == "counters":
        return _counters(workload, run_dir, config, cluster_config.get("counter_groups", {}))
    if name == "perf_record":
        return _perf_record(workload, run_dir, config)
    if name == "perf_mem":
        return _perf_mem(workload, run_dir, config)
    if name == "perf_c2c":
        return _perf_c2c(workload, run_dir, config)
    if name in APX_RECIPES:
        return _apx(name, workload, run_dir, config, source_dir=source_dir, run_name=run_name)
    raise CollectorError(f"unknown collector {name}")


def parse_perf_stat(path, events):
    # perf's -x output can drop the requested modifier ("dTLB-loads" for
    # "dTLB-loads:u"), so match on the modifier-free name.
    def base(name):
        return name.split(":", 1)[0]
    wanted = {base(event): event for event in events}
    found = {}
    for row in csv.reader(Path(path).read_text().splitlines(), delimiter=";"):
        if len(row) < 3 or base(row[2].strip()) not in wanted:
            continue
        event = wanted[base(row[2].strip())]
        try:
            value = float(row[0])
            fraction = float(row[4].strip().rstrip("%")) / 100 if len(row) > 4 and row[4].strip() else 0.0
        except ValueError:
            raise CollectorError(f"{event} is not supported or not counted: {row[0].strip()}") from None
        if not math.isfinite(value) or value < 0 or fraction < MIN_RUNNING_FRACTION:
            raise CollectorError(f"{event} ran for only {fraction:.0%} of the workload")
        found[event] = {"value": value, "running_fraction": fraction}
    missing = sorted(set(events) - set(found))
    if missing:
        raise CollectorError(f"perf stat reported no value for {missing}")
    return found


def _counters(workload, run_dir, config, groups):
    """One run per small simultaneous group, so ratios within a group share one run."""
    measured, unavailable, result = {}, {}, None
    for group, names in groups.items():
        directory = run_dir / group
        output = directory / "perf.csv"
        directory.mkdir(parents=True, exist_ok=True)
        prefix = ["perf", "stat", "--no-big-num", "-x", ";", "-o", str(output),
                  "-e", "{" + ",".join(names) + "}", "--"]
        try:
            group_result = workload.run(directory, config, prefix=prefix)
            measured[group] = {"events": parse_perf_stat(output, names), "handoffs": handoffs(group_result),
                               "ops_per_sec": group_result["ops_per_sec"]}
            result = group_result
        except (CollectorError, RuntimeError, OSError, TimeoutError) as exc:
            unavailable[group] = str(exc)
    if not measured:
        raise CollectorError(f"no counter group produced valid counts: {unavailable}")
    return {"result": result, "metrics": {"groups": measured, "unavailable": unavailable}}


def parse_overhead_table(text):
    """[[percent, samples, label], ...] from `perf report --stdio -n` output."""
    rows = []
    for line in text.splitlines():
        match = re.match(r"^\s*([\d.]+)%\s+(\d+)\s+(.*\S)\s*$", line)
        if match and not line.lstrip().startswith("#"):
            label = re.sub(r"^\[[.k]\]\s+", "", match.group(3))
            rows.append([float(match.group(1)), int(match.group(2)), label])
    return rows


def _report(command, run_dir, name):
    completed = run(command, cwd=run_dir, timeout=1800, stdout=run_dir / f"{name}.txt",
                    stderr=run_dir / f"{name}.err")
    if completed.returncode:
        raise CollectorError(f"{' '.join(command[:3])} failed: {tail(completed.stderr, 600)}")
    return completed.stdout


def _require_samples(rows, what):
    if sum(row[1] for row in rows) < MIN_SAMPLES:
        raise CollectorError(f"{what} decoded fewer than {MIN_SAMPLES} samples")


def _perf_record(workload, run_dir, config):
    data = run_dir / "perf.data"
    result = workload.run(run_dir, config, prefix=["perf", "record", "-e", "cycles:u", "-F", "299",
                                                   "--call-graph", "fp", "-o", str(data), "--"])
    symbols = parse_overhead_table(_report(
        ["perf", "report", "-i", str(data), "--stdio", "-n", "--no-children", "--sort", "symbol",
         "-g", "none", "--percent-limit", "0.2"], run_dir, "symbols"))
    _require_samples(symbols, "perf record")
    return {"result": result, "metrics": {"event": "cycles:u", "symbols": symbols[:60]}}


def spe_events(data, run_dir):
    """SPE event-flag counts split by user and kernel instruction pointers."""
    script = (f"set -o pipefail; perf script -i {shlex.quote(str(data))} -F event,ip "
              f"2>{shlex.quote(str(run_dir / 'spe.err'))} | awk {shlex.quote(SPE_AWK)}")
    completed = run(["bash", "-c", script], timeout=3600)
    if completed.returncode:
        raise CollectorError("SPE decode failed: " + tail(completed.stderr, 600))
    events = {"user": {}, "kernel": {}}
    for line in completed.stdout.splitlines():
        event, space, count = line.split("\t")
        events.setdefault(space, {})[event.rstrip(":")] = int(count)
    return events


def _perf_mem(workload, run_dir, config):
    data = run_dir / "perf.data"
    arm = platform.machine().lower() in ("aarch64", "arm64")
    prefix = ["perf", "mem", "record", "-t", "load", *(SPE_PERIOD if arm else ()),
              "-o", str(data), "--"]
    result = workload.run(run_dir, config, prefix=prefix)
    report = ["perf", "mem", "report", "-i", str(data), "--stdio", "-n"]
    levels = parse_overhead_table(_report(report + ["--sort=mem", "--percent-limit", "0"],
                                          run_dir, "levels"))
    symbols = parse_overhead_table(_report(report + ["--sort=sym", "--percent-limit", "0.5"],
                                           run_dir, "symbols"))
    _require_samples(levels, "perf mem")
    metrics = {"operation": "load", "levels": levels, "symbols": symbols[:40]}
    if arm:
        metrics["spe"] = spe_events(data, run_dir)
    return {"result": result, "metrics": metrics}


def parse_c2c_stats(text):
    stats = {}
    for line in text.splitlines():
        # Counts ("Load Local HITM : 3402") and shares ("... (HITM) : 56.1%").
        match = re.match(r"^\s*([A-Za-z][^:]*?)\s*:\s*([\d.]+)%?\s*$", line)
        if match:
            stats.setdefault(match.group(1), float(match.group(2)))
    return stats


def _perf_c2c(workload, run_dir, config):
    data = run_dir / "perf.data"
    result = workload.run(run_dir, config, prefix=["perf", "c2c", "record", "-o", str(data), "--"])
    stats = parse_c2c_stats(_report(["perf", "c2c", "report", "-i", str(data), "--stdio", "--stats"],
                                    run_dir, "stats"))
    _report(["perf", "c2c", "report", "-i", str(data), "--stdio"], run_dir, "cachelines")
    if stats.get("Total records", 0) < MIN_SAMPLES:
        raise CollectorError(f"perf c2c recorded fewer than {MIN_SAMPLES} records")
    return {"result": result, "metrics": {"stats": stats}}


def parse_apx_archive(path):
    """Root metrics, sample count, hotspots and any workload result in an APX export."""
    parsed = {"metrics": {}, "samples": 0, "hotspots": [], "workload": None}

    def member(names, suffix):
        matches = sorted((n for n in names if n.endswith(suffix)), key=lambda n: (n.count("/"), n))
        return matches[0] if matches else None

    def root_row(payload):
        return next((row for row in payload.get("rows", []) if str(row.get("call_frame_id")) == "0"), None)

    with zipfile.ZipFile(path) as archive:
        names = archive.namelist()
        found = member(names, "/output/callpath_total_metrics.json")
        if found:
            payload = json.loads(archive.read(found))
            root = root_row(payload)
            for column, value in zip(payload.get("columns", []), (root or {}).get("column_data", [])):
                name = str(column.get("name") or "")
                try:
                    number = float(value)
                except (TypeError, ValueError):
                    continue
                key = APX_METRIC_IDS.get(name) or re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")
                parsed["metrics"].setdefault(key, number)
        found = member(names, "/output/callpath_total_samples.json")
        if found:
            payload = json.loads(archive.read(found))
            root = root_row(payload) or {}
            for column, value in zip(payload.get("columns", []), root.get("column_data", [])):
                if column.get("units") == "sample_counter" or "sample" in str(column.get("name", "")).lower():
                    parsed["samples"] = max(parsed["samples"], float(value or 0))
        parsed["samples"] = max(parsed["samples"], parsed["metrics"].get("samples", 0))
        # The system-utilization recipe has no sample counter, only a timeline.
        for name in names:
            if name.endswith("/timeline.csv"):
                rows = len(archive.read(name).decode(errors="replace").splitlines()) - 1
                parsed["samples"] = max(parsed["samples"], rows)
        found = member(names, "/output/functions-capture-periodic_sampling.csv")
        if found:
            total = parsed["samples"] or None
            for row in csv.DictReader(io.StringIO(archive.read(found).decode(errors="replace"))):
                try:
                    samples = float(row.get("Periodic Samples") or "")
                except ValueError:
                    continue
                parsed["hotspots"].append([samples / total if total else None, samples,
                                           row.get("symbol") or row.get("image") or "?"])
            parsed["hotspots"].sort(key=lambda h: -h[1])
        for name in names:
            if name.endswith("/capture_log.txt") and parsed["workload"] is None:
                parsed["workload"] = find_result(archive.read(name).decode(errors="replace"))
    return parsed


def rss_bytes(result):
    """The process's resident set at the end of a bench_hardware run, if it reported one."""
    memory = result.get("memory") or {}
    for key in ("Rss_bytes", "VmRSS_bytes", "VmHWM_bytes"):
        if memory.get(key):
            return memory[key]
    return None


def find_result(text):
    """The bench_hardware result document in a stream of output, if present."""
    for line in text.splitlines():
        line = line.strip()
        if line.startswith("{"):
            try:
                value = json.loads(line)
            except ValueError:
                continue
            if isinstance(value, dict) and "benchmark" in value:
                return value
    return None


def _apx(recipe, workload, run_dir, config, *, source_dir, run_name):
    if shutil.which("apx") is None:
        raise CollectorError("apx is not on PATH; is the Performix module loaded?")
    limit = workload.timeout(config, slowdown=3)

    def apx(label, *args, timeout=300):
        return run(["apx", *args], cwd=run_dir, timeout=timeout,
                   stdout=run_dir / f"apx-{label}.out", stderr=run_dir / f"apx-{label}.err")

    # `apx version` starts apxd when it is not running. On a fresh node the
    # daemon's first start took more than 80 s to answer (batch-mismatch on
    # Grace, 2026-09-13: six probes 13 s apart all got SERVER_DID_NOT_RESPOND
    # and the arm captured next in the same allocation found it ready), so
    # the probe waits up to five minutes.
    for attempt in range(30):
        if apx(f"version-{attempt}", "version", timeout=60).returncode == 0:
            break
        time.sleep(10)
    else:
        raise CollectorError("the APX daemon never became ready")
    if apx("prepare", "target", "prepare", "--target", "localhost").returncode:
        raise CollectorError("apx target prepare failed")
    command = workload.command(run_dir, config)
    teed = run_dir / "workload-result.json"
    if recipe == "instruction_mix":
        # This recipe disassembles the workload executable; a wrapper script
        # makes it fail with DISASSEMBLER_INVOCATION_FAILED.
        target = shlex.join(command)
    else:
        # capture_log.txt drops the workload's stdout for some captures, so tee
        # the result to a known file through a wrapper.
        wrapper = run_dir / "workload.sh"
        wrapper.write_text(f"#!/bin/sh\nexec {shlex.join(command)} > {shlex.quote(str(teed))}\n")
        wrapper.chmod(0o755)
        target = shlex.quote(str(wrapper))
    common = ["--workload", target, "--working-dir", str(run_dir), "--target=localhost"]
    if apx("ready", "recipe", "ready", recipe, *common).returncode:
        raise CollectorError(f"APX recipe {recipe} is not ready on this node")
    ran = apx("run", "recipe", "run", recipe, *common, "--timeout", str(int(limit)),
              "--source", str(source_dir), "--deploy-tools", timeout=limit + 900)
    if ran.returncode:
        raise CollectorError(f"apx recipe run failed: {tail(ran.stderr or ran.stdout, 800)}")
    match = RUN_ID.search(ran.stdout + "\n" + ran.stderr)
    if not match:
        raise CollectorError("apx finished without reporting a run id")
    run_id = match.group(1)
    # A stable name lets `./ab performix` pair the arms after import; renaming
    # can fail on older CLIs, and the import retries it.
    renamed = apx("rename", "run", "rename", run_id, run_name, timeout=120).returncode == 0
    if apx("export", "run", "export", run_id, str(run_dir), timeout=900).returncode:
        raise CollectorError("apx run export failed")
    archives = sorted(run_dir.glob("*.zip"))
    if len(archives) != 1:
        raise CollectorError(f"expected one exported APX archive, found {len(archives)}")
    parsed = parse_apx_archive(archives[0])
    result = find_result(teed.read_text()) if teed.exists() else None
    result = workload.accept(result or parsed["workload"], config)
    if parsed["samples"] < MIN_SAMPLES:
        raise CollectorError(f"APX archive holds fewer than {MIN_SAMPLES} samples")
    return {"result": result, "metrics": {
        "recipe": recipe, "run_id": run_id, "run_name": run_name, "renamed": renamed,
        "archive": archives[0].name, "samples": parsed["samples"], "metrics": parsed["metrics"],
        "hotspots": parsed["hotspots"][:40]}}
