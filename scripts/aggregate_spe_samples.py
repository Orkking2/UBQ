#!/usr/bin/env python3
"""Aggregate Arm SPE (``perf mem record``) captures into an analysis-ready CSV.

``hardware_campaign.py report`` covers the ``perf stat`` counter groups but not
the statistical-profiling captures, whose decoded form is far too large to move
off the cluster: a single 72p72c capture decodes to ~7 GiB of text and the v6
Grace panel totals ~256 GiB.  This script therefore does the reduction *beside
the data* and emits only the summary table.

Layout produced by the campaign::

    results/<case-id>/<r{N}-{arm}-perf_mem>/
        samples.log            # perf script -F event,ip
        samples.log.meta.json  # decode command, returncode, elapsed
        benchmark.json         # the measured workload beside the capture

Normalisation
-------------
SPE emits one record per sampled operation, with the packet's event flags
expanded by ``perf script`` into one line per set flag.  ``memory`` is set on
every sampled memory operation, so it is the denominator; ``l1d-miss``,
``llc-access``, ``llc-miss`` and ``remote-access`` are the informative
numerators.  Reporting *rates per sampled memory operation* makes the numbers
invariant to the sampling period (``-c``), which is what allows cells captured
at different periods, and different queues at wildly different throughputs, to
be compared at all.  Absolute sample counts are retained for weighting and for
judging the statistical support behind each rate.

The instruction pointer gives a second, independent axis: whether the sampled
memory traffic issued from user code or from the kernel.  On arm64 the kernel
half of the address space is the ``0xffff...`` prefix.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import subprocess
import sys
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from typing import Any, Dict, List, Optional


OUTPUT_SCHEMA_VERSION = 1

# Set on every sampled memory operation, hence the denominator for every rate.
DENOMINATOR_EVENT = "memory"

# Emitted as columns in this order; any event seen but not listed here still
# reaches the CSV through the generic ``spe_<event>`` count columns.
KNOWN_EVENTS = (
    "memory",
    "l1d-access",
    "l1d-miss",
    "llc-access",
    "llc-miss",
    "tlb-access",
    "tlb-miss",
    "remote-access",
    "branch-miss",
)

# One awk pass per capture, bucketing every event by the address space of its
# instruction pointer.  The split is not cosmetic: across repeats of the same
# cell the user-space sample count is stable to a few percent while the kernel
# count varies several-fold, so a rate taken over the mixed pool inherits that
# instability.  Keeping the two spaces apart yields a replicable measure of the
# queue's own memory behaviour alongside the kernel work done on its behalf.
# awk rather than Python because these files are gigabytes of trivial text.
AWK_PROGRAM = r"""
{
    space = (substr($2, 1, 4) == "ffff") ? "kernel" : "user"
    c[$1 SUBSEP space]++
}
END {
    for (key in c) {
        split(key, part, SUBSEP)
        printf "E\t%s\t%s\t%d\n", part[1], part[2], c[key]
    }
}
"""


def scan(path_text):
    """Return the event histogram and IP-space split for one ``samples.log``."""
    path = Path(path_text)
    try:
        completed = subprocess.run(
            ["awk", AWK_PROGRAM, str(path)],
            capture_output=True, text=True, timeout=3600,
        )
    except subprocess.TimeoutExpired:
        return {"path": path_text, "error": "awk timed out"}
    if completed.returncode:
        return {"path": path_text, "error": completed.stderr.strip()[:400]}

    # events[space][event] = count
    events = {"user": {}, "kernel": {}}
    for line in completed.stdout.splitlines():
        fields = line.split("\t")
        if fields[0] == "E":
            events.setdefault(fields[2], {})[fields[1].rstrip(":")] = int(fields[3])
    return {
        "path": path_text,
        "events": events,
        "bytes": path.stat().st_size,
    }


def captures(campaign):
    """Yield every decoded SPE capture directory under a campaign."""
    results = campaign / "results"
    if not results.is_dir():
        raise SystemExit("no results/ directory under " + str(campaign))
    for samples in sorted(results.glob("*perf_mem*/*/samples.log")):
        yield samples


def read_json(path):
    try:
        with path.open() as handle:
            return json.load(handle)
    except (OSError, ValueError):
        return {}


def describe(samples):
    """Static metadata for one capture, independent of the sample scan."""
    run_dir = samples.parent
    case_dir = run_dir.parent
    case = read_json(case_dir / "case.json")
    benchmark = read_json(run_dir / "benchmark.json")
    meta = read_json(samples.parent / "samples.log.meta.json")
    record = read_json(run_dir / "stdout.log.meta.json")
    status = read_json(run_dir / "status.json")
    case_status = read_json(case_dir / "status.json")

    # Directory names are "r{repeat}-{arm}-{collector}".
    name = run_dir.name
    parts = name.split("-")
    repeat = parts[0][1:] if parts and parts[0].startswith("r") else ""
    arm = "-".join(parts[1:-1]) if len(parts) > 2 else ""

    # The sampling period and the load/store filter are fixed by ``perf mem
    # record``, not by the later decode, so they come from the workload's own
    # command record.  ``-t load`` means every rate below is a load-side view.
    period = operation_type = ""
    command = record.get("command") or []
    for index, token in enumerate(command):
        if token == "-c" and index + 1 < len(command):
            period = command[index + 1]
        elif token == "-t" and index + 1 < len(command):
            operation_type = command[index + 1]

    return {
        "case": case_dir.name,
        "run": name,
        "kind": case.get("kind", ""),
        "parent": case.get("parent", ""),
        "repeat": repeat,
        "arm": arm,
        "queue": benchmark.get("queue", ""),
        "queue_implementation": benchmark.get("queue_implementation", ""),
        "scenario": benchmark.get("scenario", ""),
        "batch_size": benchmark.get("batch_size", ""),
        "backoff": benchmark.get("backoff", ""),
        "completion_accounting": benchmark.get("completion_accounting", ""),
        "items": benchmark.get("items", ""),
        "ops_per_sec": benchmark.get("ops_per_sec", ""),
        "elapsed_ns": benchmark.get("elapsed_ns", ""),
        "rounds": benchmark.get("rounds", ""),
        "affinity_ok": benchmark.get("affinity_ok", ""),
        "barrier_overhead_proxy_fraction": benchmark.get("barrier_overhead_proxy_fraction", ""),
        "case_status": case_status.get("status", ""),
        "run_status": status.get("status", ""),
        "decode_returncode": meta.get("returncode", ""),
        "decode_timed_out": meta.get("timed_out", ""),
        "decode_elapsed_secs": meta.get("elapsed_secs", ""),
        "spe_period": period,
        "spe_operation_type": operation_type,
        "samples_path": str(samples),
    }


def ratio(numerator, denominator):
    return numerator / denominator if denominator else ""


def build_rows(campaign, workers):
    paths = [str(p) for p in captures(campaign)]
    if not paths:
        raise SystemExit("no decoded SPE captures found under " + str(campaign / "results"))
    print("scanning {} SPE captures with {} workers".format(len(paths), workers), file=sys.stderr)

    scanned = {}
    done = 0
    with ProcessPoolExecutor(max_workers=workers) as pool:
        for result in pool.map(scan, paths):
            scanned[result["path"]] = result
            done += 1
            if done % 10 == 0 or done == len(paths):
                print("  {}/{}".format(done, len(paths)), file=sys.stderr, flush=True)

    seen_events = set()
    for result in scanned.values():
        for space_counts in result.get("events", {}).values():
            seen_events.update(space_counts)
    ordered_events = [e for e in KNOWN_EVENTS if e in seen_events]
    ordered_events += sorted(seen_events - set(KNOWN_EVENTS))

    rows = []
    for path in paths:
        row = describe(Path(path))
        result = scanned[path]
        row["scan_error"] = result.get("error", "")
        row["samples_bytes"] = result.get("bytes", "")
        events = result.get("events", {})
        user = events.get("user", {})
        kernel = events.get("kernel", {})

        user_total = user.get(DENOMINATOR_EVENT, 0)
        kernel_total = kernel.get(DENOMINATOR_EVENT, 0)
        total = user_total + kernel_total
        row["spe_memory_samples"] = total
        row["spe_memory_samples_user"] = user_total
        row["spe_memory_samples_kernel"] = kernel_total
        row["kernel_sample_fraction"] = ratio(kernel_total, total)

        for event in ordered_events:
            name = event.replace("-", "_")
            row["spe_" + name + "_user"] = user.get(event, 0)
            row["spe_" + name + "_kernel"] = kernel.get(event, 0)
        # Rates per sampled memory operation *within the same address space*, so
        # they are invariant both to the sampling period and to how much
        # unrelated kernel time a particular run happened to accumulate.
        for event in ordered_events:
            if event == DENOMINATOR_EVENT:
                continue
            name = event.replace("-", "_")
            row["rate_" + name + "_user"] = ratio(user.get(event, 0), user_total)
            row["rate_" + name + "_kernel"] = ratio(kernel.get(event, 0), kernel_total)
        rows.append(row)
    return rows


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("campaign", type=Path)
    parser.add_argument("--output", type=Path, default=None,
                        help="CSV destination (default: <campaign>/spe.csv)")
    parser.add_argument("--workers", type=int, default=min(16, (os.cpu_count() or 4)),
                        help="parallel awk scans; the work is I/O bound on GPFS")
    args = parser.parse_args(argv)

    campaign = args.campaign.resolve()
    rows = build_rows(campaign, max(1, args.workers))
    output = args.output or campaign / "spe.csv"

    columns = []
    for row in rows:
        for key in row:
            if key not in columns:
                columns.append(key)
    with output.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)

    failed = [r for r in rows if r["scan_error"]]
    print(output)
    print("{} captures, {} scan failures, schema v{}".format(
        len(rows), len(failed), OUTPUT_SCHEMA_VERSION))
    print("Rates are per sampled memory operation within one address space, so they "
          "are invariant to the SPE sampling period and to run-to-run variation in "
          "kernel time; absolute counts show the support behind each rate.")
    print("Captured with 'perf mem record -t load': this is a load-side view only.")
    for row in failed:
        print("  FAILED {}: {}".format(row["run"], row["scan_error"]), file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
