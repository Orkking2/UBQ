#!/usr/bin/env python3
"""Summarize update-method throughput and fairness from bench_atomic_updates.

Reads schema-v4 results and reports, for every think level and updater count,
each update method's median throughput, its ratio to FAA, and how the updates
were distributed: the fraction of updates that directly followed the same
updater's previous write (same-updater succession), CAS failures per update,
and the largest share of updates taken by one updater.

A method that is faster than FAA only where its same-updater fraction is near
one is being measured in a regime where one core keeps the counter's cache
line; that is a property of the zero-work loop, not of the update method.
"""

import argparse
import csv
import json
import statistics
import sys
from collections import defaultdict
from pathlib import Path

BENCHMARK_NAME = "atomic_updates"
SCHEMA_VERSION = 4
METHODS = ("faa", "cas", "cas_backoff", "segqueue")
REFERENCE_METHOD = "faa"
CSV_FIELDS = (
    "machine",
    "ordering",
    "layout",
    "block_size",
    "think_iters",
    "think_ns",
    "updaters",
    "method",
    "samples",
    "median_mops",
    "min_mops",
    "max_mops",
    "ratio_to_faa",
    "same_updater_fraction",
    "cas_failures_per_update",
    "max_updater_share",
    "fair_share",
    "counter_addresses",
)


def load_rows(path):
    """Return one dict per sample, or [] when the file is not a v4 atomic run."""
    with Path(path).open(encoding="utf-8") as stream:
        data = json.load(stream)
    if (
        data.get("benchmark") != BENCHMARK_NAME
        or str(data.get("schema_version")) != str(SCHEMA_VERSION)
    ):
        return []
    meta = data.get("meta", {})
    machine = str(meta.get("machine_label", "local")).strip() or "local"
    ordering = str(meta.get("ordering", "ubq")).strip().lower() or "ubq"
    think_iteration_ns = float(meta.get("think_iteration_ns", 0.0))
    rows = []
    for result in data.get("results", []):
        operations = int(result["operations"])
        if operations <= 0:
            continue
        think_iters = int(result["think_iters"])
        rows.append(
            {
                "machine": machine,
                "ordering": ordering,
                "layout": str(result["layout"]),
                "block_size": int(result["block_size"]),
                "think_iters": think_iters,
                "think_ns": think_iters * think_iteration_ns,
                "updaters": int(result["updater_count"]),
                "method": str(result["method"]),
                "mops": float(result["ops_per_sec"]) / 1e6,
                "same_updater_fraction": float(result["same_updater_fraction"]),
                "cas_failures_per_update": int(result.get("cas_failures", 0))
                / operations,
                "max_updater_share": float(result["max_updater_share"]),
                "counter_address": result.get("counter_address_hex"),
            }
        )
    return rows


def summarize(rows, layout="u64", block_size=None):
    """Group samples into one summary row per configuration and method."""
    grouped = defaultdict(list)
    for row in rows:
        if row["layout"] != layout:
            continue
        if block_size is not None and row["block_size"] != block_size:
            continue
        key = (
            row["machine"],
            row["ordering"],
            row["layout"],
            row["block_size"],
            row["think_iters"],
            row["updaters"],
            row["method"],
        )
        grouped[key].append(row)

    summaries = []
    for key in sorted(grouped):
        samples = grouped[key]
        machine, ordering, group_layout, block, think_iters, updaters, method = key
        throughput = [sample["mops"] for sample in samples]
        summaries.append(
            {
                "machine": machine,
                "ordering": ordering,
                "layout": group_layout,
                "block_size": block,
                "think_iters": think_iters,
                "think_ns": statistics.median(s["think_ns"] for s in samples),
                "updaters": updaters,
                "method": method,
                "samples": len(samples),
                "median_mops": statistics.median(throughput),
                "min_mops": min(throughput),
                "max_mops": max(throughput),
                "ratio_to_faa": None,
                "same_updater_fraction": statistics.median(
                    s["same_updater_fraction"] for s in samples
                ),
                "cas_failures_per_update": statistics.median(
                    s["cas_failures_per_update"] for s in samples
                ),
                "max_updater_share": statistics.median(
                    s["max_updater_share"] for s in samples
                ),
                "fair_share": 1.0 / updaters,
                # Distinct counter allocations behind this row; a wide
                # min/max spread with more than one address may be a
                # placement effect rather than run-to-run noise.
                "counter_addresses": len(
                    {s["counter_address"] for s in samples if s["counter_address"]}
                ),
            }
        )

    reference = {
        (s["machine"], s["ordering"], s["layout"], s["block_size"], s["think_iters"], s["updaters"]): s[
            "median_mops"
        ]
        for s in summaries
        if s["method"] == REFERENCE_METHOD
    }
    for summary in summaries:
        base = reference.get(
            (
                summary["machine"],
                summary["ordering"],
                summary["layout"],
                summary["block_size"],
                summary["think_iters"],
                summary["updaters"],
            )
        )
        if base:
            summary["ratio_to_faa"] = summary["median_mops"] / base
    return summaries


def write_csv(path, summaries):
    with Path(path).open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=CSV_FIELDS)
        writer.writeheader()
        for summary in summaries:
            writer.writerow(
                {
                    field: "" if summary[field] is None else summary[field]
                    for field in CSV_FIELDS
                }
            )


def format_table(summaries):
    """Render one line per configuration with the methods side by side."""
    by_config = defaultdict(dict)
    for summary in summaries:
        config = (
            summary["machine"],
            summary["ordering"],
            summary["block_size"],
            summary["think_iters"],
            summary["think_ns"],
            summary["updaters"],
        )
        by_config[config][summary["method"]] = summary

    lines = []
    current_group = None
    for config in sorted(by_config):
        machine, ordering, block, think_iters, think_ns, updaters = config
        group = (machine, ordering, block)
        if group != current_group:
            current_group = group
            lines.append("")
            lines.append(f"{machine} | ordering {ordering} | block {block}")
            lines.append(
                f"{'think':>6} {'~ns':>7} {'upd':>4} |"
                + "".join(f" {method:>11}" for method in METHODS)
                + " Mops/s |"
                + "".join(f" {method + '/faa':>15}" for method in METHODS[1:])
                + " | same-updater fraction (max share / fair share)"
            )
        methods = by_config[config]
        throughput = "".join(
            f" {methods[m]['median_mops']:>11.2f}" if m in methods else f" {'-':>11}"
            for m in METHODS
        )
        ratios = "".join(
            f" {methods[m]['ratio_to_faa']:>15.2f}"
            if m in methods and methods[m]["ratio_to_faa"] is not None
            else f" {'-':>15}"
            for m in METHODS[1:]
        )
        fairness = "  ".join(
            f"{m}={methods[m]['same_updater_fraction']:.2f}"
            f"({methods[m]['max_updater_share']:.2f}/{methods[m]['fair_share']:.2f})"
            for m in METHODS
            if m in methods
        )
        lines.append(
            f"{think_iters:>6} {think_ns:>7.0f} {updaters:>4} |{throughput}        |{ratios} | {fairness}"
        )
    return "\n".join(lines).lstrip("\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("files", nargs="+", help="bench_atomic_updates JSON results")
    parser.add_argument("--layout", default="u64", help="Counter layout (default: u64)")
    parser.add_argument(
        "--block-size", type=int, help="Only this block size (default: all present)"
    )
    parser.add_argument("--csv", help="Also write the summary rows to this CSV path")
    args = parser.parse_args()

    rows = []
    for path in args.files:
        rows.extend(load_rows(path))
    summaries = summarize(rows, layout=args.layout, block_size=args.block_size)
    if not summaries:
        print("No schema-v4 atomic update samples matched.", file=sys.stderr)
        return 1
    print(format_table(summaries))
    if args.csv:
        write_csv(args.csv, summaries)
        print(f"\nWrote CSV: {args.csv}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
