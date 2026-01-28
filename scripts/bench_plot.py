#!/usr/bin/env python3
"""Plot benchmark CSV output from ubq_perf benches."""

import argparse
import csv
import statistics
import sys


def parse_filters(filters):
    parsed = {}
    for item in filters:
        if "=" not in item:
            raise ValueError(f"Filter must be KEY=VALUE, got: {item}")
        key, value = item.split("=", 1)
        parsed[key] = value
    return parsed


def load_rows(path):
    with open(path, newline="") as handle:
        reader = csv.DictReader(handle)
        rows = list(reader)
        fieldnames = reader.fieldnames or []
    return rows, fieldnames


def filter_rows(rows, filters):
    if not filters:
        return rows
    filtered = []
    for row in rows:
        keep = True
        for key, value in filters.items():
            if row.get(key) != value:
                keep = False
                break
        if keep:
            filtered.append(row)
    return filtered


def pick_metric(fieldnames, metric):
    if metric:
        return metric
    if "throughput_msgs_per_sec" in fieldnames:
        return "throughput_msgs_per_sec"
    return "time_estimate_ns"


def metric_label(metric):
    if metric == "throughput_msgs_per_sec":
        return "Throughput (msgs/sec)"
    if metric.endswith("_ns"):
        return "Time (ns)"
    return metric.replace("_", " ").title()


def group_metric(rows, by, metric):
    buckets = {}
    for row in rows:
        key = row.get(by, "") or "<missing>"
        raw_value = row.get(metric, "")
        try:
            value = float(raw_value)
        except (TypeError, ValueError):
            continue
        buckets.setdefault(key, []).append(value)
    return {key: statistics.mean(values) for key, values in buckets.items() if values}


def sort_items(items, sort_mode):
    if sort_mode == "label":
        return sorted(items, key=lambda item: item[0])
    if sort_mode == "value_asc":
        return sorted(items, key=lambda item: item[1])
    return sorted(items, key=lambda item: item[1], reverse=True)


def main():
    parser = argparse.ArgumentParser(
        description="Plot ubq_perf benchmark CSV data as a bar chart."
    )
    parser.add_argument("--csv", default="target/bench.csv", help="Input CSV path")
    parser.add_argument("--out", default="target/bench.png", help="Output image path")
    parser.add_argument("--metric", default=None, help="Metric column to plot")
    parser.add_argument("--by", default="bench_id", help="Column to group by")
    parser.add_argument(
        "--filter",
        action="append",
        default=[],
        help="Filter rows by exact match (KEY=VALUE), can be repeated",
    )
    parser.add_argument(
        "--sort",
        choices=["value_desc", "value_asc", "label"],
        default="value_desc",
        help="Sort order for categories",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Limit to the top N categories after sorting (0 for all)",
    )
    parser.add_argument("--title", default=None, help="Optional chart title")
    args = parser.parse_args()

    try:
        rows, fieldnames = load_rows(args.csv)
    except FileNotFoundError:
        print(f"CSV not found: {args.csv}", file=sys.stderr)
        return 1

    if not rows:
        print(f"No rows in CSV: {args.csv}", file=sys.stderr)
        return 1

    metric = pick_metric(fieldnames, args.metric)
    try:
        filters = parse_filters(args.filter)
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 1

    rows = filter_rows(rows, filters)
    if not rows:
        print("No rows match filters.", file=sys.stderr)
        return 1

    grouped = group_metric(rows, args.by, metric)
    if not grouped:
        print(f"No numeric data for metric '{metric}'.", file=sys.stderr)
        return 1

    items = sort_items(list(grouped.items()), args.sort)
    if args.limit and args.limit > 0:
        items = items[: args.limit]

    labels = [item[0] for item in items]
    values = [item[1] for item in items]

    try:
        import matplotlib.pyplot as plt
    except ImportError:
        print("matplotlib is required (pip install matplotlib)", file=sys.stderr)
        return 1

    width = max(6.0, len(labels) * 0.6)
    fig, ax = plt.subplots(figsize=(width, 4.5))
    ax.bar(range(len(labels)), values, color="#4C72B0")
    ax.set_ylabel(metric_label(metric))
    ax.set_xlabel(args.by)
    ax.set_title(args.title or f"{metric_label(metric)} by {args.by}")
    ax.grid(axis="y", linestyle="--", alpha=0.4)
    ax.set_xticks(range(len(labels)))
    ax.set_xticklabels(labels, rotation=45, ha="right")
    fig.tight_layout()
    fig.savefig(args.out, dpi=150)

    print(f"Wrote {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
