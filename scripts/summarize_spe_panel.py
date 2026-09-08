#!/usr/bin/env python3
"""Reduce ``spe.csv`` to the per-cell tables and figure used in an analysis note.

Reads the output of ``aggregate_spe_samples.py`` and writes, into an analysis
directory: a per-cell summary of user-space load behaviour, a replication table
giving the spread of each statistic across the campaign's repeats, and a figure.

Every rate here is a rate *within the user address space*, because the kernel
sample count of a cell does not replicate across repeats while the user count
does; see the header of ``aggregate_spe_samples.py``.
"""

from __future__ import annotations

import argparse
import csv
import statistics
from collections import defaultdict
from pathlib import Path


# Statistics carried into the summary, as (csv column, output name).
METRICS = (
    ("spe_memory_samples_user", "user_load_samples"),
    ("rate_l1d_miss_user", "l1d_miss_rate"),
    ("rate_llc_access_user", "llc_access_rate"),
    ("rate_llc_miss_user", "llc_miss_rate"),
    ("rate_tlb_miss_user", "tlb_miss_rate"),
    ("rate_remote_access_user", "remote_access_rate"),
    ("kernel_sample_fraction", "kernel_sample_fraction"),
)

SHAPE_ORDER = ("1p1c", "16p1c", "1p32c", "72p72c")


def cell_label(row):
    batch = row["batch_size"]
    suffix = "-b" + batch if batch not in ("1", "") else "-scalar"
    return row["queue"] + suffix


def load(spe_csv, kind_prefix="deep-core-"):
    with open(spe_csv) as handle:
        rows = list(csv.DictReader(handle))
    return [r for r in rows
            if r["case"].startswith(kind_prefix) and r["run_status"] == "completed"]


def number(row, column):
    value = row.get(column, "")
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def summarize(rows):
    groups = defaultdict(list)
    for row in rows:
        groups[(cell_label(row), row["scenario"])].append(row)

    summary, replication = [], []
    for (queue, shape), members in groups.items():
        record = {"queue": queue, "scenario": shape, "repeats": len(members)}
        spread = {"queue": queue, "scenario": shape, "repeats": len(members)}
        for column, name in METRICS:
            values = [v for v in (number(m, column) for m in members) if v is not None]
            if not values:
                record[name] = spread[name + "_spread"] = ""
                continue
            median = statistics.median(values)
            record[name] = median
            # Spread is the full range over the median: a blunt, assumption-free
            # statement of how much three repeats of one cell actually moved.
            spread[name + "_spread"] = (max(values) - min(values)) / median if median else ""
        summary.append(record)
        replication.append(spread)

    key = lambda r: (SHAPE_ORDER.index(r["scenario"]) if r["scenario"] in SHAPE_ORDER else 99,
                     r["queue"])
    return sorted(summary, key=key), sorted(replication, key=key)


def write_csv(path, rows):
    if not rows:
        return
    columns = list(rows[0])
    with open(path, "w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)


def figure(summary, destination):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    import plot_style

    shapes = [s for s in SHAPE_ORDER if any(r["scenario"] == s for r in summary)]
    queues = sorted({r["queue"] for r in summary})
    index = {(r["queue"], r["scenario"]): r for r in summary}

    fig, axes = plt.subplots(1, len(shapes), figsize=(4.1 * len(shapes), 5.4), sharey=True)
    if len(shapes) == 1:
        axes = [axes]

    # One queue order for every panel, so a row means the same queue throughout
    # and the eye can follow a queue across thread shapes. Ordering by the most
    # contended shape puts the worst offenders at the top.
    def rank(queue):
        for shape in reversed(shapes):
            record = index.get((queue, shape))
            if record and record["l1d_miss_rate"] != "":
                return -record["l1d_miss_rate"]
        return 0.0

    queues = sorted(queues, key=rank)
    positions = list(range(len(queues)))
    colors = [plot_style.family_color(q.rsplit("-", 1)[0], i) for i, q in enumerate(queues)]
    hatches = [plot_style.family_hatch(q.rsplit("-", 1)[0], i) for i, q in enumerate(queues)]

    for axis, shape in zip(axes, shapes):
        values, drawn = [], []
        for position, queue in zip(positions, queues):
            record = index.get((queue, shape))
            value = record["l1d_miss_rate"] * 100 if record and record["l1d_miss_rate"] != "" else None
            values.append(value)
            drawn.append(position)
        for position, value, color, hatch in zip(drawn, values, colors, hatches):
            if value is None:
                # A cell with no capture is left explicitly empty rather than zero.
                axis.text(0, position, " no data", va="center", fontsize=7,
                          style="italic", color="#666666")
                continue
            bar = axis.barh(position, value, color=color, edgecolor="black", linewidth=0.6)
            bar[0].set_hatch(hatch)
            axis.text(value, position, " {:.2f}".format(value), va="center", fontsize=7)
        axis.set_yticks(positions)
        axis.set_yticklabels(queues, fontsize=8)
        axis.invert_yaxis()
        axis.set_title(shape, fontsize=11)
        axis.set_xlabel("L1D miss rate, % of sampled user loads", fontsize=9)
        axis.grid(axis="x", alpha=0.3, linewidth=0.5)
        axis.set_axisbelow(True)
        axis.margins(x=0.16)

    fig.suptitle("Arm SPE: user-space load behaviour by queue and thread shape\n"
                 "Grace, weekend-hw-v6-grace; median of 3 repeats; "
                 "perf mem record -t load, period 100003",
                 fontsize=11)
    fig.tight_layout(rect=(0, 0, 1, 0.93))
    fig.savefig(destination.with_suffix(".png"), dpi=200)
    fig.savefig(destination.with_suffix(".svg"))
    plt.close(fig)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("spe_csv", type=Path)
    parser.add_argument("output_dir", type=Path)
    parser.add_argument("--no-figure", action="store_true")
    args = parser.parse_args(argv)

    args.output_dir.mkdir(parents=True, exist_ok=True)
    rows = load(args.spe_csv)
    if not rows:
        raise SystemExit("no completed deep-core SPE rows in " + str(args.spe_csv))
    summary, replication = summarize(rows)

    write_csv(args.output_dir / "spe_core_summary.csv", summary)
    write_csv(args.output_dir / "spe_replication.csv", replication)
    if not args.no_figure:
        figure(summary, args.output_dir / "spe_user_load_behaviour")

    print(args.output_dir / "spe_core_summary.csv")
    print(args.output_dir / "spe_replication.csv")
    print("{} cells from {} completed captures".format(len(summary), len(rows)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
