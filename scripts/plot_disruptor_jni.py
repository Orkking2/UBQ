#!/usr/bin/env python3
"""Plot LMAX Disruptor JNI benchmark summaries."""

from __future__ import annotations

import argparse
import csv
import os
import re
import tempfile
from dataclasses import dataclass
from pathlib import Path

if "MPLCONFIGDIR" not in os.environ:
    mpl_config_dir = Path(tempfile.gettempdir()) / "ubq-matplotlib"
    mpl_config_dir.mkdir(parents=True, exist_ok=True)
    os.environ["MPLCONFIGDIR"] = str(mpl_config_dir)

if "XDG_CACHE_HOME" not in os.environ:
    xdg_cache_home = Path(tempfile.gettempdir()) / "ubq-cache"
    xdg_cache_home.mkdir(parents=True, exist_ok=True)
    os.environ["XDG_CACHE_HOME"] = str(xdg_cache_home)

import matplotlib.pyplot as plt


SCENARIO_RE = re.compile(r"^(?P<producers>\d+)p(?P<consumers>\d+)c$")
DEFAULT_QUEUE_ORDER = ["disruptor", "ubq", "rbbq"]
QUEUE_LABELS = {
    "disruptor": "Disruptor",
    "ubq": "UBQ",
    "rbbq": "RBBQ",
}
QUEUE_COLORS = {
    "disruptor": "#4c78a8",
    "ubq": "#f58518",
    "rbbq": "#54a24b",
}


@dataclass(frozen=True)
class SummaryRow:
    queue: str
    scenario: str
    samples: int
    min_ops_per_sec: float
    median_ops_per_sec: float
    mean_ops_per_sec: float
    max_ops_per_sec: float

    def metric(self, name: str) -> float:
        if name == "min":
            return self.min_ops_per_sec
        if name == "median":
            return self.median_ops_per_sec
        if name == "mean":
            return self.mean_ops_per_sec
        if name == "max":
            return self.max_ops_per_sec
        raise ValueError(f"unknown metric: {name}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Plot summary.csv from scripts/run_disruptor_jni_bench.sh."
    )
    parser.add_argument(
        "summary_csv",
        type=Path,
        help="Path to a Disruptor JNI summary.csv file.",
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        help="Output directory. Defaults to the summary CSV's directory.",
    )
    parser.add_argument(
        "--metric",
        choices=["median", "mean", "min", "max"],
        default="median",
        help="Summary metric to plot. Defaults to median.",
    )
    parser.add_argument(
        "--formats",
        default="svg,png",
        help="Comma-separated matplotlib output formats. Defaults to svg,png.",
    )
    parser.add_argument(
        "--baseline",
        default="disruptor",
        help="Queue used as the speedup baseline. Defaults to disruptor.",
    )
    parser.add_argument(
        "--no-speedup",
        action="store_true",
        help="Only write the throughput plot.",
    )
    return parser.parse_args()


def read_summary(path: Path) -> list[SummaryRow]:
    rows: list[SummaryRow] = []
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            rows.append(
                SummaryRow(
                    queue=row["queue"],
                    scenario=row["scenario"],
                    samples=int(row["samples"]),
                    min_ops_per_sec=float(row["min_ops_per_sec"]),
                    median_ops_per_sec=float(row["median_ops_per_sec"]),
                    mean_ops_per_sec=float(row["mean_ops_per_sec"]),
                    max_ops_per_sec=float(row["max_ops_per_sec"]),
                )
            )
    if not rows:
        raise SystemExit(f"no summary rows found in {path}")
    return rows


def scenario_key(scenario: str) -> tuple[int, int, str]:
    match = SCENARIO_RE.match(scenario)
    if match is None:
        return (10_000, 10_000, scenario)
    producers = int(match.group("producers"))
    consumers = int(match.group("consumers"))
    standard_order = {
        (1, 1): 0,
        (3, 1): 1,
        (1, 3): 2,
    }
    return (standard_order.get((producers, consumers), 100), producers, consumers)


def queue_order(queues: set[str]) -> list[str]:
    ordered = [queue for queue in ["disruptor"] if queue in queues]
    ordered.extend(sorted((queue for queue in queues if queue.startswith("ubq_")), key=ubq_queue_key))
    ordered.extend([queue for queue in ["ubq", "rbbq"] if queue in queues and queue not in ordered])
    ordered.extend(sorted(queues - set(ordered)))
    return ordered


def label_for_queue(queue: str) -> str:
    if queue.startswith("ubq_"):
        return compact_ubq_label(queue)
    return QUEUE_LABELS.get(queue, queue)


def ubq_queue_key(queue: str) -> tuple[int, int, int, int, str]:
    parsed = parse_ubq_queue(queue)
    if parsed is None:
        return (1, 0, 0, 0, queue)
    pool, block, backoff = parsed
    backoff_order = {"crossbeam": 0, "yield": 1}.get(backoff, 99)
    return (0, pool, block, backoff_order, queue)


def parse_ubq_queue(queue: str) -> tuple[int, int, str] | None:
    if not queue.startswith("ubq_"):
        return None
    parts = queue[len("ubq_") :].split(",")
    if len(parts) != 4:
        return None
    try:
        pool = int(parts[1])
        block = int(parts[2])
    except ValueError:
        return None
    return (pool, block, parts[3])


def compact_ubq_label(queue: str) -> str:
    parsed = parse_ubq_queue(queue)
    if parsed is None:
        return queue
    pool, block, backoff = parsed
    backoff_label = {"crossbeam": "cb", "yield": "yield"}.get(backoff, backoff)
    return f"UBQ p{pool} b{block} {backoff_label}"


def color_for_queue(queue: str, index: int) -> str:
    if queue in QUEUE_COLORS:
        return QUEUE_COLORS[queue]
    fallback = plt.rcParams["axes.prop_cycle"].by_key()["color"]
    return fallback[index % len(fallback)]


def save_formats(fig: plt.Figure, out_dir: Path, stem: str, formats: list[str]) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    for fmt in formats:
        path = out_dir / f"{stem}.{fmt}"
        fig.savefig(path, bbox_inches="tight", dpi=180)
        print(path)


def plot_throughput(
    rows: list[SummaryRow],
    out_dir: Path,
    metric: str,
    formats: list[str],
) -> None:
    scenarios = sorted({row.scenario for row in rows}, key=scenario_key)
    queues = queue_order({row.queue for row in rows})
    by_key = {(row.queue, row.scenario): row for row in rows}

    fig_width = max(7.5, 1.6 * len(scenarios) + 2.0)
    fig, ax = plt.subplots(figsize=(fig_width, 4.8))
    group_width = 0.78
    bar_width = group_width / max(len(queues), 1)
    x_positions = list(range(len(scenarios)))

    for queue_index, queue in enumerate(queues):
        offset = (queue_index - (len(queues) - 1) / 2.0) * bar_width
        xs: list[float] = []
        values: list[float] = []
        lower_errors: list[float] = []
        upper_errors: list[float] = []
        for scenario_index, scenario in enumerate(scenarios):
            row = by_key.get((queue, scenario))
            if row is None:
                continue
            value = row.metric(metric) / 1_000_000.0
            xs.append(x_positions[scenario_index] + offset)
            values.append(value)
            lower_errors.append(max(0.0, value - row.min_ops_per_sec / 1_000_000.0))
            upper_errors.append(max(0.0, row.max_ops_per_sec / 1_000_000.0 - value))

        bars = ax.bar(
            xs,
            values,
            width=bar_width * 0.88,
            label=label_for_queue(queue),
            color=color_for_queue(queue, queue_index),
            edgecolor="#222222",
            linewidth=0.5,
            yerr=[lower_errors, upper_errors],
            capsize=3,
            error_kw={"elinewidth": 0.8, "capthick": 0.8, "alpha": 0.75},
        )
        ax.bar_label(bars, labels=[f"{value:.1f}" for value in values], padding=2, fontsize=8)

    ax.set_title(f"LMAX Disruptor JNI Throughput ({metric})")
    ax.set_ylabel("Million operations / second")
    ax.set_xticks(x_positions)
    ax.set_xticklabels(scenarios)
    ax.grid(axis="y", color="#d8d8d8", linewidth=0.8, alpha=0.8)
    ax.set_axisbelow(True)
    ax.legend(ncols=min(len(queues), 3), frameon=False, loc="upper left")
    ax.margins(y=0.12)
    ax.text(
        0.0,
        -0.18,
        "Whiskers show min/max across benchmark repetitions.",
        transform=ax.transAxes,
        fontsize=8,
        color="#555555",
    )
    fig.tight_layout()
    save_formats(fig, out_dir, f"disruptor_jni_{metric}_throughput", formats)
    plt.close(fig)


def plot_speedup(
    rows: list[SummaryRow],
    out_dir: Path,
    metric: str,
    baseline: str,
    formats: list[str],
) -> None:
    scenarios = sorted({row.scenario for row in rows}, key=scenario_key)
    queues = [queue for queue in queue_order({row.queue for row in rows}) if queue != baseline]
    by_key = {(row.queue, row.scenario): row for row in rows}
    if not queues or not any((baseline, scenario) in by_key for scenario in scenarios):
        return

    fig_width = max(7.5, 1.45 * len(scenarios) + 2.0)
    fig, ax = plt.subplots(figsize=(fig_width, 4.5))
    group_width = 0.72
    bar_width = group_width / max(len(queues), 1)
    x_positions = list(range(len(scenarios)))

    for queue_index, queue in enumerate(queues):
        offset = (queue_index - (len(queues) - 1) / 2.0) * bar_width
        xs: list[float] = []
        values: list[float] = []
        for scenario_index, scenario in enumerate(scenarios):
            row = by_key.get((queue, scenario))
            baseline_row = by_key.get((baseline, scenario))
            if row is None or baseline_row is None:
                continue
            baseline_value = baseline_row.metric(metric)
            if baseline_value <= 0:
                continue
            xs.append(x_positions[scenario_index] + offset)
            values.append(row.metric(metric) / baseline_value)

        bars = ax.bar(
            xs,
            values,
            width=bar_width * 0.88,
            label=label_for_queue(queue),
            color=color_for_queue(queue, queue_index + 1),
            edgecolor="#222222",
            linewidth=0.5,
        )
        ax.bar_label(bars, labels=[f"{value:.2f}x" for value in values], padding=2, fontsize=8)

    ax.axhline(1.0, color="#333333", linewidth=1.0)
    ax.set_title(f"Speedup vs {label_for_queue(baseline)} ({metric})")
    ax.set_ylabel("Speedup")
    ax.set_xticks(x_positions)
    ax.set_xticklabels(scenarios)
    ax.grid(axis="y", color="#d8d8d8", linewidth=0.8, alpha=0.8)
    ax.set_axisbelow(True)
    ax.legend(ncols=min(len(queues), 3), frameon=False, loc="upper left")
    ax.margins(y=0.16)
    ax.text(
        0.0,
        -0.18,
        "The 1p3c Disruptor result is multicast, while UBQ/RBBQ use replicated queue work.",
        transform=ax.transAxes,
        fontsize=8,
        color="#555555",
    )
    fig.tight_layout()
    save_formats(fig, out_dir, f"disruptor_jni_{metric}_speedup_vs_{baseline}", formats)
    plt.close(fig)


def main() -> None:
    args = parse_args()
    rows = read_summary(args.summary_csv)
    out_dir = args.out_dir or args.summary_csv.parent
    formats = [fmt.strip() for fmt in args.formats.split(",") if fmt.strip()]
    if not formats:
        raise SystemExit("at least one output format is required")

    plot_throughput(rows, out_dir, args.metric, formats)
    if not args.no_speedup:
        plot_speedup(rows, out_dir, args.metric, args.baseline, formats)


if __name__ == "__main__":
    main()
