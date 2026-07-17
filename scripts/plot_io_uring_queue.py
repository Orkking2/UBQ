#!/usr/bin/env python3
"""Plot io_uring SQ/CQ queue replacement benchmark summaries."""

from __future__ import annotations

import argparse
import csv
import os
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


QUEUE_ORDER = ["io_uring", "bbq", "ubq"]
QUEUE_COLORS = {
    "io_uring": "#4c78a8",
    "bbq": "#54a24b",
    "ubq": "#f58518",
}
BATCH_LABELS = {
    "fixed1": "batch 1",
    "random1to32": "random 1-32",
}


@dataclass(frozen=True)
class SummaryRow:
    queue: str
    sq_size: int
    cq_size: int
    batch_mode: str
    requests: int
    repeats: int
    submit_ns_per_request_median: float
    total_ns_per_request_median: float
    submit_requests_per_sec_median: float
    total_requests_per_sec_median: float
    submit_speedup_vs_io_uring: float | None
    submit_throughput_speedup_vs_io_uring: float | None

    def latency(self, metric: str) -> float:
        if metric == "submit":
            return self.submit_ns_per_request_median
        if metric == "total":
            return self.total_ns_per_request_median
        raise ValueError(f"unknown metric: {metric}")

    def throughput(self, metric: str) -> float:
        if metric == "submit":
            return self.submit_requests_per_sec_median
        if metric == "total":
            return self.total_requests_per_sec_median
        raise ValueError(f"unknown metric: {metric}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Plot summary.csv from scripts/run_io_uring_queue_bench.sh."
    )
    parser.add_argument(
        "summary_csv",
        type=Path,
        help="Path to an io_uring queue summary.csv file.",
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        help="Output directory. Defaults to the summary CSV's directory.",
    )
    parser.add_argument(
        "--metric",
        choices=["submit", "total"],
        default="submit",
        help="Latency metric to plot. Defaults to submit.",
    )
    parser.add_argument(
        "--formats",
        default="svg,png",
        help="Comma-separated matplotlib output formats. Defaults to svg,png.",
    )
    parser.add_argument(
        "--log-y",
        action="store_true",
        help="Use a logarithmic y-axis for latency.",
    )
    parser.add_argument(
        "--no-speedup",
        action="store_true",
        help="Do not write the speedup plot.",
    )
    parser.add_argument(
        "--no-throughput",
        action="store_true",
        help="Do not write the throughput plot.",
    )
    return parser.parse_args()


def read_summary(path: Path) -> list[SummaryRow]:
    rows: list[SummaryRow] = []
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        required = {
            "queue",
            "sq_size",
            "cq_size",
            "batch_mode",
            "requests",
            "repeats",
            "submit_ns_per_request_median",
            "total_ns_per_request_median",
            "submit_speedup_vs_io_uring",
        }
        missing = required - set(reader.fieldnames or [])
        if missing:
            missing_list = ", ".join(sorted(missing))
            raise SystemExit(f"{path} is missing expected column(s): {missing_list}")
        for row in reader:
            raw_speedup = row["submit_speedup_vs_io_uring"].strip()
            submit_latency = float(row["submit_ns_per_request_median"])
            total_latency = float(row["total_ns_per_request_median"])
            submit_throughput = optional_float(
                row, "submit_requests_per_sec_median"
            )
            total_throughput = optional_float(row, "total_requests_per_sec_median")
            raw_throughput_speedup = row.get(
                "submit_throughput_speedup_vs_io_uring", ""
            ).strip()
            rows.append(
                SummaryRow(
                    queue=row["queue"],
                    sq_size=int(row["sq_size"]),
                    cq_size=int(row["cq_size"]),
                    batch_mode=row["batch_mode"],
                    requests=int(row["requests"]),
                    repeats=int(row["repeats"]),
                    submit_ns_per_request_median=submit_latency,
                    total_ns_per_request_median=total_latency,
                    submit_requests_per_sec_median=(
                        submit_throughput
                        if submit_throughput is not None
                        else ns_per_request_to_requests_per_sec(submit_latency)
                    ),
                    total_requests_per_sec_median=(
                        total_throughput
                        if total_throughput is not None
                        else ns_per_request_to_requests_per_sec(total_latency)
                    ),
                    submit_speedup_vs_io_uring=(
                        float(raw_speedup) if raw_speedup else None
                    ),
                    submit_throughput_speedup_vs_io_uring=(
                        float(raw_throughput_speedup)
                        if raw_throughput_speedup
                        else None
                    ),
                )
            )
    if not rows:
        raise SystemExit(f"no summary rows found in {path}")
    return rows


def optional_float(row: dict[str, str], key: str) -> float | None:
    raw = row.get(key, "").strip()
    return float(raw) if raw else None


def ns_per_request_to_requests_per_sec(ns_per_request: float) -> float:
    return 1_000_000_000.0 / ns_per_request


def queue_family(queue: str) -> str:
    if queue.startswith("bbq_fastfifo"):
        return "bbq"
    if queue.startswith("ubq_"):
        return "ubq"
    return queue


def queue_sort_key(queue: str) -> tuple[int, str]:
    family = queue_family(queue)
    try:
        index = QUEUE_ORDER.index(family)
    except ValueError:
        index = len(QUEUE_ORDER)
    return index, queue


def label_for_queue(queue: str) -> str:
    if queue == "io_uring":
        return "io_uring"
    if queue.startswith("bbq_fastfifo_"):
        return f"BBQ block {queue.removeprefix('bbq_fastfifo_')}"
    if queue.startswith("ubq_"):
        parts = queue.removeprefix("ubq_").split(",")
        if len(parts) == 4:
            _, pool, block, backoff = parts
            backoff_label = {"crossbeam": "cb", "yield": "yield"}.get(backoff, backoff)
            return f"UBQ p{pool} b{block} {backoff_label}"
    return queue


def color_for_queue(queue: str, index: int) -> str:
    family = queue_family(queue)
    if family in QUEUE_COLORS:
        return QUEUE_COLORS[family]
    fallback = plt.rcParams["axes.prop_cycle"].by_key()["color"]
    return fallback[index % len(fallback)]


def batch_sort_key(batch_mode: str) -> tuple[int, str]:
    order = {"fixed1": 0, "random1to32": 1}
    return order.get(batch_mode, len(order)), batch_mode


def group_label(batch_mode: str, sq_size: int, cq_size: int) -> str:
    batch = BATCH_LABELS.get(batch_mode, batch_mode)
    return f"{batch}\nSQ {sq_size}, CQ {cq_size}"


def group_sort_key(group: tuple[str, int, int]) -> tuple[tuple[int, str], int, int]:
    batch_mode, sq_size, cq_size = group
    return batch_sort_key(batch_mode), sq_size, cq_size


def save_formats(fig: plt.Figure, out_dir: Path, stem: str, formats: list[str]) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    for fmt in formats:
        path = out_dir / f"{stem}.{fmt}"
        fig.savefig(path, bbox_inches="tight", dpi=180)
        print(path)


def plot_latency(
    rows: list[SummaryRow],
    out_dir: Path,
    metric: str,
    formats: list[str],
    log_y: bool,
) -> None:
    groups = sorted(
        {(row.batch_mode, row.sq_size, row.cq_size) for row in rows},
        key=group_sort_key,
    )
    queues = sorted({row.queue for row in rows}, key=queue_sort_key)
    by_key = {
        (row.queue, row.batch_mode, row.sq_size, row.cq_size): row
        for row in rows
    }

    fig_width = max(8.0, 1.55 * len(groups) + 2.2)
    fig, ax = plt.subplots(figsize=(fig_width, 4.9))
    group_width = 0.78
    bar_width = group_width / max(len(queues), 1)
    x_positions = list(range(len(groups)))

    for queue_index, queue in enumerate(queues):
        offset = (queue_index - (len(queues) - 1) / 2.0) * bar_width
        xs: list[float] = []
        values: list[float] = []
        for group_index, (batch_mode, sq_size, cq_size) in enumerate(groups):
            row = by_key.get((queue, batch_mode, sq_size, cq_size))
            if row is None:
                continue
            xs.append(x_positions[group_index] + offset)
            values.append(row.latency(metric))
        bars = ax.bar(
            xs,
            values,
            width=bar_width * 0.88,
            label=label_for_queue(queue),
            color=color_for_queue(queue, queue_index),
            edgecolor="#222222",
            linewidth=0.5,
        )
        ax.bar_label(bars, labels=[f"{value:.1f}" for value in values], padding=2, fontsize=8)

    metric_label = "submit" if metric == "submit" else "end-to-end"
    ax.set_title(f"io_uring SQ/CQ Queue Replacement: {metric_label} latency")
    ax.set_ylabel("Median ns / request")
    ax.set_xticks(x_positions)
    ax.set_xticklabels([group_label(*group) for group in groups])
    ax.grid(axis="y", color="#d8d8d8", linewidth=0.8, alpha=0.8)
    ax.set_axisbelow(True)
    if log_y:
        ax.set_yscale("log")
    ax.legend(ncols=min(len(queues), 3), frameon=False, loc="upper left")
    ax.margins(y=0.16)
    fig.tight_layout()
    save_formats(fig, out_dir, f"io_uring_queue_{metric}_latency", formats)
    plt.close(fig)


def plot_throughput(
    rows: list[SummaryRow],
    out_dir: Path,
    metric: str,
    formats: list[str],
) -> None:
    groups = sorted(
        {(row.batch_mode, row.sq_size, row.cq_size) for row in rows},
        key=group_sort_key,
    )
    queues = sorted({row.queue for row in rows}, key=queue_sort_key)
    by_key = {
        (row.queue, row.batch_mode, row.sq_size, row.cq_size): row for row in rows
    }

    fig_width = max(8.0, 1.55 * len(groups) + 2.2)
    fig, ax = plt.subplots(figsize=(fig_width, 4.9))
    group_width = 0.78
    bar_width = group_width / max(len(queues), 1)
    x_positions = list(range(len(groups)))

    for queue_index, queue in enumerate(queues):
        offset = (queue_index - (len(queues) - 1) / 2.0) * bar_width
        xs: list[float] = []
        values: list[float] = []
        for group_index, (batch_mode, sq_size, cq_size) in enumerate(groups):
            row = by_key.get((queue, batch_mode, sq_size, cq_size))
            if row is None:
                continue
            xs.append(x_positions[group_index] + offset)
            values.append(row.throughput(metric) / 1_000_000.0)
        bars = ax.bar(
            xs,
            values,
            width=bar_width * 0.88,
            label=label_for_queue(queue),
            color=color_for_queue(queue, queue_index),
            edgecolor="#222222",
            linewidth=0.5,
        )
        ax.bar_label(bars, labels=[f"{value:.1f}" for value in values], padding=2, fontsize=8)

    metric_label = "submit" if metric == "submit" else "end-to-end"
    ax.set_title(f"io_uring SQ/CQ Queue Replacement: {metric_label} throughput")
    ax.set_ylabel("Million requests / second")
    ax.set_xticks(x_positions)
    ax.set_xticklabels([group_label(*group) for group in groups])
    ax.grid(axis="y", color="#d8d8d8", linewidth=0.8, alpha=0.8)
    ax.set_axisbelow(True)
    ax.legend(ncols=min(len(queues), 3), frameon=False, loc="upper left")
    ax.margins(y=0.16)
    fig.tight_layout()
    save_formats(fig, out_dir, f"io_uring_queue_{metric}_throughput", formats)
    plt.close(fig)


def plot_speedup(rows: list[SummaryRow], out_dir: Path, formats: list[str]) -> None:
    speedup_rows = [
        row
        for row in rows
        if row.queue != "io_uring" and row.submit_speedup_vs_io_uring is not None
    ]
    if not speedup_rows:
        return

    groups = sorted(
        {(row.batch_mode, row.sq_size, row.cq_size) for row in speedup_rows},
        key=group_sort_key,
    )
    queues = sorted({row.queue for row in speedup_rows}, key=queue_sort_key)
    by_key = {
        (row.queue, row.batch_mode, row.sq_size, row.cq_size): row
        for row in speedup_rows
    }

    fig_width = max(8.0, 1.55 * len(groups) + 2.0)
    fig, ax = plt.subplots(figsize=(fig_width, 4.6))
    group_width = 0.72
    bar_width = group_width / max(len(queues), 1)
    x_positions = list(range(len(groups)))

    for queue_index, queue in enumerate(queues):
        offset = (queue_index - (len(queues) - 1) / 2.0) * bar_width
        xs: list[float] = []
        values: list[float] = []
        for group_index, (batch_mode, sq_size, cq_size) in enumerate(groups):
            row = by_key.get((queue, batch_mode, sq_size, cq_size))
            if row is None or row.submit_speedup_vs_io_uring is None:
                continue
            xs.append(x_positions[group_index] + offset)
            values.append(row.submit_speedup_vs_io_uring)
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

    ax.axhline(1.0, color="#444444", linewidth=1.0, linestyle="--")
    ax.set_title("io_uring SQ/CQ Queue Replacement: submit speedup")
    ax.set_ylabel("Speedup vs io_uring")
    ax.set_xticks(x_positions)
    ax.set_xticklabels([group_label(*group) for group in groups])
    ax.grid(axis="y", color="#d8d8d8", linewidth=0.8, alpha=0.8)
    ax.set_axisbelow(True)
    ax.legend(ncols=min(len(queues), 3), frameon=False, loc="upper left")
    ax.margins(y=0.18)
    fig.tight_layout()
    save_formats(fig, out_dir, "io_uring_queue_submit_speedup", formats)
    plt.close(fig)


def main() -> None:
    args = parse_args()
    rows = read_summary(args.summary_csv)
    formats = [fmt.strip() for fmt in args.formats.split(",") if fmt.strip()]
    if not formats:
        raise SystemExit("at least one output format is required")
    out_dir = args.out_dir or args.summary_csv.parent

    plot_latency(rows, out_dir, args.metric, formats, args.log_y)
    if not args.no_throughput:
        plot_throughput(rows, out_dir, args.metric, formats)
    if not args.no_speedup:
        plot_speedup(rows, out_dir, formats)


if __name__ == "__main__":
    main()
