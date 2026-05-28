#!/usr/bin/env python3
"""Extract paper-oriented UBQ highlight tables and plots from plot_bench CSVs."""

from __future__ import annotations

import argparse
import csv
import math
import re
from dataclasses import dataclass
from pathlib import Path

import matplotlib.pyplot as plt


SCENARIO_RE = re.compile(r"^(\d+)p(\d+)c_")


@dataclass(frozen=True)
class MetricSpec:
    folder: str
    suffix: str
    column: str
    direction: str
    label: str


METRICS = [
    MetricSpec("throughput", "throughput", "ops_per_sec", "higher", "Throughput"),
    MetricSpec(
        "complex_throughput",
        "throughput",
        "ops_per_sec",
        "higher",
        "Complex throughput",
    ),
    MetricSpec(
        "data_latency",
        "data_latency",
        "avg_data_latency_ns",
        "lower",
        "Data latency",
    ),
    MetricSpec(
        "fairness_throughput",
        "throughput",
        "ops_per_sec",
        "higher",
        "Fairness throughput",
    ),
    MetricSpec(
        "consumer_fairness",
        "consumer_fairness",
        "fairness_ratio",
        "lower",
        "Consumer fairness",
    ),
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create UBQ publication subset CSVs and speedup plots."
    )
    parser.add_argument(
        "--csv-root",
        type=Path,
        required=True,
        help="Machine csv directory emitted by scripts/plot_runs_folder.py.",
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        required=True,
        help="Output directory for subset CSV, Markdown, and plots.",
    )
    parser.add_argument(
        "--formats",
        default="png,svg",
        help="Comma-separated matplotlib output formats.",
    )
    parser.add_argument(
        "--min-speedup",
        type=float,
        default=1.0,
        help="Minimum UBQ speedup for the highlights CSV/plot.",
    )
    return parser.parse_args()


def scenario_from_path(path: Path) -> str | None:
    match = SCENARIO_RE.match(path.name)
    if not match:
        return None
    return f"{int(match.group(1))}p{int(match.group(2))}c"


def better(left: float, right: float, direction: str) -> bool:
    if direction == "higher":
        return left > right
    if direction == "lower":
        return left < right
    raise ValueError(f"unknown direction: {direction}")


def speedup(ubq_value: float, competitor_value: float, direction: str) -> float:
    if ubq_value <= 0 or competitor_value <= 0:
        return float("nan")
    if direction == "higher":
        return ubq_value / competitor_value
    if direction == "lower":
        return competitor_value / ubq_value
    raise ValueError(f"unknown direction: {direction}")


def scenario_files(folder: Path, suffix: str) -> list[Path]:
    files = []
    for path in folder.glob(f"*_{suffix}.csv"):
        name = path.name
        if "immediate_variants" in name:
            continue
        if name.startswith(("scenarios_line_", "mpsc_line_", "spmc_line_")):
            continue
        if scenario_from_path(path) is None:
            continue
        files.append(path)
    return sorted(files)


def best_rows(path: Path, spec: MetricSpec) -> dict[str, object] | None:
    rows: list[tuple[str, float]] = []
    with path.open(newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            raw = row.get(spec.column)
            if raw in (None, ""):
                continue
            rows.append((row["queue"], float(raw)))
    ubq_rows = [row for row in rows if row[0].startswith("ubq_")]
    competitor_rows = [row for row in rows if not row[0].startswith("ubq_")]
    if not ubq_rows or not competitor_rows:
        return None

    key = (lambda row: row[1])
    best_ubq = max(ubq_rows, key=key) if spec.direction == "higher" else min(ubq_rows, key=key)
    best_competitor = (
        max(competitor_rows, key=key)
        if spec.direction == "higher"
        else min(competitor_rows, key=key)
    )
    scenario = scenario_from_path(path)
    value = speedup(best_ubq[1], best_competitor[1], spec.direction)
    return {
        "metric": spec.label,
        "metric_folder": spec.folder,
        "scenario": scenario,
        "direction": spec.direction,
        "best_ubq_queue": best_ubq[0],
        "best_ubq_value": best_ubq[1],
        "best_non_ubq_queue": best_competitor[0],
        "best_non_ubq_value": best_competitor[1],
        "speedup": value,
        "ubq_won": value >= 1.0 if math.isfinite(value) else False,
    }


def collect_rows(csv_root: Path) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for spec in METRICS:
        folder = csv_root / spec.folder
        if not folder.exists():
            continue
        for path in scenario_files(folder, spec.suffix):
            row = best_rows(path, spec)
            if row is not None:
                rows.append(row)
    return rows


def write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    fields = [
        "metric",
        "metric_folder",
        "scenario",
        "direction",
        "best_ubq_queue",
        "best_ubq_value",
        "best_non_ubq_queue",
        "best_non_ubq_value",
        "speedup",
        "ubq_won",
    ]
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def write_markdown(path: Path, rows: list[dict[str, object]], highlights: list[dict[str, object]]) -> None:
    wins = sum(1 for row in rows if row["ubq_won"])
    finite_speedups = [float(row["speedup"]) for row in rows if math.isfinite(float(row["speedup"]))]
    geo = math.exp(sum(math.log(value) for value in finite_speedups) / len(finite_speedups)) if finite_speedups else float("nan")
    with path.open("w") as f:
        f.write("# UBQ Publication Subset\n\n")
        f.write(
            f"UBQ wins {wins}/{len(rows)} measured metric/scenario comparisons; "
            f"geomean speedup is {geo:.2f}x across finite comparisons.\n\n"
        )
        f.write("## Highlight Rows\n\n")
        f.write("| Metric | Scenario | UBQ | Best non-UBQ | Speedup |\n")
        f.write("|---|---:|---|---|---:|\n")
        for row in highlights:
            f.write(
                f"| {row['metric']} | {row['scenario']} | {row['best_ubq_queue']} "
                f"({float(row['best_ubq_value']):.6g}) | {row['best_non_ubq_queue']} "
                f"({float(row['best_non_ubq_value']):.6g}) | {float(row['speedup']):.2f}x |\n"
            )


def plot_speedups(path_stem: Path, rows: list[dict[str, object]], formats: list[str]) -> None:
    if not rows:
        return
    rows = sorted(rows, key=lambda row: (str(row["metric"]), str(row["scenario"])))
    labels = [f"{row['metric']}\n{row['scenario']}" for row in rows]
    values = [float(row["speedup"]) for row in rows]
    colors = ["#0072B2" if value >= 1.0 else "#999999" for value in values]

    fig_height = max(3.5, 0.42 * len(rows) + 1.2)
    fig, ax = plt.subplots(figsize=(8.0, fig_height))
    y = list(range(len(rows)))
    ax.barh(y, values, color=colors, alpha=0.86)
    ax.axvline(1.0, color="#222222", linewidth=1.1, linestyle="--")
    ax.set_xscale("log", base=2)
    ax.set_yticks(y, labels)
    ax.invert_yaxis()
    ax.set_xlabel("Speedup over best non-UBQ")
    ax.set_title("UBQ publication subset")
    ax.grid(True, axis="x", color="#dddddd", linewidth=0.8)
    for y_pos, value in zip(y, values):
        ax.text(value * 1.03, y_pos, f"{value:.2f}x", va="center", fontsize=8)
    for fmt in formats:
        kwargs = {"bbox_inches": "tight"}
        if fmt.lower() == "png":
            kwargs["dpi"] = 300
        fig.savefig(path_stem.with_suffix(f".{fmt}"), **kwargs)
    plt.close(fig)


def main() -> None:
    args = parse_args()
    formats = [item.strip() for item in args.formats.split(",") if item.strip()]
    args.out_dir.mkdir(parents=True, exist_ok=True)

    rows = collect_rows(args.csv_root)
    highlights = [row for row in rows if float(row["speedup"]) >= args.min_speedup]
    highlights = sorted(highlights, key=lambda row: float(row["speedup"]), reverse=True)

    write_csv(args.out_dir / "ubq_advantage_summary.csv", rows)
    write_csv(args.out_dir / "ubq_publication_highlights.csv", highlights)
    write_markdown(args.out_dir / "ubq_publication_subset.md", rows, highlights)
    plot_speedups(args.out_dir / "ubq_publication_highlights", highlights, formats)


if __name__ == "__main__":
    main()
