#!/usr/bin/env python3
"""Generate paper-oriented UBQ advantage plots from benchmark CSV outputs."""

from __future__ import annotations

import argparse
import csv
import math
import re
import statistics
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path

import matplotlib.pyplot as plt
from matplotlib.colors import TwoSlopeNorm


SCENARIO_RE = re.compile(r"^(\d+)p(\d+)c_")
THREAD_LEVELS = [1, 2, 4, 8, 16, 32, 64]

FAMILY_ORDER = [
    "UBQ",
    "concurrent-queue",
    "crossbeam SegQueue",
    "RBBQ/BBQ",
    "LSCQ",
]

FAMILY_LABELS = {
    "UBQ": "UBQ",
    "concurrent-queue": "concurrent-queue",
    "crossbeam SegQueue": "SegQueue",
    "RBBQ/BBQ": "RBBQ/BBQ",
    "LSCQ": "LSCQ",
}

FAMILY_COLORS = {
    "UBQ": "#0072B2",
    "concurrent-queue": "#009E73",
    "crossbeam SegQueue": "#CC79A7",
    "RBBQ/BBQ": "#D55E00",
    "LSCQ": "#666666",
}


@dataclass(frozen=True)
class MetricConfig:
    suite: str
    file_suffix: str
    column: str
    direction: str
    label: str


@dataclass(frozen=True)
class AdvantageRow:
    metric: str
    scenario: str
    producers: int
    consumers: int
    ubq_queue: str
    ubq_value: float
    competitor_family: str
    competitor_queue: str
    competitor_value: float
    speedup: float


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate paper plots from grace_results-style CSV folders."
    )
    parser.add_argument(
        "--results-dir",
        type=Path,
        default=Path("grace_results"),
        help="Root containing benchmark CSV folders.",
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=Path("grace_results/paper_plots"),
        help="Directory for generated plot artifacts.",
    )
    parser.add_argument(
        "--formats",
        default="svg,png",
        help="Comma-separated output formats supported by matplotlib.",
    )
    return parser.parse_args()


def scenario_from_path(path: Path) -> tuple[str, int, int] | None:
    match = SCENARIO_RE.match(path.name)
    if not match:
        return None
    producers = int(match.group(1))
    consumers = int(match.group(2))
    return f"{producers}p{consumers}c", producers, consumers


def load_metadata(suite_dir: Path) -> dict[str, str]:
    metadata_path = suite_dir / "queue_metadata.csv"
    if not metadata_path.exists():
        return {}

    metadata: dict[str, str] = {}
    with metadata_path.open(newline="") as f:
        for row in csv.DictReader(f):
            metadata[row["queue"]] = row["family"]
    return metadata


def family_for(queue: str, metadata: dict[str, str]) -> str:
    if queue in metadata:
        return metadata[queue]
    if queue.startswith("ubq_"):
        return "UBQ"
    return queue


def better(left: float, right: float, direction: str) -> bool:
    if direction == "higher":
        return left > right
    if direction == "lower":
        return left < right
    raise ValueError(f"unknown direction: {direction}")


def ratio(ubq_value: float, competitor_value: float, direction: str) -> float:
    if direction == "higher":
        return ubq_value / competitor_value
    if direction == "lower":
        return competitor_value / ubq_value
    raise ValueError(f"unknown direction: {direction}")


def scenario_files(suite_dir: Path, file_suffix: str) -> list[Path]:
    files = []
    for path in suite_dir.glob(f"*_{file_suffix}.csv"):
        if "immediate_variants" in path.name:
            continue
        if path.name.startswith("scenarios_line_"):
            continue
        if path.name.startswith(("mpsc_line_", "spmc_line_")):
            continue
        if scenario_from_path(path) is None:
            continue
        files.append(path)
    return sorted(files)


def read_scenario_entries(
    suite_dir: Path,
    file_suffix: str,
    column: str,
) -> dict[str, list[tuple[str, str, float]]]:
    metadata = load_metadata(suite_dir)
    entries_by_scenario: dict[str, list[tuple[str, str, float]]] = {}
    for path in scenario_files(suite_dir, file_suffix):
        scenario_info = scenario_from_path(path)
        if scenario_info is None:
            continue
        scenario, _producers, _consumers = scenario_info
        rows: list[tuple[str, str, float]] = []
        with path.open(newline="") as f:
            for row in csv.DictReader(f):
                raw_value = row.get(column)
                if not raw_value:
                    continue
                queue = row["queue"]
                rows.append((family_for(queue, metadata), queue, float(raw_value)))
        entries_by_scenario[scenario] = rows
    return entries_by_scenario


def best_by_family(
    entries: list[tuple[str, str, float]], direction: str
) -> dict[str, tuple[str, float]]:
    best: dict[str, tuple[str, float]] = {}
    for family, queue, value in entries:
        current = best.get(family)
        if current is None or better(value, current[1], direction):
            best[family] = (queue, value)
    return best


def compute_advantage_rows(
    results_dir: Path,
    config: MetricConfig,
) -> list[AdvantageRow]:
    suite_dir = results_dir / config.suite
    entries_by_scenario = read_scenario_entries(
        suite_dir, config.file_suffix, config.column
    )

    rows: list[AdvantageRow] = []
    for scenario, entries in sorted(entries_by_scenario.items()):
        scenario_match = re.match(r"^(\d+)p(\d+)c$", scenario)
        if scenario_match is None:
            continue
        producers = int(scenario_match.group(1))
        consumers = int(scenario_match.group(2))

        ubq_entries = [entry for entry in entries if entry[0] == "UBQ"]
        competitor_entries = [entry for entry in entries if entry[0] != "UBQ"]
        if not ubq_entries or not competitor_entries:
            continue

        ubq_family, ubq_queue, ubq_value = sorted(
            ubq_entries, key=lambda entry: entry[2], reverse=config.direction == "higher"
        )[0]
        _competitor_family, competitor_queue, competitor_value = sorted(
            competitor_entries,
            key=lambda entry: entry[2],
            reverse=config.direction == "higher",
        )[0]
        competitor_family = family_for(
            competitor_queue, load_metadata(results_dir / config.suite)
        )

        rows.append(
            AdvantageRow(
                metric=config.label,
                scenario=scenario,
                producers=producers,
                consumers=consumers,
                ubq_queue=ubq_queue,
                ubq_value=ubq_value,
                competitor_family=competitor_family,
                competitor_queue=competitor_queue,
                competitor_value=competitor_value,
                speedup=ratio(ubq_value, competitor_value, config.direction),
            )
        )

    return rows


def geometric_mean(values: list[float]) -> float:
    positives = [value for value in values if value > 0]
    if not positives:
        return float("nan")
    return math.exp(sum(math.log(value) for value in positives) / len(positives))


def median(values: list[float]) -> float:
    if not values:
        return float("nan")
    return statistics.median(values)


def workload_class(row: AdvantageRow) -> str:
    p = row.producers
    c = row.consumers
    if p == 1 and c == 1:
        return "1p1c"
    if p == 1:
        return "1pNc"
    if c == 1:
        return "Np1c"
    if p == c:
        return "NpNc"
    if p >= 16 and c >= 16:
        return "high MPMC"
    return "mixed MPMC"


def ensure_out_dir(out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)


def save_figure(fig: plt.Figure, out_dir: Path, stem: str, formats: list[str]) -> None:
    for fmt in formats:
        path = out_dir / f"{stem}.{fmt}"
        kwargs = {"bbox_inches": "tight"}
        if fmt.lower() == "png":
            kwargs["dpi"] = 300
        fig.savefig(path, **kwargs)


def plot_speedup_heatmap(
    rows: list[AdvantageRow],
    out_dir: Path,
    stem: str,
    title: str,
    formats: list[str],
    levels: list[int] | None = None,
) -> None:
    levels = levels or THREAD_LEVELS
    by_coord = {(row.producers, row.consumers): row.speedup for row in rows}
    matrix = [
        [by_coord.get((producer, consumer), float("nan")) for consumer in levels]
        for producer in levels
    ]
    log_matrix = [
        [math.log2(value) if value > 0 else float("nan") for value in row]
        for row in matrix
    ]
    finite_logs = [value for row in log_matrix for value in row if math.isfinite(value)]
    bound = max(0.5, min(2.0, max(abs(value) for value in finite_logs)))

    fig, ax = plt.subplots(figsize=(8.0, 6.4))
    image = ax.imshow(
        log_matrix,
        cmap="RdYlGn",
        norm=TwoSlopeNorm(vcenter=0.0, vmin=-bound, vmax=bound),
        origin="upper",
    )
    ax.set_xticks(range(len(levels)), [str(value) for value in levels])
    ax.set_yticks(range(len(levels)), [str(value) for value in levels])
    ax.set_xlabel("Consumers")
    ax.set_ylabel("Producers")
    ax.set_title(title)

    for y_index, row in enumerate(matrix):
        for x_index, value in enumerate(row):
            if not math.isfinite(value):
                continue
            text_color = "white" if abs(math.log2(value)) > bound * 0.55 else "black"
            ax.text(
                x_index,
                y_index,
                f"{value:.2f}x",
                ha="center",
                va="center",
                fontsize=8,
                color=text_color,
            )

    colorbar = fig.colorbar(image, ax=ax, fraction=0.046, pad=0.04)
    colorbar.set_label("log2 speedup over best non-UBQ")
    save_figure(fig, out_dir, stem, formats)
    plt.close(fig)


def family_values_for_scenarios(
    results_dir: Path,
    config: MetricConfig,
) -> dict[str, dict[str, tuple[str, float]]]:
    entries_by_scenario = read_scenario_entries(
        results_dir / config.suite,
        config.file_suffix,
        config.column,
    )
    return {
        scenario: best_by_family(entries, config.direction)
        for scenario, entries in entries_by_scenario.items()
    }


def plot_throughput_scaling_lines(
    results_dir: Path,
    out_dir: Path,
    formats: list[str],
) -> None:
    config = MetricConfig(
        suite="throughput",
        file_suffix="throughput",
        column="ops_per_sec",
        direction="higher",
        label="throughput",
    )
    family_by_scenario = family_values_for_scenarios(results_dir, config)
    panels = [
        ("1 producer, N consumers", [(1, c) for c in THREAD_LEVELS[1:]], "Consumers"),
        ("N producers, N consumers", [(n, n) for n in THREAD_LEVELS[1:]], "N"),
        ("N producers, 64 consumers", [(p, 64) for p in THREAD_LEVELS], "Producers"),
    ]

    fig, axes = plt.subplots(1, 3, figsize=(14.5, 4.6), sharey=True)
    for ax, (title, coords, xlabel) in zip(axes, panels):
        x_values = [coord[1] if xlabel == "Consumers" else coord[0] for coord in coords]
        for family in FAMILY_ORDER:
            ys = []
            xs = []
            for x_value, (p, c) in zip(x_values, coords):
                scenario = f"{p}p{c}c"
                family_value = family_by_scenario.get(scenario, {}).get(family)
                if family_value is None:
                    continue
                xs.append(x_value)
                ys.append(family_value[1] / 1_000_000.0)
            if not ys:
                continue
            ax.plot(
                xs,
                ys,
                marker="o",
                linewidth=2.0 if family == "UBQ" else 1.5,
                color=FAMILY_COLORS.get(family, "#333333"),
                label=FAMILY_LABELS.get(family, family),
            )
        ax.set_xscale("log", base=2)
        ax.set_xticks(x_values, [str(value) for value in x_values])
        ax.grid(True, which="major", color="#dddddd", linewidth=0.8)
        ax.set_title(title)
        ax.set_xlabel(xlabel)

    axes[0].set_ylabel("Throughput (Mops/sec)")
    handles, labels = axes[0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="upper center", ncol=len(labels), frameon=False)
    fig.suptitle("Best per-family throughput scaling", y=1.05)
    save_figure(fig, out_dir, "throughput_scaling_lines", formats)
    plt.close(fig)


def plot_elapsed_distribution(
    push_rows: list[AdvantageRow],
    pop_rows: list[AdvantageRow],
    out_dir: Path,
    formats: list[str],
) -> None:
    data = [
        [row.speedup for row in push_rows],
        [row.speedup for row in pop_rows],
    ]
    labels = [
        "push elapsed",
        "pop elapsed",
    ]
    colors = ["#56B4E9", "#0072B2"]

    fig, ax = plt.subplots(figsize=(8.2, 4.0))
    box = ax.boxplot(
        data,
        vert=False,
        labels=labels,
        patch_artist=True,
        showmeans=True,
        meanprops={
            "marker": "D",
            "markerfacecolor": "white",
            "markeredgecolor": "#333333",
            "markersize": 5,
        },
    )
    for patch, color in zip(box["boxes"], colors):
        patch.set(facecolor=color, alpha=0.65)

    ax.axvline(1.0, color="#333333", linestyle="--", linewidth=1.2)
    ax.set_xscale("log", base=2)
    ax.set_xlabel("Speedup over best non-UBQ (lower elapsed is better)")
    ax.set_title("Data-latency workload operation-cost advantage")
    ax.grid(True, axis="x", which="major", color="#dddddd", linewidth=0.8)

    for index, rows in enumerate([push_rows, pop_rows], start=1):
        speeds = [row.speedup for row in rows]
        wins = sum(value >= 1.0 for value in speeds)
        median_speedup = median(speeds)
        gmean = geometric_mean(speeds)
        ax.text(
            max(speeds) * 1.04,
            index,
            f"{wins}/{len(speeds)} wins\nmedian {median_speedup:.2f}x\ngeo {gmean:.2f}x",
            va="center",
            fontsize=8,
        )

    save_figure(fig, out_dir, "data_latency_elapsed_speedup_distribution", formats)
    plt.close(fig)


def plot_workload_class_summary(
    rows: list[AdvantageRow],
    out_dir: Path,
    formats: list[str],
) -> None:
    classes = ["1pNc", "Np1c", "NpNc", "mixed MPMC", "high MPMC"]
    grouped: dict[str, list[float]] = defaultdict(list)
    for row in rows:
        row_class = workload_class(row)
        if row_class == "1p1c":
            continue
        grouped[row_class].append(row.speedup)

    win_rates = [
        100.0 * sum(value >= 1.0 for value in grouped[row_class]) / len(grouped[row_class])
        for row_class in classes
    ]
    gmeans = [geometric_mean(grouped[row_class]) for row_class in classes]

    fig, axes = plt.subplots(2, 1, figsize=(8.5, 6.0), sharex=True)
    x_positions = range(len(classes))
    axes[0].bar(x_positions, win_rates, color="#0072B2", alpha=0.82)
    axes[0].set_ylabel("UBQ win rate (%)")
    axes[0].set_ylim(0, 110)
    axes[0].grid(True, axis="y", color="#dddddd", linewidth=0.8)
    for x, value in zip(x_positions, win_rates):
        axes[0].text(x, value + 2, f"{value:.0f}%", ha="center", fontsize=8)

    axes[1].bar(x_positions, gmeans, color="#009E73", alpha=0.82)
    axes[1].axhline(1.0, color="#333333", linestyle="--", linewidth=1.2)
    axes[1].set_ylabel("Geomean speedup")
    axes[1].set_xticks(list(x_positions), classes)
    axes[1].grid(True, axis="y", color="#dddddd", linewidth=0.8)
    for x, value in zip(x_positions, gmeans):
        axes[1].text(x, value + 0.04, f"{value:.2f}x", ha="center", fontsize=8)

    fig.suptitle("Throughput advantage by workload shape")
    save_figure(fig, out_dir, "throughput_workload_class_summary", formats)
    plt.close(fig)


def load_metric_map(
    results_dir: Path,
    suite: str,
    scenario: str,
    file_suffix: str,
    column: str,
) -> tuple[dict[str, float], dict[str, str]]:
    suite_dir = results_dir / suite
    metadata = load_metadata(suite_dir)
    path = suite_dir / f"{scenario}_{file_suffix}.csv"
    values: dict[str, float] = {}
    families: dict[str, str] = {}
    with path.open(newline="") as f:
        for row in csv.DictReader(f):
            raw_value = row.get(column)
            if not raw_value:
                continue
            queue = row["queue"]
            values[queue] = float(raw_value)
            families[queue] = family_for(queue, metadata)
    return values, families


def plot_fairness_pareto(results_dir: Path, out_dir: Path, formats: list[str]) -> None:
    scenario = "64p64c"
    throughput, throughput_families = load_metric_map(
        results_dir, "fairness_throughput", scenario, "throughput", "ops_per_sec"
    )
    fairness, fairness_families = load_metric_map(
        results_dir,
        "consumer_fairness",
        scenario,
        "consumer_fairness",
        "fairness_ratio",
    )
    common_queues = sorted(set(throughput) & set(fairness))
    if not common_queues:
        return

    fig, ax = plt.subplots(figsize=(7.5, 5.2))
    plotted_families: set[str] = set()
    for family in FAMILY_ORDER:
        xs = [
            fairness[queue]
            for queue in common_queues
            if fairness_families.get(queue, throughput_families.get(queue)) == family
        ]
        ys = [
            throughput[queue] / 1_000_000.0
            for queue in common_queues
            if fairness_families.get(queue, throughput_families.get(queue)) == family
        ]
        if not xs:
            continue
        ax.scatter(
            xs,
            ys,
            s=48 if family == "UBQ" else 42,
            alpha=0.75,
            color=FAMILY_COLORS.get(family, "#333333"),
            label=FAMILY_LABELS.get(family, family),
            edgecolors="#222222" if family == "UBQ" else "none",
            linewidths=0.45,
        )
        plotted_families.add(family)

    best_ubq_queue = max(
        (queue for queue in common_queues if throughput_families.get(queue) == "UBQ"),
        key=lambda queue: throughput[queue],
    )
    best_non_ubq_queue = max(
        (queue for queue in common_queues if throughput_families.get(queue) != "UBQ"),
        key=lambda queue: throughput[queue],
    )
    for queue, label, offset in [
        (best_ubq_queue, "top UBQ", (8, 8)),
        (best_non_ubq_queue, "top non-UBQ", (8, -12)),
    ]:
        ax.annotate(
            label,
            (fairness[queue], throughput[queue] / 1_000_000.0),
            textcoords="offset points",
            xytext=offset,
            fontsize=8,
            arrowprops={"arrowstyle": "-", "color": "#333333", "lw": 0.8},
        )

    ax.set_xscale("log", base=2)
    ax.set_xlabel("Consumer fairness ratio (lower is better)")
    ax.set_ylabel("Throughput (Mops/sec)")
    ax.set_title("64p64c fairness/throughput Pareto")
    ax.grid(True, which="major", color="#dddddd", linewidth=0.8)
    ax.legend(loc="best", frameon=False)
    save_figure(fig, out_dir, "fairness_throughput_pareto_64p64c", formats)
    plt.close(fig)


def write_summary_csv(
    out_dir: Path,
    rows_by_metric: dict[str, list[AdvantageRow]],
) -> None:
    path = out_dir / "advantage_summary.csv"
    with path.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "metric",
                "scenario",
                "producers",
                "consumers",
                "workload_class",
                "ubq_queue",
                "ubq_value",
                "competitor_family",
                "competitor_queue",
                "competitor_value",
                "speedup",
            ]
        )
        for metric, rows in rows_by_metric.items():
            for row in rows:
                writer.writerow(
                    [
                        metric,
                        row.scenario,
                        row.producers,
                        row.consumers,
                        workload_class(row),
                        row.ubq_queue,
                        row.ubq_value,
                        row.competitor_family,
                        row.competitor_queue,
                        row.competitor_value,
                        row.speedup,
                    ]
                )


def write_application_targets(
    out_dir: Path,
    throughput_rows: list[AdvantageRow],
    complex_rows: list[AdvantageRow],
    push_rows: list[AdvantageRow],
    pop_rows: list[AdvantageRow],
) -> None:
    def class_stats(rows: list[AdvantageRow], class_name: str) -> tuple[int, int, float, float]:
        speeds = [row.speedup for row in rows if workload_class(row) == class_name]
        wins = sum(speed >= 1.0 for speed in speeds)
        return wins, len(speeds), geometric_mean(speeds), median(speeds)

    fanout = class_stats(throughput_rows, "1pNc")
    balanced = class_stats(throughput_rows, "NpNc")
    high = class_stats(throughput_rows, "high MPMC")
    complex_high = class_stats(complex_rows, "high MPMC")
    push_wins = sum(row.speedup >= 1.0 for row in push_rows)
    pop_wins = sum(row.speedup >= 1.0 for row in pop_rows)

    path = out_dir / "application_targets.md"
    with path.open("w") as f:
        f.write("# UBQ Advantage States And Application Targets\n\n")
        f.write(
            "This note maps the measured `grace_results` advantage states to "
            "application shapes that should be favorable for UBQ. Speedups compare "
            "the best UBQ variant in each scenario against the best non-UBQ queue "
            "in that same scenario.\n\n"
        )
        f.write("## Strongest Measured States\n\n")
        f.write(
            f"- `1pNc` fan-out throughput: UBQ wins {fanout[0]}/{fanout[1]} "
            f"scenarios, geomean {fanout[2]:.2f}x, median {fanout[3]:.2f}x.\n"
        )
        f.write(
            f"- Balanced `NpNc` throughput: UBQ wins {balanced[0]}/{balanced[1]} "
            f"scenarios, geomean {balanced[2]:.2f}x, median {balanced[3]:.2f}x.\n"
        )
        f.write(
            f"- High-MPMC throughput: UBQ wins {high[0]}/{high[1]} scenarios, "
            f"geomean {high[2]:.2f}x, median {high[3]:.2f}x.\n"
        )
        f.write(
            f"- Complex high-MPMC throughput: UBQ wins {complex_high[0]}/"
            f"{complex_high[1]} scenarios, geomean {complex_high[2]:.2f}x, "
            f"median {complex_high[3]:.2f}x.\n"
        )
        f.write(
            f"- Data-latency operation cost: UBQ wins {push_wins}/{len(push_rows)} "
            f"push-elapsed scenarios and {pop_wins}/{len(pop_rows)} pop-elapsed "
            "scenarios.\n\n"
        )
        f.write("## Application Targets\n\n")
        f.write(
            "1. Worker-pool ingress dispatch: one or a few ingress threads enqueue "
            "work for many consumers. Examples include packet/event ingestion, "
            "telemetry processors, log fan-out, and RPC acceptor-to-worker handoff. "
            "This maps directly to the `1pNc` fan-out win.\n\n"
        )
        f.write(
            "2. Global task-injection queues in runtimes: many producers and many "
            "workers share a central queue for overflow, wakeups, or externally "
            "submitted tasks. This maps to balanced and high-MPMC throughput wins. "
            "A credible paper experiment would replace or augment a scheduler "
            "global queue and measure end-to-end job throughput under contention.\n\n"
        )
        f.write(
            "3. Parallel graph/frontier processing: BFS, graph analytics, worklist "
            "solvers, and simulation engines often create bursts where many threads "
            "push and pop small work items. UBQ's high-MPMC results are the best "
            "fit when work items are small enough that queue overhead is visible.\n\n"
        )
        f.write(
            "4. Low-latency pipeline stages: when the application is already doing "
            "small-message handoff and the queue operation is a material part of "
            "the budget, the data-latency push/pop elapsed wins are directly useful. "
            "Examples include market-data normalization, metrics aggregation, and "
            "in-process stream processing.\n\n"
        )
        f.write(
            "5. Backpressure-free burst buffers: UBQ is unbounded while the strongest "
            "bounded competitor is pre-sized. Applications that need FIFO semantics "
            "without capacity tuning can turn this into a usability advantage, not "
            "just a throughput advantage.\n\n"
        )
        f.write("## States To Avoid Or Improve Before Claiming Dominance\n\n")
        f.write(
            "- `Np1c` single-consumer throughput is not UBQ's strongest state. "
            "Treat it as a caveat or use sharding/batching to avoid forcing many "
            "producers into one consumer bottleneck.\n"
        )
        f.write(
            "- Average data latency is mixed outside the high-saturation wins. "
            "Use elapsed operation cost as the broad claim, and reserve average "
            "data-latency claims for specific saturation cases.\n"
        )
        f.write(
            "- Consumer fairness has a strong `64p64c` story but visible outliers. "
            "Keep fairness as supporting evidence until the outlier states are "
            "understood or tuned.\n"
        )


def main() -> None:
    args = parse_args()
    formats = [item.strip() for item in args.formats.split(",") if item.strip()]
    ensure_out_dir(args.out_dir)

    throughput_config = MetricConfig(
        "throughput", "throughput", "ops_per_sec", "higher", "throughput"
    )
    complex_config = MetricConfig(
        "complex_throughput",
        "throughput",
        "ops_per_sec",
        "higher",
        "complex_throughput",
    )
    push_elapsed_config = MetricConfig(
        "data_latency_push_elapsed",
        "push_elapsed",
        "push_elapsed_ns",
        "lower",
        "data_latency_push_elapsed",
    )
    pop_elapsed_config = MetricConfig(
        "data_latency_pop_elapsed",
        "pop_elapsed",
        "pop_elapsed_ns",
        "lower",
        "data_latency_pop_elapsed",
    )

    throughput_rows = compute_advantage_rows(args.results_dir, throughput_config)
    complex_rows = compute_advantage_rows(args.results_dir, complex_config)
    push_rows = compute_advantage_rows(args.results_dir, push_elapsed_config)
    pop_rows = compute_advantage_rows(args.results_dir, pop_elapsed_config)

    write_summary_csv(
        args.out_dir,
        {
            "throughput": throughput_rows,
            "complex_throughput": complex_rows,
            "data_latency_push_elapsed": push_rows,
            "data_latency_pop_elapsed": pop_rows,
        },
    )

    plot_speedup_heatmap(
        throughput_rows,
        args.out_dir,
        "throughput_speedup_heatmap",
        "Throughput speedup: best UBQ vs best non-UBQ",
        formats,
    )
    high_levels = [16, 32, 64]
    plot_speedup_heatmap(
        [
            row
            for row in complex_rows
            if row.producers in high_levels and row.consumers in high_levels
        ],
        args.out_dir,
        "complex_high_contention_speedup_heatmap",
        "Complex throughput speedup under high contention",
        formats,
        levels=high_levels,
    )
    plot_throughput_scaling_lines(args.results_dir, args.out_dir, formats)
    plot_elapsed_distribution(push_rows, pop_rows, args.out_dir, formats)
    plot_workload_class_summary(throughput_rows, args.out_dir, formats)
    plot_fairness_pareto(args.results_dir, args.out_dir, formats)
    write_application_targets(
        args.out_dir, throughput_rows, complex_rows, push_rows, pop_rows
    )


if __name__ == "__main__":
    main()
