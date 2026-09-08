#!/usr/bin/env python3
"""Render an audit-friendly visual report from aggregate_profile_results.py.

The plots consume normalized CSV files rather than APX's version-specific
archive layout.  Throughput always comes from the unprofiled shadow result;
collector recipes appear only in diagnostic plots.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import statistics
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.colors import Normalize

try:
    from scripts.plot_style import (
        ACCESSIBLE_DIVERGING_CMAP,
        family_color,
        family_linestyle,
        family_marker,
    )
except ModuleNotFoundError:
    from plot_style import (  # type: ignore
        ACCESSIBLE_DIVERGING_CMAP,
        family_color,
        family_linestyle,
        family_marker,
    )


THROUGHPUT_METRIC = "workload.ops_per_sec"
FINGERPRINT_METRICS = (
    "perf.cycles_per_op",
    "perf.instructions_per_op",
    "perf.instructions_per_cycle",
    "perf.cache_misses.per_million_ops",
    "perf.branch_misses.per_million_ops",
    "allocator.allocated_bytes_per_item",
    "allocator.alloc_calls_per_million_items",
    "os.minor_faults_per_million_items",
    "os.cpu_time_per_item_ns",
    "apx.frontend_bound_percent",
    "apx.backend_bound_percent",
    "apx.store_exclusive_failures_pki",
)
METRIC_LABELS = {
    "perf.cycles_per_op": "cycles/op",
    "perf.instructions_per_op": "instructions/op",
    "perf.instructions_per_cycle": "IPC",
    "perf.cache_misses.per_million_ops": "cache misses/Mop",
    "perf.branch_misses.per_million_ops": "branch misses/Mop",
    "allocator.allocated_bytes_per_item": "allocated B/item",
    "allocator.alloc_calls_per_million_items": "alloc calls/Mitem",
    "os.minor_faults_per_million_items": "minor faults/Mitem",
    "os.cpu_time_per_item_ns": "CPU ns/item",
    "apx.frontend_bound_percent": "front-end bound",
    "apx.backend_bound_percent": "back-end bound",
    "apx.store_exclusive_failures_pki": "STXR failures/Kinst",
    "workload.ops_per_sec": "throughput",
}
QUEUE_FAMILIES = {
    "ubq": "UBQ",
    "lubq": "LUBQ",
    "segqueue": "SegQueue",
    "concurrent-queue": "concurrent-queue",
    "mutex-vecdeque": "Mutex+VecDeque",
    "ms-queue": "MS-Queue",
}
BATCH_LINESTYLES = {"": "-", "32": "--", "256": ":", "4096": "-."}


def read_csv(path: Path) -> list[dict[str, str]]:
    if not path.is_file():
        return []
    with path.open(newline="", encoding="utf-8") as stream:
        return list(csv.DictReader(stream))


def number(value: Any) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def integer(value: Any) -> int | None:
    parsed = number(value)
    return int(parsed) if parsed is not None else None


def truthy(value: Any) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "ok"}


def apply_filters(rows: Iterable[dict[str, str]], args: argparse.Namespace) -> list[dict[str, str]]:
    result = []
    for row in rows:
        if args.machine and row.get("machine") not in args.machine:
            continue
        if args.recipe and row.get("recipe") not in args.recipe:
            continue
        if args.scenario and row.get("scenario") not in args.scenario:
            continue
        if args.variant and row.get("variant") not in args.variant:
            continue
        result.append(row)
    return result


def queue_family(row: Mapping[str, str]) -> str:
    return QUEUE_FAMILIES.get(row.get("queue", ""), row.get("queue", "unknown"))


def series_label(row: Mapping[str, str], include_variant: bool = False) -> str:
    family = queue_family(row)
    batch = row.get("batch_size", "")
    label = f"{family} {'scalar' if not batch else f'batch {batch}'}"
    variant = row.get("variant", "")
    if include_variant and variant:
        label += f" · {variant}"
    return label


def scenario_shape(row: Mapping[str, str]) -> str:
    producers = integer(row.get("producers"))
    consumers = integer(row.get("consumers"))
    if producers is None or consumers is None:
        return "other"
    if producers == consumers:
        return "balanced MPMC"
    if consumers == 1:
        return "producer contention"
    if producers == 1:
        return "consumer contention"
    return "asymmetric MPMC"


def choose_shadow_recipe(rows: Sequence[dict[str, str]]) -> str | None:
    recipes = {row.get("recipe", "") for row in rows}
    if "perf_stat" in recipes:
        return "perf_stat"
    return sorted(recipes)[0] if recipes else None


def choose_stress_scenario(rows: Sequence[dict[str, str]], requested: str | None) -> str | None:
    if requested:
        return requested
    by_scenario: dict[str, set[str]] = defaultdict(set)
    threads: dict[str, int] = {}
    for row in rows:
        producers = integer(row.get("producers"))
        consumers = integer(row.get("consumers"))
        if producers is None or consumers is None or producers != consumers:
            continue
        scenario = row.get("scenario", "")
        by_scenario[scenario].add(series_label(row, include_variant=True))
        threads[scenario] = producers + consumers
    if not by_scenario:
        for row in rows:
            scenario = row.get("scenario", "")
            by_scenario[scenario].add(series_label(row, include_variant=True))
            threads[scenario] = integer(row.get("total_threads")) or 0
    if not by_scenario:
        return None
    return max(by_scenario, key=lambda name: (len(by_scenario[name]), threads.get(name, 0), name))


def configure_style() -> None:
    plt.rcParams.update(
        {
            "figure.dpi": 120,
            "savefig.dpi": 170,
            "axes.grid": True,
            "grid.alpha": 0.22,
            "axes.spines.top": False,
            "axes.spines.right": False,
            "font.size": 9,
            "axes.titleweight": "bold",
        }
    )


def save_placeholder(path: Path, title: str, message: str) -> None:
    figure, axis = plt.subplots(figsize=(8, 3.2))
    axis.axis("off")
    axis.set_title(title)
    axis.text(0.5, 0.48, message, ha="center", va="center", wrap=True)
    figure.tight_layout()
    figure.savefig(path, bbox_inches="tight")
    plt.close(figure)


def plot_coverage(coverage: Mapping[str, Any], path: Path) -> None:
    campaigns = coverage.get("campaigns", {})
    if not campaigns:
        save_placeholder(path, "Campaign coverage", "No campaign coverage was found.")
        return
    names = list(campaigns)
    authoritative = [campaigns[name].get("authoritative_cases", 0) for name in names]
    rejected = [campaigns[name].get("rejected_cases", 0) for name in names]
    missing = [campaigns[name].get("missing_cases") or 0 for name in names]
    figure, axis = plt.subplots(figsize=(max(8, 1.25 * len(names)), 4.5))
    positions = list(range(len(names)))
    axis.bar(positions, authoritative, label="authoritative", color="#009E73")
    axis.bar(positions, rejected, bottom=authoritative, label="rejected", color="#D55E00", hatch="//")
    bottoms = [left + right for left, right in zip(authoritative, rejected)]
    axis.bar(positions, missing, bottom=bottoms, label="missing", color="#999999", hatch="xx")
    axis.set_xticks(positions, names, rotation=18, ha="right")
    axis.set_ylabel("cases")
    axis.set_title("Campaign coverage and quality disposition")
    axis.legend(frameon=False, ncols=3)
    figure.tight_layout()
    figure.savefig(path, bbox_inches="tight")
    plt.close(figure)


def plot_throughput(summary: Sequence[dict[str, str]], output: Path) -> list[Path]:
    candidates = [row for row in summary if row.get("metric_id") == THROUGHPUT_METRIC]
    recipe = choose_shadow_recipe(candidates)
    if recipe:
        candidates = [row for row in candidates if row.get("recipe") == recipe]
    paths = []
    variants = {row.get("variant", "") for row in candidates}
    for machine in sorted({row.get("machine", "unknown") for row in candidates}):
        rows = [row for row in candidates if row.get("machine") == machine]
        path = output / f"throughput_scaling_{machine}.png"
        shapes = [
            shape
            for shape in ("producer contention", "consumer contention", "balanced MPMC", "asymmetric MPMC")
            if any(scenario_shape(row) == shape for row in rows)
        ]
        if not shapes:
            save_placeholder(path, f"Throughput scaling · {machine}", "No authoritative throughput summaries.")
            paths.append(path)
            continue
        figure, axes = plt.subplots(1, len(shapes), figsize=(5.2 * len(shapes), 4.4), squeeze=False)
        legend: dict[str, Any] = {}
        for axis, shape in zip(axes[0], shapes):
            grouped: dict[str, list[dict[str, str]]] = defaultdict(list)
            for row in rows:
                if scenario_shape(row) == shape:
                    grouped[series_label(row, len(variants) > 1)].append(row)
            for index, (label, entries) in enumerate(sorted(grouped.items())):
                entries.sort(key=lambda row: integer(row.get("total_threads")) or 0)
                x = [integer(row.get("total_threads")) or 0 for row in entries]
                y = [(number(row.get("median")) or 0.0) / 1_000_000.0 for row in entries]
                low = [(number(row.get("ci_low")) or value * 1_000_000.0) / 1_000_000.0 for row, value in zip(entries, y)]
                high = [(number(row.get("ci_high")) or value * 1_000_000.0) / 1_000_000.0 for row, value in zip(entries, y)]
                family = queue_family(entries[0])
                batch = entries[0].get("batch_size", "")
                line = axis.errorbar(
                    x,
                    y,
                    yerr=[[max(0.0, value - lo) for value, lo in zip(y, low)], [max(0.0, hi - value) for value, hi in zip(y, high)]],
                    color=family_color(family, index),
                    marker=family_marker(family, index),
                    linestyle=BATCH_LINESTYLES.get(batch, family_linestyle(family, index)),
                    linewidth=1.35,
                    markersize=4,
                    capsize=2,
                    label=label,
                )
                legend.setdefault(label, line)
            axis.set_xscale("log", base=2)
            axis.set_xlabel("worker threads")
            axis.set_ylabel("million items/s")
            axis.set_title(shape)
        figure.suptitle(f"Unprofiled throughput scaling · {machine} · {recipe}")
        figure.legend(legend.values(), legend.keys(), loc="lower center", ncols=min(4, max(1, len(legend))), frameon=False)
        figure.tight_layout(rect=(0, 0.15, 1, 0.95))
        figure.savefig(path, bbox_inches="tight")
        plt.close(figure)
        paths.append(path)
    return paths


def preferred_metric_entry(entries: Sequence[dict[str, str]], metric_id: str) -> dict[str, str]:
    def score(row: dict[str, str]) -> tuple[int, int, str]:
        recipe = row.get("recipe", "")
        portable = int(recipe == "perf_stat") if not metric_id.startswith("apx.") else int(recipe != "perf_stat")
        return portable, integer(row.get("n")) or 0, recipe

    return max(entries, key=score)


def plot_fingerprint(
    summary: Sequence[dict[str, str]], output: Path, requested_stress: str | None
) -> tuple[list[Path], dict[str, str]]:
    paths = []
    selections: dict[str, str] = {}
    for machine in sorted({row.get("machine", "unknown") for row in summary}):
        machine_rows = [row for row in summary if row.get("machine") == machine]
        scenario = choose_stress_scenario(machine_rows, requested_stress)
        selections[machine] = scenario or "none"
        path = output / f"bottleneck_fingerprint_{machine}.png"
        rows = [row for row in machine_rows if row.get("scenario") == scenario]
        available_metrics = [metric for metric in FINGERPRINT_METRICS if any(row.get("metric_id") == metric for row in rows)]
        series = sorted({series_label(row, include_variant=True) for row in rows if row.get("metric_id") in available_metrics})
        if not available_metrics or not series:
            save_placeholder(path, f"Bottleneck fingerprint · {machine}", "No selected diagnostic metrics are available.")
            paths.append(path)
            continue
        candidates: dict[tuple[str, str], list[dict[str, str]]] = defaultdict(list)
        for row in rows:
            metric = row.get("metric_id", "")
            if metric in available_metrics:
                candidates[(series_label(row, include_variant=True), metric)].append(row)
        raw: dict[tuple[str, str], float] = {}
        for key, entries in candidates.items():
            chosen = preferred_metric_entry(entries, key[1])
            value = number(chosen.get("median"))
            if value is not None:
                raw[key] = value
        centers = {
            metric: statistics.median([value for (label, name), value in raw.items() if name == metric and value > 0])
            for metric in available_metrics
            if any(name == metric and value > 0 for (_label, name), value in raw.items())
        }
        matrix: list[list[float]] = []
        annotations: list[list[str]] = []
        for label in series:
            row_values = []
            row_labels = []
            for metric in available_metrics:
                value = raw.get((label, metric))
                center = centers.get(metric)
                if value is None or center is None or value <= 0:
                    row_values.append(float("nan"))
                    row_labels.append("—")
                else:
                    ratio = value / center
                    row_values.append(math.log2(ratio))
                    row_labels.append(f"×{ratio:.2g}")
            matrix.append(row_values)
            annotations.append(row_labels)
        figure, axis = plt.subplots(figsize=(max(9, 1.2 * len(available_metrics)), max(4, 0.42 * len(series) + 2)))
        image = axis.imshow(matrix, aspect="auto", cmap=ACCESSIBLE_DIVERGING_CMAP, norm=Normalize(-2, 2))
        axis.set_xticks(range(len(available_metrics)), [METRIC_LABELS.get(metric, metric) for metric in available_metrics], rotation=35, ha="right")
        axis.set_yticks(range(len(series)), series)
        for y, labels in enumerate(annotations):
            for x, label in enumerate(labels):
                axis.text(x, y, label, ha="center", va="center", fontsize=7, color="black")
        axis.grid(False)
        axis.set_title(f"Bottleneck fingerprint · {machine} · {scenario}\ncell = ratio to cross-queue median; color = log₂ ratio")
        colorbar = figure.colorbar(image, ax=axis, shrink=0.78)
        colorbar.set_label("log₂(relative diagnostic intensity)")
        figure.tight_layout()
        figure.savefig(path, bbox_inches="tight")
        plt.close(figure)
        paths.append(path)
    return paths, selections


def plot_pressure_scaling(
    summary: Sequence[dict[str, str]], output: Path
) -> list[Path]:
    metrics = (
        "allocator.allocated_bytes_per_item",
        "allocator.alloc_calls_per_million_items",
        "os.minor_faults_per_million_items",
        "os.cpu_time_per_item_ns",
    )
    paths = []
    for machine in sorted({row.get("machine", "unknown") for row in summary}):
        candidates = [
            row
            for row in summary
            if row.get("machine") == machine
            and row.get("metric_id") in metrics
            and scenario_shape(row) == "balanced MPMC"
        ]
        recipe = choose_shadow_recipe(candidates)
        if recipe:
            candidates = [row for row in candidates if row.get("recipe") == recipe]
        path = output / f"allocator_pressure_{machine}.png"
        if not candidates:
            save_placeholder(path, f"Allocator and OS pressure · {machine}", "No allocator/getrusage summaries are available.")
            paths.append(path)
            continue
        figure, axes = plt.subplots(2, 2, figsize=(12, 8), squeeze=False)
        legend: dict[str, Any] = {}
        variants = {row.get("variant", "") for row in candidates}
        for axis, metric in zip(axes.flat, metrics):
            grouped: dict[str, list[dict[str, str]]] = defaultdict(list)
            for row in candidates:
                if row.get("metric_id") == metric:
                    grouped[series_label(row, len(variants) > 1)].append(row)
            for index, (label, entries) in enumerate(sorted(grouped.items())):
                entries.sort(key=lambda row: integer(row.get("total_threads")) or 0)
                family = queue_family(entries[0])
                batch = entries[0].get("batch_size", "")
                line = axis.plot(
                    [integer(row.get("total_threads")) or 0 for row in entries],
                    [number(row.get("median")) or 0.0 for row in entries],
                    color=family_color(family, index),
                    marker=family_marker(family, index),
                    linestyle=BATCH_LINESTYLES.get(batch, family_linestyle(family, index)),
                    linewidth=1.3,
                    markersize=4,
                    label=label,
                )[0]
                legend.setdefault(label, line)
            axis.set_xscale("log", base=2)
            axis.set_xlabel("worker threads (balanced MPMC)")
            axis.set_title(METRIC_LABELS.get(metric, metric))
        figure.suptitle(f"Allocator, paging, and CPU pressure scaling · {machine} · {recipe}")
        figure.legend(legend.values(), legend.keys(), loc="lower center", ncols=min(4, max(1, len(legend))), frameon=False)
        figure.tight_layout(rect=(0, 0.14, 1, 0.95))
        figure.savefig(path, bbox_inches="tight")
        plt.close(figure)
        paths.append(path)
    return paths


def plot_atomic_scaling(summary: Sequence[dict[str, str]], output: Path) -> list[Path]:
    metrics = (
        "workload.ops_per_sec",
        "perf.cycles_per_op",
        "perf.instructions_per_cycle",
        "perf.cache_misses.per_million_ops",
    )
    paths = []
    for machine in sorted({row.get("machine", "unknown") for row in summary}):
        rows = [
            row
            for row in summary
            if row.get("machine") == machine
            and row.get("recipe") == "perf_stat"
            and row.get("metric_id") in metrics
            and scenario_shape(row) == "balanced MPMC"
        ]
        path = output / f"contention_scaling_{machine}.png"
        if not rows:
            save_placeholder(path, f"Contention scaling · {machine}", "No portable counter summaries are available.")
            paths.append(path)
            continue
        figure, axes = plt.subplots(2, 2, figsize=(12, 8), squeeze=False)
        legend: dict[str, Any] = {}
        for axis, metric in zip(axes.flat, metrics):
            grouped: dict[str, list[dict[str, str]]] = defaultdict(list)
            for row in rows:
                if row.get("metric_id") == metric:
                    grouped[series_label(row, include_variant=True)].append(row)
            for index, (label, entries) in enumerate(sorted(grouped.items())):
                entries.sort(key=lambda row: integer(row.get("total_threads")) or 0)
                family = queue_family(entries[0])
                values = [number(row.get("median")) or 0.0 for row in entries]
                if metric == THROUGHPUT_METRIC:
                    values = [value / 1_000_000.0 for value in values]
                line = axis.plot(
                    [integer(row.get("total_threads")) or 0 for row in entries],
                    values,
                    color=family_color(family, index),
                    marker=family_marker(family, index),
                    linestyle=BATCH_LINESTYLES.get(entries[0].get("batch_size", ""), "-"),
                    linewidth=1.3,
                    markersize=4,
                    label=label,
                )[0]
                legend.setdefault(label, line)
            axis.set_xscale("log", base=2)
            axis.set_xlabel("worker threads (balanced MPMC)")
            axis.set_title("throughput (Mitem/s)" if metric == THROUGHPUT_METRIC else METRIC_LABELS.get(metric, metric))
        figure.suptitle(f"Shared-state contention symptoms · {machine}")
        figure.legend(legend.values(), legend.keys(), loc="lower center", ncols=min(4, max(1, len(legend))), frameon=False)
        figure.tight_layout(rect=(0, 0.14, 1, 0.95))
        figure.savefig(path, bbox_inches="tight")
        plt.close(figure)
        paths.append(path)
    return paths


def plot_hotspots(
    hotspots: Sequence[dict[str, str]], output: Path, requested_stress: str | None
) -> tuple[list[Path], dict[str, str]]:
    paths = []
    selections: dict[str, str] = {}
    authoritative = [row for row in hotspots if truthy(row.get("authoritative"))]
    for machine in sorted({row.get("machine", "unknown") for row in authoritative}):
        machine_rows = [row for row in authoritative if row.get("machine") == machine]
        scenario = choose_stress_scenario(machine_rows, requested_stress)
        selections[machine] = scenario or "none"
        rows = [row for row in machine_rows if row.get("scenario") == scenario]
        path = output / f"hotspots_{machine}.png"
        grouped: dict[tuple[str, str], list[float]] = defaultdict(list)
        symbol_totals: dict[str, list[float]] = defaultdict(list)
        series = sorted({series_label(row, include_variant=True) for row in rows})
        for row in rows:
            value = number(row.get("self_fraction"))
            symbol = row.get("symbol", "") or "<unknown>"
            if value is None:
                continue
            label = series_label(row, include_variant=True)
            grouped[(label, symbol)].append(value)
            symbol_totals[symbol].append(value)
        symbols = sorted(
            symbol_totals,
            key=lambda symbol: statistics.median(symbol_totals[symbol]),
            reverse=True,
        )[:15]
        if not series or not symbols:
            save_placeholder(path, f"APX hotspots · {machine}", "No authoritative code-hotspot fractions are available.")
            paths.append(path)
            continue
        matrix = [
            [statistics.median(grouped[(label, symbol)]) * 100 if grouped[(label, symbol)] else 0.0 for label in series]
            for symbol in symbols
        ]
        figure, axis = plt.subplots(figsize=(max(9, 0.72 * len(series) + 5), max(5, 0.42 * len(symbols) + 2)))
        image = axis.imshow(matrix, aspect="auto", cmap="viridis")
        axis.set_xticks(range(len(series)), series, rotation=35, ha="right")
        axis.set_yticks(range(len(symbols)), symbols)
        for y, values in enumerate(matrix):
            for x, value in enumerate(values):
                if value >= 0.1:
                    axis.text(x, y, f"{value:.1f}%", ha="center", va="center", fontsize=7, color="white" if value > max(max(row) for row in matrix) * 0.45 else "black")
        axis.grid(False)
        axis.set_title(f"APX self-sample hotspot map · {machine} · {scenario}")
        colorbar = figure.colorbar(image, ax=axis, shrink=0.8)
        colorbar.set_label("median self samples (%)")
        figure.tight_layout()
        figure.savefig(path, bbox_inches="tight")
        plt.close(figure)
        paths.append(path)
    return paths, selections


def plot_effects(effects: Sequence[dict[str, str]], path: Path) -> None:
    preferred = {
        "workload.ops_per_sec",
        "perf.cycles_per_op",
        "allocator.allocated_bytes_per_item",
        "allocator.alloc_calls_per_million_items",
        "os.minor_faults_per_million_items",
    }
    rows = [
        row
        for row in effects
        if row.get("metric_id") in preferred
        and (
            number(row.get("performance_median"))
            or number(row.get("performance_geomean"))
            or 0
        )
        > 0
    ]
    if not rows:
        save_placeholder(
            path,
            "Build-variable effects",
            "No comparable multi-variant cells exist yet. Add at least two variants and mark one is_baseline=true.",
        )
        return
    rows = sorted(rows, key=lambda row: (row.get("machine", ""), row.get("metric_id", ""), row.get("scenario", "")))[:40]
    labels = [
        f"{row.get('machine')} · {row.get('queue', '')} · {row.get('scenario')} · {METRIC_LABELS.get(row.get('metric_id', ''), row.get('metric_id', ''))}\n{row.get('treatment_variant')} vs {row.get('reference_variant')}"
        for row in rows
    ]
    centers = [
        number(row.get("performance_median"))
        or number(row.get("performance_geomean"))
        or 1.0
        for row in rows
    ]
    lows = [number(row.get("performance_ci_low")) or center for row, center in zip(rows, centers)]
    highs = [number(row.get("performance_ci_high")) or center for row, center in zip(rows, centers)]
    positions = list(range(len(rows)))
    figure, axis = plt.subplots(figsize=(10, max(4, 0.46 * len(rows) + 1.5)))
    axis.errorbar(
        centers,
        positions,
        xerr=[[max(0.0, center - low) for center, low in zip(centers, lows)], [max(0.0, high - center) for center, high in zip(centers, highs)]],
        fmt="o",
        color="#0072B2",
        ecolor="#555555",
        capsize=2,
    )
    axis.axvline(1.0, color="black", linewidth=1, linestyle="--")
    axis.set_xscale("log", base=2)
    axis.set_yticks(positions, labels)
    axis.set_xlabel("performance ratio (>1 favors treatment; direction normalized)")
    designs = sorted({row.get("design", "paired") for row in rows})
    axis.set_title(f"Build-variable effects with 95% bootstrap intervals · {', '.join(designs)}")
    axis.grid(axis="x", alpha=0.25)
    axis.grid(axis="y", visible=False)
    figure.tight_layout()
    figure.savefig(path, bbox_inches="tight")
    plt.close(figure)


def write_index(
    output: Path,
    files: Sequence[Path],
    coverage: Mapping[str, Any],
    fingerprint_scenarios: Mapping[str, str],
    hotspot_scenarios: Mapping[str, str],
) -> None:
    totals = coverage.get("totals", {})
    lines = [
        "# Queue profiling visual report",
        "",
        "Throughput panels use only unprofiled shadow measurements (preferably `perf_stat` cases). APX recipe throughput is never compared across recipes. Error bars are deterministic 95% bootstrap intervals around medians.",
        "",
        f"Coverage: {totals.get('authoritative_cases', 0)} authoritative / {totals.get('observed_cases', 0)} observed; {totals.get('rejected_cases', 0)} rejected; {totals.get('missing_cases', 'unknown')} missing.",
        "",
        "Fingerprint values are ratios to the cross-queue median for one automatically selected, well-covered balanced-MPMC stress scenario. They indicate diagnostic intensity, not whether high or low is inherently good.",
        "",
        f"Fingerprint scenarios: {json.dumps(dict(fingerprint_scenarios), sort_keys=True)}.",
        f"Hotspot scenarios: {json.dumps(dict(hotspot_scenarios), sort_keys=True)}.",
        "",
        "## Figures",
        "",
    ]
    for path in sorted(files):
        lines.append(f"- [{path.stem}]({path.name})")
    lines.extend(
        [
            "",
            "## Reading order",
            "",
            "1. Check `coverage` before interpreting any comparison.",
            "2. Find scaling cliffs in `throughput_scaling` and `contention_scaling`.",
            "3. Correlate them with allocator/page-fault trends and the bottleneck fingerprint.",
            "4. Use `hotspots` for code attribution, then validate candidate changes with paired variant effects.",
            "",
        ]
    )
    (output / "index.md").write_text("\n".join(lines), encoding="utf-8")


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("aggregate_dir", type=Path)
    parser.add_argument("--out-dir", type=Path, default=Path("profiling_results/plots"))
    parser.add_argument("--machine", action="append")
    parser.add_argument("--recipe", action="append")
    parser.add_argument("--scenario", action="append")
    parser.add_argument("--variant", action="append")
    parser.add_argument("--stress-scenario", help="override automatic stress-case selection")
    args = parser.parse_args(argv)
    aggregate = args.aggregate_dir.resolve()
    output = args.out_dir.resolve()
    output.mkdir(parents=True, exist_ok=True)
    configure_style()

    summary = apply_filters(read_csv(aggregate / "cell_summary.csv"), args)
    hotspots = apply_filters(read_csv(aggregate / "hotspots.csv"), args)
    independent_effects = apply_filters(read_csv(aggregate / "variant_effects.csv"), args)
    paired_effects = apply_filters(read_csv(aggregate / "paired_effects.csv"), args)
    coverage_path = aggregate / "coverage.json"
    coverage = json.loads(coverage_path.read_text(encoding="utf-8")) if coverage_path.is_file() else {}

    files: list[Path] = []
    coverage_plot = output / "coverage.png"
    plot_coverage(coverage, coverage_plot)
    files.append(coverage_plot)
    files.extend(plot_throughput(summary, output))
    fingerprint_files, fingerprint_scenarios = plot_fingerprint(summary, output, args.stress_scenario)
    files.extend(fingerprint_files)
    files.extend(plot_pressure_scaling(summary, output))
    files.extend(plot_atomic_scaling(summary, output))
    hotspot_files, hotspot_scenarios = plot_hotspots(hotspots, output, args.stress_scenario)
    files.extend(hotspot_files)
    effects_plot = output / "variant_effects.png"
    plot_effects(independent_effects or paired_effects, effects_plot)
    files.append(effects_plot)
    write_index(output, files, coverage, fingerprint_scenarios, hotspot_scenarios)
    print(f"Wrote {len(files)} plots and {output / 'index.md'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
