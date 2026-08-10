#!/usr/bin/env python3
"""Plot block-aware atomic-update contention from bench_atomic_updates."""

import argparse
import csv
import json
import math
import statistics
import sys
from pathlib import Path


BENCHMARK_NAME = "atomic_updates"
SCHEMA_VERSION = 3
LAYOUTS = ("u64", "mixed_u128_u64")
METHODS = ("cas", "cas_backoff", "faa", "segqueue")
CAS_METHODS = ("cas", "cas_backoff", "segqueue")
SERIES = tuple(
    (layout, method)
    for layout in LAYOUTS
    for method in METHODS
    if method != "segqueue" or layout == "u64"
)
SERIES_LABELS = {
    ("u64", "cas"): "U64 CAS",
    ("u64", "cas_backoff"): "U64 CAS + spin backoff",
    ("u64", "faa"): "U64 FAA",
    ("mixed_u128_u64", "cas"): "Generation-cached U128/U64 CAS",
    ("mixed_u128_u64", "cas_backoff"): "Generation-cached U128/U64 CAS/backoff",
    ("mixed_u128_u64", "faa"): "Generation-cached U128/U64 FAA",
    ("u64", "segqueue"): "SegQueue-style U64 CAS",
}
METHOD_COLORS = {
    "cas": "#0072b2",
    "cas_backoff": "#009e73",
    "faa": "#e69f00",
    "segqueue": "#cc79a7",
}
LAYOUT_MARKERS = {
    "u64": "s",
    "mixed_u128_u64": "*",
}
LAYOUT_MARKER_SIZES = {
    "u64": 7,
    "mixed_u128_u64": 11,
}


def collect_jsons(paths, runs_dirs):
    requested = {
        Path(path)
        for value in paths
        for path in (part.strip() for part in value.split(","))
        if path
    }
    discovered = set()
    for raw_dir in runs_dirs:
        runs_dir = Path(raw_dir)
        if runs_dir.exists():
            discovered.update(
                path for path in runs_dir.rglob("*.json") if path.is_file()
            )

    if requested and discovered:
        requested_names = {path.name for path in requested}
        files = {path for path in discovered if path.name in requested_names}
        files.update(path for path in requested if path.is_file())
    elif requested:
        files = requested
    else:
        files = discovered
    return sorted(files)


def load_samples(path):
    try:
        with Path(path).open(encoding="utf-8") as stream:
            data = json.load(stream)
    except Exception as exc:
        print(f"warning: could not parse {path}: {exc}", file=sys.stderr)
        return []

    if (
        data.get("benchmark") != BENCHMARK_NAME
        or data.get("schema_version") not in (SCHEMA_VERSION, str(SCHEMA_VERSION))
    ):
        return []

    meta = data.get("meta", {})
    machine = str(meta.get("machine_label", "local")).strip() or "local"
    ordering = str(meta.get("ordering", "ubq")).strip().lower() or "ubq"
    try:
        alignment = int(meta["alignment"])
    except (KeyError, TypeError, ValueError):
        return []

    loaded = []
    for result in data.get("results", []):
        layout = str(result.get("layout", "")).strip().lower()
        method = str(result.get("method", "")).strip().lower()
        if (layout, method) not in SERIES:
            continue
        try:
            block_size = int(result["block_size"])
            updaters = int(result["updater_count"])
            repeat = int(result["repeat_index"])
            operations = int(result["operations"])
            ops_per_sec = float(result["ops_per_sec"])
            cas_failures = int(result.get("cas_failures", 0))
            wide_loads = int(result.get("wide_loads", 0))
        except (KeyError, TypeError, ValueError):
            continue
        if (
            block_size <= 0
            or updaters <= 0
            or repeat <= 0
            or operations <= 0
            or ops_per_sec <= 0
        ):
            continue
        loaded.append(
            {
                "machine": machine,
                "ordering": ordering,
                "alignment": alignment,
                "block_size": block_size,
                "updaters": updaters,
                "repeat": repeat,
                "layout": layout,
                "method": method,
                "operations": operations,
                "ops_per_sec": ops_per_sec,
                "cas_failures": cas_failures,
                "cas_retries_per_update": (
                    cas_failures / operations if method in CAS_METHODS else None
                ),
                "wide_loads_per_update": (
                    wide_loads / operations
                    if layout == "mixed_u128_u64"
                    else None
                ),
            }
        )
    return loaded


def summarize(values):
    count = len(values)
    mean = statistics.fmean(values)
    stddev = statistics.stdev(values) if count > 1 else 0.0
    sem = stddev / math.sqrt(count)
    return {"mean": mean, "stddev": stddev, "sem": sem, "samples": count}


def aggregate(samples):
    grouped = {}
    for sample in samples:
        key = (
            sample["machine"],
            sample["ordering"],
            sample["alignment"],
            sample["updaters"],
            sample["layout"],
            sample["method"],
            sample["block_size"],
        )
        entry = grouped.setdefault(
            key, {"throughput": [], "retries": [], "wide_loads": []}
        )
        entry["throughput"].append(sample["ops_per_sec"])
        if sample["cas_retries_per_update"] is not None:
            entry["retries"].append(sample["cas_retries_per_update"])
        if sample["wide_loads_per_update"] is not None:
            entry["wide_loads"].append(sample["wide_loads_per_update"])

    aggregated = {}
    for (
        machine,
        ordering,
        alignment,
        updaters,
        layout,
        method,
        block_size,
    ), values in grouped.items():
        stats = {"throughput": summarize(values["throughput"])}
        if values["retries"]:
            stats["retries"] = summarize(values["retries"])
        if values["wide_loads"]:
            stats["wide_loads"] = summarize(values["wide_loads"])
        aggregated.setdefault(machine, {}).setdefault(ordering, {}).setdefault(
            alignment, {}
        ).setdefault(updaters, {})[(layout, method, block_size)] = stats
    return aggregated


def error_value(stats, error_bars):
    if error_bars == "none":
        return 0.0
    return stats[error_bars]


def available_blocks(updater_groups, layout, method):
    return sorted(
        {
            block_size
            for groups in updater_groups.values()
            for series_layout, series_method, block_size in groups
            if series_layout == layout and series_method == method
        }
    )


def select_weighted_winners(updater_groups):
    """Select one block per layout/method, weighting larger thread counts more."""
    winners = {}
    updater_counts = sorted(updater_groups)
    for layout, method in SERIES:
        blocks = available_blocks(updater_groups, layout, method)
        if not blocks:
            continue

        maxima = {}
        for updaters in updater_counts:
            values = [
                updater_groups[updaters][(layout, method, block)]["throughput"][
                    "mean"
                ]
                for block in blocks
                if (layout, method, block) in updater_groups[updaters]
            ]
            if values:
                maxima[updaters] = max(values)

        candidates = []
        for block in blocks:
            weighted_normalized = 0.0
            weighted_raw = 0.0
            total_weight = 0.0
            covered = []
            for updaters in updater_counts:
                stats = updater_groups[updaters].get((layout, method, block))
                if stats is None or updaters not in maxima:
                    continue
                throughput = stats["throughput"]["mean"]
                weight = float(updaters)
                weighted_normalized += weight * throughput / maxima[updaters]
                weighted_raw += weight * throughput
                total_weight += weight
                covered.append(updaters)
            if total_weight:
                candidates.append(
                    (
                        weighted_normalized / total_weight,
                        len(covered),
                        weighted_raw / total_weight,
                        block,
                    )
                )
        if candidates:
            score, coverage, raw_score, block = max(candidates)
            winners[(layout, method)] = {
                "block_size": block,
                "score": score,
                "covered_thread_counts": coverage,
                "weighted_mean_ops_per_sec": raw_score,
            }
    return winners


def write_machine_csv(path, ordering_groups):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.writer(stream)
        writer.writerow(
            (
                "ordering",
                "alignment",
                "updaters",
                "block_size",
                "layout",
                "method",
                "mean_ops_per_sec",
                "stddev_ops_per_sec",
                "sem_ops_per_sec",
                "samples",
                "mean_cas_retries_per_update",
                "mean_wide_loads_per_update",
            )
        )
        for ordering in sorted(ordering_groups):
            for alignment in sorted(ordering_groups[ordering]):
                updater_groups = ordering_groups[ordering][alignment]
                for updaters in sorted(updater_groups):
                    for layout, method in SERIES:
                        for block_size in available_blocks(
                            updater_groups, layout, method
                        ):
                            stats = updater_groups[updaters].get(
                                (layout, method, block_size)
                            )
                            if stats is None:
                                continue
                            throughput = stats["throughput"]
                            retries = stats.get("retries")
                            wide_loads = stats.get("wide_loads")
                            writer.writerow(
                                (
                                    ordering,
                                    alignment,
                                    updaters,
                                    block_size,
                                    layout,
                                    method,
                                    f"{throughput['mean']:.9f}",
                                    f"{throughput['stddev']:.9f}",
                                    f"{throughput['sem']:.9f}",
                                    throughput["samples"],
                                    f"{retries['mean']:.9f}" if retries else "",
                                    (
                                        f"{wide_loads['mean']:.9f}"
                                        if wide_loads
                                        else ""
                                    ),
                                )
                            )


def write_winners_csv(path, ordering_groups):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.writer(stream)
        writer.writerow(
            (
                "ordering",
                "alignment",
                "layout",
                "method",
                "selected_block_size",
                "normalized_high_thread_weighted_score",
                "weighted_mean_ops_per_sec",
                "covered_thread_counts",
            )
        )
        for ordering in sorted(ordering_groups):
            for alignment in sorted(ordering_groups[ordering]):
                winners = select_weighted_winners(
                    ordering_groups[ordering][alignment]
                )
                for layout, method in SERIES:
                    winner = winners.get((layout, method))
                    if winner is None:
                        continue
                    writer.writerow(
                        (
                            ordering,
                            alignment,
                            layout,
                            method,
                            winner["block_size"],
                            f"{winner['score']:.9f}",
                            f"{winner['weighted_mean_ops_per_sec']:.9f}",
                            winner["covered_thread_counts"],
                        )
                    )


def monochrome_shades(base_hex, count):
    """Return light-to-dark shades that preserve one base hue."""
    base = tuple(
        int(base_hex[index : index + 2], 16) / 255.0 for index in (1, 3, 5)
    )
    if count == 1:
        positions = [0.5]
    else:
        positions = [index / (count - 1) for index in range(count)]

    shades = []
    for position in positions:
        # Small blocks begin as a 62% white tint; large blocks end as a
        # 20% black shade. Intermediate sizes pass through the base hue.
        mix = 0.62 - 0.82 * position
        if mix >= 0:
            rgb = tuple(channel * (1.0 - mix) + mix for channel in base)
        else:
            rgb = tuple(channel * (1.0 + mix) for channel in base)
        shades.append(tuple(max(0.0, min(1.0, channel)) for channel in rgb))
    return shades


def render_block_sweep_plot(
    plt,
    machine,
    ordering,
    alignment,
    updater_groups,
    layout,
    method,
    out_dir,
    error_bars,
):
    updater_counts = sorted(updater_groups)
    blocks = available_blocks(updater_groups, layout, method)
    if not blocks:
        return None

    fig, ax = plt.subplots(figsize=(max(7.8, len(updater_counts) * 1.05), 5.2))
    for block, color in zip(
        blocks, monochrome_shades(METHOD_COLORS[method], len(blocks))
    ):
        counts = []
        values = []
        errors = []
        for updaters in updater_counts:
            stats = updater_groups[updaters].get((layout, method, block))
            if stats is None:
                continue
            throughput = stats["throughput"]
            counts.append(updaters)
            values.append(throughput["mean"] / 1_000_000.0)
            errors.append(error_value(throughput, error_bars) / 1_000_000.0)
        ax.errorbar(
            counts,
            values,
            yerr=errors if error_bars != "none" else None,
            fmt=LAYOUT_MARKERS[layout],
            markersize=LAYOUT_MARKER_SIZES[layout],
            capsize=3,
            color=color,
            linestyle="-",
            linewidth=2,
            label=f"block {block}",
        )

    ax.set_xscale("log", base=2)
    ax.set_xticks(updater_counts, [str(value) for value in updater_counts])
    ax.set_xlabel("Updater threads")
    ax.set_ylabel("Successful updates (million/sec)")
    ax.set_title(
        f"{machine}: {SERIES_LABELS[(layout, method)]}\n"
        f"Block-size sweep ({ordering}, pointer alignment {alignment})"
    )
    ax.grid(axis="y", alpha=0.25)
    ax.legend(title="Shade: small (light) → large (dark)")
    fig.tight_layout()
    path = out_dir / (
        f"block_sweep_{layout}_{method}_{ordering}_align{alignment}.png"
    )
    fig.savefig(path, dpi=160)
    plt.close(fig)
    return path


def render_winner_comparison(
    plt,
    machine,
    ordering,
    alignment,
    updater_groups,
    out_dir,
    error_bars,
):
    updater_counts = sorted(updater_groups)
    winners = select_weighted_winners(updater_groups)
    if not winners:
        return []

    fig, ax = plt.subplots(figsize=(max(11.5, len(updater_counts) * 1.1), 5.8))
    for layout, method in SERIES:
        winner = winners.get((layout, method))
        if winner is None:
            continue
        block = winner["block_size"]
        counts = []
        values = []
        errors = []
        for updaters in updater_counts:
            stats = updater_groups[updaters].get((layout, method, block))
            if stats is None:
                continue
            throughput = stats["throughput"]
            counts.append(updaters)
            values.append(throughput["mean"] / 1_000_000.0)
            errors.append(error_value(throughput, error_bars) / 1_000_000.0)
        ax.errorbar(
            counts,
            values,
            yerr=errors if error_bars != "none" else None,
            fmt=LAYOUT_MARKERS[layout],
            markersize=LAYOUT_MARKER_SIZES[layout],
            capsize=3,
            color=METHOD_COLORS[method],
            linestyle="-",
            linewidth=2,
            label=f"{SERIES_LABELS[(layout, method)]} (block {block})",
        )

    ax.set_xscale("log", base=2)
    ax.set_xticks(updater_counts, [str(value) for value in updater_counts])
    ax.set_xlabel("Updater threads")
    ax.set_ylabel("Successful updates (million/sec)")
    ax.set_title(
        f"{machine}: high-thread-weighted best block per update type\n"
        f"{ordering}, pointer alignment {alignment}"
    )
    ax.grid(axis="y", alpha=0.25)
    ax.legend(
        loc="center left",
        bbox_to_anchor=(1.01, 0.5),
        fontsize="small",
        title="Color = method; square/star = layout",
    )
    fig.tight_layout()
    suffix = f"{ordering}_align{alignment}"
    comparison_path = out_dir / f"weighted_best_comparison_{suffix}.png"
    fig.savefig(comparison_path, dpi=160)
    plt.close(fig)

    retry_series = [
        (layout, method)
        for layout, method in SERIES
        if method in CAS_METHODS and (layout, method) in winners
    ]
    fig, ax = plt.subplots(figsize=(max(11.0, len(updater_counts) * 1.05), 5.2))
    plotted = False
    for layout, method in retry_series:
        block = winners[(layout, method)]["block_size"]
        counts = []
        values = []
        errors = []
        for updaters in updater_counts:
            retries = updater_groups[updaters].get(
                (layout, method, block), {}
            ).get("retries")
            if retries is None:
                continue
            counts.append(updaters)
            values.append(retries["mean"])
            errors.append(error_value(retries, error_bars))
        if not counts:
            continue
        plotted = True
        ax.errorbar(
            counts,
            values,
            yerr=errors if error_bars != "none" else None,
            fmt=LAYOUT_MARKERS[layout],
            markersize=LAYOUT_MARKER_SIZES[layout],
            capsize=3,
            color=METHOD_COLORS[method],
            linestyle="-",
            linewidth=2,
            label=f"{SERIES_LABELS[(layout, method)]} (block {block})",
        )
    if not plotted:
        plt.close(fig)
        return [comparison_path]

    ax.set_xscale("log", base=2)
    ax.set_xticks(updater_counts, [str(value) for value in updater_counts])
    ax.set_xlabel("Updater threads")
    ax.set_ylabel("Failed CAS attempts per successful update")
    ax.set_title(
        f"{machine}: retry pressure for weighted-best blocks\n"
        f"{ordering}, pointer alignment {alignment}"
    )
    ax.grid(axis="y", alpha=0.25)
    ax.legend(
        loc="center left",
        bbox_to_anchor=(1.01, 0.5),
        fontsize="small",
        title="Color = method; square/star = layout",
    )
    fig.tight_layout()
    retry_path = out_dir / f"weighted_best_cas_retries_{suffix}.png"
    fig.savefig(retry_path, dpi=160)
    plt.close(fig)
    return [comparison_path, retry_path]


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Plot monochromatic block-size sweeps and high-thread-weighted "
            "winners from bench_atomic_updates."
        )
    )
    parser.add_argument(
        "files",
        nargs="*",
        help=(
            "Atomic update benchmark JSON paths, or basenames that filter "
            "--runs-dir; may be supplied separately or comma-separated"
        ),
    )
    parser.add_argument(
        "--runs-dir",
        action="append",
        default=[],
        help="Recursively discover JSON files under this directory; repeatable",
    )
    parser.add_argument(
        "--out-dir",
        default="bench_results/plots",
        help="Plot output root (default: bench_results/plots)",
    )
    parser.add_argument(
        "--error-bars",
        choices=("sem", "stddev", "none"),
        default="sem",
        help="Error bar statistic (default: sem)",
    )
    args = parser.parse_args()

    files = collect_jsons(args.files, args.runs_dir)
    if not files:
        parser.error("provide JSON files or at least one --runs-dir")
    samples = []
    for path in files:
        samples.extend(load_samples(path))
    if not samples:
        print("No schema-v3 atomic update benchmark samples found.")
        return 1

    aggregated = aggregate(samples)
    out_root = Path(args.out_dir)
    try:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        plt = None
        print("matplotlib is unavailable; writing CSV only.", file=sys.stderr)

    for machine in sorted(aggregated):
        machine_dir = out_root / machine / "atomic_updates"
        machine_dir.mkdir(parents=True, exist_ok=True)
        samples_csv = machine_dir / "atomic_updates.csv"
        winners_csv = machine_dir / "atomic_update_winners.csv"
        write_machine_csv(samples_csv, aggregated[machine])
        write_winners_csv(winners_csv, aggregated[machine])
        print(f"Wrote CSV: {samples_csv}")
        print(f"Wrote CSV: {winners_csv}")
        if plt is None:
            continue
        for ordering in sorted(aggregated[machine]):
            for alignment in sorted(aggregated[machine][ordering]):
                updater_groups = aggregated[machine][ordering][alignment]
                for layout, method in SERIES:
                    path = render_block_sweep_plot(
                        plt,
                        machine,
                        ordering,
                        alignment,
                        updater_groups,
                        layout,
                        method,
                        machine_dir,
                        args.error_bars,
                    )
                    if path is not None:
                        print(f"Wrote plot: {path}")
                for path in render_winner_comparison(
                    plt,
                    machine,
                    ordering,
                    alignment,
                    updater_groups,
                    machine_dir,
                    args.error_bars,
                ):
                    print(f"Wrote plot: {path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
