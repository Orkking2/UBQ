#!/usr/bin/env python

import argparse
import csv
import json
import math
import os
import sys
from pathlib import Path

try:
    from scripts.ubq_labels import (
        UBQ_IMMEDIATE_DIMS,
        UBQ_MIN_POOL_SIZE,
        UBQ_NO_POOL_SIZE,
        UBQ_NO_POOL_VERSION,
        UBQ_POOLED_VERSIONS,
        bench_label_sort_key,
        format_ubq_label_parts,
        is_valid_ubq_params,
        parse_ubq_queue_label,
    )
except ImportError:
    from ubq_labels import (  # type: ignore
        UBQ_IMMEDIATE_DIMS,
        UBQ_MIN_POOL_SIZE,
        UBQ_NO_POOL_SIZE,
        UBQ_NO_POOL_VERSION,
        UBQ_POOLED_VERSIONS,
        bench_label_sort_key,
        format_ubq_label_parts,
        is_valid_ubq_params,
        parse_ubq_queue_label,
    )

LEGACY_SCENARIO_MAP = {
    "spsc": "1p1c",
    "mpsc": "4p1c",
    "spmc": "1p4c",
    "mpmc": "4p4c",
}


def normalize_scenario(name: str) -> str:
    key = str(name).strip().lower()
    return LEGACY_SCENARIO_MAP.get(key, key)


def scenario_sort_key(name: str):
    scenario = normalize_scenario(name)
    if "p" in scenario and scenario.endswith("c"):
        producer_part, consumer_part = scenario[:-1].split("p", 1)
        if producer_part.isdigit() and consumer_part.isdigit():
            return (0, int(producer_part), int(consumer_part), scenario)
    return (1, scenario)


def mode_sort_key(name: str):
    priority = {
        "throughput": 0,
        "fill_drain": 1,
        "mutable_placeholder": 2,
    }
    return (priority.get(name, 99), name)


def label_sort_key(label: str):
    if label.startswith("ubq_"):
        return (0, bench_label_sort_key(label[len("ubq_") :]))
    if label.startswith("ubq:"):
        return (0, bench_label_sort_key(label[len("ubq:") :]))
    order = {"segqueue": 1, "concurrent-queue": 2}
    return (1, order.get(label, 99), label)


def labels_by_ops_desc(entries):
    return sorted(
        entries.keys(),
        key=lambda label: (-entries[label]["mean_ops_per_sec"], label_sort_key(label)),
    )


def parse_ubq_variant(label: str):
    return parse_ubq_queue_label(label, require_valid=False)


def one_step_ubq_labels(entries):
    labels = labels_by_ops_desc(entries)
    parsed = {}
    non_ubq_labels = []
    for label in labels:
        parsed_label = parse_ubq_variant(label)
        if parsed_label is None:
            non_ubq_labels.append(label)
            continue
        parsed[label] = parsed_label

    if not parsed:
        return labels

    ubq_ranked = sorted(
        parsed.keys(),
        key=lambda label: (-entries[label]["mean_ops_per_sec"], label_sort_key(label)),
    )
    winner = ubq_ranked[0]
    winner_params = parsed[winner]
    selected = set(non_ubq_labels)
    selected.add(winner)

    param_count = len(winner_params)
    # Include the no-pool v6 baseline for any pooled winner at the same block/backoff.
    if param_count >= 3:
        winner_version, winner_pool, winner_block = winner_params[:3]
        winner_backoff = winner_params[3] if param_count >= 4 else ""
        if winner_version == UBQ_NO_POOL_VERSION or winner_version in UBQ_POOLED_VERSIONS:
            v6_baseline_label = "ubq_" + format_ubq_label_parts(
                UBQ_NO_POOL_VERSION,
                UBQ_NO_POOL_SIZE,
                winner_block,
                winner_backoff,
            )
            if v6_baseline_label in entries:
                selected.add(v6_baseline_label)

        if winner_version == UBQ_NO_POOL_VERSION:
            for version in sorted(UBQ_POOLED_VERSIONS):
                cross_version_label = "ubq_" + format_ubq_label_parts(
                    version,
                    UBQ_MIN_POOL_SIZE,
                    winner_block,
                    winner_backoff,
                )
                if cross_version_label in entries:
                    selected.add(cross_version_label)

    # Always include all versions for the winner's non-version parameters.
    if param_count >= 1:
        for label, params in parsed.items():
            if len(params) != param_count:
                continue
            if all(params[j] == winner_params[j] for j in range(1, param_count)):
                selected.add(label)

    # Keep one-step neighbors for non-version dimensions.
    for idx in range(1, param_count):
        lower = None
        upper = None
        lower_value = None
        upper_value = None

        for label, params in parsed.items():
            if label == winner:
                continue
            if len(params) != param_count:
                continue
            if any(params[j] != winner_params[j] for j in range(param_count) if j != idx):
                continue

            value = params[idx]
            winner_value = winner_params[idx]
            if value < winner_value:
                if lower is None or value > lower_value:
                    lower = label
                    lower_value = value
            elif value > winner_value:
                if upper is None or value < upper_value:
                    upper = label
                    upper_value = value

        if lower is not None:
            selected.add(lower)
        if upper is not None:
            selected.add(upper)

    return [label for label in labels if label in selected]


def immediate_domain_neighbors(value, ordered_values):
    try:
        idx = ordered_values.index(value)
    except ValueError:
        return []

    neighbors = []
    if idx > 0:
        neighbors.append(ordered_values[idx - 1])
    if idx + 1 < len(ordered_values):
        neighbors.append(ordered_values[idx + 1])
    return neighbors


def strict_immediate_winner_ubq_labels(entries):
    parsed = {}
    for label in entries:
        parsed_label = parse_ubq_variant(label)
        if parsed_label is not None and is_valid_ubq_params(parsed_label):
            parsed[label] = parsed_label

    if not parsed:
        return None, set()

    winner = max(parsed.keys(), key=lambda label: entries[label]["mean_ops_per_sec"])
    winner_params = parsed[winner]
    required = {winner}

    for idx, winner_value in enumerate(winner_params):
        ordered_values = UBQ_IMMEDIATE_DIMS.get(idx)
        if ordered_values is None:
            continue
        for neighbor_value in immediate_domain_neighbors(winner_value, ordered_values):
            variant = list(winner_params)
            variant[idx] = neighbor_value
            candidate = tuple(variant)
            if is_valid_ubq_params(candidate):
                required.add(
                    "ubq_"
                    + format_ubq_label_parts(
                        candidate[0],
                        candidate[1],
                        candidate[2],
                        candidate[3] if len(candidate) >= 4 else "",
                    )
                )

    if len(winner_params) >= 3:
        winner_version, winner_pool, winner_block = winner_params[:3]
        winner_backoff = winner_params[3] if len(winner_params) >= 4 else ""

        # Pooled-family versions should compare at the winner's pool + block.
        if winner_version in UBQ_POOLED_VERSIONS:
            for version in sorted(UBQ_POOLED_VERSIONS):
                pooled_peer = (version, winner_pool, winner_block, winner_backoff)
                if is_valid_ubq_params(pooled_peer):
                    required.add(
                        "ubq_"
                        + format_ubq_label_parts(
                            pooled_peer[0], pooled_peer[1], pooled_peer[2], pooled_peer[3]
                        )
                    )

        # If no-pool v6 wins, compare against each pooled version at its minimum pool.
        if winner_version == UBQ_NO_POOL_VERSION:
            for version in sorted(UBQ_POOLED_VERSIONS):
                pooled_peer = (version, UBQ_MIN_POOL_SIZE, winner_block, winner_backoff)
                if is_valid_ubq_params(pooled_peer):
                    required.add(
                        "ubq_"
                        + format_ubq_label_parts(
                            pooled_peer[0], pooled_peer[1], pooled_peer[2], pooled_peer[3]
                        )
                    )

        # Include the no-pool v6 baseline for any pooled winner at the same block/backoff.
        if winner_version == UBQ_NO_POOL_VERSION or winner_version in UBQ_POOLED_VERSIONS:
            v6_baseline = (UBQ_NO_POOL_VERSION, UBQ_NO_POOL_SIZE, winner_block, winner_backoff)
            if is_valid_ubq_params(v6_baseline):
                required.add(
                    "ubq_"
                    + format_ubq_label_parts(
                        v6_baseline[0], v6_baseline[1], v6_baseline[2], v6_baseline[3]
                    )
                )

    return winner, required


def has_complete_immediate_winner_variants(entries):
    _winner, required = strict_immediate_winner_ubq_labels(entries)
    if not required:
        return False
    return required.issubset(entries.keys())


def ensure_mplconfigdir(out_dir: Path):
    if os.environ.get("MPLCONFIGDIR"):
        return

    default_mpl_dir = Path.home() / ".matplotlib"
    if default_mpl_dir.exists() and os.access(default_mpl_dir, os.W_OK):
        return

    fallback_mpl_dir = out_dir / ".mplconfig"
    fallback_mpl_dir.mkdir(parents=True, exist_ok=True)
    os.environ["MPLCONFIGDIR"] = str(fallback_mpl_dir)


def clear_generated_outputs(out_root: Path):
    if not out_root.exists():
        return

    removed = 0
    for pattern in ("*_throughput.csv", "*_throughput.png"):
        for path in out_root.rglob(pattern):
            if not path.is_file():
                continue
            path.unlink()
            removed += 1

    for path in sorted(out_root.rglob("*"), reverse=True):
        if not path.is_dir():
            continue
        try:
            path.rmdir()
        except OSError:
            pass

    if removed:
        print(f"Removed {removed} stale plot artifact(s) under: {out_root}")


def load_records(path: Path):
    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as exc:
        print(f"warning: could not parse {path}: {exc}", file=sys.stderr)
        return

    meta = data.get("meta", {})
    ubq_label = str(meta.get("ubq_label", "default"))
    machine_label = str(meta.get("machine_label", "local")).strip() or "local"

    for rec in data.get("results", []):
        if rec.get("skipped_reason"):
            continue

        ops = rec.get("ops_per_sec")
        if ops is None:
            continue

        queue = rec.get("queue")
        scenario = normalize_scenario(rec.get("scenario", ""))
        mode = str(rec.get("mode", "throughput"))

        if queue == "ubq":
            queue_label = f"ubq_{ubq_label}"
        else:
            queue_label = str(queue)

        try:
            ops_value = float(ops)
        except (TypeError, ValueError):
            continue

        yield machine_label, mode, scenario, queue_label, ops_value


def summarize_ops(samples):
    sample_count = len(samples)
    mean_ops = sum(samples) / sample_count
    if sample_count > 1:
        variance = sum((value - mean_ops) ** 2 for value in samples) / (sample_count - 1)
        stddev = math.sqrt(variance)
    else:
        stddev = 0.0
    sem = stddev / math.sqrt(sample_count) if sample_count > 0 else 0.0

    return {
        "mean_ops_per_sec": mean_ops,
        "stddev_ops_per_sec": stddev,
        "sem_ops_per_sec": sem,
        "samples": sample_count,
    }


def write_csv(out_path: Path, values):
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            ["queue", "ops_per_sec", "stddev_ops_per_sec", "sem_ops_per_sec", "samples"]
        )
        for label, stats in values:
            writer.writerow(
                [
                    label,
                    f"{stats['mean_ops_per_sec']:.6f}",
                    f"{stats['stddev_ops_per_sec']:.6f}",
                    f"{stats['sem_ops_per_sec']:.6f}",
                    stats["samples"],
                ]
            )
    return out_path


def error_values(entries, labels, error_bars: str):
    if error_bars == "none":
        return None
    if error_bars == "stddev":
        return [entries[label]["stddev_ops_per_sec"] for label in labels]
    if error_bars == "sem":
        return [entries[label]["sem_ops_per_sec"] for label in labels]
    raise ValueError(f"Unknown error bar mode: {error_bars}")


def main():
    parser = argparse.ArgumentParser(description="Plot UBQ benchmark throughput.")
    parser.add_argument("files", nargs="+", help="Benchmark JSON files")
    parser.add_argument(
        "--out-dir",
        default="bench_results/plots",
        help="Output root for plots and CSVs",
    )
    parser.add_argument(
        "--error-bars",
        choices=["sem", "stddev", "none"],
        default="sem",
        help="Vertical error bars from repeated runs (default: sem)",
    )
    parser.add_argument(
        "--no-clean",
        action="store_true",
        help="Keep pre-existing *_throughput CSV/PNG outputs in --out-dir.",
    )
    args = parser.parse_args()

    out_root = Path(args.out_dir)
    raw_data = {}
    sample_points = 0

    for file in args.files:
        path = Path(file)
        for machine, mode, scenario, label, ops in load_records(path):
            key = (machine, mode, scenario, label)
            raw_data.setdefault(key, []).append(ops)
            sample_points += 1

    if sample_points == 0:
        print("No throughput records found in input files.")
        return

    if not args.no_clean:
        clear_generated_outputs(out_root)

    grouped = {}
    for (machine, mode, scenario, label), samples in raw_data.items():
        grouped.setdefault(machine, {}).setdefault(mode, {}).setdefault(scenario, {})[label] = (
            summarize_ops(samples)
        )

    for machine in sorted(grouped):
        for mode in sorted(grouped[machine], key=mode_sort_key):
            for scenario in sorted(grouped[machine][mode], key=scenario_sort_key):
                entries = grouped[machine][mode][scenario]
                labels = one_step_ubq_labels(entries)
                values = [(label, entries[label]) for label in labels]
                csv_path = out_root / machine / "csv" / mode / f"{scenario}_throughput.csv"
                write_csv(csv_path, values)
                print(f"Wrote CSV: {csv_path}")

    ensure_mplconfigdir(out_root)
    try:
        import matplotlib.pyplot as plt
    except ImportError:
        print("matplotlib not found; wrote CSVs only.")
        return

    for machine in sorted(grouped):
        for mode in sorted(grouped[machine], key=mode_sort_key):
            for scenario in sorted(grouped[machine][mode], key=scenario_sort_key):
                entries = grouped[machine][mode][scenario]
                labels = one_step_ubq_labels(entries)
                values = [entries[label]["mean_ops_per_sec"] for label in labels]
                if not values:
                    continue

                yerr = error_values(entries, labels, args.error_bars)
                if yerr is not None and all(value == 0.0 for value in yerr):
                    yerr = None

                fig, ax = plt.subplots(figsize=(10, 6))
                bar_positions = range(len(labels))
                bar_kwargs = {}
                if yerr is not None:
                    bar_kwargs["yerr"] = yerr
                    bar_kwargs["capsize"] = 3
                ax.bar(bar_positions, values, **bar_kwargs)
                ax.set_xticks(bar_positions, labels, rotation=30, ha="right")
                ax.set_ylabel("Ops/sec")
                ax.set_title(f"{machine}: {mode} {scenario}")
                ax.grid(axis="y", linestyle=":", alpha=0.4)

                if has_complete_immediate_winner_variants(entries):
                    ax.text(
                        0.99,
                        0.99,
                        "Complete: all immediate UBQ variants present",
                        transform=ax.transAxes,
                        ha="right",
                        va="top",
                        fontsize=9,
                        bbox={
                            "boxstyle": "round,pad=0.25",
                            "facecolor": "#e8f5e9",
                            "edgecolor": "#2e7d32",
                            "linewidth": 0.8,
                            "alpha": 0.9,
                        },
                    )

                best_idx = max(range(len(values)), key=lambda i: values[i])
                best_label = labels[best_idx]
                best_value = values[best_idx]
                ax.axhline(
                    best_value,
                    color="tab:red",
                    linestyle="--",
                    linewidth=1.25,
                    label=f"Best mean: {best_label} ({best_value:,.0f} ops/sec)",
                )
                ax.legend(loc="upper left")
                fig.tight_layout()

                png_path = out_root / machine / mode / f"{scenario}_throughput.png"
                png_path.parent.mkdir(parents=True, exist_ok=True)
                fig.savefig(png_path, dpi=200)
                print(f"Wrote PNG: {png_path}")
                plt.close(fig)


if __name__ == "__main__":
    main()
