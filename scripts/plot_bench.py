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
BASELINE_QUEUE_PRIORITY = {
    "segqueue": 0,
    "concurrent-queue": 1,
}
LINE_MARKERS = ("o", "s", "^", "D", "v", "P", "X", "<", ">", "*")


def collect_run_jsons(runs_dir: Path):
    if not runs_dir.exists():
        return []

    return sorted(path for path in runs_dir.rglob("*.json") if path.is_file())


def preferred_plot_python():
    script_path = Path(__file__).resolve()
    repo_root = script_path.parent.parent
    venv_candidates = (
        repo_root / ".venv" / "bin" / "python",
        repo_root / ".venv" / "Scripts" / "python.exe",
    )

    for candidate in venv_candidates:
        if candidate.is_file():
            return candidate
    return None


def normalize_scenario(name: str) -> str:
    key = str(name).strip().lower()
    return LEGACY_SCENARIO_MAP.get(key, key)


def parse_scenario_threads(name: str):
    scenario = normalize_scenario(name)
    if "p" not in scenario or not scenario.endswith("c"):
        return None
    producer_part, consumer_part = scenario[:-1].split("p", 1)
    if not producer_part.isdigit() or not consumer_part.isdigit():
        return None
    producers = int(producer_part)
    consumers = int(consumer_part)
    if producers <= 0 or consumers <= 0:
        return None
    return producers, consumers


def scenario_sort_key(name: str):
    scenario = normalize_scenario(name)
    threads = parse_scenario_threads(scenario)
    if threads is not None:
        producers, consumers = threads
        return (0, producers, consumers, scenario)
    return (1, scenario)


def scaling_scenario_sort_key(name: str):
    scenario = normalize_scenario(name)
    threads = parse_scenario_threads(scenario)
    if threads is not None:
        producers, consumers = threads
        return (0, producers + consumers, scenario)
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


def immediate_winner_variant_report(entries):
    labels = labels_by_ops_desc(entries)
    non_ubq_labels = []
    parsed = {}
    for label in labels:
        parsed_label = parse_ubq_variant(label)
        if parsed_label is None or not is_valid_ubq_params(parsed_label):
            non_ubq_labels.append(label)
            continue
        parsed[label] = parsed_label

    if not parsed:
        return {
            "selected_labels": labels,
            "winner": None,
            "required_labels": [],
            "present_required_labels": [],
            "missing_required_labels": [],
        }

    winner, required = strict_immediate_winner_ubq_labels(entries)
    required_labels = sorted(required, key=label_sort_key)
    present_required_labels = [label for label in required_labels if label in entries]
    missing_required_labels = [label for label in required_labels if label not in entries]

    selected_set = set(non_ubq_labels)
    selected_set.update(present_required_labels)
    selected_labels = [label for label in labels if label in selected_set]

    return {
        "selected_labels": selected_labels,
        "winner": winner,
        "required_labels": required_labels,
        "present_required_labels": present_required_labels,
        "missing_required_labels": missing_required_labels,
    }

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
                        candidate[4] if len(candidate) >= 5 else "cas",
                    )
                )

    if len(winner_params) >= 3:
        winner_version, winner_pool, winner_block = winner_params[:3]
        winner_backoff = winner_params[3] if len(winner_params) >= 4 else ""
        winner_sync = winner_params[4] if len(winner_params) >= 5 else "cas"

        # Pooled-family versions should compare at the winner's pool + block.
        if winner_version in UBQ_POOLED_VERSIONS:
            for version in sorted(UBQ_POOLED_VERSIONS):
                pooled_peer = (version, winner_pool, winner_block, winner_backoff, winner_sync)
                if is_valid_ubq_params(pooled_peer):
                    required.add(
                        "ubq_"
                        + format_ubq_label_parts(
                            pooled_peer[0],
                            pooled_peer[1],
                            pooled_peer[2],
                            pooled_peer[3],
                            pooled_peer[4],
                        )
                    )

        # If no-pool v6 wins, compare against each pooled version at its minimum pool.
        if winner_version == UBQ_NO_POOL_VERSION:
            for version in sorted(UBQ_POOLED_VERSIONS):
                pooled_peer = (
                    version,
                    UBQ_MIN_POOL_SIZE,
                    winner_block,
                    winner_backoff,
                    winner_sync,
                )
                if is_valid_ubq_params(pooled_peer):
                    required.add(
                        "ubq_"
                        + format_ubq_label_parts(
                            pooled_peer[0],
                            pooled_peer[1],
                            pooled_peer[2],
                            pooled_peer[3],
                            pooled_peer[4],
                        )
                    )

        # Include the no-pool v6 baseline for any pooled winner at the same block/backoff.
        if winner_version == UBQ_NO_POOL_VERSION or winner_version in UBQ_POOLED_VERSIONS:
            v6_baseline = (
                UBQ_NO_POOL_VERSION,
                UBQ_NO_POOL_SIZE,
                winner_block,
                winner_backoff,
                winner_sync,
            )
            if is_valid_ubq_params(v6_baseline):
                required.add(
                    "ubq_"
                    + format_ubq_label_parts(
                        v6_baseline[0],
                        v6_baseline[1],
                        v6_baseline[2],
                        v6_baseline[3],
                        v6_baseline[4],
                    )
                )

    return winner, required


def ensure_plot_runtime_env(out_dir: Path):
    if not os.environ.get("MPLBACKEND"):
        os.environ["MPLBACKEND"] = "Agg"

    if not os.environ.get("MPLCONFIGDIR"):
        default_mpl_dir = Path.home() / ".matplotlib"
        if not (default_mpl_dir.exists() and os.access(default_mpl_dir, os.W_OK)):
            fallback_mpl_dir = out_dir / ".mplconfig"
            fallback_mpl_dir.mkdir(parents=True, exist_ok=True)
            os.environ["MPLCONFIGDIR"] = str(fallback_mpl_dir)

    if not os.environ.get("XDG_CACHE_HOME"):
        fallback_cache_dir = out_dir / ".cache"
        fallback_cache_dir.mkdir(parents=True, exist_ok=True)
        os.environ["XDG_CACHE_HOME"] = str(fallback_cache_dir)


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

    if data.get("schema_version") not in (2, "2"):
        return

    meta = data.get("meta", {})
    ubq_label = str(meta.get("ubq_label", "default"))
    machine_label = str(meta.get("machine_label", "local")).strip() or "local"
    scenario_meta = normalize_scenario(meta.get("scenario", ""))

    for rec in data.get("results", []):
        if rec.get("skipped_reason"):
            continue

        ops = rec.get("ops_per_sec")
        if ops is None:
            continue

        queue = rec.get("queue")
        scenario = scenario_meta
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


def write_immediate_variant_csv(out_path: Path, entries, winner, required_labels):
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "queue",
                "status",
                "is_winner",
                "ops_per_sec",
                "stddev_ops_per_sec",
                "sem_ops_per_sec",
                "samples",
            ]
        )
        for label in required_labels:
            stats = entries.get(label)
            writer.writerow(
                [
                    label,
                    "present" if stats is not None else "missing",
                    "yes" if label == winner else "no",
                    f"{stats['mean_ops_per_sec']:.6f}" if stats is not None else "",
                    f"{stats['stddev_ops_per_sec']:.6f}" if stats is not None else "",
                    f"{stats['sem_ops_per_sec']:.6f}" if stats is not None else "",
                    stats["samples"] if stats is not None else "",
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


def error_value(stats, error_bars: str):
    if error_bars == "none":
        return None
    if error_bars == "stddev":
        return stats["stddev_ops_per_sec"]
    if error_bars == "sem":
        return stats["sem_ops_per_sec"]
    raise ValueError(f"Unknown error bar mode: {error_bars}")


def average_ops_per_sec(values):
    return sum(values) / len(values) if values else 0.0


def scenario_line_labels(entries_by_scenario, max_series: int):
    label_samples = {}
    label_coverage = {}
    for entries in entries_by_scenario.values():
        for label, stats in entries.items():
            label_samples.setdefault(label, []).append(stats["mean_ops_per_sec"])
            label_coverage[label] = label_coverage.get(label, 0) + 1

    labels = sorted(
        label_samples.keys(),
        key=lambda label: (
            BASELINE_QUEUE_PRIORITY.get(label, 99),
            -label_coverage[label],
            -average_ops_per_sec(label_samples[label]),
            label_sort_key(label),
        ),
    )
    if max_series <= 0:
        return labels
    return labels[:max_series]


def write_scenario_line_csv(out_path: Path, scenarios, labels, entries_by_scenario):
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            ["scenario", "queue", "ops_per_sec", "stddev_ops_per_sec", "sem_ops_per_sec", "samples"]
        )
        for scenario in scenarios:
            entries = entries_by_scenario[scenario]
            for label in labels:
                stats = entries.get(label)
                if stats is None:
                    continue
                writer.writerow(
                    [
                        scenario,
                        label,
                        f"{stats['mean_ops_per_sec']:.6f}",
                        f"{stats['stddev_ops_per_sec']:.6f}",
                        f"{stats['sem_ops_per_sec']:.6f}",
                        stats["samples"],
                    ]
                )
    return out_path


def annotate_immediate_variant_status(ax, coverage_csv_name: str, report):
    required_labels = report["required_labels"]
    if not required_labels:
        return

    missing_required_labels = report["missing_required_labels"]
    if missing_required_labels:
        note = (
            "Immediate UBQ set incomplete\n"
            f"Present: {len(report['present_required_labels'])}/{len(required_labels)}\n"
            f"See {coverage_csv_name}"
        )
        bbox = {
            "boxstyle": "round,pad=0.25",
            "facecolor": "#fff3e0",
            "edgecolor": "#ef6c00",
            "linewidth": 0.8,
            "alpha": 0.95,
        }
    else:
        note = "Complete: all immediate UBQ variants present"
        bbox = {
            "boxstyle": "round,pad=0.25",
            "facecolor": "#e8f5e9",
            "edgecolor": "#2e7d32",
            "linewidth": 0.8,
            "alpha": 0.9,
        }

    ax.text(
        0.99,
        0.99,
        note,
        transform=ax.transAxes,
        ha="right",
        va="top",
        fontsize=9,
        bbox=bbox,
    )


def plot_scenario_lines(plt, out_path: Path, machine: str, mode: str, scenarios, labels, entries_by_scenario, error_bars: str):
    if not scenarios or not labels:
        return

    width = max(11.0, 0.6 * len(scenarios) + 5.5)
    fig, ax = plt.subplots(figsize=(width, 6.5))
    x_positions = list(range(len(scenarios)))
    color_map = plt.get_cmap("tab20", max(len(labels), 1))

    for idx, label in enumerate(labels):
        xs = []
        ys = []
        yerrs = []
        for x_pos, scenario in zip(x_positions, scenarios):
            stats = entries_by_scenario[scenario].get(label)
            if stats is None:
                continue
            xs.append(x_pos)
            ys.append(stats["mean_ops_per_sec"])
            err = error_value(stats, error_bars)
            if err is not None:
                yerrs.append(err)

        if not xs:
            continue

        plot_kwargs = {
            "label": label,
            "color": color_map(idx),
            "marker": LINE_MARKERS[idx % len(LINE_MARKERS)],
            "linewidth": 1.8,
            "markersize": 5,
        }
        if yerrs and any(value != 0.0 for value in yerrs):
            ax.errorbar(xs, ys, yerr=yerrs, capsize=3, **plot_kwargs)
        else:
            ax.plot(xs, ys, **plot_kwargs)

    ax.set_xticks(x_positions, scenarios, rotation=40, ha="right")
    ax.set_xlabel("Scenario (XpYc)")
    ax.set_ylabel("Ops/sec")
    ax.set_title(f"{machine}: {mode} scaling")
    ax.grid(axis="y", linestyle=":", alpha=0.4)
    ax.legend(loc="center left", bbox_to_anchor=(1.02, 0.5), fontsize=9, frameon=False)
    fig.tight_layout(rect=(0, 0, 0.84, 1))

    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, dpi=200, bbox_inches="tight")
    plt.close(fig)


def main():
    parser = argparse.ArgumentParser(description="Plot UBQ benchmark throughput.")
    parser.add_argument("files", nargs="*", help="Benchmark JSON files")
    parser.add_argument(
        "--runs-dir",
        help="Recursively load benchmark JSON files from a runs directory tree",
    )
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
    parser.add_argument(
        "--max-line-series",
        type=int,
        default=10,
        help="Maximum configs shown in per-machine scenario line charts; <=0 shows all (default: 10)",
    )
    args = parser.parse_args()

    files = [Path(file) for file in args.files]
    if args.runs_dir:
        files.extend(collect_run_jsons(Path(args.runs_dir)))

    if not files:
        parser.error("provide at least one benchmark JSON file or --runs-dir")

    out_root = Path(args.out_dir)
    raw_data = {}
    sample_points = 0

    for path in files:
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
                report = immediate_winner_variant_report(entries)
                labels = report["selected_labels"]
                values = [(label, entries[label]) for label in labels]
                csv_path = out_root / machine / "csv" / mode / f"{scenario}_throughput.csv"
                write_csv(csv_path, values)
                print(f"Wrote CSV: {csv_path}")
                if report["required_labels"]:
                    coverage_csv_path = (
                        out_root
                        / machine
                        / "csv"
                        / mode
                        / f"{scenario}_immediate_variants_throughput.csv"
                    )
                    write_immediate_variant_csv(
                        coverage_csv_path,
                        entries,
                        report["winner"],
                        report["required_labels"],
                    )
                    print(f"Wrote CSV: {coverage_csv_path}")
                    if report["missing_required_labels"]:
                        print(
                            f"warning: {machine} {mode} {scenario} is missing "
                            f"{len(report['missing_required_labels'])} immediate UBQ variant(s)",
                            file=sys.stderr,
                        )

    for machine in sorted(grouped):
        for mode in sorted(grouped[machine], key=mode_sort_key):
            scenarios = sorted(grouped[machine][mode], key=scaling_scenario_sort_key)
            entries_by_scenario = grouped[machine][mode]
            labels = scenario_line_labels(entries_by_scenario, args.max_line_series)
            csv_path = out_root / machine / "csv" / mode / "scenarios_line_throughput.csv"
            write_scenario_line_csv(csv_path, scenarios, labels, entries_by_scenario)
            print(f"Wrote CSV: {csv_path}")

    ensure_plot_runtime_env(out_root)
    try:
        import matplotlib.pyplot as plt
    except ImportError:
        preferred_python = preferred_plot_python()
        current_python = Path(sys.executable).resolve()
        if preferred_python is not None and preferred_python.resolve() != current_python:
            print(
                "matplotlib not found in "
                f"{current_python}; try rerunning with {preferred_python}. "
                "Wrote CSVs only."
            )
        else:
            print("matplotlib not found; install requirements-plot.txt for PNG output. Wrote CSVs only.")
        return

    for machine in sorted(grouped):
        for mode in sorted(grouped[machine], key=mode_sort_key):
            for scenario in sorted(grouped[machine][mode], key=scenario_sort_key):
                entries = grouped[machine][mode][scenario]
                report = immediate_winner_variant_report(entries)
                labels = report["selected_labels"]
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
                annotate_immediate_variant_status(
                    ax,
                    f"{scenario}_immediate_variants_throughput.csv",
                    report,
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

    for machine in sorted(grouped):
        for mode in sorted(grouped[machine], key=mode_sort_key):
            scenarios = sorted(grouped[machine][mode], key=scaling_scenario_sort_key)
            entries_by_scenario = grouped[machine][mode]
            labels = scenario_line_labels(entries_by_scenario, args.max_line_series)
            png_path = out_root / machine / mode / "scenarios_line_throughput.png"
            plot_scenario_lines(
                plt,
                png_path,
                machine,
                mode,
                scenarios,
                labels,
                entries_by_scenario,
                args.error_bars,
            )
            print(f"Wrote PNG: {png_path}")


if __name__ == "__main__":
    main()
