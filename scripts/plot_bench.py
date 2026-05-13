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
        bench_label_sort_key,
        format_ubq_label_parts,
        is_valid_ubq_params,
        parse_ubq_queue_label,
    )
except ImportError:
    from ubq_labels import (  # type: ignore
        UBQ_IMMEDIATE_DIMS,
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
        "complex_throughput": 1,
        "data_latency": 2,
        "producer_fairness": 3,
        "consumer_fairness": 4,
        "fairness_throughput": 5,
        "fairness": 6,
        "fill_drain": 7,
        "mutable_placeholder": 8,
    }
    derived_suffixes = {
        "push_elapsed": 1,
        "pop_elapsed": 2,
        "fill_elapsed": 3,
        "drain_elapsed": 4,
    }
    for suffix, suffix_priority in derived_suffixes.items():
        marker = f"_{suffix}"
        if name.endswith(marker):
            base = name[: -len(marker)]
            return (priority.get(base, 99), suffix_priority, name)
    return (priority.get(name, 99), 0, name)


def metric_column(mode: str):
    if mode == "data_latency":
        return "avg_data_latency_ns"
    if mode in ("producer_fairness", "consumer_fairness", "fairness"):
        return "fairness_ratio"
    if mode.endswith("_push_elapsed"):
        return "push_elapsed_ns"
    if mode.endswith("_pop_elapsed"):
        return "pop_elapsed_ns"
    if mode.endswith("_fill_elapsed"):
        return "fill_elapsed_ns"
    if mode.endswith("_drain_elapsed"):
        return "drain_elapsed_ns"
    return "ops_per_sec"


def metric_axis_label(mode: str):
    if mode == "data_latency":
        return "Average data latency (ns)"
    if mode in ("producer_fairness", "consumer_fairness", "fairness"):
        return "Fairness ratio (max/min)"
    if mode.endswith(("_push_elapsed", "_pop_elapsed", "_fill_elapsed", "_drain_elapsed")):
        return "Elapsed time (ns)"
    return "Ops/sec"


def metric_file_slug(mode: str):
    if mode == "data_latency":
        return "data_latency"
    if mode in ("producer_fairness", "consumer_fairness", "fairness"):
        return mode
    for suffix in ("push_elapsed", "pop_elapsed", "fill_elapsed", "drain_elapsed"):
        if mode.endswith(f"_{suffix}"):
            return suffix
    return "throughput"


def source_mode_display_name(mode: str):
    names = {
        "throughput": "throughput",
        "complex_throughput": "complex throughput",
        "data_latency": "data latency",
        "fairness": "fairness",
        "fill_drain": "fill/drain",
    }
    return names.get(mode, mode.replace("_", " "))


def metric_display_name(mode: str):
    names = {
        "throughput": "throughput",
        "complex_throughput": "complex throughput",
        "data_latency": "data latency",
        "producer_fairness": "producer fairness",
        "consumer_fairness": "consumer fairness",
        "fairness_throughput": "fairness throughput",
        "fill_drain": "fill/drain throughput",
    }
    for suffix, label in (
        ("push_elapsed", "push elapsed"),
        ("pop_elapsed", "pop elapsed"),
        ("fill_elapsed", "fill elapsed"),
        ("drain_elapsed", "drain elapsed"),
    ):
        marker = f"_{suffix}"
        if mode.endswith(marker):
            base = mode[: -len(marker)]
            return f"{source_mode_display_name(base)} {label}"
    return names.get(mode, mode.replace("_", " "))


def metric_lower_is_better(mode: str):
    return (
        mode == "data_latency"
        or mode in ("producer_fairness", "consumer_fairness", "fairness")
        or mode.endswith(("_push_elapsed", "_pop_elapsed", "_fill_elapsed", "_drain_elapsed"))
    )


def label_sort_key(label: str):
    if label.startswith("ubq_"):
        return (0, bench_label_sort_key(label[len("ubq_") :]))
    if label.startswith("ubq:"):
        return (0, bench_label_sort_key(label[len("ubq:") :]))
    if label.startswith("fastfifo_"):
        try:
            block_size = int(label[len("fastfifo_") :])
        except ValueError:
            block_size = 2**31
        return (1, 2, block_size, label)
    if label.startswith("lfqueue_"):
        try:
            segment_size = int(label[len("lfqueue_") :])
        except ValueError:
            segment_size = 2**31
        return (1, 3, segment_size, label)
    if label.startswith("wcq_"):
        try:
            capacity = int(label[len("wcq_") :])
        except ValueError:
            capacity = 2**31
        return (1, 4, capacity, label)
    order = {"segqueue": 0, "concurrent-queue": 1}
    return (1, order.get(label, 99), 0, label)


def baseline_queue_priority(label: str):
    if label.startswith("fastfifo_"):
        return 2
    if label.startswith("lfqueue_"):
        return 3
    if label.startswith("wcq_"):
        return 4
    return BASELINE_QUEUE_PRIORITY.get(label, 99)


def display_label(label: str):
    if label.startswith("fastfifo_"):
        return f"RBBQ/BBQ {label[len('fastfifo_'):]}"
    if label.startswith("lfqueue_"):
        return f"LSCQ {label[len('lfqueue_'):]}"
    if label.startswith("wcq_"):
        return f"wCQ {label[len('wcq_'):]}"
    return label


def queue_metadata(label: str):
    if label.startswith("ubq_"):
        return {
            "family": "UBQ",
            "variant": label[len("ubq_") :],
            "publication": "this repository",
            "capacity_model": "unbounded",
            "ordering": "strict FIFO",
        }
    if label == "segqueue":
        return {
            "family": "crossbeam SegQueue",
            "variant": "",
            "publication": "Crossbeam production baseline",
            "capacity_model": "unbounded",
            "ordering": "strict FIFO",
        }
    if label == "concurrent-queue":
        return {
            "family": "concurrent-queue",
            "variant": "",
            "publication": "Rust production baseline",
            "capacity_model": "unbounded",
            "ordering": "strict FIFO",
        }
    if label.startswith("fastfifo_"):
        return {
            "family": "RBBQ/BBQ",
            "variant": label[len("fastfifo_") :],
            "publication": "BBQ, USENIX ATC 2022",
            "capacity_model": "bounded/pre-sized",
            "ordering": "strict FIFO",
        }
    if label.startswith("lfqueue_"):
        return {
            "family": "LSCQ",
            "variant": label[len("lfqueue_") :],
            "publication": "Nikolaev, DISC 2019",
            "capacity_model": "unbounded linked SCQ",
            "ordering": "strict FIFO",
        }
    if label.startswith("wcq_"):
        return {
            "family": "wCQ",
            "variant": label[len("wcq_") :],
            "publication": "Nikolaev/Ravindran, SPAA 2022",
            "capacity_model": "bounded capacity variant",
            "ordering": "strict FIFO",
        }
    return {
        "family": label,
        "variant": "",
        "publication": "",
        "capacity_model": "",
        "ordering": "",
    }


def labels_by_ops_desc(entries):
    return labels_by_metric(entries, "throughput")


def labels_by_metric(entries, mode: str):
    lower_is_better = metric_lower_is_better(mode)
    return sorted(
        entries.keys(),
        key=lambda label: (
            entries[label]["mean_ops_per_sec"]
            if lower_is_better
            else -entries[label]["mean_ops_per_sec"],
            label_sort_key(label),
        ),
    )


def parse_ubq_variant(label: str):
    return parse_ubq_queue_label(label, require_valid=False)


def ubq_params_valid_for_scenario(params, scenario=None) -> bool:
    if not is_valid_ubq_params(params):
        return False
    if scenario is None:
        return True
    threads = parse_scenario_threads(scenario)
    if threads is None:
        return True
    producers, _consumers = threads
    try:
        block = int(params[2])
    except (TypeError, ValueError, IndexError):
        return False
    return block >= producers


def ubq_label_has_explicit_sync(label: str) -> bool:
    text = str(label).strip().lower()
    if text.startswith("ubq_") or text.startswith("ubq:"):
        text = text[4:]
    parts = [part.strip() for part in text.split(",") if part.strip()]
    if len(parts) == 1 and "_" in text:
        parts = [part.strip() for part in text.split("_") if part.strip()]
    return len(parts) >= 5


def format_ubq_variant_label(params, include_sync: bool = False) -> str:
    return "ubq_" + format_ubq_label_parts(
        params[0],
        params[1],
        params[2],
        params[3] if len(params) >= 4 else "",
        params[4] if len(params) >= 5 else "cas",
        include_sync=include_sync,
    )


def is_zero_pool_label(label: str) -> bool:
    params = parse_ubq_variant(label)
    return params is not None and params[1] == 0


def collect_ubq_plot_context(entries, scenario=None):
    labels = labels_by_ops_desc(entries)
    non_ubq_labels = []
    parsed = {}
    label_by_params = {}
    include_sync = False

    for label in labels:
        parsed_label = parse_ubq_variant(label)
        if parsed_label is None or not ubq_params_valid_for_scenario(parsed_label, scenario):
            non_ubq_labels.append(label)
            continue
        parsed[label] = parsed_label
        label_by_params.setdefault(parsed_label, label)
        include_sync = include_sync or ubq_label_has_explicit_sync(label)

    return labels, non_ubq_labels, parsed, label_by_params, include_sync


def immediate_winner_variant_report(entries, scenario=None):
    labels, non_ubq_labels, parsed, _label_by_params, _include_sync = collect_ubq_plot_context(
        entries,
        scenario,
    )

    if not parsed:
        return {
            "selected_labels": labels,
            "winner": None,
            "required_labels": [],
            "present_required_labels": [],
            "missing_required_labels": [],
            "zero_pool_labels": [],
        }

    winner, required = strict_immediate_winner_ubq_labels(entries, scenario)
    required_labels = sorted(required, key=label_sort_key)
    present_required_labels = [label for label in required_labels if label in entries]
    missing_required_labels = [label for label in required_labels if label not in entries]
    zero_pool_labels = [label for label in required_labels if is_zero_pool_label(label)]

    selected_set = set(non_ubq_labels)
    selected_set.update(present_required_labels)
    selected_labels = [label for label in labels if label in selected_set]

    return {
        "selected_labels": selected_labels,
        "winner": winner,
        "required_labels": required_labels,
        "present_required_labels": present_required_labels,
        "missing_required_labels": missing_required_labels,
        "zero_pool_labels": zero_pool_labels,
    }


def empty_immediate_variant_report(selected_labels):
    return {
        "selected_labels": selected_labels,
        "winner": None,
        "required_labels": [],
        "present_required_labels": [],
        "missing_required_labels": [],
        "zero_pool_labels": [],
    }


def primary_plot_report(entries, mode: str, scenario=None):
    if mode in ("throughput", "complex_throughput"):
        return immediate_winner_variant_report(entries, scenario)
    return empty_immediate_variant_report(labels_by_metric(entries, mode))


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


def pool_neighbors(value, ordered_values):
    neighbors = immediate_domain_neighbors(value, ordered_values)
    if value != 0 and 0 in ordered_values and 0 not in neighbors:
        neighbors.append(0)
    return neighbors


def strict_immediate_winner_ubq_labels(entries, scenario=None):
    _labels, _non_ubq_labels, parsed, label_by_params, include_sync = collect_ubq_plot_context(
        entries,
        scenario,
    )
    if not parsed:
        return None, set()

    winner = max(parsed.keys(), key=lambda label: entries[label]["mean_ops_per_sec"])
    winner_params = parsed[winner]
    required_params = {winner_params}

    for idx, winner_value in enumerate(winner_params):
        ordered_values = UBQ_IMMEDIATE_DIMS.get(idx)
        if ordered_values is None:
            continue
        if idx == 1:
            neighbor_values = pool_neighbors(winner_value, ordered_values)
        else:
            neighbor_values = immediate_domain_neighbors(winner_value, ordered_values)
        for neighbor_value in neighbor_values:
            variant = list(winner_params)
            variant[idx] = neighbor_value
            candidate = tuple(variant)
            if ubq_params_valid_for_scenario(candidate, scenario):
                required_params.add(candidate)

    required = set()
    for params in required_params:
        required.add(
            label_by_params.get(params)
            or format_ubq_variant_label(params, include_sync=include_sync)
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
    for pattern in (
        "*_throughput.csv",
        "*_throughput.png",
        "*_data_latency.csv",
        "*_data_latency.png",
        "*_producer_fairness.csv",
        "*_producer_fairness.png",
        "*_consumer_fairness.csv",
        "*_consumer_fairness.png",
        "*_push_elapsed.csv",
        "*_push_elapsed.png",
        "*_pop_elapsed.csv",
        "*_pop_elapsed.png",
        "*_fill_elapsed.csv",
        "*_fill_elapsed.png",
        "*_drain_elapsed.csv",
        "*_drain_elapsed.png",
    ):
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

        queue = rec.get("queue")
        scenario = scenario_meta
        mode = str(rec.get("mode", "throughput"))

        if queue == "ubq":
            queue_label = f"ubq_{ubq_label}"
        else:
            queue_label = str(queue)

        metric_specs = []
        if mode == "data_latency":
            metric_specs.append(("data_latency", "avg_data_latency_ns"))
        elif mode == "fairness":
            metric_specs.extend(
                (
                    ("fairness_throughput", "ops_per_sec"),
                    ("producer_fairness", "producer_fairness_ratio"),
                    ("consumer_fairness", "consumer_fairness_ratio"),
                )
            )
        else:
            metric_specs.append((mode, "ops_per_sec"))

        for suffix, field in (
            ("push_elapsed", "push_elapsed_ns"),
            ("pop_elapsed", "pop_elapsed_ns"),
            ("fill_elapsed", "fill_elapsed_ns"),
            ("drain_elapsed", "drain_elapsed_ns"),
        ):
            if rec.get(field) is not None:
                metric_specs.append((f"{mode}_{suffix}", field))

        for output_mode, field in metric_specs:
            raw_value = rec.get(field)
            if raw_value is None:
                continue

            try:
                metric_value = float(raw_value)
            except (TypeError, ValueError):
                continue

            yield machine_label, output_mode, scenario, queue_label, metric_value


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


def write_csv(out_path: Path, mode: str, values):
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            ["queue", metric_column(mode), "stddev", "sem", "samples"]
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
                "is_zero_pool",
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
                    "yes" if is_zero_pool_label(label) else "no",
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


def format_metric_value(mode: str, value: float):
    if mode in ("producer_fairness", "consumer_fairness", "fairness"):
        return f"{value:,.3f}"
    if mode == "data_latency" or mode.endswith(("_push_elapsed", "_pop_elapsed", "_fill_elapsed", "_drain_elapsed")):
        return f"{value:,.0f} ns"
    return f"{value:,.0f}"


def average_ops_per_sec(values):
    return sum(values) / len(values) if values else 0.0


def scenario_line_labels(entries_by_scenario, max_series: int, mode: str):
    label_samples = {}
    label_coverage = {}
    for entries in entries_by_scenario.values():
        for label, stats in entries.items():
            label_samples.setdefault(label, []).append(stats["mean_ops_per_sec"])
            label_coverage[label] = label_coverage.get(label, 0) + 1

    lower_is_better = metric_lower_is_better(mode)
    labels = sorted(
        label_samples.keys(),
        key=lambda label: (
            baseline_queue_priority(label),
            -label_coverage[label],
            average_ops_per_sec(label_samples[label])
            if lower_is_better
            else -average_ops_per_sec(label_samples[label]),
            label_sort_key(label),
        ),
    )
    if max_series <= 0:
        return labels
    return labels[:max_series]


def write_scenario_line_csv(out_path: Path, mode: str, scenarios, labels, entries_by_scenario):
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            ["scenario", "queue", metric_column(mode), "stddev", "sem", "samples"]
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


def write_queue_metadata_csv(out_path: Path, labels):
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "queue",
                "family",
                "variant",
                "publication",
                "capacity_model",
                "ordering",
            ]
        )
        for label in labels:
            meta = queue_metadata(label)
            writer.writerow(
                [
                    label,
                    meta["family"],
                    meta["variant"],
                    meta["publication"],
                    meta["capacity_model"],
                    meta["ordering"],
                ]
            )
    return out_path


def annotate_immediate_variant_status(ax, coverage_csv_name: str, report):
    required_labels = report["required_labels"]
    if not required_labels:
        return

    missing_required_labels = report["missing_required_labels"]
    zero_pool_labels = report["zero_pool_labels"]
    zero_pool_missing = [
        label for label in zero_pool_labels if label in missing_required_labels
    ]
    if missing_required_labels:
        note_lines = [
            "Required UBQ set incomplete",
            f"Present: {len(report['present_required_labels'])}/{len(required_labels)}",
        ]
        if zero_pool_missing:
            note_lines.append(f"Missing pool=0: {len(zero_pool_missing)}")
        note_lines.append(f"See {coverage_csv_name}")
        note = "\n".join(note_lines)
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
            "label": display_label(label),
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
    ax.set_ylabel(metric_axis_label(mode))
    ax.set_title(f"{machine}: {metric_display_name(mode)} scaling")
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
        help="Keep pre-existing generated CSV/PNG outputs in --out-dir.",
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
        print("No benchmark records found in input files.")
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
                report = primary_plot_report(entries, mode, scenario)
                labels = report["selected_labels"]
                values = [(label, entries[label]) for label in labels]
                slug = metric_file_slug(mode)
                csv_path = out_root / machine / "csv" / mode / f"{scenario}_{slug}.csv"
                write_csv(csv_path, mode, values)
                print(f"Wrote CSV: {csv_path}")
                if report["required_labels"] and mode in ("throughput", "complex_throughput"):
                    coverage_csv_path = (
                        out_root
                        / machine
                        / "csv"
                        / mode
                        / f"{scenario}_immediate_variants_{slug}.csv"
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
                            f"{len(report['missing_required_labels'])} required UBQ variant(s)",
                            file=sys.stderr,
                        )

    for machine in sorted(grouped):
        for mode in sorted(grouped[machine], key=mode_sort_key):
            scenarios = sorted(grouped[machine][mode], key=scaling_scenario_sort_key)
            entries_by_scenario = grouped[machine][mode]
            labels = scenario_line_labels(entries_by_scenario, args.max_line_series, mode)
            slug = metric_file_slug(mode)
            csv_path = out_root / machine / "csv" / mode / f"scenarios_line_{slug}.csv"
            write_scenario_line_csv(csv_path, mode, scenarios, labels, entries_by_scenario)
            print(f"Wrote CSV: {csv_path}")
            all_labels = sorted(
                {
                    label
                    for entries in entries_by_scenario.values()
                    for label in entries.keys()
                },
                key=label_sort_key,
            )
            metadata_csv_path = out_root / machine / "csv" / mode / "queue_metadata.csv"
            write_queue_metadata_csv(metadata_csv_path, all_labels)
            print(f"Wrote CSV: {metadata_csv_path}")

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
                report = primary_plot_report(entries, mode, scenario)
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
                ax.set_xticks(
                    bar_positions,
                    [display_label(label) for label in labels],
                    rotation=30,
                    ha="right",
                )
                ax.set_ylabel(metric_axis_label(mode))
                ax.set_title(f"{machine}: {metric_display_name(mode)} {scenario}")
                ax.grid(axis="y", linestyle=":", alpha=0.4)
                slug = metric_file_slug(mode)
                if mode in ("throughput", "complex_throughput"):
                    annotate_immediate_variant_status(
                        ax,
                        f"{scenario}_immediate_variants_{slug}.csv",
                        report,
                    )

                if metric_lower_is_better(mode):
                    best_idx = min(range(len(values)), key=lambda i: values[i])
                else:
                    best_idx = max(range(len(values)), key=lambda i: values[i])
                best_label = labels[best_idx]
                best_value = values[best_idx]
                ax.axhline(
                    best_value,
                    color="tab:red",
                    linestyle="--",
                    linewidth=1.25,
                    label=(
                        f"Best mean: {display_label(best_label)} "
                        f"({format_metric_value(mode, best_value)})"
                    ),
                )
                ax.legend(loc="upper left")
                fig.tight_layout()

                png_path = out_root / machine / mode / f"{scenario}_{slug}.png"
                png_path.parent.mkdir(parents=True, exist_ok=True)
                fig.savefig(png_path, dpi=200)
                print(f"Wrote PNG: {png_path}")
                plt.close(fig)

    for machine in sorted(grouped):
        for mode in sorted(grouped[machine], key=mode_sort_key):
            scenarios = sorted(grouped[machine][mode], key=scaling_scenario_sort_key)
            entries_by_scenario = grouped[machine][mode]
            labels = scenario_line_labels(entries_by_scenario, args.max_line_series, mode)
            slug = metric_file_slug(mode)
            png_path = out_root / machine / mode / f"scenarios_line_{slug}.png"
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
