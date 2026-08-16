#!/usr/bin/env python

import argparse
import csv
import json
import math
import os
import statistics
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
THROUGHPUT_SPEEDUP_BASELINES = (
    ("segqueue", "SegQueue", lambda label: label == "segqueue"),
    ("bbq", "best BBQ", lambda label: queue_metadata(label)["family"] == "RBBQ/BBQ"),
    ("lscq", "best LSCQ", lambda label: queue_metadata(label)["family"] == "LSCQ"),
)
UBQ_BATCHED_PREFIX = "ubq_batched_"
DUBQ_BATCHED_PREFIX = "dubq_batched_"
SEGQUEUE_BATCHED_PREFIX = "segqueue_batched_"
PUBLICATION_SERIES = (
    ("segqueue", lambda label: label == "segqueue"),
    ("SegQueue batched", lambda label: parse_batched_segqueue_label(label) is not None),
    ("concurrent-queue", lambda label: label == "concurrent-queue"),
    ("BBQ", lambda label: queue_metadata(label)["family"] == "RBBQ/BBQ"),
    ("LSCQ", lambda label: queue_metadata(label)["family"] == "LSCQ"),
    ("wCQ", lambda label: queue_metadata(label)["family"] == "wCQ"),
    ("UBQ", lambda label: queue_metadata(label)["family"] == "UBQ"),
    ("UBQ batched", lambda label: queue_metadata(label)["family"] == "UBQ batched"),
    ("DUBQ", lambda label: queue_metadata(label)["family"] == "DUBQ"),
    ("DUBQ batched", lambda label: queue_metadata(label)["family"] == "DUBQ batched"),
)
FAMILY_DISPLAY_LABELS = {
    "RBBQ/BBQ": "BBQ",
    "crossbeam SegQueue batched": "SegQueueb",
    "UBQ batched": "UBQb",
    "DUBQ batched": "DUBQb",
}
QUEUE_FAMILY_PRIORITY = {
    "crossbeam SegQueue": 0,
    "crossbeam SegQueue batched": 1,
    "concurrent-queue": 2,
    "RBBQ/BBQ": 3,
    "LSCQ": 4,
    "wCQ": 5,
    "UBQ": 6,
    "UBQ batched": 7,
    "DUBQ": 8,
    "DUBQ batched": 9,
}
OBSOLETE_MODE_PREFIXES = (
    "app_log_",
    "complex_throughput",
    "data_latency",
)
MACHINE_ORDER = {
    "grace": 0,
    "hebrides": 1,
    "mn5": 2,
}
MACHINE_DISPLAY_LABELS = {
    "grace": "Grace",
    "hebrides": "N1",
    "mn5": "Xeon",
}
PUBLICATION_MACHINE_KEYS = frozenset(MACHINE_DISPLAY_LABELS)
PUBLICATION_TIME_SCALE = 1_000_000.0


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


def machine_sort_key(name: str):
    normalized = str(name).strip().lower()
    return (MACHINE_ORDER.get(normalized, 99), normalized)


def machine_display_label(name: str):
    normalized = str(name).strip().lower()
    return MACHINE_DISPLAY_LABELS.get(normalized, str(name))


def scenario_family(name: str):
    threads = parse_scenario_threads(name)
    if threads is None:
        return None
    producers, consumers = threads
    if producers == 1 and consumers == 1:
        return "spsc"
    if consumers == 1:
        return "mpsc"
    if producers == 1:
        return "spmc"
    if producers == consumers:
        return "mpmc"
    return "mixed"


def family_axis_label(family):
    return {
        "mpsc": "Producers",
        "spmc": "Consumers",
        "mpmc": "Producer/consumer threads",
        "symmetric": "Symmetric scenario (XpXc)",
    }.get(family, "Scenario (XpYc)")


def mode_sort_key(name: str):
    priority = {
        "throughput": 0,
        "producer_fairness": 3,
        "consumer_fairness": 4,
        "fairness_throughput": 5,
        "fairness": 6,
        "fill_drain": 7,
        "app_pipeline": 9,
        "app_task_roundtrip": 10,
        "mutable_placeholder": 12,
    }
    derived_suffixes = {
        "push_elapsed": 1,
        "pop_elapsed": 2,
        "fill_elapsed": 3,
        "drain_elapsed": 4,
        "data_latency": 5,
        "producer_throughput": 6,
        "consumer_throughput": 7,
    }
    for suffix, suffix_priority in derived_suffixes.items():
        marker = f"_{suffix}"
        if name.endswith(marker):
            base = name[: -len(marker)]
            return (priority.get(base, 99), suffix_priority, name)
    return (priority.get(name, 99), 0, name)


def is_obsolete_plot_mode(mode: str) -> bool:
    return str(mode).startswith(OBSOLETE_MODE_PREFIXES)


def metric_column(mode: str):
    if mode == "throughput_enqueue_ceiling":
        return "enqueue_ops_per_sec"
    if mode == "throughput_dequeue_ceiling":
        return "dequeue_ops_per_sec"
    if mode == "data_latency":
        return "avg_data_latency_ns"
    if mode.endswith("_data_latency"):
        return "avg_data_latency_ns"
    if mode.endswith("_producer_throughput"):
        return "producer_ops_per_sec"
    if mode.endswith("_consumer_throughput"):
        return "consumer_ops_per_sec"
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
    if mode == "throughput_enqueue_ceiling":
        return "Enqueue ops/sec"
    if mode == "throughput_dequeue_ceiling":
        return "Dequeue ops/sec"
    if mode == "data_latency":
        return "Average data latency (ns)"
    if mode.endswith("_data_latency"):
        return "Average data latency (ns)"
    if mode.endswith("_producer_throughput"):
        return "Producer ops/sec"
    if mode.endswith("_consumer_throughput"):
        return "Consumer ops/sec"
    if mode in ("producer_fairness", "consumer_fairness", "fairness"):
        return "Fairness ratio (max/min)"
    if mode.endswith(("_push_elapsed", "_pop_elapsed", "_fill_elapsed", "_drain_elapsed")):
        return "Elapsed time (ns)"
    return "Ops/sec"


def publication_metric_axis_label(mode: str):
    if mode.endswith("_push_elapsed"):
        return "Elapsed time (ms)"
    return metric_axis_label(mode)


def publication_metric_column(mode: str):
    if mode.endswith("_push_elapsed"):
        return "push_elapsed_ms"
    return metric_column(mode)


def publication_metric_value(mode: str, value: float):
    if mode.endswith("_push_elapsed"):
        return value / PUBLICATION_TIME_SCALE
    return value


def scenario_line_uses_log_y(mode: str):
    return mode.endswith("_push_elapsed")


def metric_file_slug(mode: str):
    if mode == "throughput_enqueue_ceiling":
        return "enqueue_ceiling"
    if mode == "throughput_dequeue_ceiling":
        return "dequeue_ceiling"
    if mode == "data_latency":
        return "data_latency"
    if mode.endswith("_data_latency"):
        return "data_latency"
    if mode.endswith("_producer_throughput"):
        return "producer_throughput"
    if mode.endswith("_consumer_throughput"):
        return "consumer_throughput"
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
        "app_log_fan_in": "app log fan-in",
        "app_pipeline": "app pipeline",
        "app_task_roundtrip": "app task roundtrip",
        "app_log_mpsc_file": "app log MPSC file",
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
        "app_log_fan_in": "app log fan-in throughput",
        "app_pipeline": "app pipeline throughput",
        "app_task_roundtrip": "app task roundtrip throughput",
        "app_log_mpsc_file": "app log MPSC file throughput",
    }
    for suffix, label in (
        ("push_elapsed", "push elapsed"),
        ("pop_elapsed", "pop elapsed"),
        ("fill_elapsed", "fill elapsed"),
        ("drain_elapsed", "drain elapsed"),
        ("data_latency", "data latency"),
        ("producer_throughput", "producer throughput"),
        ("consumer_throughput", "consumer throughput"),
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
        or mode.endswith(
            (
                "_push_elapsed",
                "_pop_elapsed",
                "_fill_elapsed",
                "_drain_elapsed",
                "_data_latency",
            )
        )
    )


def label_sort_key(label: str):
    dubq_batched = parse_batched_dubq_label(label)
    if dubq_batched is not None:
        batch_size, params = dubq_batched
        return (0, 3, batch_size, params)
    dubq = parse_dubq_variant(label)
    if dubq is not None:
        return (0, 2, dubq)
    batched = parse_batched_ubq_label(label)
    if batched is not None:
        batch_size, params = batched
        return (0, 1, batch_size, params)
    if label.startswith("ubq_"):
        return (0, 0, bench_label_sort_key(label[len("ubq_") :]))
    if label.startswith("ubq:"):
        return (0, bench_label_sort_key(label[len("ubq:") :]))
    segqueue_batch_size = parse_batched_segqueue_label(label)
    if segqueue_batch_size is not None:
        return (1, 1, segqueue_batch_size, label)
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
    order = {"segqueue": 0, "concurrent-queue": 2}
    return (1, order.get(label, 99), 0, label)


def baseline_queue_priority(label: str):
    if label.startswith("fastfifo_"):
        return 2
    if label.startswith("lfqueue_"):
        return 3
    if label.startswith("wcq_"):
        return 4
    return BASELINE_QUEUE_PRIORITY.get(label, 99)


def backoff_display_name(backoff: str) -> str:
    return {
        "": "Cycle",
        "b": "Yield",
        "crossbeam": "Cycle",
        "yield": "Yield",
    }.get(str(backoff).strip().lower(), str(backoff))


def format_ubq_display_params(params) -> str:
    _version, pool, block, backoff = params[:4]
    return f"{pool},{block},{backoff_display_name(backoff)}"


def format_dubq_display_params(params) -> str:
    pool, min_block, backoff = params
    return f"{pool},{min_block},{backoff_display_name(backoff)}"


def display_label(label: str):
    dubq_batched = parse_batched_dubq_label(label)
    if dubq_batched is not None:
        batch_size, params = dubq_batched
        return f"DUBQb ({batch_size}) {format_dubq_display_params(params)}"
    dubq = parse_dubq_variant(label)
    if dubq is not None:
        return f"DUBQ {format_dubq_display_params(dubq)}"
    batched = parse_batched_ubq_label(label)
    if batched is not None:
        batch_size, params = batched
        return f"UBQb ({batch_size}) {format_ubq_display_params(params)}"
    ubq = parse_ubq_variant(label)
    if ubq is not None:
        return f"UBQ {format_ubq_display_params(ubq)}"
    if label == "segqueue":
        return "SegQueue"
    segqueue_batch_size = parse_batched_segqueue_label(label)
    if segqueue_batch_size is not None:
        return f"SegQueueb ({segqueue_batch_size})"
    if label == "concurrent-queue":
        return "ConcurrentQueue"
    if label.startswith("fastfifo_"):
        return f"BBQ {label[len('fastfifo_'):]}"
    if label.startswith("lfqueue_"):
        return f"LSCQ {label[len('lfqueue_'):]}"
    if label.startswith("wcq_"):
        return f"wCQ {label[len('wcq_'):]}"
    return label


def queue_label_legend_title(labels) -> str | None:
    if not any(
        parse_ubq_variant(label) is not None
        or parse_batched_ubq_label(label) is not None
        or parse_dubq_variant(label) is not None
        or parse_batched_dubq_label(label) is not None
        for label in labels
    ):
        return None

    rows = (
        ("D?UBQb (BATCH_SZ)", "POOL_SZ,BLK_SZ,BACKOFF"),
        ("D?UBQ", "POOL_SZ,BLK_SZ,BACKOFF"),
    )
    left_width = max(len(left) for left, _right in rows)
    body = "\n".join(f"{left:<{left_width}}  {right}" for left, right in rows)
    return f"Queue label format\n{body}"


def queue_label_legend_kwargs(labels, *, size: int = 8):
    title = queue_label_legend_title(labels)
    if title is None:
        return {}
    return {
        "title": title,
        "title_fontproperties": {"family": "monospace", "size": size},
    }


def publication_display_label(label: str):
    family = queue_metadata(label)["family"]
    if family in (
        "crossbeam SegQueue batched",
        "UBQ",
        "UBQ batched",
        "DUBQ",
        "DUBQ batched",
        "RBBQ/BBQ",
        "LSCQ",
        "wCQ",
    ):
        return FAMILY_DISPLAY_LABELS.get(family, family)
    return display_label(label)


def queue_metadata(label: str):
    dubq_batched = parse_batched_dubq_label(label)
    if dubq_batched is not None:
        batch_size, params = dubq_batched
        return {
            "family": "DUBQ batched",
            "variant": f"({batch_size}) {format_dubq_display_params(params)}",
            "publication": "this repository (experimental)",
            "capacity_model": "unbounded",
            "ordering": "strict FIFO",
        }
    dubq = parse_dubq_variant(label)
    if dubq is not None:
        return {
            "family": "DUBQ",
            "variant": format_dubq_display_params(dubq),
            "publication": "this repository (experimental)",
            "capacity_model": "unbounded",
            "ordering": "strict FIFO",
        }
    batched = parse_batched_ubq_label(label)
    if batched is not None:
        batch_size, params = batched
        return {
            "family": "UBQ batched",
            "variant": f"({batch_size}) {format_ubq_display_params(params)}",
            "publication": "this repository",
            "capacity_model": "unbounded",
            "ordering": "strict FIFO",
        }
    if label.startswith("ubq_"):
        params = parse_ubq_variant(label)
        return {
            "family": "UBQ",
            "variant": (
                format_ubq_display_params(params)
                if params is not None
                else label[len("ubq_") :]
            ),
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
    segqueue_batch_size = parse_batched_segqueue_label(label)
    if segqueue_batch_size is not None:
        return {
            "family": "crossbeam SegQueue batched",
            "variant": f"({segqueue_batch_size})",
            "publication": "Crossbeam experimental batch branch",
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


def finite_positive(value) -> bool:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return False
    return math.isfinite(number) and number > 0.0


def mean_value(stats):
    try:
        return float(stats["mean_ops_per_sec"])
    except (KeyError, TypeError, ValueError):
        return float("nan")


def best_throughput_entry(entries, predicate):
    candidates = []
    for label, stats in entries.items():
        value = mean_value(stats)
        if (
            predicate(label)
            and finite_positive(value)
            and not stats.get("provisional", False)
        ):
            candidates.append((label, value))
    if not candidates:
        return None
    return sorted(candidates, key=lambda item: (-item[1], label_sort_key(item[0])))[0]


def best_ubq_throughput_entry(entries, scenario):
    def is_valid_ubq(label):
        params = parse_ubq_variant(label)
        return params is not None and ubq_params_valid_for_scenario(params, scenario)

    return best_throughput_entry(entries, is_valid_ubq)


def best_batched_ubq_throughput_entry(entries, scenario):
    def is_valid_batched_ubq(label):
        parsed = parse_batched_ubq_label(label)
        return parsed is not None and ubq_params_valid_for_scenario(parsed[1], scenario)

    return best_throughput_entry(entries, is_valid_batched_ubq)


def labels_by_ops_desc(entries):
    return labels_by_metric(entries, "throughput")


def labels_by_metric(entries, mode: str):
    lower_is_better = metric_lower_is_better(mode)
    return sorted(
        (
            label
            for label, stats in entries.items()
            if finite_positive(mean_value(stats))
        ),
        key=lambda label: (
            mean_value(entries[label])
            if lower_is_better
            else -mean_value(entries[label]),
            label_sort_key(label),
        ),
    )


def queue_family_sort_key(label: str):
    family = queue_metadata(label)["family"]
    return (
        QUEUE_FAMILY_PRIORITY.get(family, 99),
        family.lower(),
        label_sort_key(label),
    )


def queue_method_kind(label: str) -> str:
    if (
        parse_batched_segqueue_label(label) is not None
        or parse_batched_ubq_label(label) is not None
        or parse_batched_dubq_label(label) is not None
    ):
        return "batched"
    return "scalar"


def split_bar_entries_by_method(entries):
    groups = {"scalar": {}, "batched": {}}
    for label, stats in entries.items():
        groups[queue_method_kind(label)][label] = stats
    return groups


def queue_label_valid_for_scenario(label: str, scenario=None) -> bool:
    if parse_dubq_variant(label) is not None or parse_batched_dubq_label(label) is not None:
        return True
    params = parse_ubq_variant(label)
    if params is not None:
        return ubq_params_valid_for_scenario(params, scenario)
    batched = parse_batched_ubq_label(label)
    if batched is not None:
        return ubq_params_valid_for_scenario(batched[1], scenario)
    return True


def best_family_variation_labels(entries, mode: str, scenario=None):
    """Choose the best measured variation of every queue family for one plot."""
    entries_by_family = {}
    for label in labels_by_metric(entries, mode):
        if not queue_label_valid_for_scenario(label, scenario):
            continue
        family = queue_metadata(label)["family"]
        entries_by_family.setdefault(family, {})[label] = entries[label]

    selected = [
        labels_by_metric(family_entries, mode)[0]
        for family_entries in entries_by_family.values()
    ]
    return sorted(selected, key=queue_family_sort_key)


def parse_ubq_variant(label: str):
    return parse_ubq_queue_label(label, require_valid=False)


def parse_dubq_variant(label: str):
    text = str(label).strip().lower()
    if not text.startswith("dubq_") or text.startswith(DUBQ_BATCHED_PREFIX):
        return None
    parts = [part.strip() for part in text[len("dubq_") :].split(",")]
    if len(parts) != 3 or parts[2] not in ("crossbeam", "yield"):
        return None
    try:
        pool = int(parts[0])
        min_block = int(parts[1])
    except ValueError:
        return None
    if pool < 0 or min_block < 1:
        return None
    return pool, min_block, parts[2]


def format_batched_segqueue_label(batch_size: int) -> str:
    return f"{SEGQUEUE_BATCHED_PREFIX}{batch_size}"


def parse_batched_segqueue_label(label: str):
    text = str(label).strip().lower()
    if not text.startswith(SEGQUEUE_BATCHED_PREFIX):
        return None
    batch_text = text[len(SEGQUEUE_BATCHED_PREFIX) :]
    if not batch_text.isdigit():
        return None
    batch_size = int(batch_text)
    return batch_size if batch_size > 0 else None


def format_batched_dubq_label(dubq_label: str, batch_size: int) -> str:
    return f"{DUBQ_BATCHED_PREFIX}{batch_size}_{dubq_label}"


def parse_batched_dubq_label(label: str):
    text = str(label).strip().lower()
    if not text.startswith(DUBQ_BATCHED_PREFIX):
        return None
    remainder = text[len(DUBQ_BATCHED_PREFIX) :]
    batch_text, separator, dubq_label = remainder.partition("_")
    if not separator or not batch_text.isdigit():
        return None
    params = parse_dubq_variant(f"dubq_{dubq_label}")
    if params is None:
        return None
    return int(batch_text), params


def format_batched_ubq_label(ubq_label: str, batch_size: int) -> str:
    return f"{UBQ_BATCHED_PREFIX}{batch_size}_{ubq_label}"


def parse_batched_ubq_label(label: str):
    text = str(label)
    if not text.startswith(UBQ_BATCHED_PREFIX):
        return None
    remainder = text[len(UBQ_BATCHED_PREFIX) :]
    batch_text, separator, ubq_label = remainder.partition("_")
    if not separator or not batch_text.isdigit():
        return None
    params = parse_ubq_queue_label(f"ubq_{ubq_label}", require_valid=False)
    if params is None:
        return None
    return int(batch_text), params


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


def grid_winner_variant_report(entries, mode: str, scenario=None, coverage=None):
    selected_labels = best_family_variation_labels(entries, mode, scenario)
    scalar = next(
        (
            label
            for label in selected_labels
            if queue_metadata(label)["family"] == "UBQ"
            and not entries[label].get("provisional", False)
        ),
        None,
    )
    batched = next(
        (
            label
            for label in selected_labels
            if queue_metadata(label)["family"] == "UBQ batched"
            and not entries[label].get("provisional", False)
        ),
        None,
    )
    return {
        "selected_labels": selected_labels,
        "winner": scalar,
        "batched_winner": batched,
        "required_labels": [],
        "present_required_labels": [],
        "missing_required_labels": [],
        "zero_pool_labels": [],
        "grid_coverage": grid_coverage_report(coverage, mode),
    }


def primary_plot_report(entries, mode: str, scenario=None, coverage=None):
    return grid_winner_variant_report(entries, mode, scenario, coverage)


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
    parsed = {
        label: params
        for label, params in parsed.items()
        if not entries[label].get("provisional", False)
    }
    if not parsed:
        return None, set()

    winner = max(parsed.keys(), key=lambda label: entries[label]["mean_ops_per_sec"])
    winner_params = parsed[winner]
    required_params = {winner_params}
    historical_pool_sweep = any(params[1] != 1 for params in parsed.values())

    for idx, winner_value in enumerate(winner_params):
        ordered_values = UBQ_IMMEDIATE_DIMS.get(idx)
        if ordered_values is None:
            continue
        if idx == 1:
            if not historical_pool_sweep:
                continue
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
    obsolete_dirs = sorted(
        (
            path
            for path in out_root.rglob("*")
            if path.is_dir() and is_obsolete_plot_mode(path.name)
        ),
        reverse=True,
    )
    for mode_dir in obsolete_dirs:
        for path in sorted(mode_dir.rglob("*"), reverse=True):
            if path.is_file():
                path.unlink()
                removed += 1
            elif path.is_dir():
                try:
                    path.rmdir()
                except OSError:
                    pass
        try:
            mode_dir.rmdir()
        except OSError:
            pass

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
        "*_enqueue_ceiling.csv",
        "*_enqueue_ceiling.png",
        "*_dequeue_ceiling.csv",
        "*_dequeue_ceiling.png",
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


def load_record_samples(path: Path, include_legacy=False):
    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as exc:
        print(f"warning: could not parse {path}: {exc}", file=sys.stderr)
        return

    schema_version = data.get("schema_version")
    is_v6 = schema_version in (6, "6")
    if not is_v6 and not (
        include_legacy and schema_version in (2, "2", 3, "3", 4, "4", 5, "5")
    ):
        return

    meta = data.get("meta", {})
    core_placement = (
        str(meta.get("core_placement", "")).strip().lower()
        if schema_version in (4, "4", 5, "5", 6, "6")
        else "legacy_grouped"
    )
    if schema_version in (4, "4", 5, "5", 6, "6") and core_placement != "interleaved":
        return
    fingerprint = str(meta.get("experiment_fingerprint", "")).strip()
    if is_v6 and not fingerprint:
        return
    if not fingerprint:
        fingerprint = f"legacy-schema-{schema_version}"
    repeat_index = int(meta.get("repeat_index", 0) or 0)
    timestamp = int(meta.get("timestamp_unix_ms", 0) or 0)
    authoritative = is_v6 and bool(meta.get("affinity_authoritative", False))
    ubq_label = str(meta.get("ubq_label", "default"))
    machine_label = str(meta.get("machine_label", "local")).strip() or "local"
    scenario_meta = normalize_scenario(meta.get("scenario", ""))

    for rec in data.get("results", []):
        if rec.get("skipped_reason"):
            continue
        if rec.get("status", "completed") != "completed":
            continue

        queue = rec.get("queue")
        scenario = scenario_meta
        mode = str(rec.get("mode", "throughput"))
        if is_obsolete_plot_mode(mode):
            continue

        if queue == "ubq":
            batch_size = rec.get("batch_size")
            if batch_size is None:
                queue_label = f"ubq_{ubq_label}"
            else:
                try:
                    queue_label = format_batched_ubq_label(ubq_label, int(batch_size))
                except (TypeError, ValueError):
                    continue
        elif queue == "dubq":
            dubq_label = str(meta.get("dubq_label", ""))
            if not dubq_label:
                continue
            batch_size = rec.get("batch_size")
            if batch_size is None:
                queue_label = f"dubq_{dubq_label}"
            else:
                try:
                    queue_label = format_batched_dubq_label(dubq_label, int(batch_size))
                except (TypeError, ValueError):
                    continue
        elif queue == "segqueue" and rec.get("batch_size") is not None:
            try:
                queue_label = format_batched_segqueue_label(int(rec["batch_size"]))
            except (TypeError, ValueError):
                continue
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
            if mode == "throughput":
                throughput = rec.get("throughput_metrics") or {}
                metric_specs.extend(
                    (
                        ("throughput_enqueue_ceiling", (throughput, "enqueue_ops_per_sec")),
                        ("throughput_dequeue_ceiling", (throughput, "dequeue_ops_per_sec")),
                    )
                )
            if mode == "app_log_mpsc_file":
                metric_specs.extend(
                    (
                        (f"{mode}_producer_throughput", "producer_ops_per_sec"),
                        (f"{mode}_consumer_throughput", "consumer_ops_per_sec"),
                    )
                )
            elif mode.startswith("app_"):
                metric_specs.append((f"{mode}_data_latency", "avg_data_latency_ns"))

        for suffix, field in (
            ("push_elapsed", "push_elapsed_ns"),
            ("pop_elapsed", "pop_elapsed_ns"),
            ("fill_elapsed", "fill_elapsed_ns"),
            ("drain_elapsed", "drain_elapsed_ns"),
        ):
            if rec.get(field) is not None:
                metric_specs.append((f"{mode}_{suffix}", field))

        for output_mode, field in metric_specs:
            if isinstance(field, tuple):
                raw_value = field[0].get(field[1])
            else:
                raw_value = rec.get(field)
            if raw_value is None:
                continue

            try:
                metric_value = float(raw_value)
            except (TypeError, ValueError):
                continue

            yield {
                "machine": machine_label,
                "placement": core_placement,
                "mode": output_mode,
                "scenario": scenario,
                "queue": queue_label,
                "value": metric_value,
                "fingerprint": fingerprint,
                "repeat_index": repeat_index,
                "timestamp": timestamp,
                "authoritative": authoritative,
            }


def load_records(path: Path):
    """Compatibility view of schema-v6 records without deduplication metadata."""
    for sample in load_record_samples(path, include_legacy=True):
        yield (
            sample["machine"],
            sample["placement"],
            sample["mode"],
            sample["scenario"],
            sample["queue"],
            sample["value"],
        )


def deduplicate_logical_samples(samples):
    newest = {}
    for sample in samples:
        logical_key = (
            sample["fingerprint"],
            sample["scenario"],
            sample["queue"],
            sample["mode"],
            sample["repeat_index"],
        )
        previous = newest.get(logical_key)
        if previous is None or sample["timestamp"] >= previous["timestamp"]:
            newest[logical_key] = sample
    return list(newest.values())


def load_grid_coverage(path: Path, include_legacy=True):
    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        return

    schema_version = data.get("schema_version")
    if schema_version not in (6, "6") and not (
        include_legacy and schema_version in (3, "3", 4, "4", 5, "5")
    ):
        return
    meta = data.get("meta", {})
    core_placement = (
        str(meta.get("core_placement", "")).strip().lower()
        if schema_version in (4, "4", 5, "5", 6, "6")
        else "legacy_grouped"
    )
    if schema_version in (4, "4", 5, "5", 6, "6") and core_placement != "interleaved":
        return
    grid = str(meta.get("ubq_grid", "")).strip().lower()
    if grid not in ("sparse", "dense"):
        return
    try:
        expected_configs = int(meta.get("expected_ubq_configurations") or 0) + int(
            meta.get("expected_dubq_configurations") or 0
        )
        planned_repeats = int(meta["planned_repeats"])
        batch_sizes = tuple(sorted({int(value) for value in meta.get("ubq_batch_sizes", [])}))
        planned_items = tuple(
            sorted({int(value) for value in meta.get("planned_items_per_producer", [])})
        )
        repeat_index = int(meta["repeat_index"])
    except (KeyError, TypeError, ValueError):
        return
    if expected_configs <= 0 or planned_repeats <= 0 or not planned_items:
        return

    machine = str(meta.get("machine_label", "local")).strip() or "local"
    scenario = normalize_scenario(meta.get("scenario", ""))
    ubq_label = str(meta.get("ubq_label", ""))
    dubq_label = str(meta.get("dubq_label", ""))
    specification = {
        "grid": grid,
        "core_placement": core_placement,
        "expected_configurations": expected_configs,
        "planned_repeats": planned_repeats,
        "batch_sizes": batch_sizes,
        "planned_items": planned_items,
    }
    for record in data.get("results", []):
        queue = record.get("queue")
        if queue not in ("ubq", "dubq"):
            continue
        if record.get("status", "completed") != "completed":
            continue
        mode = str(record.get("mode", "throughput"))
        try:
            throughput = record.get("throughput_metrics") or {}
            items = int(
                throughput.get(
                    "requested_items_per_producer", record["items_per_producer"]
                )
            )
            batch_size = record.get("batch_size")
            batch_size = None if batch_size is None else int(batch_size)
        except (KeyError, TypeError, ValueError):
            continue
        config_label = ubq_label if queue == "ubq" else dubq_label
        if not config_label:
            continue
        sample = (queue, config_label, batch_size, repeat_index, items)
        record_specification = dict(specification)
        if mode == "throughput":
            record_specification["planned_items"] = planned_items[:1]
        yield machine, mode, scenario, record_specification, sample


def merge_grid_coverage(target, machine, mode, scenario, specification, sample):
    key = (machine, mode, scenario)
    current = target.get(key)
    rank = {"sparse": 0, "dense": 1}
    placement_rank = {"legacy_grouped": 0, "interleaved": 1}
    if current is None:
        current = dict(specification)
        current["present"] = set()
        target[key] = current
    elif placement_rank[specification["core_placement"]] > placement_rank[current["core_placement"]]:
        current = dict(specification)
        current["present"] = set()
        target[key] = current
    elif placement_rank[specification["core_placement"]] < placement_rank[current["core_placement"]]:
        return
    elif rank[specification["grid"]] > rank[current["grid"]]:
        present = current["present"]
        current = dict(specification)
        current["present"] = present
        target[key] = current
    elif rank[specification["grid"]] == rank[current["grid"]]:
        current["expected_configurations"] = max(
            current["expected_configurations"], specification["expected_configurations"]
        )
        current["planned_repeats"] = max(
            current["planned_repeats"], specification["planned_repeats"]
        )
        current["batch_sizes"] = tuple(
            sorted(set(current["batch_sizes"]) | set(specification["batch_sizes"]))
        )
        current["planned_items"] = tuple(
            sorted(set(current["planned_items"]) | set(specification["planned_items"]))
        )
    current["present"].add(sample)


def preferred_core_placements(raw_data):
    rank = {"legacy_grouped": 0, "interleaved": 1}
    selected = {}
    for machine, placement, mode, scenario, _label in raw_data:
        key = (machine, mode, scenario)
        if key not in selected or rank[placement] > rank[selected[key]]:
            selected[key] = placement
    return selected


def grid_coverage_report(coverage, mode: str):
    if not coverage:
        return {
            "known": False,
            "grid": None,
            "core_placement": None,
            "present": 0,
            "expected": 0,
            "percent": 0.0,
            "complete": False,
        }
    variants = 1 + len(coverage["batch_sizes"]) if mode == "throughput" else 1
    expected = (
        coverage["expected_configurations"]
        * coverage["planned_repeats"]
        * len(coverage["planned_items"])
        * variants
    )
    present = len(coverage["present"])
    return {
        "known": True,
        "grid": coverage["grid"],
        "core_placement": coverage["core_placement"],
        "present": present,
        "expected": expected,
        "percent": completion_percent_for_plot(present, expected),
        "complete": expected > 0 and present >= expected,
    }


def completion_percent_for_plot(completed: int, total: int) -> float:
    return 100.0 if total == 0 else 100.0 * completed / total


def aggregate_grid_coverage(coverages, mode: str):
    reports = [grid_coverage_report(coverage, mode) for coverage in coverages]
    if not reports or any(not report["known"] for report in reports):
        return grid_coverage_report(None, mode)
    present = sum(report["present"] for report in reports)
    expected = sum(report["expected"] for report in reports)
    grid = "dense" if any(report["grid"] == "dense" for report in reports) else "sparse"
    placements = {report["core_placement"] for report in reports}
    return {
        "known": True,
        "grid": grid,
        "core_placement": placements.pop() if len(placements) == 1 else "mixed",
        "present": present,
        "expected": expected,
        "percent": completion_percent_for_plot(present, expected),
        "complete": expected > 0 and all(report["complete"] for report in reports),
    }


def csv_stats_from_row(row, mode: str):
    metric_name = metric_column(mode)
    try:
        mean_value = float(row[metric_name])
    except (KeyError, TypeError, ValueError):
        return None

    def optional_float(name: str):
        raw = row.get(name, "")
        if raw in ("", None):
            return 0.0
        try:
            return float(raw)
        except (TypeError, ValueError):
            return 0.0

    raw_samples = row.get("samples", "")
    try:
        samples = int(float(raw_samples)) if raw_samples not in ("", None) else 1
    except (TypeError, ValueError):
        samples = 1

    return {
        "mean_ops_per_sec": mean_value,
        "median_ops_per_sec": mean_value,
        "stddev_ops_per_sec": optional_float("stddev"),
        "sem_ops_per_sec": optional_float("sem"),
        "samples": samples,
        "authoritative": row.get("authoritative", "yes") == "yes",
        "provisional": row.get("claim_status", "provisional") != "eligible",
    }


def generated_scenario_csvs(csv_dir: Path):
    skip_prefixes = ("scenarios_line_", "mpsc_line_", "spmc_line_")
    for mode_dir in sorted(path for path in csv_dir.iterdir() if path.is_dir()):
        mode = mode_dir.name
        if is_obsolete_plot_mode(mode):
            continue
        slug = metric_file_slug(mode)
        suffix = f"_{slug}.csv"
        for path in sorted(mode_dir.glob(f"*{suffix}")):
            name = path.name
            if name.startswith(skip_prefixes) or "immediate_variants" in name:
                continue
            scenario = name[: -len(suffix)]
            if parse_scenario_threads(scenario) is None:
                continue
            yield mode, scenario, path


def load_generated_csv_grouped(csv_dir: Path, machine_label: str):
    grouped = {}
    for mode, scenario, path in generated_scenario_csvs(csv_dir):
        with path.open(newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                label = row.get("queue")
                if not label:
                    continue
                stats = csv_stats_from_row(row, mode)
                if stats is None:
                    continue
                grouped.setdefault(machine_label, {}).setdefault(mode, {}).setdefault(
                    scenario, {}
                )[label] = stats
    return grouped


def infer_machine_label_from_csv_dir(csv_dir: Path):
    if csv_dir.name == "csv" and csv_dir.parent.name:
        return csv_dir.parent.name
    return csv_dir.name


def merge_grouped_records(target, source):
    for machine, modes in source.items():
        machine_group = target.setdefault(machine, {})
        for mode, scenarios in modes.items():
            mode_group = machine_group.setdefault(mode, {})
            for scenario, entries in scenarios.items():
                mode_group.setdefault(scenario, {}).update(entries)


def summarize_ops(samples, authoritative=True):
    sample_count = len(samples)
    median_ops = statistics.median(samples)
    arithmetic_mean = sum(samples) / sample_count
    if sample_count > 1:
        variance = sum((value - arithmetic_mean) ** 2 for value in samples) / (sample_count - 1)
        stddev = math.sqrt(variance)
    else:
        stddev = 0.0
    sem = stddev / math.sqrt(sample_count) if sample_count > 0 else 0.0
    provisional = sample_count < 3 or not authoritative

    return {
        # Retain the historical field name for plotting consumers; schema-v6
        # summaries intentionally store the median here.
        "mean_ops_per_sec": median_ops,
        "median_ops_per_sec": median_ops,
        "arithmetic_mean_ops_per_sec": arithmetic_mean,
        "stddev_ops_per_sec": stddev,
        "sem_ops_per_sec": sem,
        "samples": sample_count,
        "authoritative": authoritative,
        "provisional": provisional,
    }


def write_csv(out_path: Path, mode: str, values):
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "queue",
                metric_column(mode),
                "stddev",
                "sem",
                "samples",
                "authoritative",
                "claim_status",
            ]
        )
        for label, stats in values:
            writer.writerow(
                [
                    label,
                    f"{stats['mean_ops_per_sec']:.6f}",
                    f"{stats['stddev_ops_per_sec']:.6f}",
                    f"{stats['sem_ops_per_sec']:.6f}",
                    stats["samples"],
                    "yes" if stats.get("authoritative") else "no",
                    "provisional" if stats.get("provisional") else "eligible",
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


def write_grid_coverage_csv(out_path: Path, report):
    coverage = report["grid_coverage"]
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "grid",
                "core_placement",
                "status",
                "present_samples",
                "expected_samples",
                "completion_percent",
                "best_scalar_ubq",
                "best_batched_ubq",
            ]
        )
        writer.writerow(
            [
                coverage["grid"] or "unknown",
                coverage["core_placement"] or "unknown",
                "complete" if coverage["complete"] else "incomplete",
                coverage["present"],
                coverage["expected"],
                f"{coverage['percent']:.6f}",
                report.get("winner") or "",
                report.get("batched_winner") or "",
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
    if mode == "data_latency" or mode.endswith(("_push_elapsed", "_pop_elapsed", "_fill_elapsed", "_drain_elapsed", "_data_latency")):
        return f"{value:,.0f} ns"
    return f"{value:,.0f}"


def average_ops_per_sec(values):
    return sum(values) / len(values) if values else 0.0


def scenario_contention_weight(scenario: str) -> float:
    threads = parse_scenario_threads(scenario)
    if threads is None:
        return 1.0
    producers, consumers = threads
    return float(max(1, producers + consumers - 1))


def relative_metric_goodness(value: float, best_value: float, mode: str) -> float:
    """Return a normalized goodness in (0, 1], where 1 is best for a scenario."""
    if not finite_positive(value) or not finite_positive(best_value):
        return 0.0
    if metric_lower_is_better(mode):
        return min(1.0, best_value / value)
    return min(1.0, value / best_value)


def aggregate_family_variation_labels(entries_by_scenario, mode: str):
    """Pick one variation per family using contention-weighted relative goodness.

    Missing measurements score zero. Normalizing within each scenario prevents a
    high-throughput scenario from dominating merely because its units are larger.
    """
    family_labels = {}
    for scenario, entries in entries_by_scenario.items():
        for label in labels_by_metric(entries, mode):
            if queue_label_valid_for_scenario(label, scenario):
                family = queue_metadata(label)["family"]
                family_labels.setdefault(family, set()).add(label)

    selected = []
    for family, candidates in family_labels.items():
        weighted_goodness = {label: 0.0 for label in candidates}
        observed_weight = {label: 0.0 for label in candidates}
        total_weight = 0.0

        for scenario, entries in entries_by_scenario.items():
            scenario_candidates = [
                label
                for label in candidates
                if label in entries
                and queue_label_valid_for_scenario(label, scenario)
                and finite_positive(mean_value(entries[label]))
            ]
            if not scenario_candidates:
                continue
            weight = scenario_contention_weight(scenario)
            total_weight += weight
            values = {
                label: mean_value(entries[label])
                for label in scenario_candidates
            }
            best_value = (
                min(values.values())
                if metric_lower_is_better(mode)
                else max(values.values())
            )
            for label, value in values.items():
                weighted_goodness[label] += weight * relative_metric_goodness(
                    value,
                    best_value,
                    mode,
                )
                observed_weight[label] += weight

        if total_weight <= 0.0:
            continue
        winner = sorted(
            candidates,
            key=lambda label: (
                -weighted_goodness[label] / total_weight,
                -observed_weight[label],
                label_sort_key(label),
            ),
        )[0]
        selected.append(winner)

    return sorted(selected, key=queue_family_sort_key)


def scenario_line_labels(entries_by_scenario, max_series: int, mode: str):
    # max_series is retained in the API/CLI for compatibility. Family selection
    # is the limit now: every queue family gets exactly one representative.
    _ = max_series
    return aggregate_family_variation_labels(entries_by_scenario, mode)


def batch_comparison_line_labels(
    entries_by_scenario, mode: str = "throughput", queue_family: str = "UBQ"
):
    """Return scalar references plus every batch size of the best batched config."""
    scalar_family = queue_family
    batched_family = f"{queue_family} batched"
    parse_scalar = parse_dubq_variant if queue_family == "DUBQ" else parse_ubq_variant
    parse_batched = (
        parse_batched_dubq_label if queue_family == "DUBQ" else parse_batched_ubq_label
    )
    family_winners = aggregate_family_variation_labels(entries_by_scenario, mode)
    scalar_winner = next(
        (
            label
            for label in family_winners
            if queue_metadata(label)["family"] == scalar_family
        ),
        None,
    )
    batched_winner = next(
        (
            label
            for label in family_winners
            if queue_metadata(label)["family"] == batched_family
        ),
        None,
    )
    if batched_winner is None:
        return [scalar_winner] if scalar_winner is not None else []

    _winner_batch_size, winner_params = parse_batched(batched_winner)
    matching_scalar_coverage = {}
    batched_variations = set()
    for scenario, entries in entries_by_scenario.items():
        for label in entries:
            scalar_params = parse_scalar(label)
            if (
                scalar_params == winner_params
                and queue_label_valid_for_scenario(label, scenario)
                and finite_positive(mean_value(entries[label]))
            ):
                matching_scalar_coverage[label] = (
                    matching_scalar_coverage.get(label, 0.0)
                    + scenario_contention_weight(scenario)
                )
            parsed = parse_batched(label)
            if (
                parsed is not None
                and parsed[1] == winner_params
                and queue_label_valid_for_scenario(label, scenario)
                and finite_positive(mean_value(entries[label]))
            ):
                batched_variations.add(label)

    labels = [scalar_winner] if scalar_winner is not None else []
    if matching_scalar_coverage:
        matching_scalar = sorted(
            matching_scalar_coverage,
            key=lambda label: (
                -matching_scalar_coverage[label],
                label_sort_key(label),
            ),
        )[0]
        if matching_scalar not in labels:
            labels.append(matching_scalar)
    labels.extend(sorted(batched_variations, key=label_sort_key))
    return labels


def batch_comparison_series_styles(
    plt, labels, entries_by_scenario, mode="throughput", queue_family="UBQ"
):
    """Build coordinated styles around the contention-weighted winning batch."""
    scalar_family = queue_family
    batched_family = f"{queue_family} batched"
    parse_scalar = parse_dubq_variant if queue_family == "DUBQ" else parse_ubq_variant
    parse_batched = (
        parse_batched_dubq_label if queue_family == "DUBQ" else parse_batched_ubq_label
    )
    family_winners = aggregate_family_variation_labels(entries_by_scenario, mode)
    scalar_winner = next(
        (
            label
            for label in family_winners
            if queue_metadata(label)["family"] == scalar_family
        ),
        None,
    )
    batched_winner = next(
        (
            label
            for label in family_winners
            if queue_metadata(label)["family"] == batched_family
        ),
        None,
    )
    if batched_winner is None:
        return {}

    winning_batch_size, winner_params = parse_batched(batched_winner)
    matching_scalar = next(
        (
            label
            for label in labels
            if parse_scalar(label) == winner_params
        ),
        None,
    )
    batched_labels = [
        (label, parsed[0])
        for label in labels
        if (parsed := parse_batched(label)) is not None
    ]
    lower_batches = sorted(
        (
            (label, batch_size)
            for label, batch_size in batched_labels
            if batch_size < winning_batch_size
        ),
        key=lambda item: item[1],
    )
    higher_batches = sorted(
        (
            (label, batch_size)
            for label, batch_size in batched_labels
            if batch_size > winning_batch_size
        ),
        key=lambda item: item[1],
    )

    styles = {}
    if scalar_winner is not None:
        scalar_role = (
            "Best scalar / scalar counterpart"
            if scalar_winner == matching_scalar
            else "Best scalar"
        )
        styles[scalar_winner] = {
            "label": f"{scalar_role} — {display_label(scalar_winner)}",
            "color": "#111111",
            "marker": "o",
            "linewidth": 2.8,
            "markersize": 6.5,
            "zorder": 6,
        }
    if matching_scalar is not None and matching_scalar != scalar_winner:
        styles[matching_scalar] = {
            "label": f"Scalar counterpart — {display_label(matching_scalar)}",
            "color": "#7a7a7a",
            "marker": "s",
            "linestyle": "--",
            "linewidth": 2.2,
            "markersize": 6,
            "zorder": 5,
        }

    coolwarm = plt.get_cmap("coolwarm")

    def apply_batch_gradient(items, start, end, marker, group):
        count = len(items)
        for idx, (label, batch_size) in enumerate(items):
            fraction = start if count == 1 else start + (end - start) * idx / (count - 1)
            styles[label] = {
                "label": f"{display_label(label)} ({group})",
                "color": coolwarm(fraction),
                "marker": marker,
                "linewidth": 1.8,
                "markersize": 5.5,
                "zorder": 3,
            }

    apply_batch_gradient(lower_batches, 0.05, 0.40, "^", "below best")
    apply_batch_gradient(higher_batches, 0.60, 0.95, "v", "above best")
    styles[batched_winner] = {
        "label": f"{display_label(batched_winner)} (best)",
        "color": "#009E73",
        "marker": "*",
        "linewidth": 3.2,
        "markersize": 10,
        "zorder": 7,
    }
    return styles


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


def throughput_speedup_rows(entries_by_scenario):
    rows = []
    for scenario in sorted(entries_by_scenario, key=scenario_sort_key):
        threads = parse_scenario_threads(scenario)
        if threads is None:
            continue
        producers, consumers = threads
        entries = entries_by_scenario[scenario]
        ubq_variants = (
            ("scalar", "best scalar UBQ", best_ubq_throughput_entry(entries, scenario)),
            ("batched", "best batched UBQ", best_batched_ubq_throughput_entry(entries, scenario)),
        )
        for ubq_kind, ubq_title, ubq in ubq_variants:
            if ubq is None:
                continue
            ubq_label, ubq_value = ubq
            for baseline_key, baseline_title, predicate in THROUGHPUT_SPEEDUP_BASELINES:
                baseline = best_throughput_entry(entries, predicate)
                if baseline is None:
                    continue
                baseline_label, baseline_value = baseline
                rows.append(
                    {
                        "scenario": scenario,
                        "producers": producers,
                        "consumers": consumers,
                        "ubq_kind": ubq_kind,
                        "comparison": f"{ubq_kind}_{baseline_key}",
                        "comparison_label": f"{ubq_title} vs {baseline_title}",
                        "ubq_queue": ubq_label,
                        "ubq_ops_per_sec": ubq_value,
                        "baseline_queue": baseline_label,
                        "baseline_ops_per_sec": baseline_value,
                        "speedup": ubq_value / baseline_value,
                    }
                )
    return rows


def percentile(values, fraction: float):
    """Return a linearly interpolated percentile for a non-empty value list."""
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    position = (len(ordered) - 1) * fraction
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    weight = position - lower
    return ordered[lower] * (1.0 - weight) + ordered[upper] * weight


def pool_size_effect_observations(entries_by_scenario, mode: str = "throughput"):
    """Build matched UBQ observations relative to an otherwise-identical pool=0.

    The comparison key includes scenario, block size, backoff, sync mode, and,
    for batched UBQ, batch size. This keeps every queue knob except pool size
    fixed. Relative performance is oriented so values above 1 are always better.
    """
    observations = []
    for scenario in sorted(entries_by_scenario, key=scenario_sort_key):
        entries = entries_by_scenario[scenario]
        candidates = []
        no_pool_by_key = {}
        threads = parse_scenario_threads(scenario)
        producers, consumers = threads if threads is not None else (None, None)

        for label, stats in entries.items():
            batched = parse_batched_ubq_label(label)
            if batched is not None:
                batch_size, params = batched
                method = "batched"
            else:
                params = parse_ubq_variant(label)
                if params is None:
                    continue
                batch_size = None
                method = "scalar"

            if not ubq_params_valid_for_scenario(params, scenario):
                continue
            value = mean_value(stats)
            if not finite_positive(value):
                continue

            version, pool_size, block_size, backoff = params[:4]
            sync = params[4] if len(params) >= 5 else "cas"
            match_key = (method, batch_size, version, block_size, backoff, sync)
            candidate = {
                "scenario": scenario,
                "producers": producers,
                "consumers": consumers,
                "method": method,
                "batch_size": batch_size,
                "version": version,
                "pool_size": pool_size,
                "block_size": block_size,
                "backoff": backoff_display_name(backoff),
                "sync": sync,
                "queue": label,
                "metric_value": value,
                "stats": stats,
                "match_key": match_key,
            }
            candidates.append(candidate)
            if pool_size == 0:
                no_pool_by_key[match_key] = candidate

        for candidate in candidates:
            reference = no_pool_by_key.get(candidate["match_key"])
            if reference is None:
                continue
            measured = candidate["metric_value"]
            no_pool = reference["metric_value"]
            relative_performance = (
                no_pool / measured if metric_lower_is_better(mode) else measured / no_pool
            )
            stats = candidate["stats"]
            reference_stats = reference["stats"]
            authoritative = stats.get("authoritative", True) and reference_stats.get(
                "authoritative", True
            )
            provisional = stats.get("provisional", False) or reference_stats.get(
                "provisional", False
            )
            observations.append(
                {
                    key: value
                    for key, value in candidate.items()
                    if key not in ("stats", "match_key")
                }
                | {
                    "no_pool_queue": reference["queue"],
                    "no_pool_metric_value": no_pool,
                    "relative_performance_vs_pool0": relative_performance,
                    "authoritative": authoritative,
                    "provisional": provisional or not authoritative,
                }
            )

    return observations


def pool_size_effect_rows(entries_by_scenario, mode: str = "throughput"):
    """Summarize matched pool-size observations by scenario and method."""
    grouped = {}
    for observation in pool_size_effect_observations(entries_by_scenario, mode):
        key = (
            observation["scenario"],
            observation["method"],
            observation["pool_size"],
        )
        grouped.setdefault(key, []).append(observation)

    rows = []
    for (scenario, method, pool_size), observations in grouped.items():
        ratios = [row["relative_performance_vs_pool0"] for row in observations]
        batch_sizes = sorted(
            {row["batch_size"] for row in observations if row["batch_size"] is not None}
        )
        eligible = sum(not row["provisional"] for row in observations)
        rows.append(
            {
                "scenario": scenario,
                "producers": observations[0]["producers"],
                "consumers": observations[0]["consumers"],
                "method": method,
                "batch_sizes": ",".join(str(size) for size in batch_sizes),
                "pool_size": pool_size,
                "matched_configurations": len(observations),
                "eligible_configurations": eligible,
                "median_relative_performance_vs_pool0": statistics.median(ratios),
                "p25_relative_performance_vs_pool0": percentile(ratios, 0.25),
                "p75_relative_performance_vs_pool0": percentile(ratios, 0.75),
                "min_relative_performance_vs_pool0": min(ratios),
                "max_relative_performance_vs_pool0": max(ratios),
                "beneficial_fraction": sum(ratio > 1.0 for ratio in ratios) / len(ratios),
                "claim_status": "eligible" if eligible == len(observations) else "provisional",
            }
        )

    method_order = {"scalar": 0, "batched": 1}
    return sorted(
        rows,
        key=lambda row: (
            method_order.get(row["method"], 99),
            scenario_sort_key(row["scenario"]),
            row["pool_size"],
        ),
    )


def write_throughput_speedup_csv(out_path: Path, rows):
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "scenario",
                "producers",
                "consumers",
                "comparison",
                "comparison_label",
                "ubq_kind",
                "ubq_queue",
                "ubq_ops_per_sec",
                "baseline_queue",
                "baseline_ops_per_sec",
                "speedup",
            ]
        )
        for row in rows:
            writer.writerow(
                [
                    row["scenario"],
                    row["producers"],
                    row["consumers"],
                    row["comparison"],
                    row["comparison_label"],
                    row["ubq_kind"],
                    row["ubq_queue"],
                    f"{row['ubq_ops_per_sec']:.6f}",
                    row["baseline_queue"],
                    f"{row['baseline_ops_per_sec']:.6f}",
                    f"{row['speedup']:.6f}",
                ]
            )
    return out_path


def write_pool_size_observations_csv(out_path: Path, observations, mode: str = "throughput"):
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "scenario",
                "producers",
                "consumers",
                "method",
                "batch_size",
                "pool_size",
                "block_size",
                "backoff",
                "sync",
                "queue",
                f"pool_{metric_column(mode)}",
                "no_pool_queue",
                f"no_pool_{metric_column(mode)}",
                "relative_performance_vs_pool0",
                "authoritative",
                "claim_status",
            ]
        )
        for row in observations:
            writer.writerow(
                [
                    row["scenario"],
                    row["producers"] if row["producers"] is not None else "",
                    row["consumers"] if row["consumers"] is not None else "",
                    row["method"],
                    row["batch_size"] if row["batch_size"] is not None else "",
                    row["pool_size"],
                    row["block_size"],
                    row["backoff"],
                    row["sync"],
                    row["queue"],
                    f"{row['metric_value']:.6f}",
                    row["no_pool_queue"],
                    f"{row['no_pool_metric_value']:.6f}",
                    f"{row['relative_performance_vs_pool0']:.6f}",
                    "yes" if row["authoritative"] else "no",
                    "provisional" if row["provisional"] else "eligible",
                ]
            )
    return out_path


def write_pool_size_effect_csv(out_path: Path, rows):
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fields = [
        "scenario",
        "producers",
        "consumers",
        "method",
        "batch_sizes",
        "pool_size",
        "matched_configurations",
        "eligible_configurations",
        "median_relative_performance_vs_pool0",
        "p25_relative_performance_vs_pool0",
        "p75_relative_performance_vs_pool0",
        "min_relative_performance_vs_pool0",
        "max_relative_performance_vs_pool0",
        "beneficial_fraction",
        "claim_status",
    ]
    with out_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            output = dict(row)
            for field in fields:
                if "relative_performance" in field or field == "beneficial_fraction":
                    output[field] = f"{row[field]:.6f}"
            writer.writerow(output)
    return out_path


def format_speedup_label(value: float) -> str:
    if value >= 100.0:
        return f"{value:.0f}x"
    if value >= 10.0:
        return f"{value:.1f}x"
    return f"{value:.2f}x"


def family_scenarios(scenarios, family):
    return [scenario for scenario in scenarios if scenario_family(scenario) == family]


def symmetric_scenarios(scenarios):
    """Return scenarios with equal producer and consumer counts, including 1p1c."""
    selected = []
    for scenario in scenarios:
        threads = parse_scenario_threads(scenario)
        if threads is not None and threads[0] == threads[1]:
            selected.append(scenario)
    return selected


def batch_comparison_scenario_groups(scenarios):
    """Return the scenario groups that receive batch-size comparison plots."""
    return [
        ("mpsc", family_scenarios(scenarios, "mpsc")),
        ("spmc", family_scenarios(scenarios, "spmc")),
        ("symmetric", symmetric_scenarios(scenarios)),
    ]


def machine_family_entries(grouped, mode: str, family: str):
    selected = {}
    for machine in sorted(grouped, key=machine_sort_key):
        entries_by_scenario = grouped[machine].get(mode)
        if not entries_by_scenario:
            continue
        scenarios = family_scenarios(
            sorted(entries_by_scenario, key=scaling_scenario_sort_key),
            family,
        )
        if len(scenarios) < 2:
            continue
        selected[machine] = (
            scenarios,
            {scenario: entries_by_scenario[scenario] for scenario in scenarios},
        )
    return selected


def publication_machine_entries(machine_entries):
    return {
        machine: entries
        for machine, entries in machine_entries.items()
        if str(machine).strip().lower() in PUBLICATION_MACHINE_KEYS
    }


def combined_scenario_line_labels(machine_entries, max_series: int, mode: str):
    label_samples = {}
    for _machine, (_scenarios, entries_by_scenario) in machine_entries.items():
        for entries in entries_by_scenario.values():
            for label, stats in entries.items():
                value = mean_value(stats)
                if finite_positive(value):
                    label_samples.setdefault(label, []).append(value)

    _ = max_series
    labels_by_family = {}
    for label in label_samples:
        labels_by_family.setdefault(queue_metadata(label)["family"], []).append(label)

    lower_is_better = metric_lower_is_better(mode)
    selected = []
    for labels in labels_by_family.values():
        selected.append(
            sorted(
                labels,
                key=lambda label: (
                    average_ops_per_sec(label_samples[label])
                    if lower_is_better
                    else -average_ops_per_sec(label_samples[label]),
                    label_sort_key(label),
                ),
            )[0]
        )
    return sorted(selected, key=queue_family_sort_key)


def best_aggregate_label(machine_entries, mode: str, predicate):
    label_samples = {}
    for _machine, (_scenarios, entries_by_scenario) in machine_entries.items():
        for entries in entries_by_scenario.values():
            for label, stats in entries.items():
                if predicate(label):
                    label_samples.setdefault(label, []).append(stats["mean_ops_per_sec"])

    if not label_samples:
        return None

    lower_is_better = metric_lower_is_better(mode)
    return sorted(
        label_samples,
        key=lambda label: (
            average_ops_per_sec(label_samples[label])
            if lower_is_better
            else -average_ops_per_sec(label_samples[label]),
            label_sort_key(label),
        ),
    )[0]


def publication_scenario_line_labels(machine_entries, mode: str):
    selected = []
    selected_set = set()
    for _display_name, predicate in PUBLICATION_SERIES:
        label = best_aggregate_label(machine_entries, mode, predicate)
        if label is not None and label not in selected_set:
            selected.append(label)
            selected_set.add(label)
    return selected


def write_machine_line_csv(
    out_path: Path,
    mode: str,
    machine_entries,
    labels,
    value_formatter=lambda _mode, value: value,
    column_name=None,
    machine_formatter=lambda machine: machine,
):
    out_path.parent.mkdir(parents=True, exist_ok=True)
    metric_name = column_name or metric_column(mode)
    with out_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            ["machine", "scenario", "queue", metric_name, "stddev", "sem", "samples"]
        )
        for machine in sorted(machine_entries, key=machine_sort_key):
            scenarios, entries_by_scenario = machine_entries[machine]
            for scenario in scenarios:
                entries = entries_by_scenario[scenario]
                for label in labels:
                    stats = entries.get(label)
                    if stats is None:
                        continue
                    writer.writerow(
                        [
                            machine_formatter(machine),
                            scenario,
                            label,
                            f"{value_formatter(mode, stats['mean_ops_per_sec']):.6f}",
                            f"{value_formatter(mode, stats['stddev_ops_per_sec']):.6f}",
                            f"{value_formatter(mode, stats['sem_ops_per_sec']):.6f}",
                            stats["samples"],
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


def annotate_grid_coverage_status(ax, report):
    coverage = report["grid_coverage"]
    if coverage["known"] and coverage["complete"]:
        note = (
            f"{coverage['grid'].upper()} GRID EXHAUSTED\n"
            f"{coverage['present']}/{coverage['expected']} planned samples\n"
            f"{coverage['core_placement'].replace('_', ' ').upper()} CORES"
        )
        facecolor = "#e8f5e9"
        edgecolor = "#2e7d32"
    elif coverage["known"]:
        note = (
            f"{coverage['grid'].upper()} GRID INCOMPLETE\n"
            f"{coverage['present']}/{coverage['expected']} planned samples "
            f"({coverage['percent']:.1f}%)\n"
            f"{coverage['core_placement'].replace('_', ' ').upper()} CORES"
        )
        facecolor = "#ffebee"
        edgecolor = "#c62828"
    else:
        note = "GRID COVERAGE UNKNOWN\nlegacy or non-grid input"
        facecolor = "#ffebee"
        edgecolor = "#c62828"
    ax.text(
        0.99,
        0.99,
        note,
        transform=ax.transAxes,
        ha="right",
        va="top",
        fontsize=9,
        bbox={
            "boxstyle": "round,pad=0.25",
            "facecolor": facecolor,
            "edgecolor": edgecolor,
            "linewidth": 0.9,
            "alpha": 0.95,
        },
    )


def annotate_figure_grid_coverage(fig, coverage):
    report = {"grid_coverage": coverage}
    class FigureTextTarget:
        transAxes = None

        def text(self, _x, _y, note, **kwargs):
            kwargs.pop("transform", None)
            fig.text(0.99, 0.99, note, **kwargs)

    annotate_grid_coverage_status(FigureTextTarget(), report)


def plot_throughput_speedup_grid(
    plt, out_path: Path, machine: str, entries_by_scenario, coverage=None
):
    rows = throughput_speedup_rows(entries_by_scenario)
    if not rows:
        return False

    scenario_coords = {
        parse_scenario_threads(scenario)
        for scenario in entries_by_scenario
        if parse_scenario_threads(scenario) is not None
    }
    if not scenario_coords:
        return False

    producers = sorted({coord[0] for coord in scenario_coords})
    consumers = sorted({coord[1] for coord in scenario_coords})
    by_cell = {
        (row["comparison"], row["producers"], row["consumers"]): row
        for row in rows
    }
    finite_logs = [
        math.log2(row["speedup"])
        for row in rows
        if finite_positive(row["speedup"])
    ]
    if not finite_logs:
        return False

    from matplotlib.colors import TwoSlopeNorm

    vmin = min(-3.0, max(-6.0, min(finite_logs)))
    vmax = max(3.0, min(6.0, max(finite_logs)))
    norm = TwoSlopeNorm(vmin=vmin, vcenter=0.0, vmax=vmax)
    cmap = plt.get_cmap("RdYlGn").copy()
    cmap.set_bad("#f2f2f2")

    panels = []
    for ubq_kind, ubq_title in (("scalar", "best scalar UBQ"), ("batched", "best batched UBQ")):
        for baseline_key, baseline_title, _predicate in THROUGHPUT_SPEEDUP_BASELINES:
            comparison = f"{ubq_kind}_{baseline_key}"
            if any(row["comparison"] == comparison for row in rows):
                panels.append((comparison, f"{ubq_title} vs {baseline_title}"))
    panel_count = len(panels)
    width = max(12.0, panel_count * (2.8 + 0.42 * len(consumers)))
    height = max(5.6, 2.6 + 0.52 * len(producers))
    fig, axes = plt.subplots(1, panel_count, figsize=(width, height), squeeze=False)
    axes = list(axes[0])
    image = None
    text_threshold = max(abs(vmin), abs(vmax)) * 0.52

    for ax, (comparison, title) in zip(axes, panels):
        matrix = []
        for producer in producers:
            matrix_row = []
            for consumer in consumers:
                row = by_cell.get((comparison, producer, consumer))
                if row is None or not finite_positive(row["speedup"]):
                    matrix_row.append(float("nan"))
                else:
                    matrix_row.append(math.log2(row["speedup"]))
            matrix.append(matrix_row)

        image = ax.imshow(matrix, cmap=cmap, norm=norm, origin="upper", aspect="auto")
        ax.set_xticks(range(len(consumers)), [str(value) for value in consumers])
        ax.set_yticks(range(len(producers)), [str(value) for value in producers])
        ax.set_xlabel("Consumers")
        ax.set_title(title)
        ax.set_xticks([idx - 0.5 for idx in range(1, len(consumers))], minor=True)
        ax.set_yticks([idx - 0.5 for idx in range(1, len(producers))], minor=True)
        ax.grid(which="minor", color="white", linestyle="-", linewidth=1.0)
        ax.tick_params(which="minor", bottom=False, left=False)

        for y_idx, producer in enumerate(producers):
            for x_idx, consumer in enumerate(consumers):
                coord = (producer, consumer)
                row = by_cell.get((comparison, producer, consumer))
                if row is None:
                    if coord not in scenario_coords:
                        continue
                    text = "n/a"
                    color = "#666666"
                else:
                    speedup = row["speedup"]
                    text = format_speedup_label(speedup)
                    log_speedup = math.log2(speedup)
                    color = "white" if abs(log_speedup) >= text_threshold else "black"
                ax.text(
                    x_idx,
                    y_idx,
                    text,
                    ha="center",
                    va="center",
                    fontsize=8,
                    color=color,
                )

    axes[0].set_ylabel("Producers")
    fig.suptitle(
        f"{machine_display_label(machine)}: scalar and batched UBQ throughput speedup"
    )
    annotate_figure_grid_coverage(fig, coverage or grid_coverage_report(None, "throughput"))
    fig.subplots_adjust(top=0.86, right=0.84, wspace=0.28)
    if image is not None:
        colorbar = fig.colorbar(image, ax=axes, fraction=0.028, pad=0.035)
        ticks = [tick for tick in range(math.ceil(vmin), math.floor(vmax) + 1)]
        colorbar.set_ticks(ticks)
        colorbar.set_ticklabels([f"{2 ** tick:g}x" for tick in ticks])
        colorbar.set_label("UBQ speedup")

    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, dpi=220, bbox_inches="tight")
    plt.close(fig)
    return True


def plot_pool_size_effect(
    plt, out_path: Path, machine: str, entries_by_scenario, coverage=None
):
    """Plot median matched UBQ performance relative to pool=0."""
    rows = pool_size_effect_rows(entries_by_scenario, "throughput")
    methods = [
        method
        for method in ("scalar", "batched")
        if any(row["method"] == method and row["pool_size"] != 0 for row in rows)
    ]
    if not methods:
        return False

    scenarios = sorted(
        {row["scenario"] for row in rows}, key=scaling_scenario_sort_key
    )
    pool_sizes = sorted({row["pool_size"] for row in rows})
    if not scenarios or len(pool_sizes) < 2:
        return False

    by_cell = {
        (row["method"], row["scenario"], row["pool_size"]): row for row in rows
    }
    finite_logs = [
        math.log2(row["median_relative_performance_vs_pool0"])
        for row in rows
        if finite_positive(row["median_relative_performance_vs_pool0"])
    ]
    if not finite_logs:
        return False

    from matplotlib.colors import TwoSlopeNorm

    limit = max(0.1, max(abs(value) for value in finite_logs))
    norm = TwoSlopeNorm(vmin=-limit, vcenter=0.0, vmax=limit)
    cmap = plt.get_cmap("RdYlGn").copy()
    cmap.set_bad("#f2f2f2")

    width = max(8.0, len(methods) * (2.8 + 0.72 * len(pool_sizes)))
    height = max(5.8, 2.8 + 0.52 * len(scenarios))
    fig, axes = plt.subplots(1, len(methods), figsize=(width, height), squeeze=False)
    axes = list(axes[0])
    image = None
    text_threshold = limit * 0.55

    for ax, method in zip(axes, methods):
        matrix = []
        for scenario in scenarios:
            matrix_row = []
            for pool_size in pool_sizes:
                row = by_cell.get((method, scenario, pool_size))
                ratio = row["median_relative_performance_vs_pool0"] if row else None
                matrix_row.append(math.log2(ratio) if finite_positive(ratio) else float("nan"))
            matrix.append(matrix_row)

        image = ax.imshow(matrix, cmap=cmap, norm=norm, origin="upper", aspect="auto")
        pool_labels = ["0\n(no pool)" if size == 0 else str(size) for size in pool_sizes]
        ax.set_xticks(range(len(pool_sizes)), pool_labels)
        ax.set_yticks(range(len(scenarios)), scenarios)
        ax.set_xlabel("Retained blocks (pool size)")
        ax.set_ylabel("Scenario")
        ax.set_title("Scalar UBQ" if method == "scalar" else "Batched UBQ")
        ax.set_xticks([idx - 0.5 for idx in range(1, len(pool_sizes))], minor=True)
        ax.set_yticks([idx - 0.5 for idx in range(1, len(scenarios))], minor=True)
        ax.grid(which="minor", color="white", linestyle="-", linewidth=1.0)
        ax.tick_params(which="minor", bottom=False, left=False)

        for y_idx, scenario in enumerate(scenarios):
            for x_idx, pool_size in enumerate(pool_sizes):
                row = by_cell.get((method, scenario, pool_size))
                if row is None:
                    text = "n/a"
                    color = "#666666"
                else:
                    ratio = row["median_relative_performance_vs_pool0"]
                    log_ratio = math.log2(ratio)
                    text = f"{format_speedup_label(ratio)}\nn={row['matched_configurations']}"
                    color = "white" if abs(log_ratio) >= text_threshold else "black"
                ax.text(
                    x_idx,
                    y_idx,
                    text,
                    ha="center",
                    va="center",
                    fontsize=7.5,
                    color=color,
                )

    fig.suptitle(
        f"{machine_display_label(machine)}: UBQ pool-size effect vs no pool",
        fontsize=14,
    )
    fig.text(
        0.5,
        0.015,
        "Cells are medians of matched ratios with block size, backoff, and batch size fixed; "
        ">1x favors pooling.",
        ha="center",
        fontsize=9,
    )
    annotate_figure_grid_coverage(fig, coverage or grid_coverage_report(None, "throughput"))
    fig.subplots_adjust(top=0.86, bottom=0.13, right=0.87, wspace=0.28)
    if image is not None:
        colorbar = fig.colorbar(image, ax=axes, fraction=0.028, pad=0.035)
        colorbar.set_ticks([-limit, 0.0, limit])
        colorbar.set_ticklabels(
            [
                format_speedup_label(2 ** -limit),
                "1x",
                format_speedup_label(2**limit),
            ]
        )
        colorbar.set_label("Relative performance vs pool=0")

    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, dpi=220, bbox_inches="tight")
    plt.close(fig)
    return True


def plot_scenario_bar(
    plt,
    out_path: Path,
    machine: str,
    mode: str,
    scenario: str,
    entries,
    labels,
    error_bars: str,
    report,
    method_kind: str | None = None,
):
    values = [entries[label]["mean_ops_per_sec"] for label in labels]
    if not values:
        return False

    yerr = error_values(entries, labels, error_bars)
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
    method_suffix = f" ({method_kind} methods)" if method_kind else ""
    ax.set_title(f"{machine}: {metric_display_name(mode)} {scenario}{method_suffix}")
    ax.grid(axis="y", linestyle=":", alpha=0.4)
    if mode in ("throughput", "complex_throughput"):
        annotate_grid_coverage_status(ax, report)

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
    ax.legend(
        loc="upper left",
        **queue_label_legend_kwargs(labels),
    )
    fig.tight_layout()

    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, dpi=200)
    plt.close(fig)
    return True


def plot_scenario_lines(
    plt,
    out_path: Path,
    machine: str,
    mode: str,
    scenarios,
    labels,
    entries_by_scenario,
    error_bars: str,
    family=None,
    coverage=None,
    title=None,
    series_styles=None,
):
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
        if series_styles:
            plot_kwargs.update(series_styles.get(label, {}))
        if yerrs and any(value != 0.0 for value in yerrs):
            ax.errorbar(xs, ys, yerr=yerrs, capsize=3, **plot_kwargs)
        else:
            ax.plot(xs, ys, **plot_kwargs)

    ax.set_xticks(x_positions, scenarios, rotation=40, ha="right")
    ax.set_xlabel(family_axis_label(family))
    ax.set_ylabel(metric_axis_label(mode))
    log_y = scenario_line_uses_log_y(mode)
    if log_y:
        ax.set_yscale("log")
    family_prefix = f"{family.upper()} " if family else ""
    ax.set_title(
        title or f"{machine}: {family_prefix}{metric_display_name(mode)} scaling"
    )
    ax.grid(axis="y", which="both" if log_y else "major", linestyle=":", alpha=0.4)
    ax.legend(
        loc="center left",
        bbox_to_anchor=(1.02, 0.5),
        fontsize=9,
        frameon=False,
        **queue_label_legend_kwargs(labels),
    )
    if mode in ("throughput", "complex_throughput"):
        annotate_grid_coverage_status(
            ax,
            {"grid_coverage": coverage or grid_coverage_report(None, mode)},
        )
    fig.tight_layout(rect=(0, 0, 0.84, 1))

    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, dpi=200, bbox_inches="tight")
    plt.close(fig)


def plot_machine_comparison_lines(
    plt,
    out_path: Path,
    mode: str,
    machine_entries,
    labels,
    error_bars: str,
    family: str,
    label_formatter=display_label,
    value_formatter=lambda _mode, value: value,
    y_axis_label=None,
):
    if len(machine_entries) < 2 or not labels:
        return False

    machines = sorted(machine_entries, key=machine_sort_key)
    width = max(10.5, 3.7 * len(machines))
    fig, axes = plt.subplots(
        1,
        len(machines),
        figsize=(width, 4.1),
        sharey=False,
        squeeze=False,
    )
    axes = list(axes[0])
    color_map = plt.get_cmap("tab20", max(len(labels), 1))
    colors = {label: color_map(idx) for idx, label in enumerate(labels)}
    markers = {label: LINE_MARKERS[idx % len(LINE_MARKERS)] for idx, label in enumerate(labels)}
    handles_by_label = {}
    log_y = scenario_line_uses_log_y(mode)
    positive_y_values = []

    for ax_idx, (ax, machine) in enumerate(zip(axes, machines)):
        scenarios, entries_by_scenario = machine_entries[machine]
        x_labels = [
            parse_scenario_threads(scenario)[0]
            if family == "mpsc"
            else parse_scenario_threads(scenario)[1]
            for scenario in scenarios
        ]
        x_positions = list(range(len(scenarios)))

        for label in labels:
            xs = []
            ys = []
            yerrs = []
            for x_pos, scenario in zip(x_positions, scenarios):
                stats = entries_by_scenario[scenario].get(label)
                if stats is None:
                    continue
                y_value = value_formatter(mode, stats["mean_ops_per_sec"])
                xs.append(x_pos)
                ys.append(y_value)
                if y_value > 0.0:
                    positive_y_values.append(y_value)
                err = error_value(stats, error_bars)
                if err is not None:
                    yerrs.append(value_formatter(mode, err))

            if not xs:
                continue

            plot_kwargs = {
                "label": label_formatter(label),
                "color": colors[label],
                "marker": markers[label],
                "linewidth": 1.45,
                "markersize": 4.2,
            }
            if yerrs and any(value != 0.0 for value in yerrs):
                handle = ax.errorbar(xs, ys, yerr=yerrs, capsize=2, **plot_kwargs)
            else:
                line = ax.plot(xs, ys, **plot_kwargs)
                handle = line[0] if line else None
            if handle is not None:
                handles_by_label.setdefault(label, handle)

        ax.set_xticks(x_positions, [str(value) for value in x_labels], rotation=35, ha="right")
        ax.tick_params(axis="x", labelsize=8)
        ax.set_xlabel(family_axis_label(family))
        ax.set_title(machine_display_label(machine), fontsize=10)
        ax.grid(axis="y", which="both" if log_y else "major", linestyle=":", alpha=0.35)
        ax.grid(axis="x", which="major", linestyle=":", alpha=0.14)
        if ax_idx == 0:
            ax.set_ylabel(y_axis_label or metric_axis_label(mode))
        if log_y:
            ax.set_yscale("log")

    if log_y and positive_y_values:
        y_min = min(positive_y_values) / 1.8
        y_max = max(positive_y_values) * 1.8
        for ax in axes:
            ax.set_ylim(y_min, y_max)

    legend_handles = [handles_by_label[label] for label in labels if label in handles_by_label]
    legend_labels = [
        label_formatter(label) for label in labels if label in handles_by_label
    ]
    if legend_handles:
        legend_cols = min(6, max(1, len(legend_handles)))
        fig.legend(
            legend_handles,
            legend_labels,
            loc="lower center",
            bbox_to_anchor=(0.5, 0.025),
            ncol=legend_cols,
            fontsize=8,
            frameon=False,
            **queue_label_legend_kwargs(labels),
        )

    bottom = 0.105 if legend_handles else 0.08
    fig.tight_layout(rect=(0, bottom, 1, 1))
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, dpi=220, bbox_inches="tight")
    plt.close(fig)
    return True


def main():
    parser = argparse.ArgumentParser(description="Plot UBQ benchmark throughput.")
    parser.add_argument("files", nargs="*", help="Benchmark JSON files")
    parser.add_argument(
        "--runs-dir",
        help="Recursively load benchmark JSON files from a runs directory tree",
    )
    parser.add_argument(
        "--csv-dir",
        action="append",
        dest="csv_dirs",
        help=(
            "Render PNGs from an existing generated CSV machine directory. "
            "Can be passed multiple times; examples: bench_results/GraceData "
            "or bench_results/plots/grace/csv."
        ),
    )
    parser.add_argument(
        "--machine-label",
        action="append",
        dest="machine_labels",
        help=(
            "Machine label to use with --csv-dir output paths and plot titles. "
            "Can be repeated to match repeated --csv-dir arguments."
        ),
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
        help="Deprecated compatibility option; line charts now show one best variation per queue family",
    )
    args = parser.parse_args()

    files = [Path(file) for file in args.files]
    if args.runs_dir:
        files.extend(collect_run_jsons(Path(args.runs_dir)))

    if args.csv_dirs and files:
        parser.error("provide either benchmark JSON input/--runs-dir or --csv-dir, not both")
    if not files and not args.csv_dirs:
        parser.error("provide at least one benchmark JSON file, --runs-dir, or --csv-dir")
    if args.machine_labels and not args.csv_dirs:
        parser.error("--machine-label requires --csv-dir")
    if args.csv_dirs and args.machine_labels and len(args.machine_labels) != len(args.csv_dirs):
        parser.error("repeat --machine-label once for each --csv-dir, or omit it")

    out_root = Path(args.out_dir)
    grouped = {}
    grid_coverage = {}

    if args.csv_dirs:
        for idx, raw_csv_dir in enumerate(args.csv_dirs):
            csv_dir = Path(raw_csv_dir)
            if not csv_dir.is_dir():
                parser.error(f"--csv-dir does not exist or is not a directory: {csv_dir}")
            machine_label = (
                args.machine_labels[idx]
                if args.machine_labels
                else infer_machine_label_from_csv_dir(csv_dir)
            )
            merge_grouped_records(
                grouped,
                load_generated_csv_grouped(csv_dir, machine_label),
            )
        if not grouped:
            print("No generated benchmark CSV records found under the provided --csv-dir path(s).")
            return
        if not args.no_clean:
            for machine_label in sorted(grouped, key=machine_sort_key):
                clear_generated_outputs(out_root / machine_label)
    else:
        deduplicated = {}
        sample_points = 0

        for path in files:
            for sample in load_record_samples(path):
                logical_key = (
                    sample["fingerprint"],
                    sample["scenario"],
                    sample["queue"],
                    sample["mode"],
                    sample["repeat_index"],
                )
                previous = deduplicated.get(logical_key)
                if previous is None or sample["timestamp"] >= previous["timestamp"]:
                    deduplicated[logical_key] = sample
                sample_points += 1
            for machine, mode, scenario, specification, sample in load_grid_coverage(
                path, include_legacy=False
            ):
                merge_grid_coverage(
                    grid_coverage,
                    machine,
                    mode,
                    scenario,
                    specification,
                    sample,
                )

        if sample_points == 0:
            print("No benchmark records found in input files.")
            return

        if not args.no_clean:
            clear_generated_outputs(out_root)

        fingerprint_latest = {}
        for sample in deduplicated.values():
            key = (
                sample["machine"],
                sample["placement"],
                sample["mode"],
                sample["scenario"],
                sample["fingerprint"],
            )
            fingerprint_latest[key] = max(
                fingerprint_latest.get(key, 0), sample["timestamp"]
            )
        selected_fingerprints = {}
        for key, timestamp in fingerprint_latest.items():
            base = key[:4]
            candidate = (timestamp, key[4])
            if candidate > selected_fingerprints.get(base, (-1, "")):
                selected_fingerprints[base] = candidate

        raw_data = {}
        authoritative_data = {}
        for sample in deduplicated.values():
            base = (
                sample["machine"],
                sample["placement"],
                sample["mode"],
                sample["scenario"],
            )
            if sample["fingerprint"] != selected_fingerprints[base][1]:
                continue
            key = base + (sample["queue"],)
            raw_data.setdefault(key, []).append(sample["value"])
            authoritative_data[key] = authoritative_data.get(key, True) and sample["authoritative"]

        selected_placements = preferred_core_placements(raw_data)
        grid_coverage = {
            key: coverage
            for key, coverage in grid_coverage.items()
            if selected_placements.get(key) == coverage["core_placement"]
        }
        for (machine, placement, mode, scenario, label), samples in raw_data.items():
            if placement != selected_placements[(machine, mode, scenario)]:
                continue
            grouped.setdefault(machine, {}).setdefault(mode, {}).setdefault(scenario, {})[label] = (
                summarize_ops(samples, authoritative_data[(machine, placement, mode, scenario, label)])
            )

        for machine in sorted(grouped):
            for mode in sorted(grouped[machine], key=mode_sort_key):
                for scenario in sorted(grouped[machine][mode], key=scenario_sort_key):
                    entries = grouped[machine][mode][scenario]
                    coverage = grid_coverage.get((machine, mode, scenario))
                    report = primary_plot_report(entries, mode, scenario, coverage)
                    values = [
                        (label, entries[label])
                        for label in labels_by_metric(entries, mode)
                    ]
                    slug = metric_file_slug(mode)
                    csv_path = out_root / machine / "csv" / mode / f"{scenario}_{slug}.csv"
                    write_csv(csv_path, mode, values)
                    print(f"Wrote CSV: {csv_path}")
                    method_groups = split_bar_entries_by_method(entries)
                    if all(method_groups.values()):
                        for method_kind in ("scalar", "batched"):
                            method_entries = method_groups[method_kind]
                            method_csv_path = (
                                out_root
                                / machine
                                / "csv"
                                / mode
                                / f"{scenario}_{slug}_{method_kind}.csv"
                            )
                            write_csv(
                                method_csv_path,
                                mode,
                                [
                                    (label, method_entries[label])
                                    for label in labels_by_metric(method_entries, mode)
                                ],
                            )
                            print(f"Wrote CSV: {method_csv_path}")
                    if mode in ("throughput", "complex_throughput"):
                        coverage_csv_path = (
                            out_root
                            / machine
                            / "csv"
                            / mode
                            / f"{scenario}_grid_coverage_{slug}.csv"
                        )
                        write_grid_coverage_csv(coverage_csv_path, report)
                        print(f"Wrote CSV: {coverage_csv_path}")
                        if not report["grid_coverage"]["complete"]:
                            print(
                                f"warning: {machine} {mode} {scenario} does not exhaust "
                                "its declared UBQ/DUBQ grid",
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
                for family in ("mpsc", "spmc"):
                    selected_scenarios = family_scenarios(scenarios, family)
                    if len(selected_scenarios) < 2:
                        continue
                    selected_entries = {
                        scenario: entries_by_scenario[scenario]
                        for scenario in selected_scenarios
                    }
                    labels = scenario_line_labels(
                        selected_entries,
                        args.max_line_series,
                        mode,
                    )
                    csv_path = (
                        out_root
                        / machine
                        / "csv"
                        / mode
                        / f"{family}_line_{slug}.csv"
                    )
                    write_scenario_line_csv(
                        csv_path,
                        mode,
                        selected_scenarios,
                        labels,
                        selected_entries,
                    )
                    print(f"Wrote CSV: {csv_path}")

    for machine in sorted(grouped):
        entries_by_scenario = grouped[machine].get("throughput")
        if not entries_by_scenario:
            continue
        pool_observations = pool_size_effect_observations(entries_by_scenario)
        pool_rows = pool_size_effect_rows(entries_by_scenario)
        if pool_rows and any(row["pool_size"] != 0 for row in pool_rows):
            matched_csv_path = (
                out_root
                / machine
                / "csv"
                / "throughput"
                / "pool_size_matched_throughput.csv"
            )
            write_pool_size_observations_csv(matched_csv_path, pool_observations)
            print(f"Wrote CSV: {matched_csv_path}")
            effect_csv_path = (
                out_root
                / machine
                / "csv"
                / "throughput"
                / "pool_size_effect_throughput.csv"
            )
            write_pool_size_effect_csv(effect_csv_path, pool_rows)
            print(f"Wrote CSV: {effect_csv_path}")

    for machine in sorted(grouped):
        entries_by_scenario = grouped[machine].get("throughput")
        if not entries_by_scenario:
            continue
        scenarios = sorted(entries_by_scenario, key=scaling_scenario_sort_key)
        for family, selected_scenarios in batch_comparison_scenario_groups(scenarios):
            if len(selected_scenarios) < 2:
                continue
            selected_entries = {
                scenario: entries_by_scenario[scenario]
                for scenario in selected_scenarios
            }
            for queue_family in ("UBQ", "DUBQ"):
                batch_labels = batch_comparison_line_labels(
                    selected_entries, queue_family=queue_family
                )
                parse_scalar = parse_dubq_variant if queue_family == "DUBQ" else parse_ubq_variant
                parse_batch = (
                    parse_batched_dubq_label
                    if queue_family == "DUBQ"
                    else parse_batched_ubq_label
                )
                if (
                    not batch_labels
                    or parse_scalar(batch_labels[0]) is None
                    or not any(parse_batch(label) is not None for label in batch_labels)
                ):
                    continue
                suffix = "batchcomp" if queue_family == "UBQ" else "dubq_batchcomp"
                batch_csv_path = (
                    out_root
                    / machine
                    / "csv"
                    / "throughput"
                    / f"{family}_line_throughput_{suffix}.csv"
                )
                write_scenario_line_csv(
                    batch_csv_path,
                    "throughput",
                    selected_scenarios,
                    batch_labels,
                    selected_entries,
                )
                print(f"Wrote CSV: {batch_csv_path}")

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
        entries_by_scenario = grouped[machine].get("throughput")
        if not entries_by_scenario:
            continue
        coverage = aggregate_grid_coverage(
            [
                grid_coverage.get((machine, "throughput", scenario))
                for scenario in entries_by_scenario
            ],
            "throughput",
        )
        pool_png_path = out_root / machine / "throughput" / "pool_size_effect_throughput.png"
        if plot_pool_size_effect(
            plt, pool_png_path, machine, entries_by_scenario, coverage
        ):
            print(f"Wrote PNG: {pool_png_path}")

        speedup_rows = throughput_speedup_rows(entries_by_scenario)
        if not speedup_rows:
            continue
        csv_path = out_root / machine / "csv" / "throughput" / "ubq_speedup_grid_throughput.csv"
        write_throughput_speedup_csv(csv_path, speedup_rows)
        print(f"Wrote CSV: {csv_path}")
        png_path = out_root / machine / "throughput" / "ubq_speedup_grid_throughput.png"
        if plot_throughput_speedup_grid(
            plt, png_path, machine, entries_by_scenario, coverage
        ):
            print(f"Wrote PNG: {png_path}")

    for machine in sorted(grouped):
        for mode in sorted(grouped[machine], key=mode_sort_key):
            for scenario in sorted(grouped[machine][mode], key=scenario_sort_key):
                entries = grouped[machine][mode][scenario]
                coverage = grid_coverage.get((machine, mode, scenario))
                report = primary_plot_report(entries, mode, scenario, coverage)
                slug = metric_file_slug(mode)
                plot_specs = [(None, entries, report, f"{scenario}_{slug}.png")]
                method_groups = split_bar_entries_by_method(entries)
                if all(method_groups.values()):
                    for method_kind in ("scalar", "batched"):
                        method_entries = method_groups[method_kind]
                        method_report = primary_plot_report(
                            method_entries,
                            mode,
                            scenario,
                            coverage,
                        )
                        plot_specs.append(
                            (
                                method_kind,
                                method_entries,
                                method_report,
                                f"{scenario}_{slug}_{method_kind}.png",
                            )
                        )

                for method_kind, plot_entries, plot_report, filename in plot_specs:
                    labels = plot_report["selected_labels"]
                    png_path = out_root / machine / mode / filename
                    if plot_scenario_bar(
                        plt,
                        png_path,
                        machine,
                        mode,
                        scenario,
                        plot_entries,
                        labels,
                        args.error_bars,
                        plot_report,
                        method_kind,
                    ):
                        print(f"Wrote PNG: {png_path}")

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
                coverage=aggregate_grid_coverage(
                    [grid_coverage.get((machine, mode, scenario)) for scenario in scenarios],
                    mode,
                ),
            )
            print(f"Wrote PNG: {png_path}")
            for family in ("mpsc", "spmc"):
                selected_scenarios = family_scenarios(scenarios, family)
                if len(selected_scenarios) < 2:
                    continue
                selected_entries = {
                    scenario: entries_by_scenario[scenario]
                    for scenario in selected_scenarios
                }
                labels = scenario_line_labels(
                    selected_entries,
                    args.max_line_series,
                    mode,
                )
                png_path = out_root / machine / mode / f"{family}_line_{slug}.png"
                plot_scenario_lines(
                    plt,
                    png_path,
                    machine,
                    mode,
                    selected_scenarios,
                    labels,
                    selected_entries,
                    args.error_bars,
                    family=family,
                    coverage=aggregate_grid_coverage(
                        [
                            grid_coverage.get((machine, mode, scenario))
                            for scenario in selected_scenarios
                        ],
                        mode,
                    ),
                )
                print(f"Wrote PNG: {png_path}")

            if mode != "throughput":
                continue
            for family, selected_scenarios in batch_comparison_scenario_groups(scenarios):
                if len(selected_scenarios) < 2:
                    continue
                selected_entries = {
                    scenario: entries_by_scenario[scenario]
                    for scenario in selected_scenarios
                }
                for queue_family in ("UBQ", "DUBQ"):
                    batch_labels = batch_comparison_line_labels(
                        selected_entries,
                        mode,
                        queue_family,
                    )
                    parse_scalar = (
                        parse_dubq_variant
                        if queue_family == "DUBQ"
                        else parse_ubq_variant
                    )
                    parse_batch = (
                        parse_batched_dubq_label
                        if queue_family == "DUBQ"
                        else parse_batched_ubq_label
                    )
                    if not (
                        batch_labels
                        and parse_scalar(batch_labels[0]) is not None
                        and any(parse_batch(label) is not None for label in batch_labels)
                    ):
                        continue
                    suffix = (
                        "batchcomp"
                        if queue_family == "UBQ"
                        else "dubq_batchcomp"
                    )
                    batch_png_path = (
                        out_root
                        / machine
                        / mode
                        / f"{family}_line_throughput_{suffix}.png"
                    )
                    family_title = (
                        "symmetric XpXc" if family == "symmetric" else family.upper()
                    )
                    plot_scenario_lines(
                        plt,
                        batch_png_path,
                        machine,
                        mode,
                        selected_scenarios,
                        batch_labels,
                        selected_entries,
                        args.error_bars,
                        family=family,
                        coverage=aggregate_grid_coverage(
                            [
                                grid_coverage.get((machine, mode, scenario))
                                for scenario in selected_scenarios
                            ],
                            mode,
                        ),
                        title=(
                            f"{machine}: {family_title} {queue_family} throughput "
                            "batch-size comparison"
                        ),
                        series_styles=batch_comparison_series_styles(
                            plt,
                            batch_labels,
                            selected_entries,
                            mode,
                            queue_family,
                        ),
                    )
                    print(f"Wrote PNG: {batch_png_path}")


if __name__ == "__main__":
    main()
