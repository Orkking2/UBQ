#!/usr/bin/env python3
"""Select a bounded profiling plan from observed throughput scaling cliffs.

The candidate JSONL remains the declaration of what may be profiled.  This
tool accepts either an aggregated profiling ``cell_summary.csv`` or a
schema-v7 ``bench_grid`` run directory.  It selects cross-queue coordinates at
serial anchors, balanced-MPMC endpoints, and the largest loss of scaling
efficiency in each traffic shape.  The point immediately before a selected
cliff is retained by default so the collector sees both sides of the
inflection.  An optional representative mode bounds each coordinate to the
evidenced cliff subject plus fast and slow/throughput-spanning controls.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import statistics
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


class SelectionError(RuntimeError):
    """Raised when candidate and screen data cannot form a valid selection."""


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def load_plan(path: Path) -> list[dict[str, Any]]:
    cases = []
    with path.open(encoding="utf-8") as stream:
        for line_number, raw in enumerate(stream, 1):
            if not raw.strip():
                raise SelectionError(f"{path}:{line_number}: blank lines are invalid")
            try:
                case = json.loads(raw)
            except json.JSONDecodeError as exc:
                raise SelectionError(f"{path}:{line_number}: invalid JSON: {exc}") from exc
            if not isinstance(case, dict):
                raise SelectionError(f"{path}:{line_number}: case must be an object")
            cases.append(case)
    if not cases:
        raise SelectionError(f"{path} contains no cases")
    return cases


def load_summary(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as stream:
        rows = list(csv.DictReader(stream))
    rows = [
        row
        for row in rows
        if row.get("metric_id") == "workload.ops_per_sec"
        and row.get("recipe") == "perf_stat"
        and row.get("n") not in {None, "", "0"}
    ]
    if not rows:
        raise SelectionError(f"{path} contains no perf_stat workload.ops_per_sec summaries")
    return rows


def candidate_sample_keys(
    cases: Sequence[Mapping[str, Any]],
) -> dict[tuple[str, str, int | None, str, str | None], tuple[str, str]]:
    """Map a bench-grid sample identity to profiler queue and variant labels."""

    result: dict[tuple[str, str, int | None, str, str | None], tuple[str, str]] = {}
    for case in cases:
        cluster = str(case.get("cluster") or case.get("machine") or "")
        scenario = str(case.get("scenario") or "")
        batch = batch_value(case.get("batch_size"))
        queue = str(case.get("queue") or "")
        benchmark_queue = (
            str(case.get("queue_implementation") or queue)
            if queue in {"lubq", "fastfifo", "lfqueue"}
            else queue
        )
        ubq_label = str(case.get("ubq_label")) if case.get("ubq_label") else None
        queue_variant = str(case.get("queue_variant") or queue)
        key = cluster, scenario, batch, benchmark_queue, ubq_label
        value = queue, queue_variant
        previous = result.get(key)
        if previous is not None and previous != value:
            raise SelectionError(
                "candidate plan maps one benchmark identity to multiple queue variants: "
                f"{key!r}"
            )
        result[key] = value
    return result


def load_benchmark_frontier(
    path: Path,
    cases: Sequence[Mapping[str, Any]],
) -> tuple[list[dict[str, str]], dict[str, Any]]:
    """Normalize authoritative ``bench_grid`` records into selector rows."""

    record_paths = sorted(path.rglob("record.json")) if path.is_dir() else []
    if not record_paths:
        raise SelectionError(f"{path} contains no bench_grid record.json files")

    clusters = {str(case.get("cluster") or case.get("machine") or "") for case in cases}
    if "" in clusters or len(clusters) != 1:
        raise SelectionError("bench_grid evidence requires a single-cluster candidate plan")
    cluster = next(iter(clusters))
    allowed = candidate_sample_keys(cases)
    grouped: dict[tuple[str, ...], dict[int, tuple[int, float]]] = defaultdict(dict)
    discards: Counter[str] = Counter()
    machine_labels: set[str] = set()
    git_commits: set[str] = set()
    git_dirty_values: set[bool] = set()
    schemas: set[int] = set()
    protocol_cohorts: set[str] = set()
    combined_hash = hashlib.sha256()

    for record_path in record_paths:
        raw = record_path.read_bytes()
        combined_hash.update(str(record_path.relative_to(path)).encode("utf-8"))
        combined_hash.update(b"\0")
        combined_hash.update(hashlib.sha256(raw).digest())
        try:
            record = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise SelectionError(f"{record_path}: invalid JSON: {exc}") from exc
        if not isinstance(record, Mapping):
            raise SelectionError(f"{record_path}: record must be an object")
        schema = integer(record.get("schema_version"))
        if schema is None or schema < 7:
            raise SelectionError(f"{record_path}: expected bench_grid schema_version >= 7")
        schemas.add(schema)
        meta = record.get("meta")
        results = record.get("results")
        if not isinstance(meta, Mapping) or not isinstance(results, list):
            raise SelectionError(f"{record_path}: expected object meta and array results")
        machine_label = str(meta.get("machine_label") or "")
        git_commit = str(meta.get("git_commit") or "")
        scenario = str(meta.get("scenario") or "")
        producers = integer(meta.get("producers"))
        consumers = integer(meta.get("consumers"))
        if not machine_label or not git_commit or not scenario or not producers or not consumers:
            raise SelectionError(f"{record_path}: incomplete benchmark provenance")
        machine_labels.add(machine_label)
        git_commits.add(git_commit)
        git_dirty_values.add(bool(meta.get("git_dirty", False)))

        for result in results:
            if not isinstance(result, Mapping):
                discards["malformed_result"] += 1
                continue
            if result.get("mode") != "throughput":
                discards["non_throughput_mode"] += 1
                continue
            if result.get("status") not in (None, "completed"):
                discards["non_completed_status"] += 1
                continue
            queue = str(result.get("queue") or "")
            batch = batch_value(result.get("batch_size"))
            ubq_label = str(result.get("ubq_label")) if result.get("ubq_label") else None
            sample_key = cluster, scenario, batch, queue, ubq_label
            profiler_identity = allowed.get(sample_key)
            if profiler_identity is None:
                discards["outside_candidate_plan"] += 1
                continue
            profiler_queue, queue_variant = profiler_identity
            rate = number(result.get("ops_per_sec"))
            if rate is None or rate <= 0:
                discards["missing_or_invalid_throughput"] += 1
                continue
            protocol = result.get("protocol")
            authoritative = (
                protocol.get("affinity_authoritative")
                if isinstance(protocol, Mapping)
                else result.get("affinity_authoritative")
            )
            if authoritative is not True:
                discards["non_authoritative_affinity"] += 1
                continue
            expected_parallelism = {"grace": 144, "mn5": 112}.get(cluster)
            if not isinstance(protocol, Mapping) or (
                protocol.get("core_placement") != "interleaved"
                or integer(protocol.get("available_parallelism")) != expected_parallelism
            ):
                discards["incompatible_protocol"] += 1
                continue
            total_items = integer(result.get("total_items"))
            consumed_items = integer(result.get("consumed_items"))
            if total_items is None or consumed_items != total_items:
                discards["incomplete_handoff"] += 1
                continue
            identity = (
                cluster,
                profiler_queue,
                queue_variant,
                "benchmark-frontier",
                "" if batch is None else str(batch),
                "system",
                "build-defined",
                scenario,
                str(producers),
                str(consumers),
                str(producers + consumers),
            )
            protocol_cohorts.add(
                canonical_json(
                    {
                        "available_parallelism": protocol.get("available_parallelism"),
                        "core_placement": protocol.get("core_placement"),
                        "item_policy": protocol.get("item_policy"),
                        "throughput_policy": protocol.get("throughput_policy"),
                    }
                )
            )
            repeat = integer(result.get("repeat_index"))
            if repeat is None or repeat < 1:
                discards["invalid_repeat"] += 1
                continue
            timestamp = integer(result.get("timestamp_unix_ms")) or 0
            previous = grouped[identity].get(repeat)
            if previous is not None:
                discards["superseded_duplicate"] += 1
            if previous is None or timestamp >= previous[0]:
                grouped[identity][repeat] = (timestamp, rate)

    if len(machine_labels) != 1:
        raise SelectionError(
            f"{path} mixes machine labels ({', '.join(sorted(machine_labels))}); "
            "select one coherent run directory"
        )
    if len(git_commits) != 1:
        raise SelectionError(
            f"{path} mixes git revisions ({', '.join(sorted(git_commits))}); "
            "select one source-compatible frontier"
        )
    if not grouped:
        raise SelectionError(f"{path} has no authoritative samples overlapping the candidate plan")
    if len(protocol_cohorts) != 1:
        raise SelectionError(
            f"{path} mixes {len(protocol_cohorts)} benchmark protocol cohorts; "
            "select a protocol-compatible frontier"
        )

    rows = []
    for identity, samples_by_repeat in sorted(grouped.items()):
        (
            machine,
            queue,
            queue_variant,
            variant,
            batch,
            allocator,
            page_distributor,
            scenario,
            producers,
            consumers,
            total_threads,
        ) = identity
        samples = [value for _timestamp, value in samples_by_repeat.values()]
        rows.append(
            {
                "machine": machine,
                "queue": queue,
                "queue_variant": queue_variant,
                "variant": variant,
                "batch_size": batch,
                "allocator": allocator,
                "page_distributor": page_distributor,
                "scenario": scenario,
                "producers": producers,
                "consumers": consumers,
                "total_threads": total_threads,
                "median": str(statistics.median(samples)),
                "n": str(len(samples)),
            }
        )
    provenance = {
        "kind": "bench_grid_records",
        "path": str(path),
        "record_files": len(record_paths),
        "records_sha256": combined_hash.hexdigest(),
        "schema_versions": sorted(schemas),
        "machine_labels": sorted(machine_labels),
        "git_commits": sorted(git_commits),
        "git_dirty": sorted(git_dirty_values),
        "protocol_sha256": hashlib.sha256(
            next(iter(protocol_cohorts)).encode("utf-8")
        ).hexdigest(),
        "authoritative_samples": sum(len(samples) for samples in grouped.values()),
        "summary_cells": len(rows),
        "discarded_samples": dict(sorted(discards.items())),
    }
    return rows, provenance


def load_evidence(
    path: Path,
    cases: Sequence[Mapping[str, Any]],
) -> tuple[list[dict[str, str]], dict[str, Any]]:
    if path.is_dir():
        return load_benchmark_frontier(path, cases)
    rows = load_summary(path)
    return rows, {
        "kind": "profile_cell_summary",
        "path": str(path),
        "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        "summary_cells": len(rows),
    }


def integer(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def number(value: Any) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def batch_value(value: Any) -> int | None:
    return integer(value) if value not in (None, "") else None


def shape(producers: int, consumers: int) -> str:
    if producers == consumers:
        return "balanced"
    if consumers == 1:
        return "producer_contention"
    if producers == 1:
        return "consumer_contention"
    return "asymmetric"


def traffic_shapes(producers: int, consumers: int) -> tuple[str, ...]:
    # SPSC is the shared baseline of all three primary scaling series.
    if producers == 1 and consumers == 1:
        return "balanced", "producer_contention", "consumer_contention"
    return (shape(producers, consumers),)


def coordinate(value: Mapping[str, Any]) -> tuple[str, str, int | None]:
    machine = str(value.get("cluster") or value.get("machine") or "")
    return machine, str(value.get("scenario") or ""), batch_value(value.get("batch_size"))


def filter_candidate_cases(
    cases: Sequence[dict[str, Any]],
    *,
    queues: set[str] | None,
    queue_variants: set[str] | None,
    ubq_labels: set[str] | None,
    batches: set[int | None] | None,
    repeat_limit: int | None,
) -> list[dict[str, Any]]:
    selected = []
    for case in cases:
        if queues is not None and case.get("queue") not in queues:
            continue
        if queue_variants is not None and case.get("queue_variant") not in queue_variants:
            continue
        if (
            ubq_labels is not None
            and case.get("queue") == "ubq"
            and case.get("ubq_label") not in ubq_labels
        ):
            continue
        if batches is not None and batch_value(case.get("batch_size")) not in batches:
            continue
        repeat = integer(case.get("repeat"))
        if repeat_limit is not None and (repeat is None or repeat > repeat_limit):
            continue
        selected.append(case)
    if not selected:
        raise SelectionError("candidate filters select no cases")
    return selected


def series_identity(row: Mapping[str, str]) -> tuple[str, ...]:
    return (
        row.get("machine", ""),
        row.get("queue", ""),
        row.get("queue_variant", ""),
        row.get("variant", ""),
        row.get("batch_size", ""),
        row.get("allocator", ""),
        row.get("page_distributor", ""),
    )


def candidate_coordinates(cases: Sequence[Mapping[str, Any]]) -> set[tuple[str, str, int | None]]:
    return {coordinate(case) for case in cases}


def cliff_scores(
    rows: Sequence[Mapping[str, str]],
    candidates: set[tuple[str, str, int | None]],
    *,
    score_queues: set[str] | None = None,
    score_queue_variants: set[str] | None = None,
    min_evidence_repeats: int = 1,
) -> dict[tuple[str, str, int | None], list[dict[str, Any]]]:
    candidate_order: dict[tuple[str, int | None, str], list[str]] = defaultdict(list)
    for machine, scenario, batch in candidates:
        if "p" not in scenario:
            continue
        producers = integer(scenario.split("p", 1)[0])
        consumers = integer(scenario.split("p", 1)[1].removesuffix("c"))
        if producers is None or consumers is None:
            continue
        for traffic_shape in traffic_shapes(producers, consumers):
            candidate_order[(machine, batch, traffic_shape)].append(scenario)
    for key, scenarios in candidate_order.items():
        candidate_order[key] = sorted(
            set(scenarios),
            key=lambda scenario: (
                sum(int(value) for value in scenario.removesuffix("c").split("p")),
                scenario,
            ),
        )

    grouped: dict[tuple[str, ...], list[Mapping[str, str]]] = defaultdict(list)
    for row in rows:
        if coordinate(row) not in candidates:
            continue
        if score_queues is not None and row.get("queue") not in score_queues:
            continue
        if (
            score_queue_variants is not None
            and row.get("queue_variant") not in score_queue_variants
        ):
            continue
        producers = integer(row.get("producers"))
        consumers = integer(row.get("consumers"))
        throughput = number(row.get("median"))
        repeats = integer(row.get("n"))
        if repeats is None:
            repeats = 1
        if (
            producers is None
            or consumers is None
            or throughput is None
            or throughput <= 0
            or repeats is None
            or repeats < min_evidence_repeats
        ):
            continue
        for traffic_shape in traffic_shapes(producers, consumers):
            grouped[series_identity(row) + (traffic_shape,)].append(row)

    scores: dict[tuple[str, str, int | None], list[dict[str, Any]]] = defaultdict(list)
    for identity, entries in grouped.items():
        entries.sort(key=lambda row: integer(row.get("total_threads")) or 0)
        for previous, current in zip(entries, entries[1:]):
            order = candidate_order.get(
                (
                    current.get("machine", ""),
                    batch_value(current.get("batch_size")),
                    identity[-1],
                ),
                [],
            )
            previous_scenario = str(previous.get("scenario") or "")
            current_scenario = str(current.get("scenario") or "")
            if (
                previous_scenario not in order
                or current_scenario not in order
                or order.index(current_scenario) != order.index(previous_scenario) + 1
            ):
                # Missing raw points do not turn a long-range comparison into a
                # local scaling cliff. They remain visible as unavailable
                # variations in representative-selection provenance.
                continue
            previous_threads = integer(previous.get("total_threads"))
            current_threads = integer(current.get("total_threads"))
            previous_rate = number(previous.get("median"))
            current_rate = number(current.get("median"))
            if not previous_threads or not current_threads or not previous_rate or not current_rate:
                continue
            thread_ratio = current_threads / previous_threads
            throughput_ratio = current_rate / previous_rate
            scaling_efficiency = throughput_ratio / thread_ratio
            score = max(0.0, -math.log2(max(scaling_efficiency, 1e-12)))
            scores[coordinate(current)].append(
                {
                    "reason": "scaling_cliff",
                    "score": score,
                    "scaling_efficiency": scaling_efficiency,
                    "thread_ratio": thread_ratio,
                    "throughput_ratio": throughput_ratio,
                    "previous_median_ops_per_sec": previous_rate,
                    "median_ops_per_sec": current_rate,
                    "previous_scenario": previous.get("scenario"),
                    "queue": current.get("queue"),
                    "queue_variant": current.get("queue_variant"),
                    "variant": current.get("variant"),
                    "shape": identity[-1],
                }
            )
    return scores


def select_coordinates(
    cases: Sequence[Mapping[str, Any]],
    rows: Sequence[Mapping[str, str]],
    *,
    cliffs_per_shape: int,
    include_anchors: bool,
    include_endpoints: bool,
    include_predecessors: bool = True,
    score_queues: set[str] | None = None,
    score_queue_variants: set[str] | None = None,
    min_evidence_repeats: int = 1,
    selected_shapes: set[str] | None = None,
) -> dict[tuple[str, str, int | None], list[dict[str, Any]]]:
    candidates = candidate_coordinates(cases)
    scores = cliff_scores(
        rows,
        candidates,
        score_queues=score_queues,
        score_queue_variants=score_queue_variants,
        min_evidence_repeats=min_evidence_repeats,
    )
    selected: dict[tuple[str, str, int | None], list[dict[str, Any]]] = defaultdict(list)

    ranked: dict[tuple[str, str], list[tuple[float, tuple[str, str, int | None]]]] = defaultdict(list)
    for target, evidence in scores.items():
        producers = integer(target[1].split("p", 1)[0])
        consumers_text = target[1].split("p", 1)[1].removesuffix("c") if "p" in target[1] else ""
        consumers = integer(consumers_text)
        if producers is None or consumers is None:
            continue
        ranked[(target[0], shape(producers, consumers))].append(
            (max(item["score"] for item in evidence), target)
        )
    for group, entries in ranked.items():
        if selected_shapes is not None and group[1] not in selected_shapes:
            continue
        for _score, target in sorted(entries, reverse=True)[:cliffs_per_shape]:
            selected[target].extend(scores[target])

    if include_predecessors:
        selected_cliffs = list(selected.items())
        for target, evidence in selected_cliffs:
            for item in evidence:
                previous_scenario = item.get("previous_scenario")
                if item.get("reason") != "scaling_cliff" or not previous_scenario:
                    continue
                predecessor = (target[0], str(previous_scenario), target[2])
                if predecessor not in candidates:
                    continue
                selected[predecessor].append(
                    {
                        "reason": "cliff_predecessor",
                        "score": item.get("score", 0.0),
                        "target_scenario": target[1],
                        "queue": item.get("queue"),
                        "queue_variant": item.get("queue_variant"),
                        "variant": item.get("variant"),
                        "shape": item.get("shape"),
                    }
                )

    by_machine = defaultdict(list)
    for target in candidates:
        by_machine[target[0]].append(target)
    for machine, targets in by_machine.items():
        batches = sorted({batch for _machine, _scenario, batch in targets if batch is not None})
        if include_anchors:
            anchor_batches = [None] + ([batches[-1]] if batches else [])
            for batch in anchor_batches:
                target = (machine, "1p1c", batch)
                if target in candidates:
                    selected[target].append(
                        {"reason": "serial_anchor", "score": 0.0, "shape": "balanced"}
                    )
        if include_endpoints:
            balanced = []
            for target in targets:
                scenario = target[1]
                if "p" not in scenario:
                    continue
                producers = integer(scenario.split("p", 1)[0])
                consumers = integer(scenario.split("p", 1)[1].removesuffix("c"))
                if producers and consumers and producers == consumers:
                    balanced.append((producers + consumers, target))
            if balanced:
                max_threads = max(value for value, _target in balanced)
                for _threads, target in balanced:
                    if _threads == max_threads and target[2] in {None, batches[-1] if batches else None}:
                        selected[target].append(
                            {"reason": "balanced_endpoint", "score": 0.0, "shape": "balanced"}
                        )
    if not selected:
        raise SelectionError("throughput evidence does not overlap the candidate profiling plan")
    return dict(selected)


def validate_comparator_coverage(
    cases: Sequence[Mapping[str, Any]],
    rows: Sequence[Mapping[str, str]],
    selected: Mapping[tuple[str, str, int | None], Sequence[Mapping[str, Any]]],
    *,
    min_evidence_repeats: int,
) -> None:
    expected: dict[tuple[str, str, int | None], set[str]] = defaultdict(set)
    for case in cases:
        expected[coordinate(case)].add(str(case.get("queue_variant") or case.get("queue") or ""))
    observed: dict[tuple[str, str, int | None], set[str]] = defaultdict(set)
    for row in rows:
        repeats = integer(row.get("n"))
        if repeats is not None and repeats >= min_evidence_repeats:
            observed[coordinate(row)].add(str(row.get("queue_variant") or row.get("queue") or ""))
    missing = []
    for target in selected:
        absent = sorted(expected[target] - observed[target])
        if absent:
            missing.append(f"{target[0]}/{target[1]}/batch={target[2]}: {', '.join(absent)}")
    if missing:
        raise SelectionError(
            "selected coordinates lack complete comparator evidence: " + "; ".join(missing)
        )


def select_representative_variations(
    cases: Sequence[Mapping[str, Any]],
    rows: Sequence[Mapping[str, str]],
    selected: Mapping[tuple[str, str, int | None], Sequence[Mapping[str, Any]]],
    *,
    maximum: int,
    always_keep: set[str] | None = None,
    min_evidence_repeats: int,
) -> dict[tuple[str, str, int | None], list[dict[str, Any]]]:
    """Choose a small, throughput-spanning set of evidenced queue variants.

    A selected scaling-cliff subject has first priority. Explicit controls are
    next, followed by the fastest positive control, the slowest contrast, and
    additional points that maximize distance in log-throughput space.
    """

    candidate_variants: dict[tuple[str, str, int | None], dict[str, str]] = defaultdict(dict)
    for case in cases:
        target = coordinate(case)
        queue = str(case.get("queue") or "")
        queue_variant = str(case.get("queue_variant") or queue)
        previous = candidate_variants[target].get(queue_variant)
        if previous is not None and previous != queue:
            raise SelectionError(
                f"queue variant {queue_variant!r} maps to both {previous!r} and {queue!r}"
            )
        candidate_variants[target][queue_variant] = queue

    observed_rates: dict[tuple[str, str, int | None], dict[str, list[float]]] = defaultdict(
        lambda: defaultdict(list)
    )
    observed_repeats: dict[tuple[str, str, int | None], dict[str, int]] = defaultdict(dict)
    for row in rows:
        target = coordinate(row)
        queue_variant = str(row.get("queue_variant") or row.get("queue") or "")
        repeats = integer(row.get("n")) or 1
        rate = number(row.get("median"))
        if (
            target not in selected
            or queue_variant not in candidate_variants[target]
            or repeats < min_evidence_repeats
            or rate is None
            or rate <= 0
        ):
            continue
        observed_rates[target][queue_variant].append(rate)
        observed_repeats[target][queue_variant] = max(
            repeats, observed_repeats[target].get(queue_variant, 0)
        )

    result: dict[tuple[str, str, int | None], list[dict[str, Any]]] = {}
    for target, coordinate_evidence in selected.items():
        rates = {
            queue_variant: statistics.median(values)
            for queue_variant, values in observed_rates[target].items()
        }
        if not rates:
            raise SelectionError(
                f"selected coordinate {target[0]}/{target[1]}/batch={target[2]} has no "
                f"candidate variation with {min_evidence_repeats} authoritative repeat(s)"
            )

        reasons: dict[str, list[str]] = defaultdict(list)
        cliff_evidence = [
            item
            for item in coordinate_evidence
            if item.get("reason") in {"scaling_cliff", "cliff_predecessor"}
            and item.get("queue_variant") in rates
        ]
        if cliff_evidence:
            driver = max(
                cliff_evidence,
                key=lambda item: (number(item.get("score")) or 0.0, str(item.get("queue_variant"))),
            )
            reasons[str(driver["queue_variant"])].append("cliff_subject")

        for queue_variant in sorted(always_keep or set()):
            if queue_variant in candidate_variants[target] and queue_variant not in rates:
                raise SelectionError(
                    f"explicit representative {queue_variant!r} lacks authoritative evidence at "
                    f"{target[0]}/{target[1]}/batch={target[2]}"
                )
            if queue_variant in rates:
                reasons[queue_variant].append("explicit_control")

        required = set(reasons)
        if len(required) > maximum:
            raise SelectionError(
                f"{target[0]}/{target[1]}/batch={target[2]} requires {len(required)} cliff/explicit "
                f"variations, exceeding --representatives-per-coordinate={maximum}"
            )

        fastest = max(rates, key=lambda item: (rates[item], item))
        slowest = min(rates, key=lambda item: (rates[item], item))
        for queue_variant, reason in ((fastest, "fast_control"), (slowest, "slow_contrast")):
            if len(reasons) < maximum or queue_variant in reasons:
                reasons[queue_variant].append(reason)

        while len(reasons) < min(maximum, len(rates)):
            selected_logs = [math.log(rates[item]) for item in reasons]
            remaining = [item for item in rates if item not in reasons]
            next_variant = max(
                remaining,
                key=lambda item: (
                    min(abs(math.log(rates[item]) - value) for value in selected_logs),
                    rates[item],
                    item,
                ),
            )
            reasons[next_variant].append("throughput_span")

        fastest_rate = rates[fastest]
        unavailable = sorted(set(candidate_variants[target]) - set(rates))
        result[target] = [
            {
                "queue": candidate_variants[target][queue_variant],
                "queue_variant": queue_variant,
                "median_ops_per_sec": rates[queue_variant],
                "fraction_of_fastest": rates[queue_variant] / fastest_rate,
                "evidence_repeats": observed_repeats[target][queue_variant],
                "reasons": sorted(set(reasons[queue_variant])),
                "unavailable_candidate_variants": unavailable,
            }
            for queue_variant in sorted(reasons)
        ]
    return result


def batch_argument(value: str) -> int | None:
    if value.lower() == "scalar":
        return None
    parsed = integer(value)
    if parsed is None or parsed < 2:
        raise argparse.ArgumentTypeError("batch size must be 'scalar' or an integer >= 2")
    return parsed


def write_plan(cases: Sequence[Mapping[str, Any]], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    content = "".join(canonical_json(case) + "\n" for case in cases)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(content, encoding="utf-8")
    temporary.replace(path)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("candidate_plan", type=Path)
    parser.add_argument(
        "evidence",
        type=Path,
        help="profile cell_summary.csv or one coherent bench_grid run directory",
    )
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--cliffs-per-shape", type=int, default=1)
    parser.add_argument("--shape", action="append", choices=("balanced", "producer_contention", "consumer_contention", "asymmetric"))
    parser.add_argument("--queue", action="append", help="retain this candidate queue")
    parser.add_argument("--queue-variant", action="append", help="retain this exact candidate queue variant")
    parser.add_argument("--ubq-label", action="append", help="retain this exact UBQ label")
    parser.add_argument("--batch-size", action="append", type=batch_argument)
    parser.add_argument("--score-queue", action="append", help="use only this queue to rank cliffs")
    parser.add_argument(
        "--score-queue-variant",
        action="append",
        help="use only this exact queue variant to rank cliffs",
    )
    parser.add_argument("--min-evidence-repeats", type=int, default=1)
    parser.add_argument(
        "--plan-repeats",
        type=int,
        help="retain only candidate cases whose one-based repeat is at most N",
    )
    parser.add_argument("--no-anchors", action="store_true")
    parser.add_argument("--no-endpoints", action="store_true")
    parser.add_argument("--no-predecessors", action="store_true")
    parser.add_argument("--allow-missing-comparators", action="store_true")
    parser.add_argument(
        "--representatives-per-coordinate",
        type=int,
        help=(
            "retain at most N evidence-backed queue variants per selected coordinate; "
            "prioritize the cliff subject, fastest control, and slowest contrast"
        ),
    )
    parser.add_argument(
        "--always-keep-queue-variant",
        action="append",
        help="make this exact queue variant a representative wherever it has evidence",
    )
    parser.add_argument("--max-cases", type=int, default=500)
    parser.add_argument("--summary-only", action="store_true")
    args = parser.parse_args(argv)
    try:
        if (
            args.cliffs_per_shape < 0
            or args.max_cases < 1
            or args.min_evidence_repeats < 1
            or (args.plan_repeats is not None and args.plan_repeats < 1)
            or (
                args.representatives_per_coordinate is not None
                and args.representatives_per_coordinate < 1
            )
        ):
            raise SelectionError(
                "cliff count must be non-negative; case and repeat limits must be positive"
            )
        if args.always_keep_queue_variant and args.representatives_per_coordinate is None:
            raise SelectionError(
                "--always-keep-queue-variant requires --representatives-per-coordinate"
            )
        all_cases = load_plan(args.candidate_plan)
        backends = {case.get("backend") for case in all_cases}
        if len(backends) != 1 or not backends <= {"apx", "perf"}:
            raise SelectionError("candidate plan must contain one profiling backend")
        cases = filter_candidate_cases(
            all_cases,
            queues=set(args.queue) if args.queue else None,
            queue_variants=set(args.queue_variant) if args.queue_variant else None,
            ubq_labels=set(args.ubq_label) if args.ubq_label else None,
            batches=set(args.batch_size) if args.batch_size else None,
            repeat_limit=args.plan_repeats,
        )
        rows, evidence_manifest = load_evidence(args.evidence, cases)
        selected_coordinates = select_coordinates(
            cases,
            rows,
            cliffs_per_shape=args.cliffs_per_shape,
            include_anchors=not args.no_anchors,
            include_endpoints=not args.no_endpoints,
            include_predecessors=not args.no_predecessors,
            score_queues=set(args.score_queue) if args.score_queue else None,
            score_queue_variants=(
                set(args.score_queue_variant) if args.score_queue_variant else None
            ),
            min_evidence_repeats=args.min_evidence_repeats,
            selected_shapes=set(args.shape) if args.shape else None,
        )
        representatives = None
        if args.representatives_per_coordinate is not None:
            representatives = select_representative_variations(
                cases,
                rows,
                selected_coordinates,
                maximum=args.representatives_per_coordinate,
                always_keep=set(args.always_keep_queue_variant or []),
                min_evidence_repeats=args.min_evidence_repeats,
            )
        elif not args.allow_missing_comparators:
            validate_comparator_coverage(
                cases,
                rows,
                selected_coordinates,
                min_evidence_repeats=args.min_evidence_repeats,
            )
        selected = [
            case
            for case in cases
            if coordinate(case) in selected_coordinates
            and (
                representatives is None
                or str(case.get("queue_variant") or case.get("queue") or "")
                in {
                    item["queue_variant"]
                    for item in representatives[coordinate(case)]
                }
            )
        ]
        if len(selected) > args.max_cases:
            raise SelectionError(
                f"selection contains {len(selected)} cases, exceeding --max-cases={args.max_cases}"
            )
        manifest = {
            "schema_version": 1,
            "candidate_plan": str(args.candidate_plan),
            "candidate_plan_sha256": hashlib.sha256(args.candidate_plan.read_bytes()).hexdigest(),
            "evidence": evidence_manifest,
            "candidate_cases": len(all_cases),
            "filtered_candidate_cases": len(cases),
            "selected_cases": len(selected),
            "filters": {
                "queues": sorted(args.queue or []),
                "queue_variants": sorted(args.queue_variant or []),
                "ubq_labels": sorted(args.ubq_label or []),
                "batch_sizes": ["scalar" if value is None else value for value in (args.batch_size or [])],
                "score_queues": sorted(args.score_queue or []),
                "score_queue_variants": sorted(args.score_queue_variant or []),
                "shapes": sorted(args.shape or []),
                "minimum_evidence_repeats": args.min_evidence_repeats,
                "plan_repeats": args.plan_repeats,
                "include_anchors": not args.no_anchors,
                "include_endpoints": not args.no_endpoints,
                "include_predecessors": not args.no_predecessors,
                "representatives_per_coordinate": args.representatives_per_coordinate,
                "always_keep_queue_variants": sorted(args.always_keep_queue_variant or []),
            },
            "selected_coordinates": [
                {
                    "cluster": target[0],
                    "scenario": target[1],
                    "batch_size": target[2],
                    "evidence": evidence,
                    "representative_variations": (
                        representatives[target] if representatives is not None else None
                    ),
                }
                for target, evidence in sorted(
                    selected_coordinates.items(), key=lambda item: tuple(str(value) for value in item[0])
                )
            ],
        }
        if not args.summary_only:
            write_plan(selected, args.output)
            manifest_path = args.output.with_suffix(args.output.suffix + ".selection.json")
            manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        print(json.dumps(manifest, indent=2, sort_keys=True))
        return 0
    except (OSError, SelectionError) as exc:
        print(f"select_profile_plan: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
