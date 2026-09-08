#!/usr/bin/env python3
"""Normalize queue-profiler campaigns into analysis-ready CSV files.

The raw campaign layout is intentionally simple::

    RESULTS/<campaign>/<case_id>/
        case.json
        benchmark.json
        perf.csv                  # optional ``perf stat -x ';'`` output
        <apx-run-id>.zip          # optional exported Arm Performix run

This module keeps parsing separate from plotting.  It preserves raw APX metric
names and units, emits trial-level long-form data, and only aggregates trials
whose provenance and experimental factors match exactly.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import math
import random
import re
import statistics
import sys
import zipfile
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


OUTPUT_SCHEMA_VERSION = 1
DEFAULT_BOOTSTRAP_SAMPLES = 2_000


class AggregationError(RuntimeError):
    """Raised when raw result identities or measurements are ambiguous."""


@dataclass
class ParsedApx:
    path: Path
    metadata: dict[str, Any] = field(default_factory=dict)
    manifest: dict[str, Any] = field(default_factory=dict)
    workload: dict[str, Any] | None = None
    metrics: list[dict[str, Any]] = field(default_factory=list)
    hotspots: list[dict[str, Any]] = field(default_factory=list)
    issues: list[dict[str, str]] = field(default_factory=list)


@dataclass
class ParsedTrial:
    row: dict[str, Any]
    metrics: list[dict[str, Any]]
    hotspots: list[dict[str, Any]]
    issues: list[dict[str, Any]]


TRIAL_FIELDS = (
    "campaign_id",
    "case_id",
    "trial_id",
    "cell_id",
    "pair_block_id",
    "comparison_group_id",
    "comparison_role",
    "comparison_sets_json",
    "selection_evidence_json",
    "repeat_index",
    "status",
    "authoritative",
    "machine",
    "machine_label",
    "node",
    "recipe",
    "collector",
    "queue",
    "queue_variant",
    "variant",
    "is_baseline",
    "reference_variant",
    "scenario",
    "producers",
    "consumers",
    "total_threads",
    "batch_size",
    "batch_mode",
    "backoff",
    "block_size",
    "queue_capacity",
    "workload_mode",
    "allocator",
    "page_distributor",
    "distributor_params",
    "placement",
    "git_commit",
    "git_dirty",
    "binary_sha256",
    "slurm_job_id",
    "constraint",
    "requested_duration_ms",
    "warmup_ms",
    "elapsed_ns",
    "items",
    "ops_per_sec",
    "affinity_ok",
    "benchmark_source",
    "apx_archive",
    "apx_run_id",
    "apx_run_name",
    "factors_json",
    "provenance_json",
    "source_dir",
)

METRIC_FIELDS = (
    "campaign_id",
    "case_id",
    "trial_id",
    "cell_id",
    "pair_block_id",
    "repeat_index",
    "authoritative",
    "machine",
    "recipe",
    "queue",
    "queue_variant",
    "variant",
    "scenario",
    "producers",
    "consumers",
    "total_threads",
    "batch_size",
    "allocator",
    "page_distributor",
    "metric_id",
    "raw_name",
    "value",
    "unit",
    "kind",
    "source",
    "phase",
    "scope",
    "dimensions_json",
)

HOTSPOT_FIELDS = (
    "campaign_id",
    "case_id",
    "trial_id",
    "cell_id",
    "repeat_index",
    "authoritative",
    "machine",
    "recipe",
    "queue",
    "queue_variant",
    "variant",
    "scenario",
    "producers",
    "consumers",
    "batch_size",
    "image",
    "symbol",
    "inlined_from",
    "uid",
    "self_samples",
    "root_total_samples",
    "self_fraction",
    "source",
)

QUALITY_FIELDS = (
    "campaign_id",
    "case_id",
    "trial_id",
    "authoritative",
    "severity",
    "code",
    "message",
    "source_dir",
)

CELL_DIMENSIONS = (
    "campaign_id",
    "cell_id",
    "machine",
    "recipe",
    "queue",
    "queue_variant",
    "variant",
    "scenario",
    "producers",
    "consumers",
    "total_threads",
    "batch_size",
    "allocator",
    "page_distributor",
    "distributor_params",
    "placement",
    "git_commit",
    "binary_sha256",
)

SUMMARY_FIELDS = CELL_DIMENSIONS + (
    "source",
    "metric_id",
    "raw_name",
    "unit",
    "phase",
    "scope",
    "dimensions_json",
    "n",
    "median",
    "mad",
    "q1",
    "q3",
    "ci_low",
    "ci_high",
    "provisional",
)

EFFECT_FIELDS = (
    "campaign_id",
    "machine",
    "recipe",
    "scenario",
    "producers",
    "consumers",
    "total_threads",
    "batch_size",
    "metric_id",
    "unit",
    "phase",
    "scope",
    "dimensions_json",
    "reference_variant",
    "treatment_variant",
    "selection",
    "pairs",
    "geomean_treatment_over_reference",
    "ci_low",
    "ci_high",
    "higher_is_better",
    "performance_geomean",
    "performance_ci_low",
    "performance_ci_high",
)

VARIANT_EFFECT_FIELDS = (
    "campaign_id",
    "design",
    "machine",
    "recipe",
    "queue",
    "queue_variant",
    "scenario",
    "producers",
    "consumers",
    "total_threads",
    "batch_size",
    "workload_mode",
    "placement",
    "git_commit",
    "source",
    "metric_id",
    "unit",
    "phase",
    "scope",
    "dimensions_json",
    "reference_variant",
    "treatment_variant",
    "selection",
    "reference_n",
    "treatment_n",
    "median_treatment_over_reference",
    "ci_low",
    "ci_high",
    "higher_is_better",
    "performance_median",
    "performance_ci_low",
    "performance_ci_high",
)


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def stable_hash(value: Any, length: int = 20) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()[:length]


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise AggregationError(f"could not parse {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise AggregationError(f"expected a JSON object in {path}")
    return value


def dotted_get(data: Mapping[str, Any], path: str) -> Any:
    current: Any = data
    for part in path.split("."):
        if not isinstance(current, Mapping) or part not in current:
            return None
        current = current[part]
    return current


def first_value(*sources_and_paths: Any, default: Any = None) -> Any:
    """Return the first non-empty value from ``(mapping, paths...)`` tuples."""

    for source, paths in sources_and_paths:
        if not isinstance(source, Mapping):
            continue
        for path in paths:
            value = dotted_get(source, path)
            if value is not None and value != "":
                return value
    return default


def parse_bool(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)) and value in (0, 1):
        return bool(value)
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in ("true", "yes", "1", "ok", "success", "completed"):
            return True
        if normalized in ("false", "no", "0", "failed", "error"):
            return False
    return None


def parse_int(value: Any) -> int | None:
    try:
        if value is None or isinstance(value, bool):
            return None
        return int(value)
    except (TypeError, ValueError, OverflowError):
        return None


def parse_number(value: Any) -> float | None:
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, (int, float)):
        result = float(value)
        return result if math.isfinite(result) else None
    text = str(value).strip()
    if not text or text.startswith("<") or text.lower() in {
        "nan",
        "inf",
        "+inf",
        "-inf",
        "not counted",
        "not supported",
    }:
        return None
    text = text.replace("\u202f", "").replace(" ", "")
    # ``perf stat -x ';'`` is commonly used under locales with decimal commas.
    if "," in text and "." not in text:
        text = text.replace(",", ".")
    try:
        result = float(text)
    except ValueError:
        return None
    return result if math.isfinite(result) else None


def slug(value: str) -> str:
    normalized = re.sub(r"[^a-z0-9]+", "_", value.strip().lower()).strip("_")
    return normalized or "unnamed"


def scenario_parts(raw: Any, case: Mapping[str, Any]) -> tuple[str, int | None, int | None]:
    if isinstance(raw, Mapping):
        producers = parse_int(raw.get("producers"))
        consumers = parse_int(raw.get("consumers"))
        name = str(raw.get("name") or "")
    else:
        name = str(raw or "")
        producers = consumers = None
    match = re.fullmatch(r"([1-9][0-9]*)p([1-9][0-9]*)c", name.strip().lower())
    if match:
        producers = int(match.group(1))
        consumers = int(match.group(2))
        name = f"{producers}p{consumers}c"
    producers = producers or parse_int(
        first_value((case, ("producers", "workload.producers", "scenario.producers")))
    )
    consumers = consumers or parse_int(
        first_value((case, ("consumers", "workload.consumers", "scenario.consumers")))
    )
    if not name and producers and consumers:
        name = f"{producers}p{consumers}c"
    return name or "unknown", producers, consumers


def queue_parts(raw: Any) -> tuple[str, str]:
    if isinstance(raw, Mapping):
        name = str(raw.get("kind") or raw.get("name") or "unknown")
        variant = str(raw.get("variant") or raw.get("label") or "")
        return name, variant
    return str(raw or "unknown"), ""


def named_config(raw: Any, default: str) -> tuple[str, str]:
    if isinstance(raw, Mapping):
        name = str(raw.get("kind") or raw.get("name") or raw.get("policy") or default)
        params = {key: value for key, value in raw.items() if key not in {"kind", "name", "policy"}}
        return name, canonical_json(params) if params else "{}"
    if raw in (None, ""):
        return default, "{}"
    return str(raw), "{}"


def issue(severity: str, code: str, message: str) -> dict[str, str]:
    return {"severity": severity, "code": code, "message": message}


def zip_json(archive: zipfile.ZipFile, member: str) -> Any:
    with archive.open(member) as stream:
        return json.load(io.TextIOWrapper(stream, encoding="utf-8"))


def zip_text(archive: zipfile.ZipFile, member: str) -> str:
    with archive.open(member) as stream:
        return stream.read().decode("utf-8", errors="replace")


def member_by_suffix(
    names: Sequence[str], suffix: str, issues: list[dict[str, str]], required: bool = False
) -> str | None:
    candidates = sorted((name for name in names if name.endswith(suffix)), key=lambda value: (value.count("/"), value))
    if not candidates:
        if required:
            issues.append(issue("error", "apx_component_missing", f"APX archive lacks {suffix}"))
        return None
    if len(candidates) > 1:
        issues.append(
            issue(
                "warning",
                "apx_component_ambiguous",
                f"multiple APX members end with {suffix}; using {candidates[0]}",
            )
        )
    return candidates[0]


APX_METRIC_IDS = {
    "Sample Count": "apx.sample_count",
    "Instructions Per Cycle": "apx.instructions_per_cycle",
    "Cycles Per Instruction": "apx.cycles_per_instruction",
    "Frontend Stalled Cycles": "apx.frontend_stalled_cycles_percent",
    "Backend Stalled Cycles": "apx.backend_stalled_cycles_percent",
    "Frontend Bound": "apx.frontend_bound_percent",
    "Backend Bound": "apx.backend_bound_percent",
    "Retiring": "apx.retiring_percent",
    "Bad Speculation": "apx.bad_speculation_percent",
    "L1D Cache MPKI": "apx.l1d_cache_mpki",
    "L2 Cache MPKI": "apx.l2_cache_mpki",
    "L3 Cache MPKI": "apx.l3_cache_mpki",
    "DTLB MPKI": "apx.dtlb_mpki",
    "Branch MPKI": "apx.branch_mpki",
    "Branch Misprediction Percentage": "apx.branch_misprediction_percent",
    "Barrier Operations Percentage": "apx.barrier_operations_percent",
    "Load Operations Percentage": "apx.load_operations_percent",
    "Store Operations Percentage": "apx.store_operations_percent",
    "Load-Exclusive Instructions Percentage": "apx.load_exclusive_instructions_percent",
    "Store-Exclusive Instructions Percentage": "apx.store_exclusive_instructions_percent",
    "Store-Exclusive Failures PKI": "apx.store_exclusive_failures_pki",
}


def canonical_apx_metric(name: str) -> str:
    return APX_METRIC_IDS.get(name, f"apx.{slug(name)}")


def parse_apx_archive(path: Path) -> ParsedApx:
    parsed = ParsedApx(path=path)
    try:
        archive = zipfile.ZipFile(path)
    except Exception as exc:
        parsed.issues.append(issue("error", "apx_zip_invalid", str(exc)))
        return parsed

    with archive:
        names = archive.namelist()
        metadata_member = member_by_suffix(names, "/metadata.json", parsed.issues, required=True)
        manifest_member = member_by_suffix(names, "/manifest.json", parsed.issues, required=True)
        try:
            if metadata_member:
                value = zip_json(archive, metadata_member)
                parsed.metadata = value if isinstance(value, dict) else {}
            if manifest_member:
                value = zip_json(archive, manifest_member)
                parsed.manifest = value if isinstance(value, dict) else {}
        except Exception as exc:
            parsed.issues.append(issue("error", "apx_metadata_invalid", str(exc)))

        workload_candidates: list[dict[str, Any]] = []
        for member in sorted(name for name in names if name.endswith("/capture_log.txt")):
            try:
                for line in zip_text(archive, member).splitlines():
                    line = line.strip()
                    if not line.startswith("{"):
                        continue
                    try:
                        candidate = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if isinstance(candidate, dict) and {
                        "queue",
                        "scenario",
                        "elapsed_ns",
                        "items",
                    }.issubset(candidate):
                        workload_candidates.append(candidate)
            except Exception as exc:
                parsed.issues.append(issue("warning", "apx_capture_log_invalid", str(exc)))
        unique_workloads = {canonical_json(value): value for value in workload_candidates}
        if len(unique_workloads) == 1:
            parsed.workload = next(iter(unique_workloads.values()))
        elif len(unique_workloads) > 1:
            parsed.issues.append(
                issue("error", "apx_workload_ambiguous", "archive contains conflicting workload JSON objects")
            )

        total_metrics_member = member_by_suffix(
            names, "/output/callpath_total_metrics.json", parsed.issues
        )
        if total_metrics_member:
            try:
                payload = zip_json(archive, total_metrics_member)
                columns = payload.get("columns", []) if isinstance(payload, dict) else []
                rows = payload.get("rows", []) if isinstance(payload, dict) else []
                root = next(
                    (row for row in rows if parse_int(row.get("call_frame_id")) == 0),
                    None,
                )
                if root is None:
                    parsed.issues.append(issue("error", "apx_root_metrics_missing", "no call_frame_id 0"))
                else:
                    values = root.get("column_data", [])
                    if len(values) != len(columns):
                        parsed.issues.append(
                            issue("error", "apx_metric_shape_mismatch", "column_data length differs from columns")
                        )
                    seen_ids: Counter[str] = Counter()
                    for column, raw_value in zip(columns, values):
                        if not isinstance(column, Mapping):
                            continue
                        value = parse_number(raw_value)
                        if value is None:
                            continue
                        raw_name = str(column.get("name") or "unnamed")
                        metric_id = canonical_apx_metric(raw_name)
                        seen_ids[metric_id] += 1
                        if seen_ids[metric_id] > 1:
                            metric_id = f"{metric_id}.{seen_ids[metric_id]}"
                            parsed.issues.append(
                                issue("warning", "apx_metric_duplicate", f"duplicate APX metric {raw_name}")
                            )
                        parsed.metrics.append(
                            {
                                "metric_id": metric_id,
                                "raw_name": raw_name,
                                "value": value,
                                "unit": str(column.get("units") or "unknown"),
                                "kind": "gauge",
                                "source": "apx",
                                "phase": "whole_process",
                                "scope": "process_total",
                                "dimensions_json": "{}",
                            }
                        )
            except Exception as exc:
                parsed.issues.append(issue("error", "apx_metrics_invalid", str(exc)))

        root_samples: float | None = None
        total_samples_member = member_by_suffix(
            names, "/output/callpath_total_samples.json", parsed.issues
        )
        if total_samples_member:
            try:
                payload = zip_json(archive, total_samples_member)
                columns = payload.get("columns", [])
                root = next(
                    (row for row in payload.get("rows", []) if parse_int(row.get("call_frame_id")) == 0),
                    None,
                )
                if root:
                    values = root.get("column_data", [])
                    index = next(
                        (
                            index
                            for index, column in enumerate(columns)
                            if isinstance(column, Mapping)
                            and (
                                column.get("units") == "sample_counter"
                                or "sample" in str(column.get("name", "")).lower()
                            )
                        ),
                        None,
                    )
                    if index is not None and index < len(values):
                        root_samples = parse_number(values[index])
                        if root_samples is not None and not any(
                            metric["metric_id"] == "apx.sample_count" for metric in parsed.metrics
                        ):
                            parsed.metrics.append(
                                {
                                    "metric_id": "apx.sample_count",
                                    "raw_name": "Sample Count",
                                    "value": root_samples,
                                    "unit": "samples",
                                    "kind": "counter",
                                    "source": "apx",
                                    "phase": "whole_process",
                                    "scope": "process_total",
                                    "dimensions_json": "{}",
                                }
                            )
            except Exception as exc:
                parsed.issues.append(issue("warning", "apx_samples_invalid", str(exc)))

        functions_member = member_by_suffix(
            names, "/output/functions-capture-periodic_sampling.csv", parsed.issues
        )
        if functions_member:
            try:
                reader = csv.DictReader(io.StringIO(zip_text(archive, functions_member)))
                for row in reader:
                    samples = parse_number(row.get("Periodic Samples"))
                    if samples is None:
                        continue
                    parsed.hotspots.append(
                        {
                            "image": str(row.get("image") or ""),
                            "symbol": str(row.get("symbol") or ""),
                            "inlined_from": str(row.get("inlined from") or ""),
                            "uid": str(row.get("uid") or ""),
                            "self_samples": samples,
                            "root_total_samples": root_samples if root_samples is not None else "",
                            "self_fraction": (
                                samples / root_samples if root_samples and root_samples > 0 else ""
                            ),
                            "source": "apx_code_hotspots",
                        }
                    )
                if root_samples is None:
                    parsed.issues.append(
                        issue("warning", "apx_hotspot_denominator_missing", "self sample fractions unavailable")
                    )
            except Exception as exc:
                parsed.issues.append(issue("error", "apx_hotspots_invalid", str(exc)))
    return parsed


PERF_EVENT_ALIASES = {
    "cycles": "perf.cycles",
    "cpu-cycles": "perf.cycles",
    "instructions": "perf.instructions",
    "cache-references": "perf.cache_references",
    "cache-misses": "perf.cache_misses",
    "branches": "perf.branches",
    "branch-instructions": "perf.branches",
    "branch-misses": "perf.branch_misses",
    "page-faults": "perf.page_faults",
    "faults": "perf.page_faults",
    "minor-faults": "perf.minor_faults",
    "major-faults": "perf.major_faults",
    "context-switches": "perf.context_switches",
    "cpu-migrations": "perf.cpu_migrations",
    "task-clock": "perf.task_clock",
}


def canonical_perf_event(event: str) -> str:
    normalized = event.strip().lower()
    return PERF_EVENT_ALIASES.get(normalized, f"perf.{slug(normalized)}")


def parse_perf_stat(path: Path) -> tuple[list[dict[str, Any]], list[dict[str, str]]]:
    metrics: list[dict[str, Any]] = []
    issues: list[dict[str, str]] = []
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except Exception as exc:
        return metrics, [issue("error", "perf_read_failed", str(exc))]

    seen: set[str] = set()
    for line_number, fields in enumerate(csv.reader(io.StringIO(text), delimiter=";"), 1):
        if not fields or not any(field.strip() for field in fields):
            continue
        if fields[0].lstrip().startswith("#"):
            continue
        if len(fields) < 3:
            issues.append(
                issue("warning", "perf_row_short", f"line {line_number} has fewer than three fields")
            )
            continue
        value = parse_number(fields[0])
        event = fields[2].strip()
        if not event:
            continue
        if value is None:
            issues.append(
                issue("warning", "perf_event_unavailable", f"{event}: {fields[0].strip() or 'empty'}")
            )
            continue
        metric_id = canonical_perf_event(event)
        if metric_id in seen:
            issues.append(issue("error", "perf_event_duplicate", f"duplicate event {event}"))
            continue
        seen.add(metric_id)
        run_time = parse_number(fields[3]) if len(fields) > 3 else None
        running_percent = parse_number(fields[4].rstrip("%")) if len(fields) > 4 else None
        dimensions = {}
        if run_time is not None:
            dimensions["run_time"] = run_time
        if running_percent is not None:
            dimensions["running_percent"] = running_percent
            if running_percent < 90:
                issues.append(
                    issue(
                        "warning",
                        "perf_multiplexed",
                        f"{event} ran for only {running_percent:.3g}% of enabled time",
                    )
                )
        metrics.append(
            {
                "metric_id": metric_id,
                "raw_name": event,
                "value": value,
                "unit": fields[1].strip() or "count",
                "kind": "counter",
                "source": "perf",
                "phase": "measure",
                "scope": "process",
                "dimensions_json": canonical_json(dimensions),
            }
        )
    return metrics, issues


def append_structured_metrics(
    output: list[dict[str, Any]], value: Any, prefix: str, source: str = "instrumentation"
) -> None:
    if isinstance(value, list):
        for entry in value:
            if not isinstance(entry, Mapping):
                continue
            raw_value = parse_number(entry.get("value"))
            name = entry.get("metric_id") or entry.get("name")
            if raw_value is None or not name:
                continue
            output.append(
                {
                    "metric_id": str(name),
                    "raw_name": str(entry.get("raw_name") or name),
                    "value": raw_value,
                    "unit": str(entry.get("unit") or "count"),
                    "kind": str(entry.get("kind") or "counter"),
                    "source": str(entry.get("source") or source),
                    "phase": str(entry.get("phase") or "measure"),
                    "scope": str(entry.get("scope") or "process"),
                    "dimensions_json": canonical_json(entry.get("dimensions") or {}),
                }
            )
        return
    if not isinstance(value, Mapping):
        return
    if "value" in value and parse_number(value.get("value")) is not None:
        output.append(
            {
                "metric_id": prefix,
                "raw_name": prefix,
                "value": parse_number(value["value"]),
                "unit": str(value.get("unit") or "count"),
                "kind": str(value.get("kind") or "counter"),
                "source": str(value.get("source") or source),
                "phase": str(value.get("phase") or "measure"),
                "scope": str(value.get("scope") or "process"),
                "dimensions_json": canonical_json(value.get("dimensions") or {}),
            }
        )
        return
    for key, nested in value.items():
        child_prefix = f"{prefix}.{slug(str(key))}" if prefix else slug(str(key))
        numeric = parse_number(nested)
        if numeric is not None and not isinstance(nested, Mapping):
            output.append(
                {
                    "metric_id": child_prefix,
                    "raw_name": str(key),
                    "value": numeric,
                    "unit": "count",
                    "kind": "counter",
                    "source": source,
                    "phase": "measure",
                    "scope": "process",
                    "dimensions_json": "{}",
                }
            )
        else:
            append_structured_metrics(output, nested, child_prefix, source)


def benchmark_metrics(benchmark: Mapping[str, Any]) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for key, prefix in (
        ("metrics", ""),
        ("counters", "instrumentation"),
        ("instrumentation", "instrumentation"),
        ("allocator_metrics", "allocator"),
        ("atomic_metrics", "atomic"),
        ("page_distributor_metrics", "page_distributor"),
    ):
        if key in benchmark:
            append_structured_metrics(output, benchmark[key], prefix)
    return output


def metric_row(
    metric_id: str,
    value: float | int,
    unit: str,
    *,
    raw_name: str | None = None,
    kind: str = "gauge",
    source: str = "benchmark",
    phase: str = "measure",
    scope: str = "process",
    dimensions: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "metric_id": metric_id,
        "raw_name": raw_name or metric_id,
        "value": float(value),
        "unit": unit,
        "kind": kind,
        "source": source,
        "phase": phase,
        "scope": scope,
        "dimensions_json": canonical_json(dimensions or {}),
    }


def add_derived_metrics(metrics: list[dict[str, Any]], benchmark: Mapping[str, Any]) -> None:
    items = parse_number(benchmark.get("items") or benchmark.get("consumed_items"))
    elapsed_ns = parse_number(benchmark.get("elapsed_ns"))
    requested_ms = parse_number(
        benchmark.get("requested_duration_ms") or benchmark.get("duration_ms")
    )
    ops_per_sec = parse_number(benchmark.get("ops_per_sec"))
    if ops_per_sec is not None:
        metrics.append(metric_row("workload.ops_per_sec", ops_per_sec, "ops/s"))
    if elapsed_ns is not None:
        metrics.append(metric_row("workload.elapsed_ns", elapsed_ns, "ns"))
    if items is not None:
        metrics.append(metric_row("workload.items", items, "count", kind="counter"))
    if elapsed_ns is not None and requested_ms and requested_ms > 0:
        metrics.append(
            metric_row(
                "workload.actual_to_requested_duration",
                elapsed_ns / (requested_ms * 1_000_000.0),
                "ratio",
                source="derived",
            )
        )

    by_id = {row["metric_id"]: row for row in metrics}
    cycles = parse_number((by_id.get("perf.cycles") or {}).get("value"))
    instructions = parse_number((by_id.get("perf.instructions") or {}).get("value"))
    if items and items > 0:
        if cycles is not None:
            metrics.append(metric_row("perf.cycles_per_op", cycles / items, "cycles/op", source="derived"))
        if instructions is not None:
            metrics.append(
                metric_row(
                    "perf.instructions_per_op", instructions / items, "instructions/op", source="derived"
                )
            )
        for row in list(metrics):
            if row.get("kind") != "counter" or row.get("source") not in {"perf", "instrumentation"}:
                continue
            if row["metric_id"] in {"workload.items", "perf.cycles", "perf.instructions"}:
                continue
            metrics.append(
                metric_row(
                    f"{row['metric_id']}.per_million_ops",
                    float(row["value"]) * 1_000_000.0 / items,
                    f"{row.get('unit') or 'count'}/Mop",
                    raw_name=str(row.get("raw_name") or row["metric_id"]),
                    source="derived",
                    dimensions=json.loads(row.get("dimensions_json") or "{}"),
                )
            )
    if cycles and instructions and cycles > 0 and instructions > 0:
        metrics.append(metric_row("perf.instructions_per_cycle", instructions / cycles, "instructions/cycle", source="derived"))
        metrics.append(metric_row("perf.cycles_per_instruction", cycles / instructions, "cycles/instruction", source="derived"))


def load_sidecar(case_dir: Path, names: Sequence[str]) -> dict[str, Any]:
    for name in names:
        path = case_dir / name
        if path.is_file():
            return read_json(path)
    return {}


def resolve_status(case: Mapping[str, Any], status_sidecar: Mapping[str, Any], case_dir: Path) -> str:
    value = first_value(
        (status_sidecar, ("status", "result", "state")),
        (case, ("status", "result.status", "state")),
    )
    if value is None:
        for name in ("status.txt", "result-status.txt"):
            path = case_dir / name
            if path.is_file():
                value = path.read_text(encoding="utf-8", errors="replace").strip().splitlines()[0]
                break
    normalized = str(value or "completed").strip().lower().replace(" ", "_")
    return {"success": "completed", "ok": "completed", "complete": "completed"}.get(
        normalized, normalized
    )


def trial_identity(case_path: Path, root: Path, case: Mapping[str, Any]) -> tuple[str, str, str, bool]:
    try:
        relative = case_path.parent.relative_to(root)
    except ValueError as exc:
        raise AggregationError(f"{case_path} is outside {root}") from exc
    campaign_default = relative.parts[0] if len(relative.parts) >= 2 else "default"
    case_default = case_path.parent.name
    campaign = str(case.get("campaign_id") or case.get("campaign") or campaign_default)
    case_id = str(case.get("case_id") or case_default)
    explicit_trial = case.get("trial_id") is not None
    trial_id = str(case.get("trial_id") or f"{campaign}/{case_id}")
    return campaign, case_id, trial_id, explicit_trial


def parse_trial(case_path: Path, root: Path) -> ParsedTrial:
    case_dir = case_path.parent
    case = read_json(case_path)
    campaign_id, case_id, trial_id, _explicit_trial = trial_identity(case_path, root, case)
    provenance = load_sidecar(case_dir, ("provenance.json", "run-info.json", "slurm.json"))
    status_sidecar = load_sidecar(case_dir, ("status.json", "result.json"))
    apx_identity = load_sidecar(case_dir, ("apx-identity.json",))
    status = resolve_status(case, status_sidecar, case_dir)
    issues: list[dict[str, Any]] = []

    archives = sorted(case_dir.glob("*.zip"))
    if len(archives) > 1:
        raise AggregationError(f"{case_dir} contains multiple APX zip archives")
    apx = parse_apx_archive(archives[0]) if archives else None
    if apx:
        issues.extend(apx.issues)

    benchmark_path = case_dir / "benchmark.json"
    benchmark: dict[str, Any] | None = None
    benchmark_source = ""
    if benchmark_path.is_file():
        benchmark = read_json(benchmark_path)
        benchmark_source = "benchmark.json"
    elif apx and apx.workload:
        benchmark = dict(apx.workload)
        benchmark_source = "apx_capture_log"
        issues.append(issue("warning", "benchmark_fallback", "benchmark.json missing; used APX capture log"))
    else:
        benchmark = {}
        issues.append(issue("error", "benchmark_missing", "no benchmark.json or APX workload JSON"))

    if apx and apx.workload and benchmark_source == "benchmark.json":
        for key in ("queue", "scenario", "batch_size", "items", "affinity_ok"):
            if key in benchmark and key in apx.workload and benchmark[key] != apx.workload[key]:
                issues.append(
                    issue("warning", "benchmark_apx_mismatch", f"{key} differs between benchmark.json and APX capture")
                )

    if (
        benchmark_source == "benchmark.json"
        and benchmark.get("measurement_role") != "unprofiled_shadow"
    ):
        issues.append(
            issue(
                "error",
                "throughput_shadow_missing",
                "benchmark.json is not identified as an unprofiled throughput shadow",
            )
        )

    profiled_path = case_dir / "profiled-benchmark.json"
    if profiled_path.is_file():
        collected_benchmark: Mapping[str, Any] = read_json(profiled_path)
    elif apx and apx.workload:
        collected_benchmark = apx.workload
    else:
        collected_benchmark = {}

    queue_raw = first_value(
        (benchmark, ("queue",)),
        (case, ("queue", "factors.queue", "workload.queue")),
        default="unknown",
    )
    queue, nested_queue_variant = queue_parts(queue_raw)
    queue_variant = str(
        first_value(
            (case, ("queue_variant", "factors.queue_variant", "queue.variant", "variant.queue")),
            (benchmark, ("ubq_label", "queue_variant")),
            default=nested_queue_variant,
        )
        or queue
    )
    scenario_raw = first_value(
        (benchmark, ("scenario",)),
        (case, ("scenario", "factors.scenario", "workload.scenario")),
    )
    scenario, producers, consumers = scenario_parts(scenario_raw, case)
    total_threads = producers + consumers if producers and consumers else None
    batch_size = parse_int(
        first_value(
            (benchmark, ("batch_size",)),
            (case, ("batch_size", "factors.batch_size", "workload.batch_size")),
        )
    )

    allocator_raw = first_value((case, ("allocator", "factors.allocator", "variant.allocator")))
    allocator, allocator_params = named_config(allocator_raw, "system")
    distributor_raw = first_value(
        (case, ("page_distributor", "factors.page_distributor", "variant.page_distributor"))
    )
    page_distributor, distributor_params = named_config(distributor_raw, "default")
    explicit_distributor_params = first_value(
        (case, ("distributor_params", "factors.distributor_params"))
    )
    if explicit_distributor_params is not None:
        distributor_params = canonical_json(explicit_distributor_params)
    if allocator_params != "{}":
        try:
            combined = {"allocator": json.loads(allocator_params), "distributor": json.loads(distributor_params)}
            distributor_params = canonical_json(combined)
        except json.JSONDecodeError:
            pass

    collector = str(
        first_value((case, ("collector", "tool.collector")), default="apx" if apx else "native")
    )
    apx_recipe = apx.metadata.get("run.recipe_name") if apx else None
    recipe = str(
        first_value(
            (case, ("recipe", "tool.recipe", "factors.recipe", "profile.recipe")),
            (provenance, ("recipe", "tool.recipe")),
            default=apx_recipe
            or ("perf_stat" if collector == "perf" else "no_profile" if not apx else "unknown"),
        )
    )
    machine = str(
        first_value(
            (case, ("machine", "machine_class", "factors.machine", "platform.machine")),
            (provenance, ("machine", "machine_class", "platform.machine")),
            default="unknown",
        )
    )
    machine_label = str(
        first_value(
            (case, ("machine_label", "platform.machine_label")),
            (provenance, ("machine_label", "platform.machine_label")),
            default=machine,
        )
    )
    placement_raw = first_value(
        (case, ("core_placement", "placement", "factors.placement")), default="unknown"
    )
    if isinstance(placement_raw, Mapping):
        placement = str(placement_raw.get("name") or "unknown")
    else:
        placement = str(placement_raw)

    explicit_factors = case.get("factors")
    factor_payload = (
        explicit_factors
        if isinstance(explicit_factors, Mapping)
        else {
            "machine": machine,
            "recipe": recipe,
            "queue": queue,
            "queue_variant": queue_variant,
            "scenario": scenario,
            "batch_size": batch_size,
            "allocator": allocator,
            "page_distributor": page_distributor,
            "distributor_params": distributor_params,
            "placement": placement,
            "git_commit": first_value((case, ("git_commit",)), (provenance, ("git_commit",))),
            "binary_sha256": first_value((case, ("binary_sha256",)), (provenance, ("binary_sha256",))),
        }
    )
    factors_json = canonical_json(factor_payload)
    cell_id = str(case.get("cell_id") or stable_hash(factor_payload))
    variant = str(
        first_value(
            (case, ("comparison_variant", "variant_name", "variant.id", "variant.name")),
            default="|".join(
                part
                for part in (queue, queue_variant, allocator, page_distributor)
                if part and part not in {"default", "system"}
            )
            or queue,
        )
    )
    is_baseline = bool(
        parse_bool(
            first_value((case, ("is_baseline", "comparison.is_baseline", "baseline")), default=False)
        )
    )
    reference_variant = str(
        first_value((case, ("reference_variant", "comparison.reference_variant")), default="")
    )

    benchmark_affinity = parse_bool(benchmark.get("affinity_ok"))
    apx_result = str(apx.metadata.get("run.result", "")).lower() if apx else ""
    if apx and apx_result != "success":
        issues.append(issue("error", "apx_run_failed", f"APX run.result is {apx_result or 'missing'}"))
    quality_policy = case.get("quality_policy")
    quality_policy = quality_policy if isinstance(quality_policy, Mapping) else {}
    if quality_policy.get("require_affinity", True) and benchmark_affinity is not True:
        issues.append(
            issue(
                "error",
                "affinity_not_authoritative",
                "benchmark affinity_ok is false or missing",
            )
        )
    collected_affinity = parse_bool(collected_benchmark.get("affinity_ok"))
    if status == "completed" and not collected_benchmark:
        issues.append(
            issue("error", "collected_benchmark_missing", "fixed-work collector result is missing")
        )
    elif collected_benchmark:
        if parse_bool(collected_benchmark.get("fixed_work")) is not True:
            issues.append(
                issue("error", "collector_not_fixed_work", "collector workload was not exact-count fixed work")
            )
        if quality_policy.get("require_affinity", True) and collected_affinity is not True:
            issues.append(
                issue("error", "collector_affinity_failed", "collector workload affinity is false or missing")
            )
        benchmark_items = parse_int(benchmark.get("items"))
        collected_items = parse_int(collected_benchmark.get("items"))
        if benchmark_items is not None and collected_items is not None and benchmark_items != collected_items:
            issues.append(
                issue(
                    "error",
                    "fixed_work_item_mismatch",
                    f"shadow measured {benchmark_items} items but collector measured {collected_items}",
                )
            )
    duration_ratio = parse_number(benchmark.get("actual_to_requested_duration"))
    if duration_ratio is None:
        elapsed = parse_number(benchmark.get("elapsed_ns"))
        requested_ms = parse_number(benchmark.get("requested_duration_ms"))
        if elapsed is not None and requested_ms and requested_ms > 0:
            duration_ratio = elapsed / (requested_ms * 1_000_000.0)
    minimum_ratio = parse_number(quality_policy.get("duration_ratio_min"))
    maximum_ratio = parse_number(quality_policy.get("duration_ratio_max"))
    if minimum_ratio is not None and (duration_ratio is None or duration_ratio < minimum_ratio):
        issues.append(
            issue(
                "error",
                "duration_too_short",
                f"shadow duration ratio {duration_ratio!r} is below {minimum_ratio}",
            )
        )
    if maximum_ratio is not None and (duration_ratio is None or duration_ratio > maximum_ratio):
        issues.append(
            issue(
                "error",
                "duration_too_long",
                f"shadow duration ratio {duration_ratio!r} exceeds {maximum_ratio}",
            )
        )
    minimum_samples = parse_number(quality_policy.get("minimum_samples"))
    if minimum_samples is not None and collector == "apx":
        sample_count = next(
            (
                parse_number(metric.get("value"))
                for metric in apx.metrics
                if metric.get("metric_id") == "apx.sample_count"
            ),
            None,
        ) if apx else None
        if sample_count is None or sample_count < minimum_samples:
            issues.append(
                issue(
                    "error",
                    "apx_undersampled",
                    f"APX sample count {sample_count!r} is below {minimum_samples}",
                )
            )
    if collector == "apx" and apx is None:
        issues.append(issue("error", "apx_archive_missing", "completed APX case has no archive"))
    if collector == "perf" and not (case_dir / "perf.csv").is_file():
        issues.append(issue("error", "perf_stat_missing", "completed perf case has no perf.csv"))
    if case.get("allocator_probe", False):
        allocator_metrics = benchmark.get("allocator_metrics")
        if not isinstance(allocator_metrics, list) or not allocator_metrics:
            issues.append(
                issue(
                    "error",
                    "allocator_probe_missing",
                    "case requested an allocator probe but benchmark.json has no allocator metrics",
                )
            )
    if status != "completed":
        issues.append(issue("error", "trial_not_completed", f"status is {status}"))
    if machine == "unknown":
        issues.append(issue("error", "machine_missing", "machine factor is missing"))
    if recipe == "unknown":
        issues.append(issue("error", "recipe_missing", "recipe factor is missing"))

    metrics = benchmark_metrics(benchmark)
    if apx:
        metrics.extend(apx.metrics)
    perf_path = case_dir / "perf.csv"
    if perf_path.is_file():
        perf_metrics, perf_issues = parse_perf_stat(perf_path)
        metrics.extend(perf_metrics)
        issues.extend(perf_issues)
    add_derived_metrics(metrics, benchmark)

    seen_metric_keys: set[tuple[str, str, str, str, str]] = set()
    for metric in metrics:
        key = (
            str(metric.get("source")),
            str(metric.get("metric_id")),
            str(metric.get("phase")),
            str(metric.get("scope")),
            str(metric.get("dimensions_json")),
        )
        if key in seen_metric_keys:
            raise AggregationError(f"duplicate metric {key} in {case_dir}")
        seen_metric_keys.add(key)

    authoritative = not any(item["severity"] == "error" for item in issues)
    apx_run_id = ""
    if apx:
        apx_run_id = str(apx.metadata.get("run.id") or archives[0].stem)
    row = {
        "campaign_id": campaign_id,
        "case_id": case_id,
        "trial_id": trial_id,
        "cell_id": cell_id,
        "pair_block_id": str(
            first_value((case, ("pair_block_id", "comparison.pair_block_id")), default="")
        ),
        "comparison_group_id": str(case.get("comparison_group_id") or ""),
        "comparison_role": str(case.get("comparison_role") or ""),
        "comparison_sets_json": canonical_json(case.get("comparison_sets", [])),
        "selection_evidence_json": canonical_json(case.get("selection_evidence", {})),
        "repeat_index": (
            parsed_repeat
            if (parsed_repeat := parse_int(first_value((case, ("repeat_index", "repeat")), default=1)))
            is not None
            else 1
        ),
        "status": status,
        "authoritative": authoritative,
        "machine": machine,
        "machine_label": machine_label,
        "node": str(first_value((case, ("node", "hostname")), (provenance, ("node", "hostname")), default="")),
        "recipe": recipe,
        "collector": collector,
        "queue": queue,
        "queue_variant": queue_variant,
        "variant": variant,
        "is_baseline": is_baseline,
        "reference_variant": reference_variant,
        "scenario": scenario,
        "producers": producers if producers is not None else "",
        "consumers": consumers if consumers is not None else "",
        "total_threads": total_threads if total_threads is not None else "",
        "batch_size": batch_size if batch_size is not None else "",
        "batch_mode": "batched" if batch_size is not None else "scalar",
        "backoff": str(
            case.get("lubq_backoff")
            or (
                str(case.get("ubq_label")).rsplit(",", 1)[-1]
                if case.get("ubq_label")
                else ""
            )
        ),
        "block_size": (
            case.get("fastfifo_block_size")
            or case.get("lfqueue_segment_size")
            or ("page" if case.get("queue") in {"ubq", "lubq"} else "")
        ),
        "queue_capacity": case.get("fastfifo_capacity") or "",
        "workload_mode": str(first_value((case, ("workload_mode", "workload.mode")), default="handoff")),
        "allocator": allocator,
        "page_distributor": page_distributor,
        "distributor_params": distributor_params,
        "placement": placement,
        "git_commit": str(first_value((case, ("git_commit",)), (provenance, ("git_commit",)), default="")),
        "git_dirty": parse_bool(first_value((case, ("git_dirty",)), (provenance, ("git_dirty",)), default=False)),
        "binary_sha256": str(first_value((case, ("binary_sha256",)), (provenance, ("binary_sha256",)), default="")),
        "slurm_job_id": str(first_value((case, ("slurm_job_id", "scheduler.job_id")), (provenance, ("slurm_job_id", "scheduler.job_id")), default="")),
        "constraint": str(first_value((case, ("constraint", "scheduler.constraint")), (provenance, ("constraint", "scheduler.constraint")), default="")),
        "requested_duration_ms": parse_int(benchmark.get("requested_duration_ms")) or "",
        "warmup_ms": parse_int(benchmark.get("warmup_ms")) or "",
        "elapsed_ns": parse_int(benchmark.get("elapsed_ns")) or "",
        "items": parse_int(benchmark.get("items") or benchmark.get("consumed_items")) or "",
        "ops_per_sec": parse_number(benchmark.get("ops_per_sec")) or "",
        "affinity_ok": benchmark_affinity is True,
        "benchmark_source": benchmark_source,
        "apx_archive": str(archives[0].relative_to(root)) if archives else "",
        "apx_run_id": apx_run_id,
        "apx_run_name": str(
            first_value(
                (apx_identity, ("intended_run_name",)),
                (status_sidecar, ("apx_run_name",)),
                (case, ("apx_run_name",)),
                default="",
            )
        ),
        "factors_json": factors_json,
        "provenance_json": canonical_json(provenance),
        "source_dir": str(case_dir.relative_to(root)),
    }

    identity = {key: row.get(key, "") for key in METRIC_FIELDS if key in row}
    normalized_metrics = []
    for metric in metrics:
        normalized = dict(identity)
        normalized.update(metric)
        normalized_metrics.append(normalized)
    hotspot_identity = {key: row.get(key, "") for key in HOTSPOT_FIELDS if key in row}
    normalized_hotspots = []
    for hotspot in apx.hotspots if apx else []:
        normalized = dict(hotspot_identity)
        normalized.update(hotspot)
        normalized_hotspots.append(normalized)

    normalized_issues = []
    if not issues:
        issues.append(issue("ok", "authoritative", "trial passed all quality gates"))
    for item in issues:
        normalized_issues.append(
            {
                "campaign_id": campaign_id,
                "case_id": case_id,
                "trial_id": trial_id,
                "authoritative": authoritative,
                "severity": item["severity"],
                "code": item["code"],
                "message": item["message"],
                "source_dir": row["source_dir"],
            }
        )
    return ParsedTrial(row=row, metrics=normalized_metrics, hotspots=normalized_hotspots, issues=normalized_issues)


def percentile(values: Sequence[float], fraction: float) -> float:
    if not values:
        raise ValueError("percentile requires at least one value")
    ordered = sorted(values)
    position = (len(ordered) - 1) * fraction
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    weight = position - lower
    return ordered[lower] * (1 - weight) + ordered[upper] * weight


def bootstrap_median_ci(values: Sequence[float], samples: int, seed_key: Any) -> tuple[float, float]:
    if len(values) == 1 or samples <= 0:
        return values[0], values[0]
    seed = int(hashlib.sha256(canonical_json(seed_key).encode()).hexdigest()[:16], 16)
    rng = random.Random(seed)
    size = len(values)
    estimates = [statistics.median(rng.choices(values, k=size)) for _ in range(samples)]
    return percentile(estimates, 0.025), percentile(estimates, 0.975)


def summarize_values(values: Sequence[float], samples: int, seed_key: Any) -> dict[str, Any]:
    median = statistics.median(values)
    absolute = [abs(value - median) for value in values]
    low, high = bootstrap_median_ci(values, samples, seed_key)
    return {
        "n": len(values),
        "median": median,
        "mad": statistics.median(absolute),
        "q1": percentile(values, 0.25),
        "q3": percentile(values, 0.75),
        "ci_low": low,
        "ci_high": high,
        "provisional": len(values) < 3,
    }


def build_cell_summary(
    trials: Sequence[dict[str, Any]], metrics: Sequence[dict[str, Any]], bootstrap_samples: int
) -> list[dict[str, Any]]:
    trial_by_id = {str(row["trial_id"]): row for row in trials}
    grouped: dict[tuple[Any, ...], list[tuple[dict[str, Any], float]]] = defaultdict(list)
    for metric in metrics:
        trial = trial_by_id.get(str(metric["trial_id"]))
        if not trial or not trial.get("authoritative"):
            continue
        value = parse_number(metric.get("value"))
        if value is None:
            continue
        key_values = tuple(trial.get(key, "") for key in CELL_DIMENSIONS)
        metric_values = (
            metric.get("source", ""),
            metric.get("metric_id", ""),
            metric.get("unit", ""),
            metric.get("phase", ""),
            metric.get("scope", ""),
            metric.get("dimensions_json", "{}"),
        )
        grouped[key_values + metric_values].append((metric, value))

    rows = []
    for key, entries in sorted(grouped.items(), key=lambda item: tuple(str(value) for value in item[0])):
        values = [value for _, value in entries]
        dimensions = dict(zip(CELL_DIMENSIONS, key[: len(CELL_DIMENSIONS)]))
        source, metric_id, unit, phase, scope, dimensions_json = key[len(CELL_DIMENSIONS) :]
        row = {
            **dimensions,
            "source": source,
            "metric_id": metric_id,
            "raw_name": entries[0][0].get("raw_name", metric_id),
            "unit": unit,
            "phase": phase,
            "scope": scope,
            "dimensions_json": dimensions_json,
        }
        row.update(summarize_values(values, bootstrap_samples, key))
        rows.append(row)
    return rows


def higher_is_better(metric_id: str) -> bool:
    lowered = metric_id.lower()
    return not any(
        token in lowered
        for token in (
            "elapsed",
            "latency",
            "cycles_per",
            "instructions_per_op",
            "miss",
            "fault",
            "stalled",
            "bound",
            "failure",
            "alloc",
            "dealloc",
            "migration",
            "context_switch",
            "actual_to_requested",
        )
    )


def bootstrap_geomean_ci(log_values: Sequence[float], samples: int, seed_key: Any) -> tuple[float, float]:
    if len(log_values) == 1 or samples <= 0:
        value = math.exp(log_values[0])
        return value, value
    seed = int(hashlib.sha256(canonical_json(seed_key).encode()).hexdigest()[:16], 16)
    rng = random.Random(seed)
    size = len(log_values)
    estimates = [math.exp(statistics.fmean(rng.choices(log_values, k=size))) for _ in range(samples)]
    return percentile(estimates, 0.025), percentile(estimates, 0.975)


def build_paired_effects(
    trials: Sequence[dict[str, Any]], metrics: Sequence[dict[str, Any]], bootstrap_samples: int
) -> list[dict[str, Any]]:
    trial_by_id = {str(row["trial_id"]): row for row in trials if row.get("authoritative")}
    metric_groups: dict[tuple[Any, ...], dict[str, tuple[dict[str, Any], float]]] = defaultdict(dict)
    for metric in metrics:
        trial = trial_by_id.get(str(metric["trial_id"]))
        if not trial or not trial.get("pair_block_id"):
            continue
        value = parse_number(metric.get("value"))
        if value is None or value <= 0:
            continue
        identity = (
            trial["campaign_id"],
            trial["machine"],
            trial["recipe"],
            trial["pair_block_id"],
            trial["repeat_index"],
            trial["scenario"],
            trial["producers"],
            trial["consumers"],
            trial["total_threads"],
            trial["batch_size"],
            metric["metric_id"],
            metric["unit"],
            metric["phase"],
            metric["scope"],
            metric["dimensions_json"],
        )
        variant = str(trial["variant"])
        if variant in metric_groups[identity]:
            raise AggregationError(
                f"pair block {trial['pair_block_id']} contains variant {variant} more than once"
            )
        metric_groups[identity][variant] = (trial, value)

    comparisons: dict[tuple[Any, ...], list[float]] = defaultdict(list)
    selections: dict[tuple[Any, ...], str] = {}
    for identity, variants in metric_groups.items():
        if len(variants) < 2:
            continue
        baseline_variants = [name for name, (trial, _value) in variants.items() if trial.get("is_baseline")]
        declared = {
            str(trial.get("reference_variant"))
            for trial, _value in variants.values()
            if trial.get("reference_variant")
        }
        if len(baseline_variants) == 1:
            reference = baseline_variants[0]
            selection = "declared_baseline"
        elif len(declared) == 1 and next(iter(declared)) in variants:
            reference = next(iter(declared))
            selection = "declared_reference"
        elif len(baseline_variants) > 1 or len(declared) > 1:
            raise AggregationError(f"pair block {identity[3]} has ambiguous reference variants")
        else:
            reference = sorted(variants)[0]
            selection = "lexical_fallback"
        reference_value = variants[reference][1]
        common = identity[:3] + identity[5:]
        for treatment, (_trial, treatment_value) in variants.items():
            if treatment == reference:
                continue
            key = common + (reference, treatment)
            comparisons[key].append(math.log(treatment_value / reference_value))
            selections[key] = selection

    rows = []
    for key, logs in sorted(comparisons.items(), key=lambda item: tuple(str(value) for value in item[0])):
        (
            campaign,
            machine,
            recipe,
            scenario,
            producers,
            consumers,
            total_threads,
            batch_size,
            metric_id,
            unit,
            phase,
            scope,
            dimensions_json,
            reference,
            treatment,
        ) = key
        ratio = math.exp(statistics.fmean(logs))
        low, high = bootstrap_geomean_ci(logs, bootstrap_samples, key)
        is_higher = higher_is_better(str(metric_id))
        row = {
            "campaign_id": campaign,
            "machine": machine,
            "recipe": recipe,
            "scenario": scenario,
            "producers": producers,
            "consumers": consumers,
            "total_threads": total_threads,
            "batch_size": batch_size,
            "metric_id": metric_id,
            "unit": unit,
            "phase": phase,
            "scope": scope,
            "dimensions_json": dimensions_json,
            "reference_variant": reference,
            "treatment_variant": treatment,
            "selection": selections[key],
            "pairs": len(logs),
            "geomean_treatment_over_reference": ratio,
            "ci_low": low,
            "ci_high": high,
            "higher_is_better": is_higher,
            "performance_geomean": ratio if is_higher else 1.0 / ratio,
            "performance_ci_low": low if is_higher else 1.0 / high,
            "performance_ci_high": high if is_higher else 1.0 / low,
        }
        rows.append(row)
    return rows


def bootstrap_independent_median_ratio_ci(
    reference: Sequence[float],
    treatment: Sequence[float],
    samples: int,
    seed_key: Any,
) -> tuple[float, float]:
    ratio = statistics.median(treatment) / statistics.median(reference)
    if samples <= 0 or (len(reference) == 1 and len(treatment) == 1):
        return ratio, ratio
    seed = int(hashlib.sha256(canonical_json(seed_key).encode()).hexdigest()[:16], 16)
    rng = random.Random(seed)
    estimates = []
    for _index in range(samples):
        reference_sample = rng.choices(reference, k=len(reference))
        treatment_sample = rng.choices(treatment, k=len(treatment))
        denominator = statistics.median(reference_sample)
        if denominator > 0:
            estimates.append(statistics.median(treatment_sample) / denominator)
    if not estimates:
        return ratio, ratio
    return percentile(estimates, 0.025), percentile(estimates, 0.975)


def build_independent_variant_effects(
    trials: Sequence[dict[str, Any]], metrics: Sequence[dict[str, Any]], bootstrap_samples: int
) -> list[dict[str, Any]]:
    trial_by_id = {str(row["trial_id"]): row for row in trials if row.get("authoritative")}
    grouped: dict[tuple[Any, ...], dict[str, list[float]]] = defaultdict(lambda: defaultdict(list))
    trial_groups: dict[tuple[Any, ...], dict[str, list[dict[str, Any]]]] = defaultdict(
        lambda: defaultdict(list)
    )
    for metric in metrics:
        trial = trial_by_id.get(str(metric["trial_id"]))
        value = parse_number(metric.get("value"))
        if not trial or value is None or value <= 0:
            continue
        identity = (
            trial["campaign_id"],
            trial["machine"],
            trial["recipe"],
            trial["queue"],
            trial["queue_variant"],
            trial["scenario"],
            trial["producers"],
            trial["consumers"],
            trial["total_threads"],
            trial["batch_size"],
            trial.get("workload_mode", "handoff"),
            trial.get("placement", "unknown"),
            trial.get("git_commit", ""),
            metric["source"],
            metric["metric_id"],
            metric["unit"],
            metric["phase"],
            metric["scope"],
            metric["dimensions_json"],
        )
        variant = str(trial["variant"])
        grouped[identity][variant].append(value)
        trial_groups[identity][variant].append(trial)

    rows = []
    for identity, variants in sorted(
        grouped.items(), key=lambda item: tuple(str(value) for value in item[0])
    ):
        if len(variants) < 2:
            continue
        baseline_variants = [
            variant
            for variant, variant_trials in trial_groups[identity].items()
            if any(trial.get("is_baseline") for trial in variant_trials)
        ]
        declared = {
            str(trial.get("reference_variant"))
            for variant_trials in trial_groups[identity].values()
            for trial in variant_trials
            if trial.get("reference_variant")
        }
        if len(baseline_variants) == 1:
            reference = baseline_variants[0]
            selection = "declared_baseline"
        elif len(declared) == 1 and next(iter(declared)) in variants:
            reference = next(iter(declared))
            selection = "declared_reference"
        elif len(baseline_variants) > 1 or len(declared) > 1:
            raise AggregationError(f"variant cell {identity} has ambiguous reference variants")
        else:
            reference = sorted(variants)[0]
            selection = "lexical_fallback"
        reference_values = variants[reference]
        for treatment, treatment_values in sorted(variants.items()):
            if treatment == reference:
                continue
            estimate = statistics.median(treatment_values) / statistics.median(reference_values)
            low, high = bootstrap_independent_median_ratio_ci(
                reference_values, treatment_values, bootstrap_samples, identity + (reference, treatment)
            )
            (
                campaign,
                machine,
                recipe,
                queue,
                queue_variant,
                scenario,
                producers,
                consumers,
                total_threads,
                batch_size,
                workload_mode,
                placement,
                git_commit,
                source,
                metric_id,
                unit,
                phase,
                scope,
                dimensions_json,
            ) = identity
            is_higher = higher_is_better(str(metric_id))
            rows.append(
                {
                    "campaign_id": campaign,
                    "design": "independent_repeats",
                    "machine": machine,
                    "recipe": recipe,
                    "queue": queue,
                    "queue_variant": queue_variant,
                    "scenario": scenario,
                    "producers": producers,
                    "consumers": consumers,
                    "total_threads": total_threads,
                    "batch_size": batch_size,
                    "workload_mode": workload_mode,
                    "placement": placement,
                    "git_commit": git_commit,
                    "source": source,
                    "metric_id": metric_id,
                    "unit": unit,
                    "phase": phase,
                    "scope": scope,
                    "dimensions_json": dimensions_json,
                    "reference_variant": reference,
                    "treatment_variant": treatment,
                    "selection": selection,
                    "reference_n": len(reference_values),
                    "treatment_n": len(treatment_values),
                    "median_treatment_over_reference": estimate,
                    "ci_low": low,
                    "ci_high": high,
                    "higher_is_better": is_higher,
                    "performance_median": estimate if is_higher else 1.0 / estimate,
                    "performance_ci_low": low if is_higher else 1.0 / high,
                    "performance_ci_high": high if is_higher else 1.0 / low,
                }
            )
    return rows


def write_csv(path: Path, fields: Sequence[str], rows: Iterable[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fields})


def campaign_expected_count(campaign_dir: Path) -> int | None:
    manifest = campaign_dir / "campaign.json"
    if not manifest.is_file():
        return None
    value = read_json(manifest)
    for key in ("expected_cases", "planned_case_count"):
        parsed = parse_int(value.get(key))
        if parsed is not None:
            return parsed
    for key in ("expected_case_ids", "planned_cases", "cases"):
        entries = value.get(key)
        if isinstance(entries, (list, dict)):
            return len(entries)
    return None


def build_coverage(root: Path, trials: Sequence[dict[str, Any]], quality: Sequence[dict[str, Any]]) -> dict[str, Any]:
    by_campaign: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for trial in trials:
        by_campaign[str(trial["campaign_id"])].append(trial)
    issue_counts = Counter(
        str(row["code"]) for row in quality if str(row.get("severity")) in {"warning", "error"}
    )
    campaigns = {}
    total_expected = 0
    known_expected = True
    for campaign, rows in sorted(by_campaign.items()):
        expected = campaign_expected_count(root / campaign)
        if expected is None:
            known_expected = False
        else:
            total_expected += expected
        status_counts = Counter(str(row["status"]) for row in rows)
        cells: dict[str, dict[str, Any]] = {}
        for row in rows:
            cell = cells.setdefault(
                str(row["cell_id"]),
                {
                    "machine": row["machine"],
                    "recipe": row["recipe"],
                    "queue": row["queue"],
                    "variant": row["variant"],
                    "scenario": row["scenario"],
                    "observed_repeats": 0,
                    "authoritative_repeats": 0,
                },
            )
            cell["observed_repeats"] += 1
            cell["authoritative_repeats"] += int(bool(row["authoritative"]))
        campaigns[campaign] = {
            "expected_cases": expected,
            "observed_cases": len(rows),
            "completed_cases": status_counts.get("completed", 0),
            "authoritative_cases": sum(bool(row["authoritative"]) for row in rows),
            "rejected_cases": sum(not bool(row["authoritative"]) for row in rows),
            "failed_cases": sum(count for status, count in status_counts.items() if status != "completed"),
            "missing_cases": max(0, expected - len(rows)) if expected is not None else None,
            "status_counts": dict(sorted(status_counts.items())),
            "cells": cells,
        }
    observed = len(trials)
    authoritative = sum(bool(row["authoritative"]) for row in trials)
    return {
        "schema_version": OUTPUT_SCHEMA_VERSION,
        "results_root": str(root),
        "totals": {
            "expected_cases": total_expected if known_expected else None,
            "observed_cases": observed,
            "authoritative_cases": authoritative,
            "rejected_cases": observed - authoritative,
            "missing_cases": max(0, total_expected - observed) if known_expected else None,
        },
        "quality_issue_counts": dict(sorted(issue_counts.items())),
        "campaigns": campaigns,
    }


def collect_trials(root: Path) -> list[ParsedTrial]:
    case_paths = sorted(root.rglob("case.json"))
    if not case_paths:
        raise AggregationError(f"no case.json files found under {root}")
    seen_cases: dict[tuple[str, str], Path] = {}
    seen_explicit_trials: dict[str, Path] = {}
    parsed = []
    for path in case_paths:
        case = read_json(path)
        campaign, case_id, trial_id, explicit_trial = trial_identity(path, root, case)
        case_key = (campaign, case_id)
        if case_key in seen_cases:
            raise AggregationError(
                f"duplicate campaign/case id {campaign}/{case_id}: {seen_cases[case_key]} and {path}"
            )
        seen_cases[case_key] = path
        if explicit_trial:
            if trial_id in seen_explicit_trials:
                raise AggregationError(
                    f"duplicate explicit trial id {trial_id}: {seen_explicit_trials[trial_id]} and {path}"
                )
            seen_explicit_trials[trial_id] = path
        parsed.append(parse_trial(path, root))
    return parsed


def aggregate_results(
    results_root: str | Path,
    out_dir: str | Path,
    *,
    bootstrap_samples: int = DEFAULT_BOOTSTRAP_SAMPLES,
) -> dict[str, Path]:
    root = Path(results_root).resolve()
    output = Path(out_dir).resolve()
    if bootstrap_samples < 0:
        raise AggregationError("bootstrap_samples must be non-negative")
    parsed = collect_trials(root)
    trials = [item.row for item in parsed]
    metrics = [row for item in parsed for row in item.metrics]
    hotspots = [row for item in parsed for row in item.hotspots]
    quality = [row for item in parsed for row in item.issues]
    summaries = build_cell_summary(trials, metrics, bootstrap_samples)
    effects = build_paired_effects(trials, metrics, bootstrap_samples)
    variant_effects = build_independent_variant_effects(trials, metrics, bootstrap_samples)
    coverage = build_coverage(root, trials, quality)

    paths = {
        "trials": output / "trials.csv",
        "metrics": output / "metrics_long.csv",
        "hotspots": output / "hotspots.csv",
        "quality": output / "quality.csv",
        "summary": output / "cell_summary.csv",
        "effects": output / "paired_effects.csv",
        "variant_effects": output / "variant_effects.csv",
        "coverage": output / "coverage.json",
    }
    write_csv(paths["trials"], TRIAL_FIELDS, trials)
    write_csv(paths["metrics"], METRIC_FIELDS, metrics)
    write_csv(paths["hotspots"], HOTSPOT_FIELDS, hotspots)
    write_csv(paths["quality"], QUALITY_FIELDS, quality)
    write_csv(paths["summary"], SUMMARY_FIELDS, summaries)
    write_csv(paths["effects"], EFFECT_FIELDS, effects)
    write_csv(paths["variant_effects"], VARIANT_EFFECT_FIELDS, variant_effects)
    paths["coverage"].parent.mkdir(parents=True, exist_ok=True)
    paths["coverage"].write_text(json.dumps(coverage, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return paths


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("results_root", help="root containing <campaign>/<case_id>/case.json")
    parser.add_argument(
        "--out-dir",
        default="profiling_results/aggregate",
        help="normalized CSV/JSON output directory (default: profiling_results/aggregate)",
    )
    parser.add_argument(
        "--bootstrap-samples",
        type=int,
        default=DEFAULT_BOOTSTRAP_SAMPLES,
        help=f"deterministic bootstrap resamples (default: {DEFAULT_BOOTSTRAP_SAMPLES})",
    )
    args = parser.parse_args(argv)
    try:
        paths = aggregate_results(
            args.results_root, args.out_dir, bootstrap_samples=args.bootstrap_samples
        )
    except AggregationError as exc:
        print(f"aggregate_profile_results: {exc}", file=sys.stderr)
        return 1
    for name, path in paths.items():
        print(f"Wrote {name}: {path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
