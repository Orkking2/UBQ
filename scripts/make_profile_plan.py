#!/usr/bin/env python3
"""Expand a versioned queue-profiling campaign into deterministic JSONL cases.

The generated plan is intentionally data-only.  Slurm submits one line per
array element and ``run_profile_case.py`` executes exactly that line.  Keeping
the Cartesian expansion outside the allocation makes planned/observed coverage
auditable and lets a campaign be resumed without silently changing its matrix.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import random
import re
import sys
from collections import Counter
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


CONFIG_SCHEMA_VERSION = 1
CASE_SCHEMA_VERSION = 1
CLUSTER_CPUS = {"mn5": 112, "grace": 144}
SUPPORTED_QUEUES = {
    "ubq",
    "lubq",
    "segqueue",
    "concurrent-queue",
    "mutex-vecdeque",
    "ms-queue",
    "fastfifo",
    "lfqueue",
    "moodycamel-cq",
}
NATIVE_BATCH_QUEUES = {
    "ubq",
    "lubq",
    "segqueue",
    "mutex-vecdeque",
    "moodycamel-cq",
}
APX_RECIPES = {
    "grace": {
        "code_hotspots",
        "cpu_microarchitecture",
        "instruction_mix",
        "memory_access",
        "system_utilization",
        "system_characterization",
    },
    # Performix Linux-x64 target support is intentionally narrow.  Capability
    # checks in the runner remain authoritative because MN5 has no proven APX
    # module deployment in this repository.
    "mn5": {"code_hotspots"},
}
DEFAULT_PERF_EVENTS = [
    "task-clock",
    "cycles",
    "instructions",
    "branches",
    "branch-misses",
    "cache-references",
    "cache-misses",
    "page-faults",
    "minor-faults",
    "major-faults",
    "context-switches",
    "cpu-migrations",
]
SCENARIO_RE = re.compile(r"^([1-9][0-9]*)p([1-9][0-9]*)c$")


class PlanError(ValueError):
    """Raised when a campaign would create an ambiguous or invalid plan."""


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def stable_id(value: Any, length: int = 16) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()[:length]


def slug(value: Any) -> str:
    text = re.sub(r"[^A-Za-z0-9._-]+", "-", str(value)).strip("-_.").lower()
    return text or "unnamed"


def require_mapping(value: Any, where: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise PlanError(f"{where} must be a JSON object")
    return value


def require_nonempty_string(value: Any, where: str) -> str:
    if not isinstance(value, str) or not value.strip() or "\n" in value:
        raise PlanError(f"{where} must be a non-empty single-line string")
    return value.strip()


def require_positive_int(value: Any, where: str, *, allow_zero: bool = False) -> int:
    minimum = 0 if allow_zero else 1
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        adjective = "non-negative" if allow_zero else "positive"
        raise PlanError(f"{where} must be a {adjective} integer")
    return value


def as_string_list(value: Any, where: str) -> list[str]:
    if not isinstance(value, list) or not value:
        raise PlanError(f"{where} must be a non-empty JSON array")
    result = [require_nonempty_string(item, f"{where}[{index}]") for index, item in enumerate(value)]
    if len(result) != len(set(result)):
        raise PlanError(f"{where} contains duplicates")
    return result


def scenario_parts(value: Any, where: str) -> tuple[str, int, int]:
    raw = require_nonempty_string(value, where).lower()
    match = SCENARIO_RE.fullmatch(raw)
    if not match:
        raise PlanError(f"{where} must look like 8p8c")
    producers, consumers = (int(match.group(1)), int(match.group(2)))
    return raw, producers, consumers


def cluster_values(value: Any, cluster: str, where: str) -> list[Any]:
    """Resolve either a list or ``{"all": [...], "grace": [...]}``."""

    if isinstance(value, list):
        return list(value)
    mapping = require_mapping(value, where)
    result: list[Any] = []
    for key in ("all", cluster):
        selected = mapping.get(key, [])
        if not isinstance(selected, list):
            raise PlanError(f"{where}.{key} must be a JSON array")
        result.extend(selected)
    return result


def recipe_values(stage: Mapping[str, Any], cluster: str, where: str) -> list[str | None]:
    if stage["backend"] == "perf":
        return [None]
    raw = cluster_values(stage.get("recipes", []), cluster, f"{where}.recipes")
    recipes = [require_nonempty_string(item, f"{where}.recipes") for item in raw]
    if not recipes:
        raise PlanError(f"{where}.recipes selects nothing for {cluster}")
    allow_unsupported = bool(stage.get("allow_unsupported_recipes", False))
    unsupported = sorted(set(recipes) - APX_RECIPES[cluster])
    if unsupported and not allow_unsupported:
        raise PlanError(
            f"{where} selects unsupported APX recipe(s) for {cluster}: "
            f"{', '.join(unsupported)}"
        )
    return recipes


def perf_events(stage: Mapping[str, Any], cluster: str, where: str) -> list[str]:
    raw = stage.get("perf_events", DEFAULT_PERF_EVENTS)
    values = cluster_values(raw, cluster, f"{where}.perf_events") if isinstance(raw, Mapping) else raw
    return as_string_list(values, f"{where}.perf_events")


def queue_implementation(
    queue: str,
    batch_size: int | None,
    lubq_backoff: str | None = None,
    fastfifo_block_size: int | None = None,
    fastfifo_capacity: int | None = None,
    lfqueue_segment_size: int | None = None,
) -> str:
    if queue == "lubq" and lubq_backoff == "yield":
        return "lubq-yield"
    if queue == "segqueue":
        return "crossbeam-batchqueue" if batch_size is not None else "crossbeam-segqueue"
    if queue == "mutex-vecdeque" and batch_size is not None:
        return "mutex-vecdeque-native-batch"
    if queue == "fastfifo":
        return f"fastfifo_b{fastfifo_block_size}_c{fastfifo_capacity}"
    if queue == "lfqueue":
        return f"lfqueue_{lfqueue_segment_size}"
    return queue


def normalize_variants(config: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    raw_variants = config.get("variants")
    if not isinstance(raw_variants, list) or not raw_variants:
        raise PlanError("variants must be a non-empty JSON array")
    variants: dict[str, dict[str, Any]] = {}
    for index, raw in enumerate(raw_variants):
        variant = dict(require_mapping(raw, f"variants[{index}]"))
        name = require_nonempty_string(variant.get("name"), f"variants[{index}].name")
        if name in variants:
            raise PlanError(f"duplicate variant name {name!r}")
        binary = require_nonempty_string(variant.get("binary"), f"variants[{index}].binary")
        allocator_binary = variant.get("allocator_binary")
        if allocator_binary is not None:
            allocator_binary = require_nonempty_string(
                allocator_binary, f"variants[{index}].allocator_binary"
            )
        factors = variant.get("factors", {})
        require_mapping(factors, f"variants[{index}].factors")
        environment = variant.get("env", {})
        require_mapping(environment, f"variants[{index}].env")
        normalized_environment = {}
        for key, value in environment.items():
            if not isinstance(key, str) or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", key):
                raise PlanError(f"variants[{index}].env has invalid variable name {key!r}")
            if not isinstance(value, str) or "\n" in value or "\x00" in value:
                raise PlanError(f"variants[{index}].env.{key} must be a single-line string")
            normalized_environment[key] = value
        variants[name] = {
            "name": name,
            "binary": binary,
            "allocator_binary": allocator_binary,
            "factors": dict(factors),
            "env": normalized_environment,
            "is_baseline": bool(variant.get("is_baseline", False)),
        }
    return variants


def normalize_placements(stage: Mapping[str, Any], cluster: str, where: str) -> list[dict[str, Any]]:
    raw = cluster_values(stage.get("placements", [{"name": "interleaved"}]), cluster, f"{where}.placements")
    placements: list[dict[str, Any]] = []
    seen: set[str] = set()
    for index, item in enumerate(raw):
        if isinstance(item, str):
            placement = {"name": require_nonempty_string(item, f"{where}.placements[{index}]")}
        else:
            placement = dict(require_mapping(item, f"{where}.placements[{index}]"))
            placement["name"] = require_nonempty_string(
                placement.get("name"), f"{where}.placements[{index}].name"
            )
        name = placement["name"]
        if name in seen:
            raise PlanError(f"{where}.placements has duplicate name {name!r}")
        seen.add(name)
        placement["core_offset"] = require_positive_int(
            placement.get("core_offset", 0),
            f"{where}.placements[{index}].core_offset",
            allow_zero=True,
        )
        core_ids = placement.get("core_ids")
        if core_ids is not None:
            if not isinstance(core_ids, list) or not core_ids:
                raise PlanError(f"{where}.placements[{index}].core_ids must be a non-empty array")
            if any(isinstance(value, bool) or not isinstance(value, int) or value < 0 for value in core_ids):
                raise PlanError(f"{where}.placements[{index}].core_ids must contain non-negative integers")
            if len(core_ids) != len(set(core_ids)):
                raise PlanError(f"{where}.placements[{index}].core_ids contains duplicates")
        placements.append(placement)
    return placements


def normalize_queues(stage: Mapping[str, Any], where: str) -> list[dict[str, Any]]:
    raw_queues = stage.get("queues")
    if not isinstance(raw_queues, list) or not raw_queues:
        raise PlanError(f"{where}.queues must be a non-empty JSON array")
    queues: list[dict[str, Any]] = []
    for index, raw in enumerate(raw_queues):
        if isinstance(raw, str):
            queue = {"name": raw, "batch_sizes": [None]}
        else:
            queue = dict(require_mapping(raw, f"{where}.queues[{index}]"))
        name = require_nonempty_string(queue.get("name"), f"{where}.queues[{index}].name")
        if name not in SUPPORTED_QUEUES:
            raise PlanError(f"{where}.queues[{index}] selects unsupported profiler queue {name!r}")
        batches = queue.get("batch_sizes", [None])
        if not isinstance(batches, list) or not batches:
            raise PlanError(f"{where}.queues[{index}].batch_sizes must be a non-empty array")
        normalized_batches: list[int | None] = []
        for batch_index, batch in enumerate(batches):
            if batch is None:
                normalized_batches.append(None)
            else:
                size = require_positive_int(batch, f"{where}.queues[{index}].batch_sizes[{batch_index}]")
                if size < 2:
                    raise PlanError(f"{where}.queues[{index}] batch sizes must be at least 2")
                if name not in NATIVE_BATCH_QUEUES and not queue.get("allow_emulated_batch", False):
                    raise PlanError(
                        f"{where}.queues[{index}] requests batching for scalar-only queue {name!r}"
                    )
                normalized_batches.append(size)
        if len(normalized_batches) != len(set(normalized_batches)):
            raise PlanError(f"{where}.queues[{index}].batch_sizes contains duplicates")
        labels = queue.get("ubq_labels", [])
        if name == "ubq":
            labels = labels or ["balanced,1,page,crossbeam"]
            labels = as_string_list(labels, f"{where}.queues[{index}].ubq_labels")
        elif labels:
            raise PlanError(f"{where}.queues[{index}].ubq_labels is valid only for ubq")
        else:
            labels = [None]
        lubq_backoffs = queue.get("lubq_backoffs", [])
        if name == "lubq":
            lubq_backoffs = lubq_backoffs or ["crossbeam"]
            lubq_backoffs = as_string_list(
                lubq_backoffs, f"{where}.queues[{index}].lubq_backoffs"
            )
            unsupported_backoffs = sorted(set(lubq_backoffs) - {"crossbeam", "yield"})
            if unsupported_backoffs:
                raise PlanError(
                    f"{where}.queues[{index}].lubq_backoffs contains unsupported values: "
                    f"{', '.join(unsupported_backoffs)}"
                )
        elif lubq_backoffs:
            raise PlanError(
                f"{where}.queues[{index}].lubq_backoffs is valid only for lubq"
            )
        else:
            lubq_backoffs = [None]

        def sizing_values(field: str, owner: str, defaults: list[int | None]) -> list[int | None]:
            raw_values = queue.get(field, [])
            if name == owner:
                raw_values = raw_values or defaults
                if not isinstance(raw_values, list) or not raw_values:
                    raise PlanError(f"{where}.queues[{index}].{field} must be a non-empty array")
                values = [
                    require_positive_int(
                        value, f"{where}.queues[{index}].{field}[{value_index}]"
                    )
                    for value_index, value in enumerate(raw_values)
                ]
                if len(values) != len(set(values)):
                    raise PlanError(f"{where}.queues[{index}].{field} contains duplicates")
                return values
            if raw_values:
                raise PlanError(
                    f"{where}.queues[{index}].{field} is valid only for {owner}"
                )
            return [None]

        fastfifo_block_sizes = sizing_values(
            "fastfifo_block_sizes", "fastfifo", [256]
        )
        fastfifo_capacities = sizing_values(
            "fastfifo_capacities", "fastfifo", [1_048_576]
        )
        lfqueue_segment_sizes = sizing_values(
            "lfqueue_segment_sizes", "lfqueue", [256]
        )
        queues.append(
            {
                "name": name,
                "batch_sizes": normalized_batches,
                "ubq_labels": labels,
                "lubq_backoffs": lubq_backoffs,
                "fastfifo_block_sizes": fastfifo_block_sizes,
                "fastfifo_capacities": fastfifo_capacities,
                "lfqueue_segment_sizes": lfqueue_segment_sizes,
                "batch_native": name in NATIVE_BATCH_QUEUES,
            }
        )
    return queues


def expand_campaign(
    config: Mapping[str, Any],
    *,
    stages_filter: set[str] | None = None,
    clusters_filter: set[str] | None = None,
    backends_filter: set[str] | None = None,
) -> list[dict[str, Any]]:
    if config.get("schema_version") != CONFIG_SCHEMA_VERSION:
        raise PlanError(f"campaign schema_version must be {CONFIG_SCHEMA_VERSION}")
    campaign = require_nonempty_string(config.get("campaign"), "campaign")
    schedule_seed = require_positive_int(
        config.get("schedule_seed", 0x55425106), "schedule_seed", allow_zero=True
    )
    variants = normalize_variants(config)
    raw_stages = config.get("stages")
    if not isinstance(raw_stages, list) or not raw_stages:
        raise PlanError("stages must be a non-empty JSON array")

    cases: list[dict[str, Any]] = []
    stage_names: set[str] = set()
    for stage_index, raw_stage in enumerate(raw_stages):
        where = f"stages[{stage_index}]"
        stage = dict(require_mapping(raw_stage, where))
        name = require_nonempty_string(stage.get("name"), f"{where}.name")
        if name in stage_names:
            raise PlanError(f"duplicate stage name {name!r}")
        stage_names.add(name)
        if stages_filter is not None and name not in stages_filter:
            continue
        backend = require_nonempty_string(stage.get("backend"), f"{where}.backend")
        if backend not in {"apx", "perf"}:
            raise PlanError(f"{where}.backend must be 'apx' or 'perf'")
        if backends_filter is not None and backend not in backends_filter:
            continue
        clusters = as_string_list(stage.get("clusters"), f"{where}.clusters")
        unknown_clusters = sorted(set(clusters) - set(CLUSTER_CPUS))
        if unknown_clusters:
            raise PlanError(f"{where}.clusters contains unknown values: {', '.join(unknown_clusters)}")
        selected_variants = as_string_list(stage.get("variants"), f"{where}.variants")
        missing_variants = sorted(set(selected_variants) - set(variants))
        if missing_variants:
            raise PlanError(f"{where}.variants contains unknown values: {', '.join(missing_variants)}")
        if stage.get("pair_variants", False):
            raise PlanError(
                f"{where}.pair_variants is unsafe for independent Slurm array elements; use \
                 independent variant effects or import a genuinely same-allocation paired design"
            )
        baselines = [name for name in selected_variants if variants[name]["is_baseline"]]
        if len(baselines) > 1:
            raise PlanError(f"{where}.variants selects more than one baseline")
        reference_variant = baselines[0] if baselines else ""
        queues = normalize_queues(stage, where)
        repeats = require_positive_int(stage.get("repeats", 1), f"{where}.repeats")
        duration_secs = require_positive_int(stage.get("duration_secs", 30), f"{where}.duration_secs")
        warmup_secs = require_positive_int(
            stage.get("warmup_secs", 2), f"{where}.warmup_secs", allow_zero=True
        )
        workload = require_nonempty_string(stage.get("workload", "handoff"), f"{where}.workload")
        if workload != "handoff":
            raise PlanError(f"{where}.workload currently supports only 'handoff'")
        allocator_probe = bool(stage.get("allocator_probe", backend == "perf"))
        fixed_items = stage.get("items_per_producer")
        if fixed_items is not None:
            fixed_items = require_positive_int(fixed_items, f"{where}.items_per_producer")
        calibrate_outside_collector = stage.get(
            "calibrate_outside_collector", fixed_items is None
        )
        if not isinstance(calibrate_outside_collector, bool):
            raise PlanError(f"{where}.calibrate_outside_collector must be a boolean")
        if calibrate_outside_collector != (fixed_items is None):
            expectation = "true without items_per_producer" if fixed_items is None else (
                "false when items_per_producer is explicit"
            )
            raise PlanError(
                f"{where}.calibrate_outside_collector must be {expectation}; every case keeps "
                "an unprofiled throughput shadow"
            )
        prewarm_items = stage.get("prewarm_items_per_producer")
        if prewarm_items is not None:
            prewarm_items = require_positive_int(
                prewarm_items, f"{where}.prewarm_items_per_producer"
            )
            if fixed_items is None:
                raise PlanError(
                    f"{where}.prewarm_items_per_producer requires items_per_producer"
                )

        for cluster in clusters:
            if clusters_filter is not None and cluster not in clusters_filter:
                continue
            recipes = recipe_values(stage, cluster, where)
            events = perf_events(stage, cluster, where) if backend == "perf" else []
            placements = normalize_placements(stage, cluster, where)
            scenario_values = cluster_values(stage.get("scenarios", []), cluster, f"{where}.scenarios")
            if not scenario_values:
                raise PlanError(f"{where}.scenarios selects nothing for {cluster}")
            scenarios = [scenario_parts(value, f"{where}.scenarios") for value in scenario_values]
            if len(scenarios) != len({item[0] for item in scenarios}):
                raise PlanError(f"{where}.scenarios contains duplicates for {cluster}")

            for scenario, producers, consumers in scenarios:
                threads = producers + consumers
                if threads > CLUSTER_CPUS[cluster]:
                    raise PlanError(
                        f"{where} scenario {scenario} needs {threads} cores, but {cluster} has "
                        f"{CLUSTER_CPUS[cluster]}"
                    )
                for placement in placements:
                    required_slots = placement["core_offset"] + threads
                    core_ids = placement.get("core_ids")
                    available_slots = len(core_ids) if core_ids is not None else CLUSTER_CPUS[cluster]
                    if required_slots > available_slots:
                        raise PlanError(
                            f"{where} placement {placement['name']!r} has {available_slots} slots, "
                            f"but {scenario} with its offset needs {required_slots}"
                        )
                    for variant_name in selected_variants:
                        variant = variants[variant_name]
                        try:
                            binary = variant["binary"].format(cluster=cluster, variant=variant_name)
                            allocator_binary_template = variant.get("allocator_binary")
                            if allocator_binary_template is not None:
                                allocator_binary = allocator_binary_template.format(
                                    cluster=cluster, variant=variant_name
                                )
                            elif binary.endswith("bench_profile"):
                                allocator_binary = f"{binary}_alloc"
                            else:
                                allocator_binary = None
                            variant_env = {
                                key: value.format(cluster=cluster, variant=variant_name)
                                for key, value in variant["env"].items()
                            }
                        except (KeyError, ValueError) as exc:
                            raise PlanError(
                                f"invalid binary/environment template for variant {variant_name!r}: {exc}"
                            ) from exc
                        if allocator_probe and allocator_binary is None:
                            raise PlanError(
                                f"variant {variant_name!r} needs allocator_binary for allocator_probe"
                            )
                        for queue in queues:
                            configurations = (
                                (
                                    ubq_label,
                                    lubq_backoff,
                                    fastfifo_block_size,
                                    fastfifo_capacity,
                                    lfqueue_segment_size,
                                    batch_size,
                                )
                                for ubq_label in queue["ubq_labels"]
                                for lubq_backoff in queue["lubq_backoffs"]
                                for fastfifo_block_size in queue["fastfifo_block_sizes"]
                                for fastfifo_capacity in queue["fastfifo_capacities"]
                                for lfqueue_segment_size in queue["lfqueue_segment_sizes"]
                                for batch_size in queue["batch_sizes"]
                            )
                            for (
                                ubq_label,
                                lubq_backoff,
                                fastfifo_block_size,
                                fastfifo_capacity,
                                lfqueue_segment_size,
                                batch_size,
                            ) in configurations:
                                queue_impl = queue_implementation(
                                    queue["name"],
                                    batch_size,
                                    lubq_backoff,
                                    fastfifo_block_size,
                                    fastfifo_capacity,
                                    lfqueue_segment_size,
                                )
                                for recipe in recipes:
                                    for repeat_index in range(repeats):
                                        factors = {
                                            "campaign": campaign,
                                            "stage": name,
                                            "cluster": cluster,
                                            "backend": backend,
                                            "recipe": recipe,
                                            "variant": variant_name,
                                            "variant_factors": variant["factors"],
                                            "variant_env": variant_env,
                                            "queue": queue["name"],
                                            "queue_implementation": queue_impl,
                                            "ubq_label": ubq_label,
                                            "lubq_backoff": lubq_backoff,
                                            "fastfifo_block_size": fastfifo_block_size,
                                            "fastfifo_capacity": fastfifo_capacity,
                                            "lfqueue_segment_size": lfqueue_segment_size,
                                            "batch_size": batch_size,
                                            "scenario": scenario,
                                            "producers": producers,
                                            "consumers": consumers,
                                            "workload": workload,
                                            "allocator_probe": allocator_probe,
                                            "placement": placement,
                                            "duration_secs": duration_secs,
                                            "warmup_secs": warmup_secs,
                                            "items_per_producer": fixed_items,
                                            "prewarm_items_per_producer": prewarm_items,
                                        }
                                        cell_id = stable_id(factors)
                                        pair_factors = dict(factors)
                                        pair_factors.pop("variant")
                                        pair_factors.pop("variant_factors")
                                        pair_factors.pop("variant_env")
                                        pair_block_id = (
                                            stable_id({**pair_factors, "repeat_index": repeat_index})
                                            if stage.get("pair_variants", False)
                                            else None
                                        )
                                        suffix = stable_id(
                                            {**factors, "repeat_index": repeat_index}, 8
                                        )
                                        batch_tag = (
                                            "scalar" if batch_size is None else f"b{batch_size}"
                                        )
                                        recipe_tag = recipe or "perf"
                                        case_id = "-".join(
                                            slug(part)
                                            for part in (
                                                name,
                                                cluster,
                                                variant_name,
                                                queue_impl,
                                                scenario,
                                                batch_tag,
                                                recipe_tag,
                                                f"r{repeat_index:02d}",
                                                suffix,
                                            )
                                        )
                                        queue_variant = (
                                            f"{queue_impl}:{ubq_label}"
                                            if ubq_label
                                            else queue_impl
                                        )
                                        comparison_group_id = stable_id(
                                            {
                                                "campaign": campaign,
                                                "stage": name,
                                                "cluster": cluster,
                                                "recipe": recipe,
                                                "variant": variant_name,
                                                "scenario": scenario,
                                                "placement": placement,
                                                "repeat_index": repeat_index,
                                            },
                                            12,
                                        )
                                        run_suffix = stable_id(
                                            {**factors, "repeat_index": repeat_index}, 8
                                        )
                                        apx_run_name = "__".join(
                                            slug(part)
                                            for part in (
                                                cluster,
                                                queue_variant,
                                                scenario,
                                                batch_tag,
                                                recipe_tag,
                                                f"r{repeat_index + 1:02d}",
                                                comparison_group_id,
                                                run_suffix,
                                            )
                                        )
                                        case = {
                                            "schema_version": CASE_SCHEMA_VERSION,
                                            "benchmark": "queue_profile_case",
                                            "campaign": campaign,
                                            "stage": name,
                                            "case_id": case_id,
                                            "trial_id": f"{campaign}/{case_id}",
                                            "cell_id": cell_id,
                                            "pair_block_id": pair_block_id,
                                            "comparison_group_id": comparison_group_id,
                                            "comparison_role": "candidate",
                                            "apx_run_name": apx_run_name,
                                            "cluster": cluster,
                                            "machine": cluster,
                                            "backend": backend,
                                            "collector": backend,
                                            "recipe": recipe,
                                            "binary": binary,
                                            "allocator_binary": allocator_binary,
                                            "variant": variant_name,
                                            "comparison_variant": variant_name,
                                            "is_baseline": variant["is_baseline"],
                                            "reference_variant": reference_variant,
                                            "variant_factors": variant["factors"],
                                            "variant_env": variant_env,
                                            "allocator": variant["factors"].get(
                                                "allocator", "system"
                                            ),
                                            "page_distributor": variant["factors"].get(
                                                "page_distributor", "build-defined"
                                            ),
                                            "distributor_params": variant["factors"],
                                            "queue": queue["name"],
                                            "queue_implementation": queue_impl,
                                            "queue_variant": queue_variant,
                                            "ubq_label": ubq_label,
                                            "lubq_backoff": lubq_backoff,
                                            "fastfifo_block_size": fastfifo_block_size,
                                            "fastfifo_capacity": fastfifo_capacity,
                                            "lfqueue_segment_size": lfqueue_segment_size,
                                            "batch_size": batch_size,
                                            "batch_mode": (
                                                "scalar"
                                                if batch_size is None
                                                else "native"
                                                if queue["batch_native"]
                                                else "emulated"
                                            ),
                                            "scenario": scenario,
                                            "producers": producers,
                                            "consumers": consumers,
                                            "total_threads": threads,
                                            "workload": workload,
                                            "workload_mode": workload,
                                            "allocator_probe": allocator_probe,
                                            "placement": placement,
                                            "core_placement": placement["name"],
                                            "factors": factors,
                                            "repeat": repeat_index + 1,
                                            "repeat_index": repeat_index,
                                            "duration_secs": duration_secs,
                                            "warmup_secs": warmup_secs,
                                            "items_per_producer": fixed_items,
                                            "prewarm_items_per_producer": prewarm_items,
                                            "calibrate_outside_collector": (
                                                calibrate_outside_collector
                                            ),
                                            "perf_events": events,
                                            "collector_timeout_secs": require_positive_int(
                                                stage.get(
                                                    "collector_timeout_secs",
                                                    duration_secs
                                                    * (12 if backend == "apx" else 3)
                                                    + 300,
                                                ),
                                                f"{where}.collector_timeout_secs",
                                            ),
                                            "quality_policy": dict(
                                                stage.get("quality_policy", {})
                                            ),
                                        }
                                        cases.append(case)

    if stages_filter is not None:
        unknown = stages_filter - stage_names
        if unknown:
            raise PlanError(f"unknown requested stage(s): {', '.join(sorted(unknown))}")
    if not cases:
        raise PlanError("filters selected no profiling cases")
    rng = random.Random(schedule_seed)
    rng.shuffle(cases)
    return cases


def summarize(cases: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    cases = list(cases)
    return {
        "cases": len(cases),
        "campaigns": sorted({str(case["campaign"]) for case in cases}),
        "by_stage": dict(sorted(Counter(str(case["stage"]) for case in cases).items())),
        "by_cluster": dict(sorted(Counter(str(case["cluster"]) for case in cases).items())),
        "by_backend": dict(sorted(Counter(str(case["backend"]) for case in cases).items())),
        "by_queue": dict(sorted(Counter(str(case["queue"]) for case in cases).items())),
    }


def write_plan(cases: Sequence[Mapping[str, Any]], output: Path | None) -> None:
    content = "".join(canonical_json(case) + "\n" for case in cases)
    if output is None:
        sys.stdout.write(content)
        return
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_suffix(output.suffix + ".tmp")
    temporary.write_text(content, encoding="utf-8")
    temporary.replace(output)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("config", type=Path, help="campaign JSON file")
    parser.add_argument("--output", type=Path, help="JSONL output (stdout when omitted)")
    parser.add_argument("--stage", action="append", dest="stages", help="include only this stage")
    parser.add_argument("--cluster", action="append", choices=sorted(CLUSTER_CPUS))
    parser.add_argument("--backend", action="append", choices=("apx", "perf"))
    parser.add_argument("--summary-only", action="store_true", help="validate and print only a summary")
    args = parser.parse_args(argv)
    try:
        config = json.loads(args.config.read_text(encoding="utf-8"))
        require_mapping(config, str(args.config))
        cases = expand_campaign(
            config,
            stages_filter=set(args.stages) if args.stages else None,
            clusters_filter=set(args.cluster) if args.cluster else None,
            backends_filter=set(args.backend) if args.backend else None,
        )
    except (OSError, json.JSONDecodeError, PlanError) as exc:
        print(f"make_profile_plan: {exc}", file=sys.stderr)
        return 2
    summary = summarize(cases)
    if args.summary_only:
        print(json.dumps(summary, indent=2, sort_keys=True))
        return 0
    write_plan(cases, args.output)
    print(canonical_json(summary), file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
