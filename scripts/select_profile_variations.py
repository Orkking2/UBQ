#!/usr/bin/env python3
"""Select throughput winners or matched one-factor APX contrasts.

The input plan is only a candidate universe.  Authoritative schema-v7
``bench_grid`` results determine the best observed configuration of each queue
at each scenario.  ``winners`` retains that configuration; ``one-factor`` also
retains candidates that change one requested tuning dimension while holding
the winner's other dimensions fixed.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any, Mapping, Sequence

try:
    from scripts.select_profile_plan import (
        SelectionError,
        batch_value,
        canonical_json,
        integer,
        load_evidence,
        load_plan,
        number,
        write_plan,
    )
except ModuleNotFoundError:  # Direct ``python3 scripts/...`` execution.
    from select_profile_plan import (  # type: ignore[no-redef]
        SelectionError,
        batch_value,
        canonical_json,
        integer,
        load_evidence,
        load_plan,
        number,
        write_plan,
    )


DIMENSIONS = ("batch_size", "backoff", "block_size")


def stable_id(value: Any, length: int = 12) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()[:length]


def backoff_value(case: Mapping[str, Any]) -> str | None:
    queue = str(case.get("queue") or "")
    if queue == "lubq":
        value = case.get("lubq_backoff")
        return str(value) if value else "crossbeam"
    if queue == "ubq":
        label = str(case.get("ubq_label") or "")
        return label.rsplit(",", 1)[-1] if label else None
    return None


def block_value(case: Mapping[str, Any]) -> int | str | None:
    queue = str(case.get("queue") or "")
    if queue == "fastfifo":
        return integer(case.get("fastfifo_block_size"))
    if queue == "lfqueue":
        return integer(case.get("lfqueue_segment_size"))
    if queue in {"ubq", "lubq"}:
        # Both algorithms deliberately use one host base page.  It is a fixed
        # design property, not a numeric tuning level in the current build.
        return "page"
    return None


def parameters(case: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "batch_size": batch_value(case.get("batch_size")),
        "backoff": backoff_value(case),
        "block_size": block_value(case),
        "capacity": integer(case.get("fastfifo_capacity")),
    }


def config_identity(case: Mapping[str, Any]) -> tuple[Any, ...]:
    values = parameters(case)
    return (
        str(case.get("cluster") or case.get("machine") or ""),
        str(case.get("scenario") or ""),
        str(case.get("queue") or ""),
        str(case.get("queue_variant") or case.get("queue") or ""),
        values["batch_size"],
        values["backoff"],
        values["block_size"],
        values["capacity"],
    )


def evidence_identity(value: Mapping[str, Any]) -> tuple[str, str, int | None, str, str]:
    return (
        str(value.get("machine") or value.get("cluster") or ""),
        str(value.get("scenario") or ""),
        batch_value(value.get("batch_size")),
        str(value.get("queue") or ""),
        str(value.get("queue_variant") or value.get("queue") or ""),
    )


def case_evidence_identity(case: Mapping[str, Any]) -> tuple[str, str, int | None, str, str]:
    return (
        str(case.get("cluster") or case.get("machine") or ""),
        str(case.get("scenario") or ""),
        batch_value(case.get("batch_size")),
        str(case.get("queue") or ""),
        str(case.get("queue_variant") or case.get("queue") or ""),
    )


def differs_only(
    candidate: Mapping[str, Any], winner: Mapping[str, Any], dimension: str
) -> bool:
    candidate_parameters = parameters(candidate)
    winner_parameters = parameters(winner)
    if candidate_parameters[dimension] == winner_parameters[dimension]:
        return False
    return all(
        candidate_parameters[key] == winner_parameters[key]
        for key in candidate_parameters
        if key != dimension
    )


def select_variations(
    cases: Sequence[dict[str, Any]],
    rows: Sequence[Mapping[str, Any]],
    *,
    policy: str,
    dimensions: set[str],
    minimum_repeats: int,
    repeat_limit: int,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    rates: dict[tuple[str, str, int | None, str, str], tuple[float, int]] = {}
    for row in rows:
        rate = number(row.get("median"))
        repeats = integer(row.get("n")) or 1
        if rate is None or rate <= 0 or repeats < minimum_repeats:
            continue
        key = evidence_identity(row)
        previous = rates.get(key)
        if previous is not None and previous != (rate, repeats):
            raise SelectionError(f"ambiguous throughput evidence for {key!r}")
        rates[key] = (rate, repeats)

    unique_configs: dict[tuple[Any, ...], dict[str, Any]] = {}
    for case in cases:
        unique_configs.setdefault(config_identity(case), case)

    grouped: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    unavailable = []
    for config in unique_configs.values():
        key = case_evidence_identity(config)
        if key not in rates:
            unavailable.append(
                {
                    "machine": key[0],
                    "scenario": key[1],
                    "batch_size": key[2],
                    "queue": key[3],
                    "queue_variant": key[4],
                }
            )
            continue
        grouped[(key[0], key[1], key[3])].append(config)

    selected_configs: dict[tuple[Any, ...], dict[str, Any]] = {}
    group_manifest = []
    for group, configurations in sorted(grouped.items()):
        winner = max(
            configurations,
            key=lambda case: (
                rates[case_evidence_identity(case)][0],
                canonical_json(config_identity(case)),
            ),
        )
        winner_id = config_identity(winner)
        chosen: dict[tuple[Any, ...], set[str]] = {winner_id: {"throughput_winner"}}
        if policy == "one-factor":
            for dimension in sorted(dimensions):
                found_contrast = False
                for candidate in configurations:
                    if differs_only(candidate, winner, dimension):
                        found_contrast = True
                        chosen.setdefault(config_identity(candidate), set()).add(
                            f"{dimension}_contrast"
                        )
                if found_contrast:
                    chosen[winner_id].add(f"{dimension}_control")
        for identity, reasons in chosen.items():
            selected_configs.setdefault(identity, {"reasons": set()})["reasons"].update(reasons)
        group_manifest.append(
            {
                "machine": group[0],
                "scenario": group[1],
                "queue": group[2],
                "winner": {
                    "queue_variant": winner.get("queue_variant"),
                    **parameters(winner),
                    "median_ops_per_sec": rates[case_evidence_identity(winner)][0],
                    "evidence_repeats": rates[case_evidence_identity(winner)][1],
                },
                "selected_configurations": len(chosen),
            }
        )

    selected = []
    for case in cases:
        repeat = integer(case.get("repeat"))
        identity = config_identity(case)
        metadata = selected_configs.get(identity)
        if metadata is None or repeat is None or repeat > repeat_limit:
            continue
        rate, evidence_repeats = rates[case_evidence_identity(case)]
        output = dict(case)
        reasons = sorted(metadata["reasons"])
        comparison_group_id = stable_id(
            {
                "policy": policy,
                "campaign": case.get("campaign"),
                "stage": case.get("stage"),
                "machine": case.get("cluster") or case.get("machine"),
                "scenario": case.get("scenario"),
                "recipe": case.get("recipe"),
                "repeat": repeat,
            }
        )
        output["comparison_group_id"] = comparison_group_id
        run_name = str(output.get("apx_run_name") or "")
        run_name_parts = run_name.split("__")
        if len(run_name_parts) >= 2:
            run_name_parts[-2] = comparison_group_id
            output["apx_run_name"] = "__".join(run_name_parts)
        output["comparison_role"] = (
            "throughput_winner" if "throughput_winner" in reasons else "ab_contrast"
        )
        output["selection_evidence"] = {
            "policy": policy,
            "reasons": reasons,
            "median_ops_per_sec": rate,
            "evidence_repeats": evidence_repeats,
            "parameters": parameters(case),
        }
        output["comparison_sets"] = [
            {
                "comparison_set_id": stable_id(
                    {
                        "campaign": case.get("campaign"),
                        "stage": case.get("stage"),
                        "machine": case.get("cluster") or case.get("machine"),
                        "scenario": case.get("scenario"),
                        "queue": case.get("queue"),
                        "dimension": reason.removesuffix("_contrast").removesuffix("_control"),
                        "recipe": case.get("recipe"),
                        "repeat": repeat,
                    }
                ),
                "dimension": reason.removesuffix("_contrast").removesuffix("_control"),
                "role": "control" if reason.endswith("_control") else "contrast",
            }
            for reason in reasons
            if reason.endswith(("_contrast", "_control"))
        ]
        selected.append(output)

    if not selected:
        raise SelectionError("no evidence-backed profiling cases remain")
    return selected, {
        "policy": policy,
        "dimensions": sorted(dimensions),
        "minimum_evidence_repeats": minimum_repeats,
        "plan_repeats": repeat_limit,
        "groups": group_manifest,
        "unavailable_configurations": unavailable,
    }


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("candidate_plan", type=Path)
    parser.add_argument("evidence", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--policy", choices=("winners", "one-factor"), default="winners")
    parser.add_argument("--dimension", action="append", choices=DIMENSIONS)
    parser.add_argument("--scenario", action="append")
    parser.add_argument("--queue", action="append")
    parser.add_argument("--min-evidence-repeats", type=int, default=3)
    parser.add_argument("--plan-repeats", type=int, default=1)
    parser.add_argument("--max-cases", type=int, default=500)
    args = parser.parse_args(argv)
    try:
        if args.min_evidence_repeats < 1 or args.plan_repeats < 1 or args.max_cases < 1:
            raise SelectionError("repeat and case limits must be positive")
        cases = load_plan(args.candidate_plan)
        if args.scenario:
            scenarios = set(args.scenario)
            cases = [case for case in cases if case.get("scenario") in scenarios]
        if args.queue:
            queues = set(args.queue)
            cases = [case for case in cases if case.get("queue") in queues]
        if not cases:
            raise SelectionError("candidate filters select no cases")
        rows, evidence_manifest = load_evidence(args.evidence, cases)
        dimensions = set(args.dimension or DIMENSIONS)
        selected, selection = select_variations(
            cases,
            rows,
            policy=args.policy,
            dimensions=dimensions,
            minimum_repeats=args.min_evidence_repeats,
            repeat_limit=args.plan_repeats,
        )
        if len(selected) > args.max_cases:
            raise SelectionError(
                f"selection contains {len(selected)} cases, exceeding --max-cases={args.max_cases}"
            )
        write_plan(selected, args.output)
        manifest = {
            "schema_version": 1,
            "candidate_plan": str(args.candidate_plan),
            "candidate_plan_sha256": hashlib.sha256(
                args.candidate_plan.read_bytes()
            ).hexdigest(),
            "evidence": evidence_manifest,
            "selected_cases": len(selected),
            "filters": {
                "scenarios": sorted(args.scenario or []),
                "queues": sorted(args.queue or []),
            },
            "selection": selection,
        }
        manifest_path = args.output.with_suffix(args.output.suffix + ".selection.json")
        manifest_path.write_text(
            json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        print(json.dumps(manifest, indent=2, sort_keys=True))
        return 0
    except (OSError, SelectionError) as exc:
        print(f"select_profile_variations: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
