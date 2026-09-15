"""Study specifications: which arms an A/B study compares, where, and how hard.

A spec is a JSON file under lab/studies/. Only `name` and `arms` (or `grid`)
are required; every other field falls back to DEFAULTS. Scenario tokens may
use N (worker cores - 1) and H (worker cores / 2), so one spec fits every
cluster.

A study may have any number of arms: the first is the control and every
other arm is compared with it. Arms belong to families, by default the queue
name without its geometry suffix (`ubq-v8-s256-v32` is in `ubq-v8`), so a
sweep over many geometries, or over both backoff policies, of a few queues
can be summarized per family: the best variation of each family per cell,
and overall.
"""
from __future__ import annotations

import copy
import re

from .common import batch_order, batch_token, canonical, digest_text

WORKTREE = "WORKTREE"
NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,47}$")
ARM_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,23}$")
SCENARIO = re.compile(r"^(\d+|N|H)p(\d+|N|H)c$")
# An owned-buffer queue's geometry suffix: slots per VBlock, VBlocks per block.
GEOMETRY = re.compile(r"-s\d+(?:-v\d+)?$")
GEOMETRY_TOKEN = re.compile(r"^s[1-9]\d*(?:-v[1-9]\d*)?$")
# Fields an arm may override. They map one-to-one onto bench_hardware's
# Config; shape, mode and schedule belong to the lab.
WORKLOAD_FIELDS = {"queue", "backoff", "block_size", "capacity", "segment_size", "rss_limit_mib"}
# The backoff policies every registered queue and LUBQ take (src/backoff.rs).
BACKOFFS = ("crossbeam", "yield")
ARM_FIELDS = {"name", "ref", "features", "workload", "family"}

DEFAULTS = {
    "question": "",
    "workload": {"queue": "ubq-v8-s4096"},
    "grid": None,
    "clusters": ["grace", "mn5"],
    "throughput": {
        # Producer scaling, consumer scaling, balanced scaling, and two
        # asymmetric shapes, so a queue's behaviour is a curve, not a point.
        "scenarios": ["1p1c", "2p1c", "4p1c", "8p1c", "16p1c", "32p1c", "64p1c", "Np1c",
                      "1p2c", "1p4c", "1p8c", "1p16c", "1p32c", "1p64c", "1pNc",
                      "2p2c", "4p4c", "8p8c", "16p16c", "32p32c", "HpHc",
                      "4p16c", "16p4c"],
        "batch_sizes": [None, 16, 256, 4096],
        # Uneven pairs for the skewed shapes only: [producer, consumer] at a
        # producer-heavy shape, mirrored at a consumer-heavy one, so the
        # outnumbered side takes the same batch at either skew (here the
        # larger one, since it amortises more per call).
        "skewed_batch_sizes": [[16, 256], [16, 4096], [256, 4096]],
        "replicates": 3,
        "pairs": 3,
        "run_seconds": 8,
        "max_run_seconds": 60,
    },
    "memory": {"scenarios": ["Np1c"], "depths": [524288]},
    "deep": {
        "auto": True,
        "winners": 2,
        "losers": 2,
        "cells": [],
        "replicates": 1,
        "run_seconds": 20,
        "collectors": None,
    },
    "build": {"tests": ["v8::", "bench_harness::hardware"]},
    "allow_harness_mismatch": False,
}


class SpecError(ValueError):
    pass


def merged(raw):
    """`raw` over DEFAULTS; nested sections merge one level deep."""
    if not isinstance(raw, dict):
        raise SpecError("a study spec must be a JSON object")
    unknown = set(raw) - set(DEFAULTS) - {"name", "arms"}
    if unknown:
        raise SpecError(f"unknown spec fields: {', '.join(sorted(unknown))}")
    spec = copy.deepcopy(DEFAULTS)
    for key, value in raw.items():
        if isinstance(DEFAULTS.get(key), dict) and key != "workload":
            if not isinstance(value, dict):
                raise SpecError(f"{key} must be an object")
            unknown = set(value) - set(DEFAULTS[key])
            if unknown:
                raise SpecError(f"unknown {key} fields: {', '.join(sorted(unknown))}")
            spec[key].update(copy.deepcopy(value))
        else:
            spec[key] = copy.deepcopy(value)
    return spec


def short_queue(queue):
    return queue[4:] if queue.startswith("ubq-") else queue


def expand_grid(spec):
    """Append one arm per queue x geometry of `grid` to `arms`.

    `grid` is {"queues": [...], "geometries": [...], "ref": REF}; a geometry
    is `s<slots>[-v<vblocks>]` or "default". Explicit `arms` come first, so
    a grid study's control is either its first explicit arm or the first
    queue at the first geometry.
    """
    grid = spec.get("grid")
    if grid is None:
        return spec
    if (not isinstance(grid, dict) or set(grid) - {"queues", "geometries", "ref"}
            or not isinstance(grid.get("queues"), list) or not grid["queues"]
            or not isinstance(grid.get("geometries"), list) or not grid["geometries"]):
        raise SpecError("grid must be {\"queues\": [...], \"geometries\": [...]} with optional \"ref\"")
    arms = list(spec.get("arms") or [])
    for queue in grid["queues"]:
        if not isinstance(queue, str) or not queue:
            raise SpecError("grid.queues must be queue names")
        for geometry in grid["geometries"]:
            if geometry == "default":
                suffix = ""
            elif isinstance(geometry, str) and GEOMETRY_TOKEN.match(geometry):
                suffix = "-" + geometry
            else:
                raise SpecError(f"grid geometry {geometry!r} is not s<slots>[-v<vblocks>] or \"default\"")
            arms.append({"name": f"{short_queue(queue)}-{geometry}", "ref": grid.get("ref", WORKTREE),
                         "workload": {"queue": queue + suffix}, "family": queue})
    return {**spec, "arms": arms}


def _positive_int(value, what, minimum=1):
    if not isinstance(value, int) or isinstance(value, bool) or value < minimum:
        raise SpecError(f"{what} must be an integer >= {minimum}")


def _check_workload(workload, what):
    if not isinstance(workload, dict):
        raise SpecError(f"{what} must be an object")
    unknown = set(workload) - WORKLOAD_FIELDS
    if unknown:
        raise SpecError(f"{what} has unsupported fields {sorted(unknown)}; "
                        f"allowed: {sorted(WORKLOAD_FIELDS)}")
    if "backoff" in workload and workload["backoff"] not in BACKOFFS:
        raise SpecError(f"{what}: backoff must be one of {list(BACKOFFS)}, not {workload['backoff']!r}")


def validate(raw, fifo_class=None):
    """Return the complete spec, or raise SpecError naming the first problem.

    `fifo_class(queue)` returns "strict" or "relaxed"; when given, every arm
    must share one class, since a relaxed queue buys speed by giving up the
    global order a strict queue guarantees.
    """
    spec = expand_grid(merged(raw))
    if not isinstance(spec.get("name"), str) or not NAME.match(spec["name"]):
        raise SpecError("name must be 1-48 characters of letters, digits, '.', '_' or '-'")
    arms = spec.get("arms")
    if not isinstance(arms, list) or len(arms) < 2:
        raise SpecError("arms must list at least two arms; the first is the control")
    seen = set()
    for arm in arms:
        if not isinstance(arm, dict) or set(arm) - ARM_FIELDS:
            raise SpecError(f"each arm is an object with fields {sorted(ARM_FIELDS)}")
        name = arm.get("name")
        if not isinstance(name, str) or not ARM_NAME.match(name) or name in seen:
            raise SpecError(f"arm names must be unique, 1-24 of [A-Za-z0-9_-]: {name!r}")
        seen.add(name)
        if not isinstance(arm.get("ref"), str) or not arm["ref"].strip():
            raise SpecError(f"arm {name}: ref must be a git revision or {WORKTREE}")
        features = arm.setdefault("features", [])
        if not isinstance(features, list) or not all(isinstance(f, str) and f for f in features):
            raise SpecError(f"arm {name}: features must be a list of Cargo feature names")
        _check_workload(arm.setdefault("workload", {}), f"arm {name} workload")
        if "family" in arm and (not isinstance(arm["family"], str) or not arm["family"]):
            raise SpecError(f"arm {name}: family must be a non-empty string")
    _check_workload(spec["workload"], "workload")
    if "queue" not in spec["workload"]:
        raise SpecError("workload.queue is required")
    clusters = spec["clusters"]
    if not isinstance(clusters, list) or not clusters or not all(isinstance(c, str) for c in clusters):
        raise SpecError("clusters must be a non-empty list of cluster names")

    throughput = spec["throughput"]
    for token in throughput["scenarios"] + spec["memory"]["scenarios"]:
        if not isinstance(token, str) or not SCENARIO.match(token):
            raise SpecError(f"scenario {token!r} is not <P>p<C>c (P, C: a count, N or H)")
    if not throughput["scenarios"]:
        raise SpecError("throughput.scenarios must not be empty")
    if not isinstance(throughput["batch_sizes"], list) or not throughput["batch_sizes"]:
        raise SpecError("throughput.batch_sizes must be a non-empty list; null means scalar")
    throughput["batch_sizes"] = list(dict.fromkeys(batch_pair(e) for e in throughput["batch_sizes"]))
    if not isinstance(throughput["skewed_batch_sizes"], list):
        raise SpecError("throughput.skewed_batch_sizes must be a list of [producer, consumer] pairs")
    skewed = list(dict.fromkeys(batch_pair(e) for e in throughput["skewed_batch_sizes"]))
    if any(producer == consumer for producer, consumer in skewed):
        raise SpecError("throughput.skewed_batch_sizes pairs must set the sides apart; "
                        "matched batches belong in throughput.batch_sizes")
    throughput["skewed_batch_sizes"] = skewed
    _positive_int(throughput["replicates"], "throughput.replicates")
    _positive_int(throughput["pairs"], "throughput.pairs")
    for key in ("run_seconds", "max_run_seconds"):
        if not isinstance(throughput[key], (int, float)) or throughput[key] <= 0:
            raise SpecError(f"throughput.{key} must be positive")
    if throughput["max_run_seconds"] < throughput["run_seconds"]:
        raise SpecError("throughput.max_run_seconds must be at least run_seconds")
    for depth in spec["memory"]["depths"]:
        _positive_int(depth, "each memory.depths entry")

    deep = spec["deep"]
    if not isinstance(deep["auto"], bool):
        raise SpecError("deep.auto must be true or false")
    _positive_int(deep["winners"], "deep.winners", 0)
    _positive_int(deep["losers"], "deep.losers", 0)
    _positive_int(deep["replicates"], "deep.replicates")
    if not isinstance(deep["run_seconds"], (int, float)) or deep["run_seconds"] <= 0:
        raise SpecError("deep.run_seconds must be positive")
    if not isinstance(deep["cells"], list) or not all(isinstance(c, str) for c in deep["cells"]):
        raise SpecError("deep.cells must be a list of cell names such as 72p72c-b256")
    collectors = deep["collectors"]
    if collectors is not None and not isinstance(collectors, (list, dict)):
        raise SpecError("deep.collectors must be null, a list, or an object keyed by cluster")
    if not isinstance(spec["build"]["tests"], list):
        raise SpecError("build.tests must be a list of libtest filters")

    if fifo_class is not None:
        classes = {}
        for arm in arms:
            queue = arm_workload(spec, arm)["queue"]
            try:
                classes[fifo_class(queue)] = queue
            except KeyError:
                raise SpecError(f"queue {queue} has no FIFO class; add it to "
                                "FIFO_ORDERING in scripts/plot_style.py") from None
        if len(classes) > 1:
            raise SpecError(f"arms mix strict and relaxed FIFOs ({classes}); compare "
                            "queues only within one ordering guarantee")
    return spec


def arm_workload(spec, arm):
    return {**spec["workload"], **arm.get("workload", {})}


def arm_family(spec, arm):
    """An arm's family: as declared, else its queue name without geometry."""
    return arm.get("family") or GEOMETRY.sub("", arm_workload(spec, arm)["queue"])


def expand_scenario(token, cores):
    match = SCENARIO.match(token)
    if not match:
        raise SpecError(f"bad scenario {token!r}")

    def count(part):
        return cores - 1 if part == "N" else cores // 2 if part == "H" else int(part)
    return count(match.group(1)), count(match.group(2))


def batch_pair(entry):
    """A `throughput.batch_sizes` entry as (producer batch, consumer batch).

    `null` or an integer applies to both sides; `[producer, consumer]` sets
    them apart, so `[null, 256]` pushes one item at a time and pops 256. A
    side of `null` or `1` is the scalar API, kept as None.
    """
    if isinstance(entry, list):
        if len(entry) != 2:
            raise SpecError(f"a batch pair is [producer, consumer], not {entry!r}")
        producer, consumer = entry
    else:
        producer = consumer = entry

    def side(value, name):
        if value is None:
            return None
        _positive_int(value, f"the {name} side of throughput.batch_sizes entry {entry!r} (or null for scalar)")
        return None if value == 1 else value
    return (side(producer, "producer"), side(consumer, "consumer"))


def cell_name(scenario, batch):
    return f"{scenario}-{batch_token(batch)}"


def batch_pairs_for(throughput, producers, consumers):
    """The batch pairs a PpCc shape measures: `batch_sizes`, plus `skewed_batch_sizes` when
    the sides differ, as given at a producer-heavy shape and mirrored at a consumer-heavy one."""
    pairs = [tuple(batch) for batch in throughput["batch_sizes"]]
    if producers > consumers:
        pairs += [tuple(batch) for batch in throughput["skewed_batch_sizes"]]
    elif producers < consumers:
        pairs += [tuple(batch)[::-1] for batch in throughput["skewed_batch_sizes"]]
    return list(dict.fromkeys(pairs))


def cells(spec, cores):
    """Feasible measurement cells on `cores` worker cores, and skipped tokens.

    A scenario is feasible when every worker gets its own physical core.
    Tokens that expand to the same shape are measured once.
    """
    result, skipped, seen = [], [], set()

    def feasible(token):
        producers, consumers = expand_scenario(token, cores)
        if producers < 1 or consumers < 1 or producers + consumers > cores:
            skipped.append(token)
            return None
        return producers, consumers

    for token in spec["throughput"]["scenarios"]:
        shape = feasible(token)
        if not shape:
            continue
        scenario = "{}p{}c".format(*shape)
        for batch in batch_pairs_for(spec["throughput"], *shape):
            name = cell_name(scenario, batch)
            if name not in seen:
                seen.add(name)
                result.append({"cell": name, "kind": "throughput", "scenario": scenario,
                               "producer_batch": batch[0], "consumer_batch": batch[1]})
    if spec["memory"]["depths"]:
        for token in spec["memory"]["scenarios"]:
            shape = feasible(token)
            scenario = "{}p{}c".format(*shape) if shape else None
            name = f"{scenario}-mem"
            if scenario and name not in seen:
                seen.add(name)
                result.append({"cell": name, "kind": "memory", "scenario": scenario,
                               "producer_batch": None, "consumer_batch": None})
    return result, skipped


def collectors_for(spec, cluster, cluster_config):
    chosen = spec["deep"]["collectors"]
    if isinstance(chosen, dict):
        chosen = chosen.get(cluster)
    available = cluster_config.get("collectors", [])
    if chosen is None:
        return list(available)
    unknown = [c for c in chosen if c not in available]
    if unknown:
        raise SpecError(f"{cluster} does not offer collectors {unknown}; available: {available}")
    return list(chosen)


def quick(raw):
    """A seconds-long version of a spec, for smoke-testing arms on a laptop."""
    raw = copy.deepcopy(raw)
    raw["name"] = raw["name"] + "-quick"
    raw["throughput"] = {**raw.get("throughput", {}), "scenarios": ["1p1c", "2p2c", "4p1c"],
                         "batch_sizes": [None, 256], "skewed_batch_sizes": [[16, 256]],
                         "replicates": 1, "pairs": 2, "run_seconds": 1, "max_run_seconds": 10}
    raw["memory"] = {"scenarios": ["2p1c"], "depths": [65536]}
    raw["deep"] = {**raw.get("deep", {}), "run_seconds": 2, "winners": 1, "losers": 1}
    return raw


def study_id(spec, identity):
    """`<name>-<hash>`: identical inputs name the identical study, so relaunching resumes it."""
    body = {key: value for key, value in spec.items() if key != "clusters"}
    return f"{spec['name']}-{digest_text(canonical({'spec': body, **identity}))[:8]}"
