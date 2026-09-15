"""Turn task results into A/B effects, winner/loser selection and deep comparisons.

Runs on the cluster (selection, status summaries) and locally (reports). It
reads only a deployment directory and needs only the standard library.

Effects are paired: within one allocation, each pair is one control run and
one candidate run on the same node with the same fixed work, so node-to-node
variation cancels. A cell's estimate is the geometric mean over replicates
(independent allocations) of each replicate's mean log ratio, and its 95%
interval is a t interval across replicates. With one replicate the interval
falls back to the pairs, which only reflects one node.
"""
from __future__ import annotations

import math
import re
import statistics
from collections import defaultdict
from pathlib import Path

from .collectors import APX_RECIPES
from .common import batch_of, batch_order, read_json_or, t975
from .spec import BACKOFFS, GEOMETRY

# perf c2c --stats lines worth comparing: coherence traffic (HITM) and the
# operation totals it is a share of.
C2C_KEYS = ("HITM", "Total records", "Load Operations", "Store Operations", "Locked", "LLC Misses",
            "Shared Cache Lines")
# Absolute throughput is trustworthy when a run timed at least this long and
# the harness's empty-round rendezvous cost at most this share of it.
MIN_TIMED_NS = 2e9
MAX_BARRIER_FRACTION = 0.05
INTEGRITY_WORDS = ("integrity", "exact-once", "never-delivered")
# Ratios formed within one perf stat group, which counted them over the same run.
DERIVED = {
    "ipc": ("instructions", "cycles"),
    "cache_miss_rate": ("cache-misses", "cache-references"),
    "dtlb_miss_rate": ("dTLB-load-misses", "dTLB-loads"),
    "branch_miss_rate": ("branch-misses", "branches"),
    "frontend_stall_share": ("stalled-cycles-frontend", "cycles"),
    "backend_stall_share": ("stalled-cycles-backend", "cycles"),
}


def plan_tasks(dep_path):
    return read_json_or(Path(dep_path) / "plan.json", {"tasks": []})["tasks"]


def finished(dep_path, kind=None):
    """(task, result) for every task whose final attempt completed."""
    dep_path = Path(dep_path)
    states = read_json_or(dep_path / "state.json", {"tasks": {}})["tasks"]
    for task in plan_tasks(dep_path):
        entry = states.get(task["id"], {})
        if entry.get("status") != "done" or (kind and task["kind"] != kind):
            continue
        result = read_json_or(dep_path / "tasks" / task["id"] / f"a{entry['attempts']}" / "result.json", None)
        if result:
            yield task, result


def quality_ok(run):
    return (not run.get("error") and (run.get("elapsed_ns") or 0) >= MIN_TIMED_NS
            and run.get("barrier_fraction") is not None
            and run["barrier_fraction"] <= MAX_BARRIER_FRACTION)


def _run_rss(dep_path, task, entry, arm, run):
    """A run's end-of-run RSS: recorded by the task, else read from its benchmark.json."""
    if "rss_bytes" in run:
        return run["rss_bytes"]
    from .collectors import rss_bytes
    path = Path(dep_path) / "tasks" / task["id"] / f"a{entry['attempts']}" / f"p{run['pair']}-{arm}" / "benchmark.json"
    return rss_bytes(read_json_or(path, {}))


def throughput_runs(dep_path):
    rows = []
    states = read_json_or(Path(dep_path) / "state.json", {"tasks": {}})["tasks"]
    for task, result in finished(dep_path, "throughput"):
        entry = states.get(task["id"], {"attempts": 1})
        for arm, record in result.get("arms", {}).items():
            for run in record.get("runs", []):
                rows.append({
                    "cell": task["cell"], "scenario": task["scenario"], "producer_batch": task["producer_batch"],
                    "consumer_batch": task["consumer_batch"], "replicate": task["replicate"], "pair": run["pair"], "slot": run["slot"], "arm": arm,
                    "node": result.get("node"), "ops_per_sec": run.get("ops_per_sec"),
                    "elapsed_ns": run.get("elapsed_ns"), "rounds": run.get("rounds"),
                    "round_items": run.get("round_items"), "barrier_fraction": run.get("barrier_fraction"),
                    "rss_bytes": None if run.get("error") else _run_rss(dep_path, task, entry, arm, run),
                    "quality_ok": quality_ok(run), "error": run.get("error") or ""})
    return rows


def integrity_problems(dep_path):
    """{cell: {arm: [message]}}: failed exact-once checks and delivery mismatches."""
    problems = defaultdict(lambda: defaultdict(list))
    for task, result in finished(dep_path, "throughput"):
        for arm, record in result.get("arms", {}).items():
            verify = record.get("verify") or {}
            if verify.get("ok") is False:
                problems[task["cell"]][arm].append(f"r{task['replicate']} verify: {verify.get('error')}")
            for run in record.get("runs", []):
                error = run.get("error") or ""
                if any(word in error for word in INTEGRITY_WORDS):
                    problems[task["cell"]][arm].append(f"r{task['replicate']} pair {run['pair']}: {error}")
    return problems


def _interval(values):
    mean = statistics.fmean(values)
    if len(values) < 2:
        return mean, None, None
    half = t975(len(values) - 1) * statistics.stdev(values) / math.sqrt(len(values))
    return mean, mean - half, mean + half


def _median(values):
    values = [v for v in values if v is not None]
    return statistics.median(values) if values else None


def effects(study, dep_path):
    """One row per (throughput cell, candidate arm): candidate over control."""
    arms = [arm["name"] for arm in study["arms"]]
    control = arms[0]
    runs = throughput_runs(dep_path)
    problems = integrity_problems(dep_path)
    ops = {(r["cell"], r["replicate"], r["pair"], r["arm"]): r for r in runs
           if not r["error"] and r["ops_per_sec"]}
    cells = {}
    for task in plan_tasks(dep_path):
        if task["kind"] == "throughput":
            cells.setdefault(task["cell"], task)
    rows = []
    for cell, task in cells.items():
        for candidate in arms[1:]:
            per_replicate = defaultdict(list)
            for (c, replicate, pair, arm), run in ops.items():
                base = ops.get((c, replicate, pair, control))
                if c == cell and arm == candidate and base:
                    per_replicate[replicate].append(math.log(run["ops_per_sec"] / base["ops_per_sec"]))
            pairs = [value for values in per_replicate.values() for value in values]
            flagged = [arm for arm in (control, candidate) if problems.get(cell, {}).get(arm)]
            in_cell = [r for r in runs if r["cell"] == cell and r["arm"] in (control, candidate)]
            row = {"cell": cell, "scenario": task["scenario"], "producer_batch": task["producer_batch"],
                   "consumer_batch": task["consumer_batch"], "control": control, "candidate": candidate, "replicates": len(per_replicate),
                   "pairs": len(pairs), "effect": None, "ci_low": None, "ci_high": None, "ci_basis": "",
                   "pair_wins": sum(value > 0 for value in pairs),
                   "control_ops_median": _median(r["ops_per_sec"] for r in in_cell if r["arm"] == control),
                   "candidate_ops_median": _median(r["ops_per_sec"] for r in in_cell if r["arm"] == candidate),
                   "quality_share": (sum(r["quality_ok"] for r in in_cell) / len(in_cell)) if in_cell else None,
                   "integrity": flagged}
            if len(per_replicate) >= 2:
                mean, low, high = _interval([statistics.fmean(v) for v in per_replicate.values()])
                row["ci_basis"] = "replicates"
            elif pairs:
                mean, low, high = _interval(pairs)
                row["ci_basis"] = "pairs on one node"
            if pairs:
                row["effect"] = math.exp(mean)
                if low is not None:
                    row["ci_low"], row["ci_high"] = math.exp(low), math.exp(high)
            if flagged:
                row["verdict"] = "integrity"
            elif not pairs:
                row["verdict"] = "no-data"
            elif row["ci_low"] is not None and row["ci_low"] > 1:
                row["verdict"] = "win"
            elif row["ci_high"] is not None and row["ci_high"] < 1:
                row["verdict"] = "loss"
            else:
                row["verdict"] = "unclear"
            rows.append(row)
    return rows


def family_of(arm):
    """An arm's family: as declared at launch, else its queue without geometry."""
    return arm.get("family") or GEOMETRY.sub("", arm["workload"]["queue"])


def backoff_of(arm):
    """An arm's backoff policy; bench_hardware's default when the workload names none."""
    return arm["workload"].get("backoff", BACKOFFS[0])


def backoffs(study):
    """The backoff policies the study's arms use, in arm order."""
    return list(dict.fromkeys(backoff_of(arm) for arm in study["arms"]))


def variation_of(study, arm):
    """What sets an arm apart within its family: its geometry, and its backoff
    when the study compares several. Empty for a plain queue."""
    parts = []
    geometry = geometry_of(arm)
    if geometry != "default":
        parts.append(geometry)
    if len(backoffs(study)) > 1:
        parts.append(backoff_of(arm))
    return ", ".join(parts)


def families(study):
    """{family: [arm names]} in arm order; the control's family comes first."""
    result = {}
    for arm in study["arms"]:
        result.setdefault(family_of(arm), []).append(arm["name"])
    return result


def best_per_cell(study, effect_rows):
    """{family: {cell: effect row}}: each family's best variation per cell.

    Candidates compete within their family on the paired effect against the
    control, so the choice is made on the same node pairs the effect is. Rows
    without an effect (no data, integrity problems) never win.
    """
    arms = {arm["name"]: arm for arm in study["arms"]}
    best = defaultdict(dict)
    for row in effect_rows:
        if row["effect"] is None or row["verdict"] in ("integrity", "no-data"):
            continue
        family = family_of(arms[row["candidate"]])
        current = best[family].get(row["cell"])
        if current is None or row["effect"] > current["effect"]:
            best[family][row["cell"]] = row
    return best


def best_overall(study, effect_rows):
    """One row per family: its variation with the largest geometric-mean effect over cells.

    Cells count for a variation only where it has an effect; `cells` says how
    many, so a variation measured in few cells can be told from a good one.
    """
    arms = {arm["name"]: arm for arm in study["arms"]}
    control = study["arms"][0]["name"]
    per_arm = defaultdict(list)
    for row in effect_rows:
        if row["effect"] is not None and row["verdict"] not in ("integrity", "no-data"):
            per_arm[row["candidate"]].append(row)
    rows = []
    for family, names in families(study).items():
        scored = []
        for name in names:
            if name == control:
                continue
            found = per_arm.get(name, [])
            if found:
                mean = math.exp(statistics.fmean(math.log(r["effect"]) for r in found))
                scored.append({"family": family, "arm": name, "geomean_effect": mean, "cells": len(found),
                               "wins": sum(r["verdict"] == "win" for r in found),
                               "losses": sum(r["verdict"] == "loss" for r in found)})
        if scored:
            rows.append(max(scored, key=lambda r: r["geomean_effect"]))
        elif control in names:
            rows.append({"family": family, "arm": control, "geomean_effect": 1.0, "cells": None,
                         "wins": None, "losses": None})
    return rows


def ranking(study, dep_path, effect_rows=None):
    """One row per (cell, measured arm), ordered within the cell by effect.

    The control ranks at 1.0. `best_in_family` marks the first arm of each
    family in that order, the control included, which is what the plots
    show; `share_of_best` is the arm's median throughput over the cell's best.
    """
    arms = study["arms"]
    control = arms[0]["name"]
    effect_rows = effects(study, dep_path) if effect_rows is None else effect_rows
    by_candidate = {(row["cell"], row["candidate"]): row for row in effect_rows}
    ops, rss = defaultdict(list), defaultdict(list)
    for run in throughput_runs(dep_path):
        if not run["error"] and run["ops_per_sec"]:
            ops[(run["cell"], run["arm"])].append(run["ops_per_sec"])
            if run["rss_bytes"]:
                rss[(run["cell"], run["arm"])].append(run["rss_bytes"])
    cells = {}
    for task in plan_tasks(dep_path):
        if task["kind"] == "throughput":
            cells.setdefault(task["cell"], task)
    rows = []
    for cell, task in cells.items():
        entries = []
        for arm in arms:
            values = ops.get((cell, arm["name"]))
            if not values:
                continue
            row = by_candidate.get((cell, arm["name"]))
            if arm["name"] == control:
                effect, verdict = 1.0, "control"
            elif row is None or row["effect"] is None:
                continue
            else:
                effect, verdict = row["effect"], row["verdict"]
            entries.append({"cell": cell, "scenario": task["scenario"], "producer_batch": task["producer_batch"],
                            "consumer_batch": task["consumer_batch"], "arm": arm["name"], "family": family_of(arm), "ops_median": statistics.median(values),
                            "rss_median": _median(rss.get((cell, arm["name"]), [])),
                            "runs": len(values), "effect": effect, "verdict": verdict})
        if not entries:
            continue
        entries.sort(key=lambda e: -e["effect"])
        best = max(e["ops_median"] for e in entries)
        seen = set()
        for rank, entry in enumerate(entries, 1):
            entry["rank"] = rank
            entry["share_of_best"] = entry["ops_median"] / best if best else None
            entry["best_in_family"] = entry["family"] not in seen
            seen.add(entry["family"])
        rows += entries
    return rows


def geometry_matrix(study, ranking_rows):
    """Every arm of every family per batch size: throughput and memory over the cells it ran.

    `ops_geomean` is the geometric mean of the arm's median throughput over
    its cells, and `share_of_best` that arm over the batch's best arm (the
    one with the largest geometric mean, named in `best_arm`), as a
    geometric mean over the cells both ran, so 1.0 marks one real arm
    rather than a per-cell oracle. `rss_max` and `rss_geomean` are the
    largest and typical median end-of-run RSS over the cells. Nothing here
    depends on which arm is the control.
    """
    arms = {arm["name"]: arm for arm in study["arms"]}
    per_arm = defaultdict(dict)
    for row in ranking_rows:
        per_arm[(batch_of(row), row["arm"])][row["cell"]] = row
    batches = sorted({batch for batch, _ in per_arm}, key=batch_order)

    def geomean(values):
        values = list(values)
        return math.exp(statistics.fmean(math.log(v) for v in values)) if values else None

    rows = []
    for batch in batches:
        cells_of = {name: cells for (b, name), cells in per_arm.items() if b == batch}
        means = {name: geomean(r["ops_median"] for r in cells.values()) for name, cells in cells_of.items()}
        best = max(means, key=means.get)
        for name in (arm["name"] for arm in study["arms"] if arm["name"] in cells_of):
            cells = cells_of[name]
            common = [cell for cell in cells if cell in cells_of[best]]
            rss = [r["rss_median"] for r in cells.values() if r["rss_median"]]
            slots, vblocks = _geometry(arms[name]["workload"]["queue"])
            rows.append({"producer_batch": batch[0], "consumer_batch": batch[1],
                         "family": family_of(arms[name]), "arm": name,
                         "slots_per_vblock": slots, "vblocks_per_block": vblocks,
                         "backoff": backoff_of(arms[name]), "cells": len(cells),
                         "ops_geomean": means[name], "best_arm": best,
                         "share_of_best": geomean(cells[c]["ops_median"] / cells_of[best][c]["ops_median"] for c in common),
                         "rss_max": max(rss) if rss else None, "rss_geomean": geomean(rss)})
    return rows


def batch_matrix(study, ranking_rows):
    """Every arm at every batch pair per shape: how the producer and consumer batch
    sizes, matched or not, change one arm's throughput at one shape.

    Per (arm, scenario), `share_of_best` is the pair's median throughput over
    the fastest pair of that arm at that shape (`best_batch`), so 1.0 is the
    batch pair to use there. The control plays no part.
    """
    per_shape = defaultdict(dict)
    for row in ranking_rows:
        per_shape[(row["arm"], row["scenario"])][batch_of(row)] = row
    order = {arm["name"]: i for i, arm in enumerate(study["arms"])}
    rows = []
    for (arm, scenario), pairs in sorted(per_shape.items(), key=lambda item: order[item[0][0]]):
        best = max(pairs, key=lambda b: pairs[b]["ops_median"])
        for batch in sorted(pairs, key=batch_order):
            row = pairs[batch]
            rows.append({"arm": arm, "family": row["family"], "scenario": scenario,
                         "producer_batch": batch[0], "consumer_batch": batch[1], "cell": row["cell"],
                         "ops_median": row["ops_median"], "rss_median": row["rss_median"],
                         "best_batch": best, "share_of_best": row["ops_median"] / pairs[best]["ops_median"]})
    return rows


def _geometry(queue):
    """(slots per VBlock, VBlocks per block) named by an owned-buffer queue; None for an omitted dimension."""
    match = re.search(r"-s(\d+)(?:-v(\d+))?$", queue)
    return (int(match.group(1)), int(match.group(2)) if match.group(2) else None) if match else (None, None)


def geometry_of(arm):
    """An arm's geometry suffix (`s4096-v32`), or `default`."""
    match = GEOMETRY.search(arm["workload"]["queue"])
    return match.group(0)[1:] if match else "default"


def same_geometry(study, effect_rows):
    """Families against the control's family at equal geometry and backoff.

    For every other family and every geometry and backoff both families ran,
    each cell gives the ratio of the two arms' effects against the control,
    which cancels the control out of the comparison; the control arm itself
    counts as 1.0. Rows carry the geometric mean of that ratio over all cells
    and per batch size, with the number of cells behind each mean.
    """
    arms = study["arms"]
    control = arms[0]
    reference = family_of(control)
    by_key = {(family_of(arm), geometry_of(arm), backoff_of(arm)): arm["name"] for arm in arms}
    effect = {(row["candidate"], row["cell"]): row for row in effect_rows
              if row["effect"] is not None and row["verdict"] not in ("integrity", "no-data")}
    cells = {row["cell"]: batch_of(row) for row in effect_rows}

    def value(name, cell):
        if name == control["name"]:
            return 1.0 if cell in cells else None
        row = effect.get((name, cell))
        return row["effect"] if row else None

    def geomean(values):
        return math.exp(statistics.fmean(math.log(v) for v in values)) if values else None

    rows = []
    for family in families(study):
        if family == reference:
            continue
        for (fam, geometry, backoff), name in by_key.items():
            if fam != family or (reference, geometry, backoff) not in by_key:
                continue
            other = by_key[(reference, geometry, backoff)]
            ratios = {}
            for cell, batch in cells.items():
                mine, theirs = value(name, cell), value(other, cell)
                if mine and theirs:
                    ratios[cell] = (mine / theirs, batch)
            if not ratios:
                continue
            row = {"family": family, "reference": reference, "geometry": geometry, "backoff": backoff, "arm": name,
                   "reference_arm": other, "cells": len(ratios), "geomean_ratio": geomean([r for r, _ in ratios.values()]),
                   "cells_ahead": sum(r > 1 for r, _ in ratios.values()), "batches": {}}
            for batch in sorted({b for _, b in ratios.values()}, key=batch_order):
                values = [r for r, b in ratios.values() if b == batch]
                row["batches"][batch] = {"geomean_ratio": geomean(values), "cells": len(values)}
            rows.append(row)
    return rows


def select(study, effect_rows):
    """Cells for the deep stage: each family's biggest wins and losses.

    Within a family only its best variation per cell competes, so a sweep of
    many geometries deep-dives the families at their best rather than every
    variation. Significant verdicts rank ahead of larger but uncertain
    effects. Cells with an integrity problem are never selected: a queue that
    loses or duplicates items has no performance to explain.
    """
    deep = study["spec"]["deep"]
    control = study["arms"][0]["name"]
    everyone = [arm["name"] for arm in study["arms"]]
    if deep["cells"]:
        known = {row["cell"]: row for row in effect_rows}
        return [{"cell": cell, "scenario": known[cell]["scenario"], "producer_batch": known[cell]["producer_batch"],
                 "consumer_batch": known[cell]["consumer_batch"], "arms": everyone, "reasons": ["requested in spec"]}
                for cell in deep["cells"] if cell in known]
    chosen = {}
    for family, per_cell in best_per_cell(study, effect_rows).items():
        rows = list(per_cell.values())
        winners = sorted((r for r in rows if r["effect"] > 1),
                         key=lambda r: (r["verdict"] == "win", r["effect"]), reverse=True)[:deep["winners"]]
        losers = sorted((r for r in rows if r["effect"] < 1),
                        key=lambda r: (r["verdict"] == "loss", -r["effect"]), reverse=True)[:deep["losers"]]
        for direction, picked in (("wins", winners), ("loses", losers)):
            for row in picked:
                entry = chosen.setdefault(row["cell"], {"cell": row["cell"], "scenario": row["scenario"],
                                                        "producer_batch": row["producer_batch"],
                                                        "consumer_batch": row["consumer_batch"],
                                                        "arms": [control], "reasons": []})
                if row["candidate"] not in entry["arms"]:
                    entry["arms"].append(row["candidate"])
                entry["reasons"].append(f"{row['candidate']} {direction} {row['effect']:.3f}x ({row['verdict']})")
    return list(chosen.values())


def cell_schedule(dep_path, cell):
    """The work schedule stage 1 settled on for `cell`, for the deep stage to replay."""
    schedules = [result["schedule"] for task, result in finished(dep_path, "throughput")
                 if task["cell"] == cell and result.get("schedule")]
    if not schedules:
        return None
    sizes = [s["round_items"] for s in schedules]
    round_items = max(set(sizes), key=sizes.count)
    rounds = statistics.median_low([s["rounds"] for s in schedules if s["round_items"] == round_items])
    return {"round_items": round_items, "rounds": rounds}


def memory_rows(dep_path):
    rows = []
    for task, result in finished(dep_path, "memory"):
        for arm, record in result.get("arms", {}).items():
            for depth, entry in record.get("depths", {}).items():
                base = {"cell": task["cell"], "scenario": task["scenario"], "replicate": task["replicate"],
                        "arm": arm, "depth": int(depth), "node": result.get("node"),
                        "error": entry.get("error", "")}
                if base["error"]:
                    rows.append({**base, "phase": "", "rss_bytes": None})
                for phase, rss in (entry.get("rss") or {}).items():
                    rows.append({**base, "phase": phase, "rss_bytes": rss})
    return rows


def memory_summary(study, dep_path):
    """Median RSS per (cell, depth, phase, arm), with each candidate's ratio to control."""
    control = study["arms"][0]["name"]
    grouped = defaultdict(list)
    for row in memory_rows(dep_path):
        if row["rss_bytes"] is not None:
            grouped[(row["cell"], row["depth"], row["phase"], row["arm"])].append(row["rss_bytes"])
    summary = []
    for (cell, depth, phase, arm), values in sorted(grouped.items()):
        median = statistics.median(values)
        base = grouped.get((cell, depth, phase, control))
        summary.append({"cell": cell, "depth": depth, "phase": phase, "arm": arm, "rss_median_bytes": median,
                        "samples": len(values),
                        "ratio_to_control": median / statistics.median(base) if base and arm != control else None})
    return summary


def deep_rows(dep_path):
    """One row per (deep task, arm), metrics included, failures included."""
    dep_path = Path(dep_path)
    states = read_json_or(dep_path / "state.json", {"tasks": {}})["tasks"]
    rows = []
    for task in plan_tasks(dep_path):
        if task["kind"] != "deep":
            continue
        entry = states.get(task["id"], {})
        result = read_json_or(dep_path / "tasks" / task["id"] / f"a{entry.get('attempts', 0)}" / "result.json", {})
        base = {"task": task["id"], "cell": task["cell"], "collector": task["collector"],
                "replicate": task["replicate"], "status": entry.get("status", "new"),
                "task_error": entry.get("last_error", "")}
        arms = result.get("arms", {})
        if not arms:
            rows.append({**base, "arm": "", "metrics": {}})
        for arm, record in arms.items():
            shadow, capture = record.get("shadow") or {}, record.get("capture") or {}
            rows.append({**base, "arm": arm, "shadow_ops": shadow.get("ops_per_sec"),
                         "capture_ops": capture.get("ops_per_sec"),
                         "error": record.get("error") or record.get("shadow_error") or "",
                         "metrics": record.get("metrics") or {}})
    return rows


def flatten(collector, metrics):
    """A collector summary's scalar metrics as {name: value}, for tables and CSVs."""
    values = {}
    if collector == "counters":
        for data in (metrics.get("groups") or {}).values():
            handoffs = data.get("handoffs") or 0
            events = {event.split(":", 1)[0]: item["value"] for event, item in data.get("events", {}).items()}
            for event, value in events.items():
                if handoffs:
                    values.setdefault(f"{event} per handoff", value / handoffs)
            for name, (numerator, denominator) in DERIVED.items():
                if events.get(numerator) is not None and events.get(denominator):
                    values.setdefault(name, events[numerator] / events[denominator])
    elif collector == "perf_record":
        for percent, _, label in metrics.get("symbols", []):
            values[f"cycles % {label}"] = percent
    elif collector == "perf_mem":
        for percent, _, label in metrics.get("levels", []):
            values[f"loads % {label}"] = percent
        # Rates per sampled operation within one address space, which makes
        # them independent of the sampling period (see collectors.SPE_AWK).
        for space, counts in (metrics.get("spe") or {}).items():
            total = counts.get("memory")
            for event, count in sorted(counts.items()):
                if event != "memory" and total:
                    values[f"SPE {event} per sampled op ({space})"] = count / total
    elif collector == "perf_c2c":
        for key, value in (metrics.get("stats") or {}).items():
            if any(word in key for word in C2C_KEYS):
                values[f"c2c {key}"] = value
    elif collector in APX_RECIPES:
        for key, value in (metrics.get("metrics") or {}).items():
            values[f"apx {key}"] = value
        for fraction, _, symbol in metrics.get("hotspots", []):
            if fraction is not None:
                values[f"hotspot % {symbol}"] = 100 * fraction
    return values


def summary(study, dep_path):
    dep_path = Path(dep_path)
    selection = read_json_or(dep_path / "selection.json", {}).get("cells")
    rows = effects(study, dep_path)
    return {"effects": rows, "selection": selection, "best": best_overall(study, rows),
            "memory": memory_summary(study, dep_path)}


def arm_medians(study, dep_path):
    """{(cell, arm): shape, median throughput and RSS} over successful runs.

    An arm that failed an exact-once check in a cell is left out of that
    cell: a queue that loses or duplicates items has no throughput there.
    Nothing depends on which arm is the control.
    """
    problems = integrity_problems(dep_path)
    ops, rss, meta = defaultdict(list), defaultdict(list), {}
    for run in throughput_runs(dep_path):
        if run["error"] or not run["ops_per_sec"] or problems.get(run["cell"], {}).get(run["arm"]):
            continue
        key = (run["cell"], run["arm"])
        ops[key].append(run["ops_per_sec"])
        if run["rss_bytes"]:
            rss[key].append(run["rss_bytes"])
        meta.setdefault(key, {"scenario": run["scenario"], "producer_batch": run["producer_batch"],
                              "consumer_batch": run["consumer_batch"]})
    return {key: {**meta[key], "ops_median": statistics.median(values), "rss_median": _median(rss.get(key, []))}
            for key, values in ops.items()}


def shape_matrix(study, medians):
    """Per cell, each family's fastest arm by median throughput: one number per family per shape.

    `medians` is `arm_medians()`. The pick is by absolute throughput, not by
    paired effect, so the table reads as measured numbers; `best_family`
    names the cell's fastest family and `share_of_best` is the family's
    number over that one. The control plays no part.
    """
    arms = {arm["name"]: arm for arm in study["arms"]}
    by_cell = {}
    for (cell, arm), entry in medians.items():
        if arm in arms:
            by_cell.setdefault(cell, []).append({**entry, "cell": cell, "arm": arm, "family": family_of(arms[arm])})
    order = {family: index for index, family in enumerate(families(study))}
    rows = []
    for cell, entries in by_cell.items():
        fastest = {}
        for entry in entries:
            current = fastest.get(entry["family"])
            if current is None or entry["ops_median"] > current["ops_median"]:
                fastest[entry["family"]] = entry
        top = max(fastest.values(), key=lambda e: e["ops_median"])
        for family in sorted(fastest, key=lambda f: order.get(f, len(order))):
            entry = fastest[family]
            rows.append({"cell": cell, "scenario": entry["scenario"], "producer_batch": entry["producer_batch"],
                         "consumer_batch": entry["consumer_batch"], "family": family, "arm": entry["arm"],
                         "ops_median": entry["ops_median"], "rss_median": entry["rss_median"],
                         "best_family": top["family"], "share_of_best": entry["ops_median"] / top["ops_median"]})
    return rows


def family_summary(study, shape_rows):
    """Per batch size and family, over the shapes: the geometric mean of the
    family's fastest arm, its ratio to the control's family over the shapes
    both ran, the shapes where it was ahead of that family, and the shapes
    where it was the fastest family of all."""
    reference = family_of(study["arms"][0])
    per = defaultdict(dict)
    for row in shape_rows:
        per[(batch_of(row), row["family"])][row["cell"]] = row
    batches = sorted({batch for batch, _ in per}, key=batch_order)

    def geomean(values):
        values = list(values)
        return math.exp(statistics.fmean(math.log(v) for v in values)) if values else None

    rows = []
    for batch in batches:
        base = per.get((batch, reference), {})
        for family in families(study):
            cells = per.get((batch, family))
            if not cells:
                continue
            # The reference family is not compared with itself.
            common = [cell for cell in cells if cell in base] if family != reference else []
            rows.append({"producer_batch": batch[0], "consumer_batch": batch[1], "family": family,
                         "reference": reference, "cells": len(cells),
                         "arms": sorted({row["arm"] for row in cells.values()}),
                         "ops_geomean": geomean(row["ops_median"] for row in cells.values()),
                         "ratio_to_reference": geomean(cells[c]["ops_median"] / base[c]["ops_median"] for c in common),
                         "cells_compared": len(common) if common else None,
                         "cells_ahead": sum(cells[c]["ops_median"] > base[c]["ops_median"] for c in common) if common else None,
                         "cells_fastest": sum(row["best_family"] == family for row in cells.values())})
    return rows
