"""The local report: stage-1 effects, memory footprints, stage-2 hardware comparisons."""
from __future__ import annotations

import csv
import statistics
from collections import defaultdict
from pathlib import Path

from . import analysis
from .collectors import APX_RECIPES
from .common import batch_of, batch_title, batch_token, read_json_or

# Long symbol lists are cut to the entries with the largest share in any arm.
TOP_SYMBOLS = 12
RANKED_PREFIXES = ("cycles % ", "hotspot % ")


def _mops(value):
    return "—" if value is None else f"{value / 1e6:,.1f}"


def _mib(value):
    return "—" if value is None else f"{value / 2 ** 20:,.1f}"


def _num(value):
    if value is None:
        return "—"
    if abs(value) >= 1000:
        return f"{value:,.0f}"
    return f"{value:.3f}" if abs(value) >= 1 else f"{value:.4g}"


def _ratio(value, base):
    return f"{value / base:.3f}" if value is not None and base else "—"


def table(headers, rows):
    lines = ["| " + " | ".join(headers) + " |", "|" + "|".join("---" for _ in headers) + "|"]
    lines += ["| " + " | ".join(str(cell) for cell in row) + " |" for row in rows]
    return lines + [""]


def write_csv(path, rows):
    if not rows:
        return
    fields = list(dict.fromkeys(key for row in rows for key in row))
    with path.open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def arms_table(study):
    rows = []
    for index, arm in enumerate(study["arms"]):
        if arm["commit"] == "WORKTREE":
            source = f"WORKTREE on `{arm['head'][:9]}` ({len(arm['dirty'])} changed files)"
        else:
            source = f"`{arm['ref']}` = `{arm['commit'][:9]}`"
        rows.append([arm["name"], "control" if index == 0 else "candidate", source,
                     arm["workload"]["queue"], analysis.backoff_of(arm), ", ".join(arm["features"]) or "—",
                     f"`{arm['source'][:10]}`"])
    return table(["arm", "role", "source", "queue", "backoff", "features", "frozen source"], rows)


def effects_section(study, dep, selected):
    lines = ["### Throughput: candidate / control", "",
             "Pairs are adjacent runs in one allocation with identical work. The effect is the geometric "
             "mean over replicates (allocations) of each replicate's mean pair ratio, with a 95% t "
             "interval across replicates. ◆ marks cells chosen for the deep stage.", ""]
    rows = analysis.effects(study, dep)
    control = study["arms"][0]["name"]
    table_rows = []
    for row in rows:
        ci = f"[{row['ci_low']:.3f}, {row['ci_high']:.3f}]" if row["ci_low"] is not None else "—"
        effect = f"{row['effect']:.3f}x" if row["effect"] is not None else "—"
        quality = f"{row['quality_share']:.0%}" if row["quality_share"] is not None else "—"
        table_rows.append([("◆ " if row["cell"] in selected else "") + row["cell"], row["candidate"],
                           _mops(row["control_ops_median"]), _mops(row["candidate_ops_median"]), effect, ci,
                           row["replicates"], f"{row['pair_wins']}/{row['pairs']}", quality, row["verdict"]])
    lines += table(["cell", "candidate", f"{control} M/s", "candidate M/s", "effect", "95% CI", "replicates",
                    "pair wins", "quality", "verdict"], table_rows)
    lines.append("*quality*: share of runs timed ≥ 2 s with harness barrier overhead ≤ 5%.")
    lines.append("")
    problems = analysis.integrity_problems(dep)
    if problems:
        lines += ["**Integrity problems.** These arms lost or duplicated items, or failed to run, so "
                  "their cells have no effect estimate:", ""]
        for cell, arms in sorted(problems.items()):
            for arm, messages in sorted(arms.items()):
                lines.append(f"- `{cell}` {arm}: {messages[0]}" + (f" (+{len(messages) - 1} more)" if len(messages) > 1 else ""))
        lines.append("")
    return lines, rows


def best_section(study, effect_rows):
    """Each family's best variation, overall and per cell; only for studies with several arms."""
    if len(study["arms"]) <= 2:
        return [], [], []
    overall = analysis.best_overall(study, effect_rows)
    per_cell = analysis.best_per_cell(study, effect_rows)
    lines = ["### Best variation per family", "",
             "Within a family, variations compete on their paired effect against the control. "
             "Overall is the geometric mean of that effect over the cells a variation was measured in; "
             "a family whose best is below 1.000x is best served by the control's own geometry.", ""]
    rows = []
    for row in overall:
        if row["cells"] is None:
            rows.append([row["family"], row["arm"], "control", "—", "—"])
        else:
            rows.append([row["family"], row["arm"], f"{row['geomean_effect']:.3f}x", row["cells"],
                         f"{row['wins']} / {row['losses']}"])
    lines += table(["family", "best variation", "geomean effect", "cells", "wins / losses"], rows)
    names = list(per_cell)
    csv_rows = []
    if names:
        cells = list(dict.fromkeys(row["cell"] for row in effect_rows))
        table_rows = []
        for cell in cells:
            entry = [cell]
            for family in names:
                best = per_cell[family].get(cell)
                entry.append(f"{best['candidate']} {best['effect']:.3f}x ({best['verdict']})" if best else "—")
                if best:
                    csv_rows.append({"cell": cell, "family": family, "arm": best["candidate"], "effect": best["effect"],
                                     "ci_low": best["ci_low"], "ci_high": best["ci_high"], "verdict": best["verdict"]})
            table_rows.append(entry)
        lines += table(["cell", *names], table_rows)
    return lines, overall, csv_rows


def same_geometry_section(study, effect_rows):
    rows = analysis.same_geometry(study, effect_rows)
    if not rows:
        return [], []
    batches = list(dict.fromkeys(b for row in rows for b in row["batches"]))
    label = batch_token
    several = len(analysis.backoffs(study)) > 1
    lines = [f"### Layouts at equal geometry: family / {rows[0]['reference']}", "",
             "For each geometry" + (" and backoff" if several else "") + " both families ran, the ratio of "
             "their effects against the control in every cell (the control cancels out), as a geometric "
             "mean over all cells and per batch size; *ahead* counts the cells where the family was faster.", ""]
    table_rows, csv_rows = [], []
    for row in rows:
        table_rows.append([row["family"], row["geometry"] + (f", {row['backoff']}" if several else ""),
                           f"{row['geomean_ratio']:.3f}x", f"{row['cells_ahead']} / {row['cells']}"] +
                          [f"{row['batches'][b]['geomean_ratio']:.3f}x ({row['batches'][b]['cells']})"
                           if b in row["batches"] else "—" for b in batches])
        csv_rows.append({"family": row["family"], "reference": row["reference"], "geometry": row["geometry"],
                         "backoff": row["backoff"], "arm": row["arm"], "reference_arm": row["reference_arm"],
                         "cells": row["cells"], "geomean_ratio": row["geomean_ratio"], "cells_ahead": row["cells_ahead"],
                         **{f"geomean_ratio_{label(b)}": row["batches"][b]["geomean_ratio"] for b in row["batches"]}})
    lines += table(["family", "variation", "all cells", "ahead", *(label(b) for b in batches)], table_rows)
    return lines, csv_rows


def _variation_label(study, arm):
    """Geometry, and backoff when the study compares several; — for the plain queue."""
    return analysis.variation_of(study, arm) or "—"


def geometry_matrix_section(study, ranking_rows):
    """Every arm per batch size against the batch's best arm; nothing here depends on the control."""
    rows = analysis.geometry_matrix(study, ranking_rows)
    if len(study["arms"]) <= 2 or not rows:
        return [], rows
    arms = {arm["name"]: arm for arm in study["arms"]}
    lines = ["### Geometry matrix: throughput and memory per batch size", "",
             "Per batch size, every arm's geometric-mean throughput over its cells, its share of the "
             "batch's best arm (the largest geometric mean; the share is a geometric mean over the "
             "cells both ran), and its largest and typical median end-of-run RSS over the cells. "
             "`./ab plot` draws this as the geometry and memory matrices.", ""]
    by_batch = defaultdict(list)
    for row in rows:
        by_batch[batch_of(row)].append(row)
    for batch, entries in by_batch.items():
        lines += [f"**{batch_title(batch)}** (best: {entries[0]['best_arm']})", ""]
        table_rows = [[e["arm"], e["family"], _variation_label(study, arms[e["arm"]]), e["cells"], _mops(e["ops_geomean"]),
                       f"{100 * e['share_of_best']:.0f}%" if e["share_of_best"] else "—",
                       _mib(e["rss_max"]), _mib(e["rss_geomean"])]
                      for e in sorted(entries, key=lambda e: -(e["share_of_best"] or 0))]
        lines += table(["arm", "family", "variation", "cells", "geomean M/s", "of best", "largest RSS (MiB)",
                        "typical RSS (MiB)"], table_rows)
    return lines, rows


def batch_matrix_section(study, ranking_rows):
    """Every arm's throughput at every batch pair per shape, against the arm's best pair there."""
    rows = analysis.batch_matrix(study, ranking_rows)
    batches = list(dict.fromkeys(batch_of(row) for row in rows))
    if len(batches) <= 1:
        return [], rows
    lines = ["### Batch matrix: producer and consumer batch sizes per shape", "",
             "For every arm and shape, the median throughput at each batch pair (push size / pop size; "
             "1 is the scalar API on that side) and its share of the arm's fastest pair at that shape, "
             "which the **best** column names. `./ab plot` draws this as the batch matrix.", ""]
    table_rows = []
    by_shape = defaultdict(list)
    for row in rows:
        by_shape[(row["arm"], row["scenario"])].append(row)
    for (arm, scenario), entries in by_shape.items():
        values = {batch_of(e): e for e in entries}
        table_rows.append([arm, scenario, batch_token(entries[0]["best_batch"])] +
                          [f"{_mops(values[b]['ops_median'])} ({100 * values[b]['share_of_best']:.0f}%)"
                           if b in values else "—" for b in batches])
    lines += table(["arm", "shape", "best", *(batch_token(b) + " M/s" for b in batches)], table_rows)
    return lines, rows


def _within_family_label(study, family, arm):
    """What sets `arm` apart from the rest of its family: the geometry when
    the family spans several, the backoff when it spans several; else empty."""
    arms = {a["name"]: a for a in study["arms"]}
    members = [arms[name] for name in analysis.families(study).get(family, [])]
    parts = []
    if len({analysis.geometry_of(a) for a in members}) > 1:
        parts.append(analysis.geometry_of(arm))
    if len({analysis.backoff_of(a) for a in members}) > 1:
        parts.append(analysis.backoff_of(arm))
    return ", ".join(parts)


def shape_section(study, dep):
    """Absolute throughput of each family's fastest arm at every shape, per batch size."""
    rows = analysis.shape_matrix(study, analysis.arm_medians(study, dep))
    if not rows:
        return [], rows, []
    summary = analysis.family_summary(study, rows)
    names = list(analysis.families(study))
    arms = {arm["name"]: arm for arm in study["arms"]}
    reference = summary[0]["reference"]
    lines = ["### Throughput by shape: each family's fastest arm (M items/s)", "",
             "Per batch size, one row per shape and one column per family, giving the median throughput of "
             "the family's fastest arm at that shape in millions of items per second, followed by its ratio "
             f"to the `{reference}` family at that shape. The fastest family in a row is in bold, and a family "
             "with several arms names the arm behind each number by what sets it apart (its backoff or "
             "geometry). An arm that failed an exact-once check in a cell is left out of that cell. "
             "The summary first gives each family's geometric mean over "
             f"the shapes, its ratio to the `{reference}` family over the shapes both ran, the shapes where it "
             f"was ahead of `{reference}`, and the shapes where it was the fastest family of all. The control "
             "arm plays no part in these numbers; `./ab plot` draws them as the scaling figure.", ""]
    table_rows = []
    for row in summary:
        ratio = f"{row['ratio_to_reference']:.2f}x" if row["ratio_to_reference"] is not None else "—"
        ahead = f"{row['cells_ahead']} / {row['cells_compared']}" if row["cells_compared"] else "—"
        table_rows.append([batch_title(batch_of(row)), row["family"], ", ".join(row["arms"]), row["cells"],
                           _mops(row["ops_geomean"]), ratio, ahead, f"{row['cells_fastest']} / {row['cells']}"])
    lines += table(["batch", "family", "arms", "shapes", "geomean M/s", f"/ {reference}",
                    f"ahead of {reference}", "fastest of all"], table_rows)
    by_batch = defaultdict(dict)
    for row in rows:
        by_batch[batch_of(row)].setdefault(row["scenario"], {})[row["family"]] = row
    for batch, shapes in by_batch.items():
        lines += [f"**{batch_title(batch)}**", ""]
        table_rows = []
        for scenario, entries in shapes.items():
            cells = [scenario]
            base = entries.get(reference)
            for family in names:
                entry = entries.get(family)
                if entry is None:
                    cells.append("—")
                    continue
                text = _mops(entry["ops_median"])
                if entry["best_family"] == family:
                    text = f"**{text}**"
                notes = []
                label = _within_family_label(study, family, arms[entry["arm"]])
                if label:
                    notes.append(label)
                if base is not None and family != reference:
                    notes.append(f"{entry['ops_median'] / base['ops_median']:.2f}x")
                cells.append(f"{text} ({', '.join(notes)})" if notes else text)
            table_rows.append(cells)
        lines += table(["shape", *names], table_rows)
    return lines, rows, summary


def ranking_section(study, dep, effect_rows):
    rows = analysis.ranking(study, dep, effect_rows)
    if len(study["arms"]) <= 2 or not rows:
        return [], rows
    control = study["arms"][0]["name"]
    lines = ["### Ranking per cell", "",
             "Every measured arm in order of its effect against the control (the control is 1.000x); "
             "◆ marks each family's best variation, the control included.", ""]
    by_cell = defaultdict(list)
    for row in rows:
        by_cell[row["cell"]].append(row)
    table_rows = []
    for cell, entries in by_cell.items():
        control_ops = next((e["ops_median"] for e in entries if e["arm"] == control), None)
        text = " · ".join(("◆ " if e["best_in_family"] else "") + f"{e['arm']} {e['effect']:.3f}x" for e in entries)
        footprint = [e for e in entries if e["rss_median"]]
        widest = max(footprint, key=lambda e: e["rss_median"]) if footprint else None
        table_rows.append([cell, _mops(control_ops), text,
                           f"{widest['arm']} {_mib(widest['rss_median'])}" if widest else "—"])
    lines[2] += " The last column is the largest median end-of-run RSS in the cell and which arm had it."
    return lines + table(["cell", f"{control} M/s", "ranking", "largest RSS (MiB)"], table_rows), rows


def memory_section(study, dep):
    summary = analysis.memory_summary(study, dep)
    if not summary:
        return []
    arms = [arm["name"] for arm in study["arms"]]
    control = arms[0]
    by_key = defaultdict(dict)
    for row in summary:
        by_key[(row["cell"], row["depth"], row["phase"])][row["arm"]] = row["rss_median_bytes"]
    phases = ("constructed_empty", "full", "drained", "idle_1s", "refilled", "redrained", "destroyed")
    rows = []
    for (cell, depth, phase), values in sorted(by_key.items(), key=lambda kv: (kv[0][0], kv[0][1], phases.index(kv[0][2]) if kv[0][2] in phases else 99)):
        rows.append([cell, depth, phase] + [_mib(values.get(arm)) for arm in arms] +
                    [_ratio(values.get(arm), values.get(control)) for arm in arms[1:]])
    return (["### Memory: median process RSS (MiB)", "",
             "Process footprint at each lifecycle checkpoint, including worker stacks and allocator "
             "retention; a drop after `drained` means memory was returned.", ""] +
            table(["cell", "depth", "phase", *arms, *(f"{a}/{control}" for a in arms[1:])], rows))


def deep_section(study, dep, effect_rows, cluster_config):
    rows = analysis.deep_rows(dep)
    if not rows:
        return [], []
    arms = [arm["name"] for arm in study["arms"]]
    control = arms[0]
    plan = {task["id"]: task for task in analysis.plan_tasks(dep)}
    metrics_csv, lines = [], ["### Deep stage", ""]
    order = {name: index for index, name in enumerate(cluster_config.get("collectors", []))}
    by_cell = defaultdict(list)
    for row in rows:
        by_cell[row["cell"]].append(row)
    for cell, cell_rows in by_cell.items():
        reasons = next((plan[r["task"]].get("reasons") for r in cell_rows if plan[r["task"]].get("reasons")), [])
        lines += [f"#### {cell}" + (f" — {'; '.join(reasons)}" if reasons else ""), ""]
        by_collector = defaultdict(list)
        for row in cell_rows:
            by_collector[row["collector"]].append(row)
        for collector in sorted(by_collector, key=lambda c: order.get(c, 99)):
            collected = by_collector[collector]
            present = [a for a in arms if any(r["arm"] == a for r in collected)]
            pivot = defaultdict(lambda: defaultdict(list))
            notes = []
            for row in collected:
                if row["arm"] == "":
                    notes.append(f"{row['status']}: {row['task_error'][:200]}" if row["task_error"] else row["status"])
                    continue
                if row["error"]:
                    notes.append(f"{row['arm']} r{row['replicate']}: {row['error'][:200]}")
                if row.get("shadow_ops") and row.get("capture_ops"):
                    notes.append(f"{row['arm']} r{row['replicate']}: {row['shadow_ops'] / row['capture_ops']:.2f}x "
                                 "slower under the collector")
                for metric, value in analysis.flatten(collector, row["metrics"]).items():
                    pivot[metric][row["arm"]].append(value)
                    metrics_csv.append({"cell": cell, "collector": collector, "replicate": row["replicate"],
                                        "arm": row["arm"], "metric": metric, "value": value})
            lines.append(f"**{collector}**")
            lines.append("")
            ranked = [m for m in pivot if m.startswith(RANKED_PREFIXES)]
            ranked = set(sorted(ranked, key=lambda m: -max(statistics.fmean(v) for v in pivot[m].values()))[:TOP_SYMBOLS])
            table_rows = []
            for metric in pivot:
                if metric.startswith(RANKED_PREFIXES) and metric not in ranked:
                    continue
                means = {arm: statistics.fmean(values) for arm, values in pivot[metric].items()}
                table_rows.append([metric.replace("|", "\\|")] + [_num(means.get(arm)) for arm in present] +
                                  [_ratio(means.get(arm), means.get(control)) for arm in present[1:]])
            if table_rows:
                lines += table(["metric", *present, *(f"{a}/{control}" for a in present[1:])], table_rows)
            if collector in APX_RECIPES:
                names = sorted({row["metrics"].get("run_name") for row in collected if row["metrics"].get("run_name")})
                if names:
                    lines += ["Performix runs: " + ", ".join(f"`{name}`" for name in names), ""]
            if notes:
                lines += [f"- {note}" for note in notes] + [""]
    return lines, metrics_csv


def write(study, directory, clusters):
    """Write report.md and CSVs under `directory`; return (path, terminal headline)."""
    directory = Path(directory)
    lines = [f"# {study['id']}", ""]
    if study["spec"].get("question"):
        lines += [f"*{study['spec']['question']}*", ""]
    lines += arms_table(study)
    headline = []
    csvs = defaultdict(list)
    for cluster in clusters:
        dep = directory / cluster
        state = read_json_or(dep / "state.json", None)
        if state is None:
            lines += [f"## {cluster}", "", "No results here yet; run `./ab fetch`.", ""]
            headline.append(f"{cluster}: no results fetched yet (./ab fetch)")
            continue
        config = read_json_or(dep / "deploy.json", {}).get("config", {})
        counts = defaultdict(int)
        for entry in state["tasks"].values():
            counts[entry["status"]] += 1
        lines += [f"## {cluster} — {state['phase']}", "",
                  " · ".join(f"{n} {s}" for s, n in sorted(counts.items())) + " tasks", ""]
        selected = {entry["cell"] for entry in read_json_or(dep / "selection.json", {}).get("cells") or []}
        section, effects = effects_section(study, dep, selected)
        shape_lines, shape_rows, family_rows = shape_section(study, dep)
        lines += shape_lines
        lines += section
        section, best_overall, best_cells = best_section(study, effects)
        lines += section
        section, same_geometry = same_geometry_section(study, effects)
        lines += section
        section, ranking = ranking_section(study, dep, effects)
        lines += section
        section, matrix = geometry_matrix_section(study, ranking)
        lines += section
        section, batch_rows = batch_matrix_section(study, ranking)
        lines += section
        lines += memory_section(study, dep)
        section, metrics = deep_section(study, dep, effects, config)
        lines += section
        for row in analysis.throughput_runs(dep):
            csvs["throughput_runs"].append({"cluster": cluster, **row})
        for row in effects:
            csvs["effects"].append({"cluster": cluster, **{k: v for k, v in row.items() if k != "integrity"},
                                    "integrity": " ".join(row["integrity"])})
        for row in best_overall:
            csvs["best_overall"].append({"cluster": cluster, **row})
        for row in best_cells:
            csvs["best_per_cell"].append({"cluster": cluster, **row})
        for row in same_geometry:
            csvs["same_geometry"].append({"cluster": cluster, **row})
        for row in ranking:
            csvs["ranking"].append({"cluster": cluster, **row})
        for row in matrix:
            csvs["geometry_matrix"].append({"cluster": cluster, **row})
        for row in batch_rows:
            csvs["batch_matrix"].append({"cluster": cluster, **{k: v for k, v in row.items() if k != "best_batch"},
                                         "best_batch": batch_token(row["best_batch"])})
        for row in shape_rows:
            csvs["shape_matrix"].append({"cluster": cluster, **row})
        for row in family_rows:
            csvs["family_summary"].append({"cluster": cluster, **{k: v for k, v in row.items() if k != "arms"},
                                           "arms": " ".join(row["arms"])})
        for row in analysis.memory_rows(dep):
            csvs["memory"].append({"cluster": cluster, **row})
        for row in metrics:
            csvs["deep_metrics"].append({"cluster": cluster, **row})
        headline.append(f"{cluster} ({state['phase']}):")
        for row in family_rows:
            # Absolute numbers per family and batch, the way the shape tables read.
            against = ""
            if row["ratio_to_reference"]:
                against = (f"{row['ratio_to_reference']:.2f}x {row['reference']}, ahead "
                           f"{row['cells_ahead']}/{row['cells_compared']}")
            headline.append(f"  {batch_title(batch_of(row)):<20} {row['family']:<16} {_mops(row['ops_geomean']):>9} M/s  "
                            f"fastest {row['cells_fastest']}/{row['cells']:<3} {against}")
        shown = effects
        if len(study["arms"]) > 3:
            # Many arms: the family bests overall, then each family's best per cell.
            for row in best_overall:
                if row["cells"] is not None:
                    headline.append(f"  {row['family']:<18} best {row['arm']:<24} {row['geomean_effect']:.3f}x "
                                    f"over {row['cells']} cells ({row['wins']} wins, {row['losses']} losses)")
            shown = [row for per_cell in analysis.best_per_cell(study, effects).values() for row in per_cell.values()]
        for row in sorted(shown, key=lambda r: -(r["effect"] or 0)):
            effect = f"{row['effect']:.3f}x" if row["effect"] is not None else "—"
            ci = f"[{row['ci_low']:.3f}, {row['ci_high']:.3f}]" if row["ci_low"] is not None else ""
            headline.append(f"  {('◆' if row['cell'] in selected else ' ')} {row['candidate']:<24} "
                            f"{row['cell']:<16} {effect:>8} {ci:<17} {row['verdict']}")
    path = directory / "report.md"
    path.write_text("\n".join(lines) + "\n")
    (directory / "csv").mkdir(exist_ok=True)
    for name, rows in csvs.items():
        write_csv(directory / "csv" / f"{name}.csv", rows)
    return path, headline
