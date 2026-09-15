"""Figures for a fetched study, in absolute units and with every series named.

- `scaling`: throughput against thread count on the producer, consumer and
  balanced axes per batch size, one line per family at its best variation
  (geometry and backoff) for that batch, named in a legend inside each panel,
  so a family that wants small VBlocks at scalar and big blocks at batch 4096
  is drawn at each.
- `geometry-<family>[-<backoff>]`: throughput against slots per VBlock for
  representative shapes, one line per VBlock count, so the geometry matrix
  is read directly; one figure per backoff when the study compares several.
- `geometry-matrix`: per family (and backoff) and batch size, every
  geometry's throughput as a share of the batch's best arm, over all cells.
- `memory-matrix`: the same grid with end-of-run RSS, worst and typical cell.
- `layout-matrix`: each other family over the control's family at equal
  geometry and backoff, from the paired effects.
- `cells`: every arm in every cell as a share of that cell's best arm.
- `batch-matrix`: per arm and shape, a producer-batch x consumer-batch grid
  of absolute throughput, coloured by its share of the arm's best pair
  there, when the study varies the batch pair.

Nothing is scaled by the control arm: a control is the pairing reference
for the statistics, not a meaningful baseline for a reader. matplotlib is
imported lazily; scripts/plot_style.py supplies family colours when present.
"""
from __future__ import annotations

import math
import re
import sys
from collections import defaultdict
from pathlib import Path

from . import analysis
from .common import batch_of, batch_order, batch_title, read_json_or

SCENARIO = re.compile(r"^(\d+)p(\d+)c$")
AXES = ("producers", "consumers", "balanced")
AXIS_LABELS = {"producers": "producers (one consumer)", "consumers": "consumers (one producer)",
               "balanced": "producers = consumers"}
MARKERS = ("o", "d", "s", "^", "v", "P", "X", "<", ">", "*")
SI = ((1e12, "T"), (1e9, "B"), (1e6, "M"), (1e3, "K"))
BYTES = ((2 ** 40, "TiB"), (2 ** 30, "GiB"), (2 ** 20, "MiB"), (2 ** 10, "KiB"))
SEQUENTIAL_CMAP = "viridis"


def style_module():
    """scripts/plot_style.py, so a queue family looks the same in every figure."""
    scripts = Path(__file__).resolve().parents[2] / "scripts"
    if str(scripts) not in sys.path:
        sys.path.insert(0, str(scripts))
    try:
        import plot_style
        return plot_style
    except ImportError:
        return None


def si(value, unit=""):
    """1.2e10 -> '12B', 3.5e8 -> '350M'; `unit` follows the suffix."""
    for scale, suffix in SI:
        if abs(value) >= scale:
            return f"{value / scale:.3g}{suffix}{unit}"
    return f"{value:.3g}{unit}"


def si_bytes(value):
    """8.2e9 -> '7.6 GiB'."""
    for scale, suffix in BYTES:
        if value >= scale:
            return f"{value / scale:.2g} {suffix}" if value / scale < 10 else f"{value / scale:.0f} {suffix}"
    return f"{value:.0f} B"


def log_ticks(ax, axis="y", formatter=si):
    """Label a log axis with plain numbers (10B, 2B, 500M), dense enough to read a narrow range."""
    from matplotlib.ticker import FixedLocator, FuncFormatter, NullFormatter, NullLocator
    target = ax.yaxis if axis == "y" else ax.xaxis
    low, high = ax.get_ylim() if axis == "y" else ax.get_xlim()
    if low <= 0 or high <= low:
        return
    decades = range(math.floor(math.log10(low)), math.ceil(math.log10(high)) + 1)
    for subs in ((1,), (1, 2, 5), (1, 1.5, 2, 3, 4, 5, 6, 8)):
        ticks = [m * 10 ** d for d in decades for m in subs if low <= m * 10 ** d <= high]
        if len(ticks) >= 3:
            break
    target.set_major_locator(FixedLocator(ticks))
    target.set_major_formatter(FuncFormatter(lambda v, _: formatter(v)))
    target.set_minor_locator(NullLocator())
    target.set_minor_formatter(NullFormatter())


batch_label = batch_title


def axes_of(scenario):
    """The scaling axes a PpCc shape lies on, with its thread count on each."""
    match = SCENARIO.match(scenario)
    if not match:
        return []
    producers, consumers = int(match.group(1)), int(match.group(2))
    found = []
    if consumers == 1:
        found.append(("producers", producers))
    if producers == 1:
        found.append(("consumers", consumers))
    if producers == consumers:
        found.append(("balanced", producers))
    return found


def cell_order(row):
    match = SCENARIO.match(row["scenario"])
    producers, consumers = (int(match.group(1)), int(match.group(2))) if match else (0, 0)
    axis = next((AXES.index(a) for a, _ in axes_of(row["scenario"])), len(AXES))
    return (batch_order(batch_of(row)), axis, producers + consumers, producers)


def geometry_label(arm):
    slots, vblocks = analysis._geometry(arm["workload"]["queue"])
    if slots is None:
        return ""
    return f"s{slots}" + (f"-v{vblocks}" if vblocks is not None else "")


class Palette:
    def __init__(self, families):
        self.style = style_module()
        self.families = list(families)

    def color(self, family):
        index = self.families.index(family)
        if self.style:
            return self.style.family_color(family, index)
        return f"C{index}"

    def marker(self, family):
        index = self.families.index(family)
        if self.style:
            return self.style.family_marker(family, index)
        return MARKERS[index % len(MARKERS)]

    def ramp(self, family, position):
        """A light-to-dark shade of the family's colour for an ordered series (VBlock counts)."""
        if self.style:
            return self.style.family_color_ramp(family, self.families.index(family))(position)
        from matplotlib.colors import to_rgb
        base = to_rgb(self.color(family))
        light = tuple(c + (1 - c) * 0.5 for c in base)
        return tuple(l + (b - l) * position for l, b in zip(light, base))

    def diverging(self):
        return self.style.ACCESSIBLE_DIVERGING_CMAP if self.style else "PuOr"


def finish(fig, title, handles, labels):
    """Title above, one shared legend below the panels, nothing overlapping."""
    if handles:
        fig.legend(handles, labels, loc="lower center", ncol=min(4, len(labels)), fontsize=8,
                   bbox_to_anchor=(0.5, 0.0), frameon=False)
    fig.suptitle(title, fontsize=11)
    fig.tight_layout(rect=(0, 0.05 if handles else 0, 1, 0.96))


def best_per_batch(matrix_rows):
    """{(batch, family): arm name}: each family's best variation at each batch
    pair, from the geometry matrix: the arm whose throughput over the batch's
    cells is the largest share of the batch's best arm (a geometric mean over
    the cells both ran), so the choice matches the geometry matrix's rings."""
    best = {}
    for row in matrix_rows:
        key = (batch_of(row), row["family"])
        score = (row["share_of_best"] or 0.0, row["ops_geomean"] or 0.0)
        if key not in best or score > best[key][0]:
            best[key] = (score, row["arm"])
    return {key: arm for key, (_, arm) in best.items()}


def series_label(study, family, arm):
    """`ubq-v8 (s4096-v128, yield)`: the family and what sets the arm apart in
    it, or just the family when it has one arm."""
    variation = analysis.variation_of(study, arm)
    single = len(analysis.families(study)[family]) == 1
    return family if single or not variation else f"{family} ({variation})"


def matrix_groups(study, families):
    """Heatmap rows: (family, None), or one (family, backoff) per policy the
    family ran when the study compares several backoffs."""
    policies = analysis.backoffs(study)
    if len(policies) == 1:
        return [(family, None) for family in families]
    return [(family, backoff) for family in families for backoff in policies
            if any(analysis.family_of(a) == family and analysis.backoff_of(a) == backoff for a in study["arms"])]


def group_label(family, backoff):
    return family if backoff is None else f"{family} ({backoff})"


def batch_groups(rows):
    """The scaling figure's rows: one per batch pair, except that an uneven pair and
    its mirror share a row when no shape measures both, as with the skewed pairs of
    the default matrix (push 16 / pop 256 at producer-heavy shapes, the mirror at
    consumer-heavy ones): each then fills its own axis of the row."""
    shapes = defaultdict(set)
    for row in rows:
        shapes[batch_of(row)].add(row["scenario"])
    groups, placed = [], set()
    for batch in sorted(shapes, key=batch_order):
        if batch in placed:
            continue
        group = [batch]
        mirror = batch[::-1]
        if mirror != batch and mirror in shapes and not shapes[batch] & shapes[mirror]:
            group.append(mirror)
        placed.update(group)
        groups.append(group)
    return groups


def scaling_figure(plt, rows, study, matrix_rows, families, palette, title):
    """Throughput against thread count per axis and batch, one line per family
    at its best variation for that batch. A family with several arms gets a
    legend in every panel naming the variation drawn there; a study whose
    families have one arm each gets one shared legend instead."""
    arms = {arm["name"]: arm for arm in study["arms"]}
    chosen = best_per_batch(matrix_rows)
    varied = any(len(names) > 1 for names in analysis.families(study).values())
    groups = batch_groups(rows)
    fig, axes = plt.subplots(len(groups), len(AXES), figsize=(5 * len(AXES), 3.6 * len(groups)),
                             squeeze=False, sharey="row")
    for i, group in enumerate(groups):
        for j, axis in enumerate(AXES):
            ax = axes[i][j]
            series = []
            for family in families:
                for batch in group:
                    arm = chosen.get((batch, family))
                    points = {}
                    for row in rows:
                        if arm is None or batch_of(row) != batch or row["arm"] != arm:
                            continue
                        for name, threads in axes_of(row["scenario"]):
                            if name == axis:
                                points[threads] = row["ops_median"]
                    if points:
                        series.append((family, batch, arm, points))
            if not series:
                ax.set_visible(False)
                continue
            present = sorted({batch for _, batch, _, _ in series}, key=batch_order)
            for family, batch, arm, points in series:
                label = series_label(study, family, arms[arm])
                if len(present) > 1:
                    label += f", {batch_label(batch)}"
                xs = sorted(points)
                ax.plot(xs, [points[x] for x in xs], marker=palette.marker(family), color=palette.color(family),
                        label=label, linewidth=1.6, markersize=5)
            ax.set_xscale("log", base=2)
            ax.set_yscale("log")
            ax.grid(True, which="major", alpha=0.3)
            ax.set_title(f"{AXIS_LABELS[axis]}, {', '.join(batch_label(b) for b in present)}", fontsize=10)
            if varied:
                ax.legend(loc="best", fontsize=7, framealpha=0.85, handlelength=1.6, borderpad=0.4,
                          labelspacing=0.3, borderaxespad=0.4)
            if j == 0:
                ax.set_ylabel("items/s")
        for ax in axes[i]:
            if ax.get_visible():
                log_ticks(ax)
    for j in range(len(AXES)):
        visible = [axes[i][j] for i in range(len(groups)) if axes[i][j].get_visible()]
        if visible:
            visible[-1].set_xlabel("threads on this axis")
    handles, labels = {}, []
    if not varied:
        for ax in fig.axes:
            for handle, label in zip(*ax.get_legend_handles_labels()):
                if label not in handles:
                    handles[label] = handle
                    labels.append(label)
    finish(fig, title, [handles[l] for l in labels], labels)
    return fig


def representative_scenarios(rows):
    """1p1c, the widest shape on each axis, a mid-size balanced shape, and the asymmetric ones."""
    shapes = {}
    for row in rows:
        match = SCENARIO.match(row["scenario"])
        if match:
            shapes[row["scenario"]] = (int(match.group(1)), int(match.group(2)))
    chosen = []
    for axis in AXES:
        on_axis = [s for s in shapes if any(a == axis for a, _ in axes_of(s))]
        if on_axis:
            chosen.append(max(on_axis, key=lambda s: shapes[s]))
    balanced = sorted((s for s in shapes if shapes[s][0] == shapes[s][1]), key=lambda s: shapes[s])
    if len(balanced) > 2:
        chosen.append(balanced[len(balanced) // 2])
    chosen += [s for s in shapes if not axes_of(s)]
    ordered = ["1p1c"] if "1p1c" in shapes else []
    return ordered + [s for s in dict.fromkeys(chosen) if s not in ordered]


def geometries(study, family, backoff=None):
    """{arm name: (slots, vblocks)} for the family's arms that name a geometry,
    at one backoff policy when given."""
    found = {}
    for arm in study["arms"]:
        if analysis.family_of(arm) == family and (backoff is None or analysis.backoff_of(arm) == backoff):
            slots, vblocks = analysis._geometry(arm["workload"]["queue"])
            if slots is not None:
                found[arm["name"]] = (slots, vblocks)
    return found


def vblock_shades(palette, family, counts):
    ordered = sorted(counts, key=lambda v: (v is None, v or 0))
    return {v: palette.ramp(family, i / max(1, len(ordered) - 1)) for i, v in enumerate(ordered)}


def geometry_figure(plt, rows, study, family, backoff, palette, title):
    """One family (at one backoff when the study compares several): throughput
    against slots per VBlock, one line per VBlock count, per shape and batch."""
    geometry = geometries(study, family, backoff)
    if len(geometry) < 2:
        return None
    scenarios = representative_scenarios(rows)
    batches = sorted({batch_of(row) for row in rows}, key=batch_order)
    shades = vblock_shades(palette, family, {v for _, v in geometry.values()})
    fig, axes = plt.subplots(len(batches), len(scenarios), figsize=(3.6 * len(scenarios), 3.3 * len(batches)),
                             squeeze=False, sharey="row")
    for i, batch in enumerate(batches):
        for j, scenario in enumerate(scenarios):
            ax = axes[i][j]
            in_cell = {r["arm"]: r for r in rows if batch_of(r) == batch and r["scenario"] == scenario
                       and r["arm"] in geometry}
            for vblocks, shade in shades.items():
                names = sorted((n for n in in_cell if geometry[n][1] == vblocks), key=lambda n: geometry[n][0])
                if names:
                    ax.plot([geometry[n][0] for n in names], [in_cell[n]["ops_median"] for n in names],
                            marker=palette.marker(family), color=shade, markersize=4, linewidth=1.4,
                            label="default VBlocks" if vblocks is None else f"{vblocks} VBlocks per block")
            if in_cell:
                best = max(in_cell.values(), key=lambda r: r["ops_median"])
                ax.plot([geometry[best["arm"]][0]], [best["ops_median"]], marker=palette.marker(family),
                        color=palette.color(family), markersize=11, markerfacecolor="none", markeredgewidth=1.8)
            ax.set_xscale("log", base=2)
            ax.set_yscale("log")
            ax.grid(True, which="major", alpha=0.3)
            ax.set_title(f"{scenario}, {batch_label(batch)}", fontsize=10)
            if i == len(batches) - 1:
                ax.set_xlabel("slots per VBlock")
            if j == 0:
                ax.set_ylabel("items/s")
        for ax in axes[i]:
            log_ticks(ax)
    handles = {}
    for ax in fig.axes:
        for handle, label in zip(*ax.get_legend_handles_labels()):
            handles.setdefault(label, handle)
    finish(fig, f"{title}: {group_label(family, backoff)} throughput by geometry (ring: fastest geometry in the cell)",
           list(handles.values()), list(handles))
    return fig


def matrix_axes(plt, families, batches, width=3.4, height=3.1):
    """A grid of heatmaps that leaves room for two-line titles and one shared colour bar."""
    fig, axes = plt.subplots(len(families), len(batches), figsize=(width * len(batches) + 1.2, height * len(families) + 0.9),
                             squeeze=False, constrained_layout=True)
    return fig, axes


def draw_matrix(ax, grid, slots, vblocks, values, labels, cmap, vmin, vmax, ring=None):
    """A (VBlocks x slots) heatmap with text in every cell; `ring` outlines one (slots, vblocks)."""
    import numpy as np
    from matplotlib.patches import Rectangle
    matrix = np.full((len(vblocks), len(slots)), np.nan)
    for (s, v), value in values.items():
        matrix[vblocks.index(v)][slots.index(s)] = value
    image = ax.imshow(matrix, cmap=cmap, vmin=vmin, vmax=vmax, aspect="auto", origin="lower")
    for (s, v), text in labels.items():
        value = values[(s, v)]
        position = (value - vmin) / (vmax - vmin) if vmax > vmin else 0.5
        luminance = sum(w * c for w, c in zip((0.299, 0.587, 0.114), image.cmap(image.norm(value))[:3]))
        ax.text(slots.index(s), vblocks.index(v), text, ha="center", va="center", fontsize=7,
                color="white" if luminance < 0.5 else "black")
    if ring in values:
        ax.add_patch(Rectangle((slots.index(ring[0]) - 0.5, vblocks.index(ring[1]) - 0.5), 1, 1,
                               fill=False, edgecolor="black", linewidth=2))
    ax.set_xticks(range(len(slots)))
    ax.set_xticklabels([str(s) for s in slots], fontsize=8)
    ax.set_yticks(range(len(vblocks)))
    ax.set_yticklabels(["default" if v is None else str(v) for v in vblocks], fontsize=8)
    return image


def matrix_layout(study, family, backoff=None):
    geometry = geometries(study, family, backoff)
    slots = sorted({s for s, _ in geometry.values()})
    vblocks = sorted({v for _, v in geometry.values()}, key=lambda v: (v is None, v or 0))
    return geometry, slots, vblocks


def geometry_matrix_figure(plt, matrix_rows, study, families, palette, title):
    """Per family and batch size: each geometry's throughput as a share of the batch's best arm."""
    groups = [(f, b) for f, b in matrix_groups(study, families) if len(geometries(study, f, b)) >= 2]
    batches = sorted({batch_of(row) for row in matrix_rows}, key=batch_order)
    if not groups or not batches:
        return None
    fig, axes = matrix_axes(plt, groups, batches)
    image = None
    for i, (family, backoff) in enumerate(groups):
        geometry, slots, vblocks = matrix_layout(study, family, backoff)
        for j, batch in enumerate(batches):
            ax = axes[i][j]
            rows = [r for r in matrix_rows if r["family"] == family and batch_of(r) == batch and r["arm"] in geometry]
            values = {geometry[r["arm"]]: r["share_of_best"] for r in rows if r["share_of_best"]}
            labels = {g: f"{100 * v:.0f}%" for g, v in values.items()}
            best = max(values, key=values.get) if values else None
            image = draw_matrix(ax, None, slots, vblocks, values, labels, SEQUENTIAL_CMAP, 0.0, 1.0, ring=best)
            reference = next((r["best_arm"] for r in rows), None)
            note = f"100% = {reference}" if reference else ""
            ax.set_title(f"{group_label(family, backoff)}, {batch_label(batch)}\n{note}", fontsize=8)
            if i == len(groups) - 1:
                ax.set_xlabel("slots per VBlock", fontsize=8)
            if j == 0:
                ax.set_ylabel("VBlocks per block", fontsize=8)
    if image is not None:
        bar = fig.colorbar(image, ax=axes.ravel().tolist(), fraction=0.02, pad=0.02)
        bar.set_label("geomean over cells of throughput / the batch's best arm")
    fig.suptitle(f"{title}: geometry matrix (ring: the family's best geometry)", fontsize=11)
    return fig


def memory_matrix_figure(plt, matrix_rows, study, families, palette, title):
    """Per family and batch size: worst and typical end-of-run RSS of every geometry over the cells."""
    import numpy as np
    groups = [(f, b) for f, b in matrix_groups(study, families) if len(geometries(study, f, b)) >= 2]
    batches = sorted({batch_of(row) for row in matrix_rows}, key=batch_order)
    rows = [r for r in matrix_rows if r["rss_max"] and any(r["family"] == f for f, _ in groups)]
    if not rows or not batches:
        return None
    fig, axes = matrix_axes(plt, groups, batches)
    lows = [math.log2(r["rss_geomean"]) for r in rows]
    highs = [math.log2(r["rss_max"]) for r in rows]
    vmin, vmax = min(lows), max(highs)
    image = None
    for i, (family, backoff) in enumerate(groups):
        geometry, slots, vblocks = matrix_layout(study, family, backoff)
        for j, batch in enumerate(batches):
            ax = axes[i][j]
            mine = {geometry[r["arm"]]: r for r in rows if r["family"] == family and batch_of(r) == batch
                    and r["arm"] in geometry}
            values = {g: math.log2(r["rss_max"]) for g, r in mine.items()}
            labels = {g: f"{si_bytes(r['rss_max'])}\n{si_bytes(r['rss_geomean'])}" for g, r in mine.items()}
            image = draw_matrix(ax, None, slots, vblocks, values, labels, SEQUENTIAL_CMAP + "_r", vmin, vmax)
            ax.set_title(f"{group_label(family, backoff)}, {batch_label(batch)}", fontsize=8)
            if i == len(groups) - 1:
                ax.set_xlabel("slots per VBlock", fontsize=8)
            if j == 0:
                ax.set_ylabel("VBlocks per block", fontsize=8)
    if image is not None:
        bar = fig.colorbar(image, ax=axes.ravel().tolist(), fraction=0.02, pad=0.02)
        ticks = [t for t in range(math.floor(vmin), math.ceil(vmax) + 1) if vmin <= t <= vmax][::2]
        bar.set_ticks(ticks)
        bar.set_ticklabels([si_bytes(2 ** t) for t in ticks])
        bar.set_label("largest median end-of-run RSS over the cells")
    fig.suptitle(f"{title}: memory matrix (top: largest cell, bottom: typical cell)", fontsize=11)
    return fig


def layout_matrix_figure(plt, effect_rows, study, families, palette, title):
    """Each other family over the control's family at equal geometry, per batch size, from paired effects."""
    same = analysis.same_geometry(study, effect_rows)
    if not same:
        return None
    groups = [(f, b) for f, b in matrix_groups(study, families)
              if any(r["family"] == f and (b is None or r["backoff"] == b) for r in same)]
    batches = list(dict.fromkeys(b for row in same for b in row["batches"]))
    fig, axes = matrix_axes(plt, groups, batches)
    spread = max(abs(math.log2(r["batches"][b]["geomean_ratio"])) for r in same for b in r["batches"]) or 0.1
    image = None
    for i, (family, backoff) in enumerate(groups):
        geometry, slots, vblocks = matrix_layout(study, family, backoff)
        for j, batch in enumerate(batches):
            ax = axes[i][j]
            values, labels = {}, {}
            for row in same:
                if row["family"] == family and batch in row["batches"] and row["arm"] in geometry:
                    ratio = row["batches"][batch]["geomean_ratio"]
                    values[geometry[row["arm"]]] = math.log2(ratio)
                    labels[geometry[row["arm"]]] = f"{ratio:.2f}x"
            image = draw_matrix(ax, None, slots, vblocks, values, labels, palette.diverging(), -spread, spread)
            ax.set_title(f"{group_label(family, backoff)} / {same[0]['reference']}, {batch_label(batch)}", fontsize=8)
            if i == len(groups) - 1:
                ax.set_xlabel("slots per VBlock", fontsize=8)
            if j == 0:
                ax.set_ylabel("VBlocks per block", fontsize=8)
    if image is not None:
        bar = fig.colorbar(image, ax=axes.ravel().tolist(), fraction=0.02, pad=0.02)
        bar.set_label("throughput ratio at equal geometry (paired, geomean over cells)")
        ticks = [t for t in (-2, -1, -0.5, 0, 0.5, 1, 2) if -spread <= t <= spread] or [0]
        bar.set_ticks(ticks)
        bar.set_ticklabels([f"{2 ** t:.2g}x" for t in ticks])
    fig.suptitle(f"{title}: layouts at equal geometry", fontsize=11)
    return fig


def cells_figure(plt, ranking_rows, study, palette, title):
    """Every arm in every cell as a share of the cell's fastest arm; ◆ marks each family's best there."""
    import numpy as np
    names = [arm["name"] for arm in study["arms"]]
    cells = [row["cell"] for row in sorted({r["cell"]: r for r in ranking_rows}.values(), key=cell_order)]
    if not cells:
        return None
    index = {cell: i for i, cell in enumerate(cells)}
    matrix = np.full((len(names), len(cells)), np.nan)
    for row in ranking_rows:
        if row["share_of_best"] is not None:
            matrix[names.index(row["arm"])][index[row["cell"]]] = row["share_of_best"]
    floor = np.nanmin(matrix) if np.isfinite(matrix).any() else 0.0
    fig, ax = plt.subplots(figsize=(max(6.0, 0.32 * len(cells) + 3), max(3.0, 0.32 * len(names) + 2)))
    image = ax.imshow(matrix, cmap=SEQUENTIAL_CMAP, vmin=floor, vmax=1.0, aspect="auto")
    ax.set_xticks(range(len(cells)))
    ax.set_xticklabels(cells, rotation=90, fontsize=7)
    ax.set_yticks(range(len(names)))
    ax.set_yticklabels(names, fontsize=8)
    for row in ranking_rows:
        if row["best_in_family"]:
            ax.text(index[row["cell"]], names.index(row["arm"]), "◆", ha="center", va="center", fontsize=6,
                    color="white" if (row["share_of_best"] or 1) < (floor + 1) / 2 else "black")
    bar = fig.colorbar(image, ax=ax, fraction=0.03, pad=0.02)
    bar.set_label("throughput / the cell's fastest arm")
    ax.set_title(f"{title}: every arm in every cell (◆ the family's fastest there)", fontsize=10)
    fig.tight_layout()
    return fig


def batch_matrix_figure(plt, batch_rows, study, palette, title):
    """Per arm (rows) and shape (columns): producer batch x consumer batch, each
    cell annotated with absolute throughput and coloured by its share of the
    arm's fastest pair at that shape (ringed)."""
    import numpy as np
    if len({batch_of(r) for r in batch_rows}) <= 1:
        return None
    arms = [arm["name"] for arm in study["arms"] if any(r["arm"] == arm["name"] for r in batch_rows)]
    scenarios = sorted({r["scenario"] for r in batch_rows},
                       key=lambda s: cell_order({"scenario": s, "producer_batch": None, "consumer_batch": None}))
    producers = sorted({r["producer_batch"] or 1 for r in batch_rows})
    consumers = sorted({r["consumer_batch"] or 1 for r in batch_rows})
    floor = min(r["share_of_best"] for r in batch_rows)
    fig, axes = plt.subplots(len(arms), len(scenarios),
                             figsize=(0.6 * len(consumers) * len(scenarios) + 3, 0.55 * len(producers) * len(arms) + 2.2),
                             squeeze=False, constrained_layout=True)
    image = None
    for i, arm in enumerate(arms):
        for j, scenario in enumerate(scenarios):
            ax = axes[i][j]
            grid = np.full((len(producers), len(consumers)), np.nan)
            here = [r for r in batch_rows if r["arm"] == arm and r["scenario"] == scenario]
            for r in here:
                grid[producers.index(r["producer_batch"] or 1)][consumers.index(r["consumer_batch"] or 1)] = r["share_of_best"]
            image = ax.imshow(grid, cmap=SEQUENTIAL_CMAP, vmin=floor, vmax=1.0, aspect="auto")
            for r in here:
                y, x = producers.index(r["producer_batch"] or 1), consumers.index(r["consumer_batch"] or 1)
                dark = r["share_of_best"] < (floor + 1) / 2
                ax.text(x, y, si(r["ops_median"]), ha="center", va="center", fontsize=6,
                        color="white" if dark else "black")
                if batch_of(r) == r["best_batch"]:
                    ax.add_patch(plt.Circle((x, y), 0.42, fill=False, color="red", linewidth=1.2))
            ax.set_xticks(range(len(consumers)))
            ax.set_xticklabels(consumers if i == len(arms) - 1 else [], fontsize=7)
            ax.set_yticks(range(len(producers)))
            ax.set_yticklabels(producers if j == 0 else [], fontsize=7)
            ax.set_title(f"{arm}, {scenario}", fontsize=8, pad=4)
            if i == len(arms) - 1:
                ax.set_xlabel("pop batch (1 = scalar)", fontsize=7)
            if j == 0:
                ax.set_ylabel("push batch (1 = scalar)", fontsize=7)
    if image is not None:
        bar = fig.colorbar(image, ax=axes, fraction=0.03, pad=0.02, shrink=0.8)
        bar.set_label("throughput / the arm's\nfastest pair at this shape", fontsize=8)
    fig.suptitle(f"{title}: items/s per push and pop batch (ring: the fastest pair)", fontsize=10)
    return fig


def write(study, directory, clusters):
    """Write the figures for every fetched cluster under <directory>/plots; return their paths."""
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    directory = Path(directory)
    out = directory / "plots"
    out.mkdir(exist_ok=True)
    families = list(analysis.families(study))
    palette = Palette(families)
    written = []
    for cluster in clusters:
        dep = directory / cluster
        if read_json_or(dep / "state.json", None) is None:
            continue
        effect_rows = analysis.effects(study, dep)
        rows = analysis.ranking(study, dep, effect_rows)
        if not rows:
            continue
        matrix_rows = analysis.geometry_matrix(study, rows)
        title = f"{study['id']} on {cluster}"
        figures = [("scaling", scaling_figure(plt, rows, study, matrix_rows, families, palette,
                                              f"{title}: each family at its best variation per batch"))]
        if len(study["arms"]) > 2:
            for family, backoff in matrix_groups(study, families):
                name = f"geometry-{family}" + (f"-{backoff}" if backoff else "")
                figures.append((name, geometry_figure(plt, rows, study, family, backoff, palette, title)))
            figures.append(("geometry-matrix", geometry_matrix_figure(plt, matrix_rows, study, families, palette, title)))
            figures.append(("memory-matrix", memory_matrix_figure(plt, matrix_rows, study, families, palette, title)))
            figures.append(("layout-matrix", layout_matrix_figure(plt, effect_rows, study, families, palette, title)))
            figures.append(("cells", cells_figure(plt, rows, study, palette, title)))
        figures.append(("batch-matrix", batch_matrix_figure(plt, analysis.batch_matrix(study, rows), study, palette, title)))
        for name, figure in figures:
            if figure is None:
                continue
            path = out / f"{cluster}-{name}.png"
            figure.savefig(path, dpi=140, bbox_inches="tight")
            plt.close(figure)
            written.append(path)
    return written
