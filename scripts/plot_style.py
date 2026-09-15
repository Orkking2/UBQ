"""Shared, colorblind-safe visual identities for UBQ plots.

Colors use the Okabe-Ito palette, extended with a few Tol-style dark tones for
dense plots. Queue identity is also encoded with a marker, line pattern, or
hatch so no categorical comparison relies on color alone. Keep semantic
assignments here rather than choosing colors by series position: a queue
should look the same in every figure.
"""

from __future__ import annotations

from collections.abc import Callable
import re
from pathlib import Path


OKABE_ITO = {
    "black": "#000000",
    "orange": "#E69F00",
    "sky_blue": "#56B4E9",
    "bluish_green": "#009E73",
    "yellow": "#F0E442",
    "blue": "#0072B2",
    "vermillion": "#D55E00",
    "reddish_purple": "#CC79A7",
}

# The first five entries are the families that normally share the dense
# batch-comparison plots.  Their base colors are deliberately well separated;
# LUBQ (vermillion) and SegQueue (bluish green) also use different line styles.
QUEUE_FAMILY_COLORS = {
    "UBQ": OKABE_ITO["blue"],
    # Chosen with the dataviz palette validator against every strict family it
    # shares panels with: worst CVD delta E 10.3 (Mutex+VecDeque gray), 22.6
    # from UBQ; its light-to-dark ramp passes the ordinal checks, which the v8
    # geometry sweep uses.
    "UBQ v8": "#A21C10",
    "LUBQ": OKABE_ITO["vermillion"],
    "SegQueue": OKABE_ITO["bluish_green"],
    "moodycamel::CQ": "#882255",
    "Mutex+VecDeque": "#666666",
    "concurrent-queue": "#A88900",
    "RBBQ/BBQ": OKABE_ITO["reddish_purple"],
    "LSCQ": OKABE_ITO["sky_blue"],
    "wCQ": "#117733",
    "MS-Queue": "#332288",
    "Naive FAA": "#AA4499",
    "Disruptor": OKABE_ITO["orange"],
    "io_uring": "#666666",
}

_FAMILY_ALIASES = {
    "ubq": "UBQ",
    "UBQ batched": "UBQ",
    "lubq": "LUBQ",
    "lubq-yield": "LUBQ",
    "LUBQ-yield": "LUBQ",
    "LUBQ batched": "LUBQ",
    "segqueue": "SegQueue",
    "crossbeam SegQueue": "SegQueue",
    "crossbeam SegQueue batched": "SegQueue",
    "moodycamel-cq": "moodycamel::CQ",
    "moodycamel-cq batched": "moodycamel::CQ",
    "mutex-vecdeque": "Mutex+VecDeque",
    "mutex-vecdeque batched": "Mutex+VecDeque",
    "bbq": "RBBQ/BBQ",
    "rbbq": "RBBQ/BBQ",
    "RBBQ": "RBBQ/BBQ",
    "fastfifo": "RBBQ/BBQ",
    "lfqueue": "LSCQ",
    "ms-queue": "MS-Queue",
    "naive-faa-queue": "Naive FAA",
    "disruptor": "Disruptor",
}

_FALLBACK_COLORS = (
    OKABE_ITO["blue"],
    OKABE_ITO["vermillion"],
    OKABE_ITO["bluish_green"],
    OKABE_ITO["reddish_purple"],
    OKABE_ITO["orange"],
    OKABE_ITO["sky_blue"],
    "#666666",
    "#7A6F00",
)

_FAMILY_MARKERS = {
    "UBQ": "o",
    "UBQ v8": "d",
    "LUBQ": "s",
    "SegQueue": "^",
    "moodycamel::CQ": "D",
    "Mutex+VecDeque": "v",
    "concurrent-queue": "P",
    "RBBQ/BBQ": "X",
    "LSCQ": "<",
    "wCQ": ">",
    "MS-Queue": "h",
    "Naive FAA": "*",
    "Disruptor": "p",
    "io_uring": "8",
}

_FAMILY_LINESTYLES = {
    "UBQ": "-",
    "UBQ v8": "-",
    "LUBQ": (0, (7, 2)),
    "SegQueue": (0, (1, 1.5)),
    "moodycamel::CQ": (0, (5, 1.5, 1, 1.5)),
    "Mutex+VecDeque": (0, (3, 1.5, 1, 1.5)),
    "concurrent-queue": (0, (5, 2)),
    "RBBQ/BBQ": (0, (3, 1)),
    "LSCQ": (0, (5, 1, 1, 1)),
    "wCQ": (0, (1, 2)),
}

_FAMILY_HATCHES = {
    "UBQ": "",
    "UBQ v8": "/.",
    "LUBQ": "//",
    "SegQueue": "xx",
    "moodycamel::CQ": "\\\\",
    "Mutex+VecDeque": "..",
    "concurrent-queue": "++",
    "RBBQ/BBQ": "oo",
    "LSCQ": "--",
    "wCQ": "**",
    "Disruptor": "||",
    "io_uring": "..",
}

# Purple-orange is a color-vision-deficiency-friendly diverging map.  Numeric
# cell labels remain the authoritative encoding in heatmaps.
ACCESSIBLE_DIVERGING_CMAP = "PuOr"

# Ordering guarantee per family. Strict queues deliver every item in one total
# order across all producers. Relaxed queues keep only each producer's own
# order, because items sit in per-producer shards or subqueues. Relaxed queues
# gain throughput by giving up that global order, so the two classes are
# plotted and ranked separately, never on the same axes. Crossbeam's
# BatchQueue (the batched SegQueue config) has no documented contract; it
# counts as strict because, like UBQ, all items pass through one shared
# producer head and one consumer head. Disruptor and io_uring are not general
# MPMC queues and stay unclassified.
FIFO_ORDERING = {
    "UBQ": "strict",
    "UBQ v8": "strict",
    "SegQueue": "strict",
    "RBBQ/BBQ": "strict",
    "LSCQ": "strict",
    "wCQ": "strict",
    "MS-Queue": "strict",
    "Mutex+VecDeque": "strict",
    "concurrent-queue": "strict",
    "Naive FAA": "strict",
    "LUBQ": "relaxed",
    "moodycamel::CQ": "relaxed",
}


def canonical_family(family: str) -> str:
    # Queue names carry v8's VBlock geometry (`ubq-v8-s4096-v32`); a fork
    # registered under its own name is its own family with its own colour.
    if family == "ubq-v8" or re.match(r"ubq-v8-s\d+(?:-v\d+)?$", family):
        return "UBQ v8"
    return _FAMILY_ALIASES.get(family, family)


def fifo_ordering(family: str) -> str:
    """Return "strict" or "relaxed"; unclassified families raise KeyError.

    Raising forces each new queue to be classified deliberately. A relaxed
    derivative of v8, built the way LUBQ is derived from UBQ, belongs under
    "relaxed".
    """
    registry = Path(__file__).resolve().parents[1] / "src/queue_variants.tsv"
    if registry.is_file():
        rows = [line.split("\t") for line in registry.read_text().splitlines()
                if line.strip() and not line.startswith("#")]
        for name, module, source, adapter, fifo in sorted(rows, key=lambda row: len(row[0]), reverse=True):
            prefix = "ubq" if name == "ubq" else "ubq-" + name
            if family == prefix or (adapter == "v8" and family.startswith(prefix + "-s")):
                return fifo
    return FIFO_ORDERING[canonical_family(family)]


def family_color(family: str, fallback_index: int = 0) -> str:
    canonical = canonical_family(family)
    return QUEUE_FAMILY_COLORS.get(
        canonical, _FALLBACK_COLORS[fallback_index % len(_FALLBACK_COLORS)]
    )


def family_marker(family: str, fallback_index: int = 0) -> str:
    fallback = ("o", "s", "^", "D", "v", "P", "X", "<", ">", "*")
    return _FAMILY_MARKERS.get(
        canonical_family(family), fallback[fallback_index % len(fallback)]
    )


def family_linestyle(family: str, fallback_index: int = 0):
    fallback = ("-", "--", ":", "-.")
    return _FAMILY_LINESTYLES.get(
        canonical_family(family), fallback[fallback_index % len(fallback)]
    )


def family_hatch(family: str, fallback_index: int = 0) -> str:
    fallback = ("", "//", "xx", "\\\\", "..", "++", "oo", "--")
    return _FAMILY_HATCHES.get(
        canonical_family(family), fallback[fallback_index % len(fallback)]
    )


def _hex_rgb(color: str) -> tuple[float, float, float]:
    value = color.removeprefix("#")
    return tuple(int(value[index : index + 2], 16) / 255.0 for index in (0, 2, 4))


def _mix(
    left: tuple[float, float, float],
    right: tuple[float, float, float],
    amount: float,
) -> tuple[float, float, float]:
    return tuple(a + (b - a) * amount for a, b in zip(left, right))


def family_color_ramp(family: str, fallback_index: int = 0) -> Callable[[float], tuple]:
    """Return a perceptually simple light-to-dark ramp around one family hue.

    The light endpoint is intentionally not near-white; even the smallest
    batch remains visible and retains enough saturation to signal its family.
    """

    base = _hex_rgb(family_color(family, fallback_index))
    light = _mix(base, (1.0, 1.0, 1.0), 0.48)
    dark = _mix(base, (0.0, 0.0, 0.0), 0.15)

    def ramp(position: float) -> tuple[float, float, float, float]:
        position = max(0.0, min(1.0, float(position)))
        if position <= 0.78:
            rgb = _mix(light, base, position / 0.78)
        else:
            rgb = _mix(base, dark, (position - 0.78) / 0.22)
        return (*rgb, 1.0)

    return ramp
