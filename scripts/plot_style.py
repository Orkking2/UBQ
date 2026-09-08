"""Shared, colorblind-safe visual identities for UBQ plots.

Colors use the Okabe-Ito palette, extended with a few Tol-style dark tones for
dense plots. Queue identity is also encoded with a marker, line pattern, or
hatch so no categorical comparison relies on color alone. Keep semantic
assignments here rather than choosing colors by series position: a queue
should look the same in every figure.
"""

from __future__ import annotations

from collections.abc import Callable


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


def canonical_family(family: str) -> str:
    return _FAMILY_ALIASES.get(family, family)


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
