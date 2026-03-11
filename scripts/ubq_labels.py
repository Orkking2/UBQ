#!/usr/bin/env python3

from __future__ import annotations

from typing import Sequence

VERSION_TO_PRESET = {
    3: "aggressive_prepare",
    4: "balanced",
    5: "pool_conservative",
    6: "no_pool",
    7: "consumer_pool_only",
}
PRESET_TO_VERSION = {value: key for key, value in VERSION_TO_PRESET.items()}
BACKOFF_SUFFIX_TO_NAME = {"": "crossbeam", "b": "yield"}
BACKOFF_NAME_TO_SUFFIX = {value: key for key, value in BACKOFF_SUFFIX_TO_NAME.items()}

UBQ_POOLED_VERSIONS = (3, 4, 5, 7)
UBQ_NO_POOL_VERSION = 6
UBQ_MIN_POOL_SIZE = 1
UBQ_NO_POOL_SIZE = 0

UBQ_VERSIONS = (3, 4, 5, 6, 7)
UBQ_POOL_VALUES = (1, 2, 4, 8, 16, 32, 64)
UBQ_BLOCK_VALUES = (31, 63, 127, 255, 511, 1023, 2047, 4095)
UBQ_BACKOFF_VALUES = ("", "b")

UBQ_IMMEDIATE_DIMS = {
    0: list(UBQ_VERSIONS),
    1: [UBQ_NO_POOL_SIZE, *UBQ_POOL_VALUES],
    2: list(UBQ_BLOCK_VALUES),
    3: list(UBQ_BACKOFF_VALUES),
}


def _strip_ubq_prefix(token: str) -> str:
    text = str(token).strip().lower()
    if text.startswith("ubq_") or text.startswith("ubq:"):
        return text[4:]
    return text


def format_ubq_label_parts(version: int, pool: int, block: int, backoff: str = "") -> str:
    preset = VERSION_TO_PRESET.get(version)
    if preset is None:
        raise ValueError(f"unknown UBQ version: {version}")

    backoff_name = BACKOFF_SUFFIX_TO_NAME.get(backoff)
    if backoff_name is None:
        raise ValueError(f"unknown UBQ backoff: {backoff}")

    return f"{preset},{pool},{block},{backoff_name}"


def parse_ubq_queue_label(token: str, require_valid: bool = True):
    text = _strip_ubq_prefix(token)
    parts = [part.strip() for part in text.split(",") if part.strip()]
    if len(parts) == 1 and "_" in text:
        parts = [part.strip() for part in text.split("_") if part.strip()]
    if len(parts) not in (3, 4):
        return None

    if parts[0].startswith("v"):
        try:
            version = int(parts[0][1:])
            pool = int(parts[1])
            block = int(parts[2])
        except ValueError:
            return None
        backoff = parts[3] if len(parts) == 4 else ""
    else:
        preset = parts[0]
        version = PRESET_TO_VERSION.get(preset)
        if version is None:
            return None
        try:
            pool = int(parts[1])
            block = int(parts[2])
        except ValueError:
            return None
        backoff_name = parts[3] if len(parts) == 4 else "crossbeam"
        backoff = BACKOFF_NAME_TO_SUFFIX.get(backoff_name)
        if backoff is None:
            return None

    params = (version, pool, block, backoff)
    if require_valid and not is_valid_ubq_params(params):
        return None
    return params


def is_valid_ubq_params(params: Sequence[object]) -> bool:
    if len(params) < 3:
        return False

    try:
        version = int(params[0])
        pool = int(params[1])
        block = int(params[2])
    except (TypeError, ValueError):
        return False

    backoff = str(params[3]) if len(params) >= 4 else ""
    if version not in UBQ_VERSIONS:
        return False
    if block not in UBQ_BLOCK_VALUES:
        return False
    if backoff not in UBQ_BACKOFF_VALUES:
        return False
    if version == UBQ_NO_POOL_VERSION:
        return pool == UBQ_NO_POOL_SIZE
    return pool in UBQ_POOL_VALUES


def bench_label_sort_key(label: str):
    parsed = parse_ubq_queue_label(label, require_valid=False)
    if parsed is None:
        return (255, 255, 65535, 255, str(label))

    version, pool, block, backoff = parsed
    backoff_idx = 1 if backoff == "b" else 0
    return (version, pool, block, backoff_idx, backoff)
