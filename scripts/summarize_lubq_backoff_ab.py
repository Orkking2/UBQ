#!/usr/bin/env python3
"""Summarize a slurm/lubq_backoff_ab.sbatch results.jsonl.

Groups bench_profile handoff results by (lubq_backoff, scenario), reports
median/IQR throughput, and flags scenarios where the yield and crossbeam
interquartile ranges do not overlap -- a lightweight, repeats-appropriate
"meaningfully different" signal. This is a fast verdict, not a replacement for
scripts/aggregate_profile_results.py's bootstrap intervals.
"""

import argparse
import json
import statistics
import sys
from collections import defaultdict
from pathlib import Path

POLICIES = ("crossbeam", "yield")


def load_rows(path: Path) -> list[dict]:
    rows = []
    with path.open(encoding="utf-8") as stream:
        for line_number, line in enumerate(stream, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError as error:
                raise SystemExit(f"{path}:{line_number}: invalid JSON: {error}")
    return rows


def quartiles(values: list[float]) -> tuple[float, float, float]:
    ordered = sorted(values)
    return (
        statistics.median(ordered[: len(ordered) // 2 + len(ordered) % 2]) if len(ordered) > 1 else ordered[0],
        statistics.median(ordered),
        statistics.median(ordered[len(ordered) // 2 :]) if len(ordered) > 1 else ordered[0],
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("results", type=Path, help="results.jsonl from lubq_backoff_ab.sbatch")
    args = parser.parse_args()

    rows = load_rows(args.results)
    if not rows:
        print(f"{args.results}: no rows", file=sys.stderr)
        return 2

    cells: dict[tuple[str, str], list[float]] = defaultdict(list)
    for row in rows:
        policy = row.get("lubq_backoff")
        scenario = row.get("scenario")
        ops = row.get("ops_per_sec")
        if policy not in POLICIES or scenario is None or ops is None:
            print(f"skipping malformed row: {row}", file=sys.stderr)
            continue
        cells[(policy, scenario)].append(float(ops))

    scenarios = sorted({scenario for _, scenario in cells})
    print(f"{'scenario':<10} {'policy':<10} {'n':>3} {'q1':>14} {'median':>14} {'q3':>14}  (ops/s)")
    stats: dict[tuple[str, str], tuple[float, float, float]] = {}
    for scenario in scenarios:
        for policy in POLICIES:
            values = cells.get((policy, scenario))
            if not values:
                continue
            q1, med, q3 = quartiles(values)
            stats[(policy, scenario)] = (q1, med, q3)
            print(f"{scenario:<10} {policy:<10} {len(values):>3} {q1:>14,.0f} {med:>14,.0f} {q3:>14,.0f}")

    print()
    print("verdict (non-overlapping IQR across 5 repeats is a fast signal, not a bootstrap CI):")
    for scenario in scenarios:
        crossbeam = stats.get(("crossbeam", scenario))
        yield_ = stats.get(("yield", scenario))
        if crossbeam is None or yield_ is None:
            print(f"  {scenario}: incomplete data, skipping")
            continue
        cb_q1, cb_med, cb_q3 = crossbeam
        yd_q1, yd_med, yd_q3 = yield_
        ratio = yd_med / cb_med if cb_med else float("nan")
        if yd_q1 > cb_q3:
            verdict = f"yield MEANINGFULLY FASTER ({ratio:.2f}x median, IQRs disjoint)"
        elif cb_q1 > yd_q3:
            verdict = f"yield MEANINGFULLY SLOWER ({ratio:.2f}x median, IQRs disjoint)"
        else:
            verdict = f"inconclusive at 5 repeats ({ratio:.2f}x median, IQRs overlap)"
        print(f"  {scenario}: {verdict}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
