#!/usr/bin/env python3
"""Summarize LMAX Disruptor JNI benchmark logs."""

from __future__ import annotations

import argparse
import csv
import re
import statistics
from pathlib import Path


RUN_RE = re.compile(
    r"Run\s+(?P<run>\d+),\s+"
    r"(?P<label>BlockingQueue|Disruptor)=(?P<ops>[\d,]+)\s+ops/sec"
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("run_dir", type=Path)
    parser.add_argument("--out-dir", type=Path)
    return parser.parse_args()


def iter_samples(run_dir: Path) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for log_path in sorted((run_dir / "logs").glob("*.log")):
        if "__" in log_path.stem:
            queue, scenario = log_path.stem.rsplit("__", 1)
        else:
            stem_parts = log_path.stem.split("_", 1)
            if len(stem_parts) != 2:
                continue
            queue, scenario = stem_parts
        for line in log_path.read_text(encoding="utf-8").splitlines():
            match = RUN_RE.search(line)
            if not match:
                continue
            rows.append(
                {
                    "queue": queue,
                    "scenario": scenario,
                    "run": match.group("run"),
                    "ops_per_sec": match.group("ops").replace(",", ""),
                    "source_label": match.group("label"),
                    "log": str(log_path),
                }
            )
    return rows


def summarize(samples: list[dict[str, str]]) -> list[dict[str, str]]:
    grouped: dict[tuple[str, str], list[int]] = {}
    for row in samples:
        grouped.setdefault((row["queue"], row["scenario"]), []).append(
            int(row["ops_per_sec"])
        )

    summary: list[dict[str, str]] = []
    for (queue, scenario), values in sorted(grouped.items()):
        summary.append(
            {
                "queue": queue,
                "scenario": scenario,
                "samples": str(len(values)),
                "min_ops_per_sec": str(min(values)),
                "median_ops_per_sec": str(int(statistics.median(values))),
                "mean_ops_per_sec": f"{statistics.fmean(values):.2f}",
                "max_ops_per_sec": str(max(values)),
            }
        )
    return summary


def write_csv(path: Path, rows: list[dict[str, str]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    args = parse_args()
    out_dir = args.out_dir or args.run_dir
    samples = iter_samples(args.run_dir)
    summary = summarize(samples)

    write_csv(
        out_dir / "samples.csv",
        samples,
        ["queue", "scenario", "run", "ops_per_sec", "source_label", "log"],
    )
    write_csv(
        out_dir / "summary.csv",
        summary,
        [
            "queue",
            "scenario",
            "samples",
            "min_ops_per_sec",
            "median_ops_per_sec",
            "mean_ops_per_sec",
            "max_ops_per_sec",
        ],
    )

    for row in summary:
        print(
            f"{row['queue']:>9} {row['scenario']:>5} "
            f"median={int(row['median_ops_per_sec']):,} ops/sec "
            f"mean={float(row['mean_ops_per_sec']):,.2f}"
        )


if __name__ == "__main__":
    main()
