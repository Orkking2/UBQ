#!/usr/bin/env python

import argparse
import shlex
import subprocess
import sys
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent


def collect_run_jsons(runs_dir: Path):
    if not runs_dir.exists():
        return []

    return sorted(path for path in runs_dir.rglob("*.json") if path.is_file())


def resolve_plot_python() -> str:
    current_python = Path(sys.executable).resolve()
    venv_candidates = (
        REPO_ROOT / ".venv" / "bin" / "python",
        REPO_ROOT / ".venv" / "Scripts" / "python.exe",
    )

    for candidate in venv_candidates:
        if not candidate.is_file():
            continue
        resolved_candidate = candidate.resolve()
        if resolved_candidate == current_python:
            return str(candidate)
        return str(candidate)

    return sys.executable


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Collect all benchmark JSON files under a runs directory tree and generate "
            "per-machine CSV/PNG outputs via scripts/plot_bench.py."
        )
    )
    parser.add_argument(
        "--runs-dir",
        default="bench_results/runs",
        help="Root directory containing machine/scenario record.json files (default: bench_results/runs)",
    )
    parser.add_argument(
        "--out-dir",
        default="bench_results/plots",
        help="Output root for generated CSV/PNG artifacts (default: bench_results/plots)",
    )
    parser.add_argument(
        "--error-bars",
        choices=["sem", "stddev", "none"],
        default="sem",
        help="Error bar mode forwarded to plot_bench.py (default: sem)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the plot command without running it.",
    )
    parser.add_argument(
        "--no-clean",
        action="store_true",
        help="Pass through to plot_bench.py to keep pre-existing output files.",
    )
    parser.add_argument(
        "--max-line-series",
        type=int,
        default=10,
        help="Deprecated compatibility option; plots now show one best variation per queue family",
    )
    args = parser.parse_args()

    runs_dir = Path(args.runs_dir)
    out_dir = Path(args.out_dir)
    if not collect_run_jsons(runs_dir):
        print(f"No benchmark JSON files found under: {runs_dir}")
        return 1

    plot_python = resolve_plot_python()
    cmd = [
        plot_python,
        str(REPO_ROOT / "scripts" / "plot_bench.py"),
        "--runs-dir",
        str(runs_dir),
        "--out-dir",
        str(out_dir),
        "--error-bars",
        args.error_bars,
        "--max-line-series",
        str(args.max_line_series),
    ]
    if args.no_clean:
        cmd.append("--no-clean")
    print(f"Plot command: {shlex.join(cmd)}")

    if args.dry_run:
        return 0

    subprocess.run(cmd, check=True, cwd=REPO_ROOT)
    return 0


if __name__ == "__main__":
    sys.exit(main())
