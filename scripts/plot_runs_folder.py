#!/usr/bin/env python

import argparse
import shlex
import subprocess
import sys
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent


def collect_run_jsons(runs_dir: Path):
    files = []
    if not runs_dir.exists():
        return files

    for run_path in sorted(runs_dir.iterdir()):
        if not run_path.is_dir():
            continue
        for json_path in sorted(run_path.glob("*.json")):
            if json_path.is_file():
                files.append(json_path)

    return files


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Collect all benchmark JSON files under a runs directory and generate "
            "per-machine CSV/PNG outputs via scripts/plot_bench.py."
        )
    )
    parser.add_argument(
        "--runs-dir",
        default="bench_results/runs",
        help="Root directory containing per-label run folders (default: bench_results/runs)",
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
    args = parser.parse_args()

    runs_dir = Path(args.runs_dir)
    out_dir = Path(args.out_dir)
    files = collect_run_jsons(runs_dir)

    if not files:
        print(f"No benchmark JSON files found under: {runs_dir}")
        return 1

    cmd = [
        sys.executable,
        str(REPO_ROOT / "scripts" / "plot_bench.py"),
        "--out-dir",
        str(out_dir),
        "--error-bars",
        args.error_bars,
        *[str(path) for path in files],
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
