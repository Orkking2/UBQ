#!/usr/bin/env python3
"""Render the token-gated/always-wide throughput ratio grid."""

import argparse
import csv
import json
import math
import os
import statistics
import sys
import tempfile
from pathlib import Path


BENCHMARK_NAME = "head_reload"
SCHEMA_VERSION = 1
STRATEGIES = ("always_wide", "token_gated")
BLUE = "#2166ac"
PURPLE = "#762a83"
WHITE = "#ffffff"

# Keep matplotlib's font cache within a writable temporary directory when the
# repository is benchmarked from a restricted environment.
MPL_CONFIG_DIR = Path(tempfile.gettempdir()) / "ubq-head-reload-matplotlib"
MPL_CONFIG_DIR.mkdir(parents=True, exist_ok=True)
os.environ.setdefault("MPLCONFIGDIR", str(MPL_CONFIG_DIR))


def load_samples(path):
    try:
        with Path(path).open(encoding="utf-8") as stream:
            data = json.load(stream)
    except Exception as exc:
        print(f"warning: could not parse {path}: {exc}", file=sys.stderr)
        return []
    if data.get("benchmark") != BENCHMARK_NAME or int(data.get("schema_version", -1)) != SCHEMA_VERSION:
        return []

    machine = str(data.get("meta", {}).get("machine_label", "local")).strip() or "local"
    loaded = []
    for row in data.get("results", []):
        try:
            strategy = str(row["strategy"])
            sample = {
                "machine": machine,
                "repeat": int(row["repeat_index"]),
                "threads": int(row["thread_count"]),
                "batch": int(row["batch_size"]),
                "strategy": strategy,
                "throughput": float(row["reservations_per_sec"]),
                "failures": float(row["cas_failures_per_reservation"]),
                "wide_loads": float(row["wide_loads_per_reservation"]),
            }
        except (KeyError, TypeError, ValueError):
            continue
        if (
            strategy in STRATEGIES
            and sample["repeat"] > 0
            and sample["threads"] > 0
            and sample["batch"] > 0
            and sample["throughput"] > 0
        ):
            loaded.append(sample)
    return loaded


def summarize(values):
    return {
        "mean": statistics.fmean(values),
        "stddev": statistics.stdev(values) if len(values) > 1 else 0.0,
        "samples": len(values),
    }


def aggregate(samples):
    grouped = {}
    for sample in samples:
        key = (sample["machine"], sample["threads"], sample["batch"])
        cell = grouped.setdefault(key, {strategy: {} for strategy in STRATEGIES})
        cell[sample["strategy"]][sample["repeat"]] = sample

    machines = {}
    for (machine, threads, batch), strategies in grouped.items():
        repeats = sorted(set(strategies["always_wide"]) & set(strategies["token_gated"]))
        if not repeats:
            continue
        always = [strategies["always_wide"][repeat] for repeat in repeats]
        gated = [strategies["token_gated"][repeat] for repeat in repeats]
        paired_log_ratios = [
            math.log(gate["throughput"] / wide["throughput"])
            for wide, gate in zip(always, gated)
        ]
        machines.setdefault(machine, {})[(threads, batch)] = {
            "always_throughput": summarize([row["throughput"] for row in always]),
            "token_throughput": summarize([row["throughput"] for row in gated]),
            "always_failures": summarize([row["failures"] for row in always]),
            "token_failures": summarize([row["failures"] for row in gated]),
            "always_wide_loads": summarize([row["wide_loads"] for row in always]),
            "token_wide_loads": summarize([row["wide_loads"] for row in gated]),
            "ratio": math.exp(statistics.fmean(paired_log_ratios)),
            "paired_samples": len(repeats),
        }
    return machines


def write_csv(path, cells):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.writer(stream)
        writer.writerow(
            (
                "threads",
                "batch_size",
                "token_gated_over_always_wide",
                "mean_always_wide_reservations_per_sec",
                "mean_token_gated_reservations_per_sec",
                "mean_always_wide_failures_per_reservation",
                "mean_token_gated_failures_per_reservation",
                "mean_always_wide_loads_per_reservation",
                "mean_token_gated_loads_per_reservation",
                "paired_samples",
            )
        )
        for (threads, batch), cell in sorted(cells.items()):
            writer.writerow(
                (
                    threads,
                    batch,
                    f"{cell['ratio']:.9f}",
                    f"{cell['always_throughput']['mean']:.3f}",
                    f"{cell['token_throughput']['mean']:.3f}",
                    f"{cell['always_failures']['mean']:.9f}",
                    f"{cell['token_failures']['mean']:.9f}",
                    f"{cell['always_wide_loads']['mean']:.9f}",
                    f"{cell['token_wide_loads']['mean']:.9f}",
                    cell["paired_samples"],
                )
            )


def cell_label(ratio):
    if abs(ratio - 1.0) < 0.005:
        return "1.00×"
    if ratio > 1.0:
        return f"{ratio:.2f}× T"
    return f"{1.0 / ratio:.2f}× A"


def render(plt, colors, machine, cells, out_dir):
    threads = sorted({key[0] for key in cells})
    batches = sorted({key[1] for key in cells})
    log_ratios = [
        [math.log2(cells[(thread, batch)]["ratio"]) for batch in batches]
        for thread in threads
    ]
    max_abs = max(abs(value) for row in log_ratios for value in row)
    limit = max(max_abs, math.log2(1.01))
    cmap = colors.LinearSegmentedColormap.from_list(
        "always_equal_token", (BLUE, WHITE, PURPLE)
    )

    fig_width = max(8.5, 0.9 * len(batches) + 2.8)
    fig_height = max(4.5, 0.75 * len(threads) + 2.3)
    fig, ax = plt.subplots(figsize=(fig_width, fig_height))
    image = ax.imshow(log_ratios, cmap=cmap, vmin=-limit, vmax=limit, aspect="auto")
    ax.set_xticks(range(len(batches)), [str(value) for value in batches])
    ax.set_yticks(range(len(threads)), [str(value) for value in threads])
    ax.set_xlabel("Reservation batch size (counter increment)")
    ax.set_ylabel("Contending threads")
    ax.set_title(
        f"{machine}: 16-bit token gate / unconditional U128 retry reload\n"
        "purple = token-gated faster; blue = always-wide faster; white = equal"
    )

    for row, thread in enumerate(threads):
        for column, batch in enumerate(batches):
            ratio = cells[(thread, batch)]["ratio"]
            rgba = cmap((math.log2(ratio) + limit) / (2 * limit))
            luminance = 0.2126 * rgba[0] + 0.7152 * rgba[1] + 0.0722 * rgba[2]
            ax.text(
                column,
                row,
                cell_label(ratio),
                ha="center",
                va="center",
                color="white" if luminance < 0.52 else "#202020",
                fontsize=8,
            )

    colorbar = fig.colorbar(image, ax=ax, pad=0.02)
    colorbar.set_label("log₂(token-gated / always-wide throughput)")
    fig.text(
        0.5,
        0.015,
        "T/A labels name the faster strategy; geometric mean of paired repeats",
        ha="center",
        fontsize=8,
        color="#444444",
    )
    fig.tight_layout(rect=(0, 0.035, 1, 1))
    out_dir.mkdir(parents=True, exist_ok=True)
    paths = []
    for suffix in ("png", "svg"):
        path = out_dir / f"head_reload_ratio_grid.{suffix}"
        fig.savefig(path, dpi=180 if suffix == "png" else None)
        paths.append(path)
    plt.close(fig)
    return paths


def collect_files(paths, runs_dirs):
    files = {Path(path) for path in paths if Path(path).is_file()}
    for raw in runs_dirs:
        root = Path(raw)
        if root.exists():
            files.update(path for path in root.rglob("*.json") if path.is_file())
    return sorted(files)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("files", nargs="*", help="head_reload JSON result files")
    parser.add_argument("--runs-dir", action="append", default=[], help="discover JSON recursively")
    parser.add_argument("--out-dir", default="bench_results/plots", help="plot output root")
    args = parser.parse_args()

    files = collect_files(args.files, args.runs_dir)
    if not files:
        parser.error("provide result files or --runs-dir")
    samples = []
    for path in files:
        samples.extend(load_samples(path))
    machines = aggregate(samples)
    if not machines:
        print("No paired head-reload benchmark samples found.", file=sys.stderr)
        return 1

    try:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.colors as colors
        import matplotlib.pyplot as plt
    except ImportError:
        colors = None
        plt = None
        print("matplotlib unavailable; writing CSV only", file=sys.stderr)

    for machine, cells in sorted(machines.items()):
        out_dir = Path(args.out_dir) / machine / "head_reload"
        csv_path = out_dir / "head_reload_ratio_grid.csv"
        write_csv(csv_path, cells)
        print(f"Wrote CSV: {csv_path}")
        if plt is not None:
            for path in render(plt, colors, machine, cells, out_dir):
                print(f"Wrote plot: {path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
