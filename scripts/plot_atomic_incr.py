#!/usr/bin/env python
import argparse
import csv
import math
import os
from collections import defaultdict
from pathlib import Path

METHOD_SORT_ORDER = {"CAS": 0, "CASB": 1, "FAA": 2, "MAX": 3, "MAXB": 4}


def ensure_mplconfigdir(out_dir: Path):
    if os.environ.get("MPLCONFIGDIR"):
        return

    default_mpl_dir = Path.home() / ".matplotlib"
    if default_mpl_dir.exists() and os.access(default_mpl_dir, os.W_OK):
        return

    fallback_mpl_dir = out_dir / ".mplconfig"
    fallback_mpl_dir.mkdir(parents=True, exist_ok=True)
    os.environ["MPLCONFIGDIR"] = str(fallback_mpl_dir)


def load_rows(path: Path):
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        required = {
            "method",
            "threads",
            "increments_per_thread",
            "total_increments",
            "elapsed_ns",
            "elapsed_ms",
        }
        missing = required - set(reader.fieldnames or [])
        if missing:
            raise ValueError(
                f"{path} is missing required columns: {', '.join(sorted(missing))}"
            )

        for row in reader:
            yield {
                "method": row["method"],
                "threads": int(row["threads"]),
                "increments_per_thread": int(row["increments_per_thread"]),
                "total_increments": int(row["total_increments"]),
                "elapsed_ns": int(row["elapsed_ns"]),
                "elapsed_ms": float(row["elapsed_ms"]),
            }


def aggregate(rows, x_field: str):
    grouped = defaultdict(list)
    for row in rows:
        key = (row["method"], row["threads"], row[x_field])
        grouped[key].append(row["elapsed_ns"])

    summary = []
    for (method, threads, x_value), elapsed_values in grouped.items():
        samples = len(elapsed_values)
        avg_ns = sum(elapsed_values) / samples
        if samples > 1:
            variance_ns = sum((v - avg_ns) ** 2 for v in elapsed_values) / (samples - 1)
            stddev_ns = math.sqrt(variance_ns)
        else:
            stddev_ns = 0.0
        sem_ns = stddev_ns / math.sqrt(samples) if samples > 0 else 0.0
        summary.append(
            {
                "method": method,
                "threads": threads,
                x_field: x_value,
                "mean_elapsed_ns": avg_ns,
                "mean_elapsed_ms": avg_ns / 1_000_000.0,
                "stddev_elapsed_ns": stddev_ns,
                "stddev_elapsed_ms": stddev_ns / 1_000_000.0,
                "sem_elapsed_ns": sem_ns,
                "sem_elapsed_ms": sem_ns / 1_000_000.0,
                "samples": samples,
            }
        )

    summary.sort(key=lambda r: (r["method"], r["threads"], r[x_field]))
    return summary


def write_summary_csv(rows, out_dir: Path, x_field: str):
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"atomic_incr_summary_{x_field}.csv"
    fieldnames = [
        "method",
        "threads",
        x_field,
        "mean_elapsed_ns",
        "mean_elapsed_ms",
        "stddev_elapsed_ns",
        "stddev_elapsed_ms",
        "sem_elapsed_ns",
        "sem_elapsed_ms",
        "samples",
    ]

    with out_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)

    return out_path


def sort_methods(methods):
    return sorted(methods, key=lambda m: (METHOD_SORT_ORDER.get(m, 999), m))


def n_label(rows):
    sample_counts = sorted({int(r["samples"]) for r in rows})
    if not sample_counts:
        return "n = ?"
    if len(sample_counts) == 1:
        return f"n = {sample_counts[0]} run(s) per point"
    return f"n varies ({sample_counts[0]}-{sample_counts[-1]} runs/point)"


def yerr_values(points, error_bars: str):
    if error_bars == "none":
        return None
    if error_bars == "stddev":
        return [p["stddev_elapsed_ms"] for p in points]
    if error_bars == "sem":
        return [p["sem_elapsed_ms"] for p in points]
    raise ValueError(f"Unknown error bar mode: {error_bars}")


def plot(
    rows,
    out_dir: Path,
    x_field: str,
    log_x: bool,
    log_y: bool,
    facet_by: str,
    error_bars: str,
):
    if not rows:
        print("No rows to plot.")
        return

    ensure_mplconfigdir(out_dir)
    try:
        import matplotlib.pyplot as plt
    except ImportError:
        print("matplotlib not found; wrote summary CSV only.")
        return

    methods = sort_methods({r["method"] for r in rows})
    thread_counts = sorted({r["threads"] for r in rows})

    if facet_by == "threads":
        facets = thread_counts
        n_facets = len(facets)
        cols = 2 if n_facets > 1 else 1
        rows_n = math.ceil(n_facets / cols)
    else:
        facets = methods
        n_facets = len(facets)
        cols = 2 if n_facets > 1 else 1
        rows_n = math.ceil(n_facets / cols)

    fig, axes = plt.subplots(rows_n, cols, figsize=(7 * cols, 4.5 * rows_n), squeeze=False)
    flat_axes = axes.flatten()

    x_label = (
        "Increments per thread"
        if x_field == "increments_per_thread"
        else "Total increments"
    )

    for ax in flat_axes[n_facets:]:
        ax.axis("off")

    if facet_by == "threads":
        for ax, threads in zip(flat_axes, facets):
            facet_rows = [r for r in rows if r["threads"] == threads]
            for method in methods:
                points = [r for r in facet_rows if r["method"] == method]
                if not points:
                    continue
                xs = [r[x_field] for r in points]
                ys = [r["mean_elapsed_ms"] for r in points]
                yerr = yerr_values(points, error_bars)
                if yerr is None:
                    ax.plot(xs, ys, marker="o", linewidth=1.8, label=method)
                else:
                    ax.errorbar(
                        xs,
                        ys,
                        yerr=yerr,
                        marker="o",
                        linewidth=1.8,
                        capsize=3,
                        label=method,
                    )

            if log_x:
                ax.set_xscale("log")
            if log_y:
                ax.set_yscale("log")
            ax.set_title(f"{threads} thread(s)")
            ax.set_xlabel(x_label)
            ax.set_ylabel("Elapsed time (ms)")
            ax.grid(True, linestyle=":", alpha=0.35)
            ax.legend(fontsize=8)
    else:
        for ax, method in zip(flat_axes, facets):
            facet_rows = [r for r in rows if r["method"] == method]
            for threads in thread_counts:
                points = [r for r in facet_rows if r["threads"] == threads]
                if not points:
                    continue
                xs = [r[x_field] for r in points]
                ys = [r["mean_elapsed_ms"] for r in points]
                yerr = yerr_values(points, error_bars)
                if yerr is None:
                    ax.plot(xs, ys, marker="o", linewidth=1.8, label=f"{threads} thread(s)")
                else:
                    ax.errorbar(
                        xs,
                        ys,
                        yerr=yerr,
                        marker="o",
                        linewidth=1.8,
                        capsize=3,
                        label=f"{threads} thread(s)",
                    )

            if log_x:
                ax.set_xscale("log")
            if log_y:
                ax.set_yscale("log")
            ax.set_title(method)
            ax.set_xlabel(x_label)
            ax.set_ylabel("Elapsed time (ms)")
            ax.grid(True, linestyle=":", alpha=0.35)
            ax.legend(fontsize=8)

    if facet_by == "threads":
        fig.suptitle(
            "Atomic Increment Benchmark: Time vs Increments "
            f"(facets=threads, lines=method, {n_label(rows)})",
            y=0.995,
        )
    else:
        fig.suptitle(
            "Atomic Increment Benchmark: Time vs Increments "
            f"(facets=method, lines=threads, {n_label(rows)})",
            y=0.995,
        )
    fig.tight_layout()

    out_dir.mkdir(parents=True, exist_ok=True)
    suffix_x = "logx" if log_x else "linearx"
    suffix_y = "logy" if log_y else "lineary"
    suffix = f"{suffix_x}_{suffix_y}"
    out_path = out_dir / f"atomic_incr_{x_field}_facet-{facet_by}_{suffix}.png"
    fig.savefig(out_path, dpi=200)
    plt.close(fig)
    print(f"Wrote PNG: {out_path}")


def main():
    parser = argparse.ArgumentParser(
        description="Plot CSV output from `cargo run --bin atomic_incr_bench -- sweep`."
    )
    parser.add_argument(
        "csv_file",
        nargs="?",
        default="bench_results/plots/atomic_incr.csv",
        help="Input CSV path (default: bench_results/plots/atomic_incr.csv)",
    )
    parser.add_argument(
        "--out-dir",
        default="bench_results/plots",
        help="Output directory for summary CSV and plots",
    )
    parser.add_argument(
        "--x-field",
        choices=["increments_per_thread", "total_increments"],
        default="increments_per_thread",
        help="X-axis field to plot",
    )
    parser.add_argument(
        "--log-x",
        action="store_true",
        help="Use log scale on the x-axis",
    )
    parser.add_argument(
        "--log-y",
        action="store_true",
        help="Use log scale on the y-axis",
    )
    parser.add_argument(
        "--facet-by",
        choices=["threads", "method"],
        default="threads",
        help="Create subplots by this field (default: threads)",
    )
    parser.add_argument(
        "--error-bars",
        choices=["stddev", "sem", "none"],
        default="stddev",
        help="Vertical error bars from repeated runs (default: stddev)",
    )
    args = parser.parse_args()

    input_path = Path(args.csv_file)
    out_dir = Path(args.out_dir)

    raw_rows = list(load_rows(input_path))
    if not raw_rows:
        print(f"No data rows found in {input_path}.")
        return

    summary_rows = aggregate(raw_rows, args.x_field)
    summary_csv = write_summary_csv(summary_rows, out_dir, args.x_field)
    print(f"Wrote summary CSV: {summary_csv}")

    plot(
        summary_rows,
        out_dir,
        args.x_field,
        args.log_x,
        args.log_y,
        args.facet_by,
        args.error_bars,
    )


if __name__ == "__main__":
    main()
