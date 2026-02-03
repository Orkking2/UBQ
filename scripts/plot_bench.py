#!/usr/bin/env python3
import argparse
import json
import math
from pathlib import Path

SCENARIOS = ["spsc", "mpsc", "spmc", "mpmc"]


def load_records(path: Path):
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    records = data.get("results", [])
    for rec in records:
        if rec.get("mode") != "throughput":
            continue
        ops = rec.get("ops_per_sec")
        if ops is None:
            continue
        scenario = rec.get("scenario")
        queue = rec.get("queue")
        block_cap = rec.get("block_cap")
        if queue == "ubq" and block_cap is not None:
            label = f"ubq({block_cap})"
        else:
            label = queue
        yield scenario, label, float(ops)


def sort_labels(labels):
    def key(label):
        if label.startswith("ubq(") and label.endswith(")"):
            size = label[len("ubq(") : -1]
            try:
                size_val = int(size)
            except ValueError:
                size_val = math.inf
            return (0, size_val)
        if label == "ubq":
            return (0, 0)
        order = {"crossbeam": 1, "flume": 2, "async-channel": 3}
        return (1, order.get(label, 99), label)

    return sorted(labels, key=key)


def write_csv(out_dir: Path, scenario: str, values):
    out_dir.mkdir(parents=True, exist_ok=True)
    csv_path = out_dir / f"{scenario}_throughput.csv"
    lines = ["queue,ops_per_sec"]
    for label, ops in values:
        lines.append(f"{label},{ops:.6f}")
    csv_path.write_text("\n".join(lines), encoding="utf-8")


def main():
    parser = argparse.ArgumentParser(description="Plot UBQ benchmark throughput.")
    parser.add_argument("files", nargs="+", help="Benchmark JSON files")
    parser.add_argument(
        "--out-dir",
        default="bench_results/plots",
        help="Output directory for plots/CSVs",
    )
    args = parser.parse_args()

    out_dir = Path(args.out_dir)

    data = {scenario: {} for scenario in SCENARIOS}

    for file in args.files:
        path = Path(file)
        for scenario, label, ops in load_records(path):
            if scenario not in data:
                continue
            data[scenario].setdefault(label, []).append(ops)

    # Always write CSVs
    for scenario in SCENARIOS:
        entries = data.get(scenario, {})
        labels = sort_labels(entries.keys())
        values = [(label, sum(entries[label]) / len(entries[label])) for label in labels]
        write_csv(out_dir, scenario, values)

    try:
        import matplotlib.pyplot as plt
    except ImportError:
        print("matplotlib not found; wrote CSVs only.")
        return

    for scenario in SCENARIOS:
        entries = data.get(scenario, {})
        if not entries:
            continue
        labels = sort_labels(entries.keys())
        values = [sum(entries[label]) / len(entries[label]) for label in labels]

        fig, ax = plt.subplots(figsize=(10, 6))
        ax.bar(range(len(labels)), values)
        ax.set_xticks(range(len(labels)), labels, rotation=30, ha="right")
        ax.set_ylabel("Ops/sec (throughput)")
        ax.set_title(f"Throughput: {scenario.upper()}")
        ax.grid(axis="y", linestyle=":", alpha=0.4)

        out_dir.mkdir(parents=True, exist_ok=True)
        fig.tight_layout()
        fig.savefig(out_dir / f"{scenario}_throughput.png", dpi=200)
        plt.close(fig)


if __name__ == "__main__":
    main()
