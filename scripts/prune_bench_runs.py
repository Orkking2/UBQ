#!/usr/bin/env python
import argparse
import json
import re
import shutil
import sys
from collections import defaultdict
from pathlib import Path

LEGACY_SCENARIO_MAP = {
    "spsc": "1p1c",
    "mpsc": "4p1c",
    "spmc": "1p4c",
    "mpmc": "4p4c",
}

MACHINE_CANONICAL_MAP = {
    "arm": "arm",
    "local": "arm",
    "hebrides": "arm",
    "x86": "x86",
    "lab": "x86",
}

TIMESTAMPED_RUN_RE = re.compile(r"^\d{8}T\d{6}Z__(.+)$")


def parse_label(dirname: str) -> str:
    match = TIMESTAMPED_RUN_RE.match(dirname)
    if match:
        return match.group(1)
    return dirname


def canonical_machine_label(name: str) -> str:
    raw = str(name).strip()
    return MACHINE_CANONICAL_MAP.get(raw.lower(), raw)


def normalize_scenario(name: str) -> str:
    key = str(name).strip().lower()
    return LEGACY_SCENARIO_MAP.get(key, key)


def iter_ubq_scores(json_path: Path):
    try:
        data = json.loads(json_path.read_text(encoding="utf-8"))
    except Exception as exc:
        print(f"warning: could not parse {json_path}: {exc}", file=sys.stderr)
        return

    meta = data.get("meta", {})
    machine = canonical_machine_label(meta.get("machine_label", "local"))

    for rec in data.get("results", []):
        if rec.get("queue") != "ubq":
            continue
        if rec.get("skipped_reason"):
            continue
        if str(rec.get("mode", "throughput")) != "throughput":
            continue

        ops = rec.get("ops_per_sec")
        if ops is None:
            continue

        scenario = normalize_scenario(rec.get("scenario", ""))
        if not scenario:
            continue

        try:
            ops_value = float(ops)
        except (TypeError, ValueError):
            continue

        yield (machine, scenario), ops_value


def keep_newest_per_label(runs_dir: Path, latest_label: str):
    by_label = defaultdict(list)
    for path in runs_dir.iterdir():
        if not path.is_dir():
            continue
        label = parse_label(path.name)
        if label:
            by_label[label].append(path)

    canonical_dirs = {}
    for label in sorted(by_label):
        paths = by_label[label]
        canonical_path = runs_dir / label
        preferred = canonical_path if label == latest_label and canonical_path in paths else None

        source = max(
            paths,
            key=lambda p: ((preferred is not None and p == preferred), p.stat().st_mtime_ns, p.name),
        )

        if source != canonical_path:
            if canonical_path.exists():
                shutil.rmtree(canonical_path)
            source.rename(canonical_path)
            source = canonical_path

        for other in paths:
            if other == source:
                continue
            if other.exists():
                shutil.rmtree(other)

        canonical_dirs[label] = source

    return canonical_dirs


def compute_label_scores(canonical_dirs):
    scores_by_label = {}
    for label, run_dir in canonical_dirs.items():
        score_samples = defaultdict(list)
        for json_path in sorted(run_dir.glob("*.json")):
            for key, ops in iter_ubq_scores(json_path):
                score_samples[key].append(ops)
        scores = {}
        for key, samples in score_samples.items():
            scores[key] = sum(samples) / len(samples)
        scores_by_label[label] = scores
    return scores_by_label


def labels_that_win_any_key(scores_by_label):
    best = {}
    for label, scores in scores_by_label.items():
        for key, ops in scores.items():
            current = best.get(key)
            if current is None or ops > current[1]:
                best[key] = (label, ops)
    return {label for label, _ops in best.values()}


def load_purged_labels(path: Path):
    if not path.exists():
        return set()

    labels = set()
    for line in path.read_text(encoding="utf-8").splitlines():
        label = line.strip()
        if not label:
            continue
        labels.add(label)
    return labels


def save_purged_labels(path: Path, labels):
    path.parent.mkdir(parents=True, exist_ok=True)
    content = "\n".join(sorted(labels))
    if content:
        content += "\n"
    path.write_text(content, encoding="utf-8")


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Normalize benchmark runs by keeping one directory per UBQ label. "
            "Optionally purge non-winning labels."
        )
    )
    parser.add_argument("--runs-dir", default="bench_results/runs", help="Root directory with run subdirectories")
    parser.add_argument("--latest-label", required=True, help="UBQ label from the run that just completed")
    parser.add_argument(
        "--purge-losers",
        action="store_true",
        help="Delete labels that do not lead any machine/scenario throughput key (by mean ops/sec); always keep --latest-label",
    )
    parser.add_argument(
        "--purged-labels-file",
        default=None,
        help="Optional file to persist labels purged for not winning any machine/scenario (used with --purge-losers)",
    )
    args = parser.parse_args()

    runs_dir = Path(args.runs_dir)
    latest_label = args.latest_label
    if args.purged_labels_file:
        purged_labels_file = Path(args.purged_labels_file)
    else:
        purged_labels_file = runs_dir.parent / "purged_ubq_labels.txt"

    if not runs_dir.exists():
        print(f"Run directory does not exist: {runs_dir}")
        return

    canonical_dirs = keep_newest_per_label(runs_dir, latest_label)
    if not canonical_dirs:
        print(f"No run directories found under: {runs_dir}")
        return

    if not args.purge_losers:
        labels = sorted(canonical_dirs)
        print(
            f"Run normalization complete: kept {len(labels)} label(s), removed 0 label(s) by score."
        )
        print(f"Kept labels: {', '.join(labels)}")
        print("Loser purge disabled. Re-run with --purge-losers to drop non-winning labels.")
        return

    scores_by_label = compute_label_scores(canonical_dirs)
    winners = labels_that_win_any_key(scores_by_label)

    if winners:
        keep_labels = set(winners)
    else:
        keep_labels = set(canonical_dirs.keys())

    if latest_label in canonical_dirs:
        keep_labels.add(latest_label)

    removed = []
    for label, run_dir in sorted(canonical_dirs.items()):
        if label in keep_labels:
            continue
        shutil.rmtree(run_dir)
        removed.append(label)

    historical_purged = load_purged_labels(purged_labels_file)
    historical_purged.update(removed)
    save_purged_labels(purged_labels_file, historical_purged)

    kept_sorted = sorted(keep_labels)
    print(
        f"Run pruning complete: kept {len(kept_sorted)} label(s), removed {len(removed)} label(s)."
    )
    if removed:
        print(f"Removed labels: {', '.join(removed)}")
    print(f"Kept labels: {', '.join(kept_sorted)}")
    print(f"Purged label history file: {purged_labels_file}")


if __name__ == "__main__":
    main()
