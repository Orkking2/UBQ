#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import shutil
from pathlib import Path

from ubq_labels import format_ubq_label_parts, parse_ubq_queue_label


def canonical_safe_label(label: str) -> str | None:
    parsed = parse_ubq_queue_label(label, require_valid=True)
    if parsed is None:
        return None
    return format_ubq_label_parts(*parsed).replace(",", "_")


def canonical_label(label: str) -> str | None:
    parsed = parse_ubq_queue_label(label, require_valid=True)
    if parsed is None:
        return None
    return format_ubq_label_parts(*parsed)


def migrate_json(path: Path, dry_run: bool) -> bool:
    try:
        payload = json.loads(path.read_text())
    except Exception as exc:
        raise RuntimeError(f"failed to read JSON {path}: {exc}") from exc

    meta = payload.get("meta")
    if not isinstance(meta, dict):
        return False

    ubq_label = meta.get("ubq_label")
    if not isinstance(ubq_label, str):
        return False

    new_label = canonical_label(ubq_label)
    if new_label is None or new_label == ubq_label:
        return False

    meta["ubq_label"] = new_label
    if not dry_run:
        path.write_text(json.dumps(payload, indent=2) + "\n")
    return True


def merge_tree(src: Path, dst: Path, dry_run: bool) -> tuple[int, int]:
    moved_files = 0
    removed_dirs = 0

    if dry_run:
        json_files = sum(1 for _ in src.rglob("*.json"))
        return json_files, 1

    dst.mkdir(parents=True, exist_ok=True)
    for child in sorted(src.iterdir()):
        target = dst / child.name
        if child.is_dir():
            files, dirs = merge_tree(child, target, dry_run=False)
            moved_files += files
            removed_dirs += dirs
            continue
        if target.exists():
            raise RuntimeError(f"refusing to overwrite existing file: {target}")
        shutil.move(str(child), str(target))
        moved_files += 1

    src.rmdir()
    removed_dirs += 1
    return moved_files, removed_dirs


def migrate_runs(root: Path, dry_run: bool) -> tuple[int, int, int]:
    updated_json = 0
    moved_files = 0
    renamed_dirs = 0

    for json_path in sorted(root.rglob("*.json")):
        if migrate_json(json_path, dry_run=dry_run):
            updated_json += 1

    machine_dirs = sorted(path for path in root.iterdir() if path.is_dir())
    for machine_dir in machine_dirs:
        for label_dir in sorted(path for path in machine_dir.iterdir() if path.is_dir()):
            new_name = canonical_safe_label(label_dir.name)
            if new_name is None or new_name == label_dir.name:
                continue

            target = machine_dir / new_name
            if target.exists():
                files, dirs = merge_tree(label_dir, target, dry_run=dry_run)
                moved_files += files
                renamed_dirs += dirs
                continue

            if not dry_run:
                label_dir.rename(target)
            renamed_dirs += 1

    return updated_json, moved_files, renamed_dirs


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Migrate legacy UBQ benchmark run labels from vN/pool/block[/b] naming "
            "to preset,pool,block,backoff naming."
        )
    )
    parser.add_argument(
        "--runs-dir",
        default="bench_results/runs",
        help="Root runs directory to migrate (default: bench_results/runs)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report the changes without modifying files.",
    )
    args = parser.parse_args()

    root = Path(args.runs_dir)
    if not root.exists():
        raise SystemExit(f"runs dir does not exist: {root}")

    updated_json, moved_files, renamed_dirs = migrate_runs(root, dry_run=args.dry_run)
    mode = "Dry run" if args.dry_run else "Migration"
    print(
        f"{mode} complete: updated_json={updated_json} moved_files={moved_files} renamed_dirs={renamed_dirs}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
