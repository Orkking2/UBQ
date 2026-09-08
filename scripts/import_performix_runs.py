#!/usr/bin/env python3
"""Inventory and optionally import exported APX runs with stable identities.

The generated CSV/JSON sidecars are the bridge between Performix/MCP run IDs,
the profiling case, its unprofiled throughput shadow, and its comparison sets.
Without ``--import`` this command is read-only apart from writing the manifests.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any, Mapping, Sequence


RUN_ID_RE = re.compile(r"(?:Run ID|run[_ .-]?id)[\"':= ]+([A-Za-z0-9._-]+)", re.I)
FIELDS = (
    "apx_run_id",
    "local_run_id",
    "apx_run_name",
    "import_status",
    "case_id",
    "campaign",
    "comparison_group_id",
    "comparison_role",
    "queue",
    "queue_variant",
    "scenario",
    "batch_size",
    "backoff",
    "block_size",
    "recipe",
    "repeat",
    "shadow_ops_per_sec",
    "archive",
    "archive_sha256",
    "source_dir",
    "comparison_sets_json",
    "selection_evidence_json",
    "error",
)


class ImportError(RuntimeError):
    """A manifest or APX import operation is incomplete."""


def read_object(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ImportError(f"{path} must contain a JSON object")
    return value


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def backoff(case: Mapping[str, Any]) -> str:
    if case.get("lubq_backoff"):
        return str(case["lubq_backoff"])
    if case.get("ubq_label"):
        return str(case["ubq_label"]).rsplit(",", 1)[-1]
    return ""


def block_size(case: Mapping[str, Any]) -> Any:
    return (
        case.get("fastfifo_block_size")
        or case.get("lfqueue_segment_size")
        or ("page" if case.get("queue") in {"ubq", "lubq"} else "")
    )


def discover(root: Path) -> list[dict[str, Any]]:
    rows = []
    for case_path in sorted(root.rglob("case.json")):
        case_dir = case_path.parent
        case = read_object(case_path)
        if case.get("backend") != "apx" and case.get("collector") != "apx":
            continue
        status_path = case_dir / "status.json"
        status = read_object(status_path) if status_path.is_file() else {}
        archives = sorted(case_dir.glob("*.zip"))
        error = ""
        if status.get("status") != "completed":
            error = f"case status is {status.get('status', 'missing')}"
        elif len(archives) != 1:
            error = f"expected one APX archive, found {len(archives)}"
        archive = archives[0] if len(archives) == 1 else None
        run_id_path = case_dir / "apx-run-id.txt"
        run_name_path = case_dir / "apx-run-name.txt"
        run_id = (
            run_id_path.read_text(encoding="utf-8").strip()
            if run_id_path.is_file()
            else str(status.get("apx_run_id") or "")
        )
        run_name = (
            run_name_path.read_text(encoding="utf-8").strip()
            if run_name_path.is_file()
            else str(case.get("apx_run_name") or case.get("case_id") or run_id)
        )
        benchmark_path = case_dir / "benchmark.json"
        benchmark = read_object(benchmark_path) if benchmark_path.is_file() else {}
        rows.append(
            {
                "apx_run_id": run_id,
                "local_run_id": "",
                "apx_run_name": run_name,
                "import_status": "invalid" if error else "ready",
                "case_id": case.get("case_id", ""),
                "campaign": case.get("campaign", ""),
                "comparison_group_id": case.get("comparison_group_id", ""),
                "comparison_role": case.get("comparison_role", ""),
                "queue": case.get("queue", ""),
                "queue_variant": case.get("queue_variant", ""),
                "scenario": case.get("scenario", ""),
                "batch_size": case.get("batch_size") if case.get("batch_size") is not None else "",
                "backoff": backoff(case),
                "block_size": block_size(case),
                "recipe": case.get("recipe", ""),
                "repeat": case.get("repeat", ""),
                "shadow_ops_per_sec": benchmark.get("ops_per_sec", ""),
                "archive": str(archive.resolve()) if archive else "",
                "archive_sha256": sha256(archive) if archive else "",
                "source_dir": str(case_dir.resolve()),
                "comparison_sets_json": json.dumps(
                    case.get("comparison_sets", []), sort_keys=True, separators=(",", ":")
                ),
                "selection_evidence_json": json.dumps(
                    case.get("selection_evidence", {}),
                    sort_keys=True,
                    separators=(",", ":"),
                ),
                "error": error,
            }
        )
    if not rows:
        raise ImportError(f"{root} contains no APX case.json files")
    return rows


def find_run_id(text: str) -> str:
    try:
        value = json.loads(text)
    except json.JSONDecodeError:
        value = None

    def visit(item: Any) -> str:
        if isinstance(item, Mapping):
            for wanted in ("run_id", "run.id", "runId"):
                child = item.get(wanted)
                if isinstance(child, (str, int)):
                    return str(child)
            for key, child in item.items():
                found = visit(child)
                if found:
                    return found
            child = item.get("id")
            if isinstance(child, (str, int)):
                return str(child)
        elif isinstance(item, list):
            for child in item:
                found = visit(child)
                if found:
                    return found
        return ""

    found = visit(value)
    if found:
        return found
    match = RUN_ID_RE.search(text)
    return match.group(1) if match else ""


def apx_call(apx: str, args: Sequence[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [apx, *args], text=True, capture_output=True, check=False
    )


def import_rows(
    rows: list[dict[str, Any]], *, apx: str, source: Path | None
) -> None:
    for row in rows:
        if row["import_status"] != "ready":
            continue
        imported = apx_call(apx, ("--json", "run", "import", str(row["archive"])))
        combined = f"{imported.stdout}\n{imported.stderr}"
        if imported.returncode != 0:
            row["import_status"] = "failed"
            row["error"] = combined.strip()
            continue
        local_id = find_run_id(combined) or str(row["apx_run_id"])
        if not local_id:
            row["import_status"] = "failed"
            row["error"] = "APX import succeeded without a discoverable run ID"
            continue
        row["local_run_id"] = local_id
        renamed = apx_call(apx, ("run", "rename", local_id, str(row["apx_run_name"])))
        if renamed.returncode != 0:
            row["import_status"] = "failed"
            row["error"] = f"rename failed: {renamed.stderr or renamed.stdout}".strip()
            continue
        if source is not None:
            updated = apx_call(apx, ("run", "update", local_id, "--source", str(source)))
            if updated.returncode != 0:
                row["import_status"] = "failed"
                row["error"] = f"source update failed: {updated.stderr or updated.stdout}".strip()
                continue
        row["import_status"] = "imported"


def write_outputs(rows: Sequence[Mapping[str, Any]], manifest: Path, context: Path) -> None:
    manifest.parent.mkdir(parents=True, exist_ok=True)
    with manifest.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=FIELDS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    context.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "runs": [dict(row) for row in rows],
                "correlation": (
                    "Join Performix/MCP run.id to local_run_id (or apx_run_id before import), "
                    "then join apx_run_id to aggregate trials.csv. comparison_group_id groups "
                    "the queue winners for one scenario/recipe/repeat."
                ),
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )


def resume_imported(rows: list[dict[str, Any]], manifest: Path) -> None:
    if not manifest.is_file():
        return
    with manifest.open(newline="", encoding="utf-8") as stream:
        previous = {
            row.get("archive_sha256", ""): row
            for row in csv.DictReader(stream)
            if row.get("archive_sha256") and row.get("import_status") == "imported"
        }
    for row in rows:
        if row.get("import_status") != "ready":
            continue
        prior = previous.get(str(row.get("archive_sha256") or ""))
        if prior is None:
            continue
        row["local_run_id"] = prior.get("local_run_id", "")
        row["import_status"] = "imported"


def apply_quality_gate(rows: list[dict[str, Any]], trials_csv: Path) -> None:
    if not trials_csv.is_file():
        raise ImportError(f"trials CSV does not exist: {trials_csv}")
    with trials_csv.open(newline="", encoding="utf-8") as stream:
        trials = list(csv.DictReader(stream))
    authoritative_ids = {
        str(row.get("apx_run_id") or "")
        for row in trials
        if str(row.get("authoritative") or "").lower() == "true" and row.get("apx_run_id")
    }
    if not authoritative_ids:
        raise ImportError(f"{trials_csv} contains no authoritative APX run IDs")
    for row in rows:
        if row["import_status"] == "ready" and row["apx_run_id"] not in authoritative_ids:
            row["import_status"] = "excluded"
            row["error"] = "not authoritative in trials.csv"


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("results_root", type=Path)
    parser.add_argument("--manifest", type=Path)
    parser.add_argument("--context", type=Path)
    parser.add_argument("--import", dest="do_import", action="store_true")
    parser.add_argument("--apx", default=shutil.which("apx") or "apx")
    parser.add_argument("--source", type=Path)
    parser.add_argument(
        "--trials-csv",
        type=Path,
        help="import only runs marked authoritative in aggregate trials.csv",
    )
    parser.add_argument(
        "--force-import",
        action="store_true",
        help="ignore imported rows in an existing manifest and import the archives again",
    )
    args = parser.parse_args(argv)
    try:
        root = args.results_root.resolve()
        manifest = args.manifest or root / "apx-import-manifest.csv"
        context = args.context or root / "performix-mcp-context.json"
        rows = discover(root)
        if args.trials_csv is not None:
            apply_quality_gate(rows, args.trials_csv.resolve())
        if not args.force_import:
            resume_imported(rows, manifest.resolve())
        if args.do_import:
            if shutil.which(args.apx) is None and not Path(args.apx).is_file():
                raise ImportError(f"APX executable not found: {args.apx}")
            source = args.source.resolve() if args.source is not None else None
            if source is not None and not source.is_dir():
                raise ImportError(f"source directory does not exist: {source}")
            import_rows(rows, apx=args.apx, source=source)
        write_outputs(rows, manifest.resolve(), context.resolve())
        counts: dict[str, int] = {}
        for row in rows:
            counts[str(row["import_status"])] = counts.get(str(row["import_status"]), 0) + 1
        print(
            json.dumps(
                {
                    "runs": len(rows),
                    "by_status": counts,
                    "manifest": str(manifest.resolve()),
                    "mcp_context": str(context.resolve()),
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 1 if args.do_import and counts.get("failed", 0) else 0
    except (OSError, json.JSONDecodeError, ImportError) as exc:
        print(f"import_performix_runs: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
