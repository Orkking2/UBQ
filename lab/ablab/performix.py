"""Import a study's fetched APX captures into the local Arm Performix app.

Each run keeps the name it was given on the cluster,
<study>.<cluster>.<cell>.<arm>.<recipe>.r<replicate>, so the control and
candidate captures of one cell sort side by side, and each run points at its
own arm's frozen source tree for line attribution. Imports are remembered by
archive hash, so an interrupted import resumes where it stopped.
"""
from __future__ import annotations

import json
import re
import shutil
from pathlib import Path

from .collectors import APX_RECIPES
from .common import read_json_or, run, sha256_file, tail, write_json

DEFAULT_APX = "/Applications/Arm Performix.app/Contents/assets/apx/apx"
RUN_ID = re.compile(r"(?:Run ID|run[_ .-]?id)[\"':= ]+([A-Za-z0-9._-]+)", re.I)


def find_run_id(text):
    """The imported run's id, from `apx --json run import` output or plain text."""
    def visit(item):
        if isinstance(item, dict):
            for key in ("run_id", "runId", "run.id", "new_id"):
                value = item.get(key)
                # APX 1.20 answers `run import` with {"data": {"new_id": {"value": ID}}}.
                if isinstance(value, dict) and isinstance(value.get("value"), (str, int)):
                    return str(value["value"])
                if isinstance(value, (str, int)):
                    return str(value)
            for child in item.values():
                found = visit(child)
                if found:
                    return found
            if isinstance(item.get("id"), (str, int)):
                return str(item["id"])
        elif isinstance(item, list):
            for child in item:
                found = visit(child)
                if found:
                    return found
        return ""
    for line in [text] + text.splitlines():
        try:
            found = visit(json.loads(line))
        except ValueError:
            continue
        if found:
            return found
    match = RUN_ID.search(text)
    return match.group(1) if match else ""


def captures(directory, clusters):
    for cluster in clusters:
        dep = Path(directory) / cluster
        states = read_json_or(dep / "state.json", {"tasks": {}})["tasks"]
        for task in read_json_or(dep / "plan.json", {"tasks": []})["tasks"]:
            if task["kind"] != "deep" or task["collector"] not in APX_RECIPES:
                continue
            attempt_dir = dep / "tasks" / task["id"] / f"a{states.get(task['id'], {}).get('attempts', 0)}"
            for arm, record in read_json_or(attempt_dir / "result.json", {}).get("arms", {}).items():
                metrics = record.get("metrics") or {}
                archive = attempt_dir / f"{arm}-{task['collector']}" / str(metrics.get("archive"))
                if metrics.get("archive") and archive.is_file():
                    yield {"cluster": cluster, "cell": task["cell"], "arm": arm, "recipe": task["collector"],
                           "name": metrics["run_name"], "archive": archive}


def local_runs(apx):
    """{name: id} of the runs the local app holds, from `apx --json run list`."""
    listed = run([apx, "--json", "run", "list"], timeout=120)
    if listed.returncode:
        return {}
    try:
        runs = json.loads(listed.stdout).get("data", {}).get("runs", [])
    except ValueError:
        return {}
    return {r["name"]: r["id"] for r in runs if isinstance(r, dict) and r.get("name") and r.get("id")}


def import_study(study, directory, clusters, local_root, apx=None, dry_run=False):
    apx = apx or shutil.which("apx") or DEFAULT_APX
    found = sorted(captures(directory, clusters), key=lambda c: c["name"])
    if not found:
        print("no fetched APX captures in this study; run ./ab fetch after its deep stage")
        return
    if not dry_run and not (Path(apx).is_file() or shutil.which(apx)):
        raise RuntimeError(f"no APX CLI at {apx}; pass --apx PATH")
    manifest_path = Path(directory) / "performix-import.json"
    manifest = read_json_or(manifest_path, {})
    sources = {arm["name"]: Path(local_root) / "sources" / arm["source"] / "src" for arm in study["arms"]}
    present = {} if dry_run else local_runs(apx)
    for capture in found:
        key = sha256_file(capture["archive"])
        if key in manifest:
            print(f"already imported  {capture['name']}")
            continue
        if dry_run:
            print(f"would import      {capture['name']}")
            continue
        # A capture renamed on the cluster keeps its name through import, so a
        # run already in the app (an import this record missed) is adopted.
        run_id = present.get(capture["name"], "")
        if not run_id:
            imported = run([apx, "--json", "run", "import", str(capture["archive"])], timeout=900)
            run_id = find_run_id(imported.stdout + "\n" + imported.stderr)
            if imported.returncode or not run_id:
                print(f"FAILED            {capture['name']}: {tail(imported.stderr or imported.stdout, 300)}")
                continue
            run([apx, "run", "rename", run_id, capture["name"]], timeout=120)
        source = sources.get(capture["arm"])
        if source is not None and source.is_dir():
            run([apx, "run", "update", run_id, "--source", str(source)], timeout=120)
        manifest[key] = {**{k: str(v) for k, v in capture.items()}, "local_run_id": run_id}
        write_json(manifest_path, manifest)
        print(f"imported          {capture['name']} (run {run_id})")
    print(f"import record: {manifest_path}")
