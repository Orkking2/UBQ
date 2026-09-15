"""./ab: A/B studies of the queue on the BSC clusters, from launch to explanation.

Stage 1 measures throughput. In every allocation each arm first passes an
exact-once check, then all arms run identical fixed work in interleaved
pairs. Stage 2 takes each candidate's biggest wins and losses and captures
both arms there with perf and, on Grace, Arm Performix. See lab/README.md.

  ./ab sync [HOST]             mirror this worktree to HOST (default: stg)
  ./ab new NAME [--src DIR]    create a study, optionally copying a queue module
  ./ab queues                 list registered queue implementations
  ./ab launch SPEC             freeze the arms, build, measure, deep-dive
  ./ab status [STUDY]          advance and show every deployment of a study
  ./ab fetch [STUDY]           copy results home
  ./ab report [STUDY]          effects, rankings, memory and hardware comparisons
  ./ab plot [STUDY]            scaling, geometry and effect figures from fetched results
  ./ab deep [STUDY] --cell C   deep-dive more cells after stage 1
  ./ab cancel [STUDY]          cancel a study's jobs
  ./ab list                    studies launched from this checkout
  ./ab performix [STUDY]       import fetched APX captures into Arm Performix

STUDY defaults to the most recent launch and may be any unique prefix.
Every command is safe to interrupt and rerun.
"""
from __future__ import annotations

import argparse
import concurrent.futures
import json
import os
import shlex
import shutil
import signal
import sys
import time
from pathlib import Path

from . import spec as specs
from . import variants
from .common import canonical, digest_text, emit, read_json, read_json_or, write_json
from .scheduler import SubmitError
from .snapshot import SnapshotError
from .transport import TransportError

LAB = Path(__file__).resolve().parents[1]
REPO = LAB.parent
RESULTS = REPO / "results"
# Local mirror of the remote root: frozen sources, and builds for --local runs.
LOCAL_ROOT = RESULTS / ".lab"
LATEST = RESULTS / ".latest"
ENDED = {"done", "failed", "superseded", "cancelled"}
# Raw perf captures stay on the cluster; their summaries come home.
FETCH_EXCLUDES = ["--exclude=perf.data*", "--exclude=lock", "--exclude=.*.tmp"]


class LabError(RuntimeError):
    pass


def load_config():
    return read_json(LAB / "config.json")


def find_spec(argument):
    path = Path(argument)
    if path.is_file():
        return path
    named = LAB / "studies" / f"{argument}.json"
    if named.is_file():
        return named
    raise LabError(f"no spec {argument!r}: neither {path} nor {named} exists")


def fifo_class():
    """plot_style's strict/relaxed classification, which every queue must have."""
    sys.path.insert(0, str(REPO / "scripts"))
    try:
        from plot_style import fifo_ordering
    except ImportError:
        return None
    rows = variants.entries(REPO)

    def classify(queue):
        try:
            return variants.lookup(rows, queue)["fifo"]
        except KeyError:
            return fifo_ordering(queue)
    return classify


def tool_files():
    files = {"ab.py": LAB / "ab.py"}
    for path in sorted((LAB / "ablab").iterdir()):
        if path.suffix in (".py", ".sbatch"):
            files[f"ablab/{path.name}"] = path
    return files


def prepare(spec_path, config, quick=False, again=False):
    """Freeze the arms and the tool; return (study, local study directory)."""
    from . import snapshot

    raw = read_json(spec_path)
    if quick:
        raw = specs.quick(raw)
    spec = specs.validate(raw, fifo_class())
    vendor = snapshot.vendor_hash(REPO)
    frozen, arms = {}, []
    for arm in spec["arms"]:
        if arm["ref"] not in frozen:
            frozen[arm["ref"]] = snapshot.snapshot(REPO, arm["ref"], LOCAL_ROOT / "sources", vendor)
        arms.append({**frozen[arm["ref"]], "name": arm["name"], "features": sorted(arm["features"]),
                     "workload": specs.arm_workload(spec, arm), "family": specs.arm_family(spec, arm)})
    if len({arm["harness"] for arm in arms}) > 1 and not spec["allow_harness_mismatch"]:
        raise LabError(
            "the arms differ in the measurement harness (src/bench_harness/ or "
            "src/bin/bench_hardware.rs or build.rs), so they would not be measured the same way. "
            "Commit the harness change to every ref, or set \"allow_harness_mismatch\": true.")
    files = tool_files()
    tool = digest_text(canonical({name: digest_text(path.read_text()) for name, path in files.items()}))
    identity = {"arms": [{key: arm[key] for key in ("name", "source", "features", "workload")} for arm in arms],
                "vendor": vendor, "tool": tool, "config": config["clusters"],
                "nonce": time.time() if again else None}
    study_id = specs.study_id(spec, identity)
    directory = RESULTS / study_id
    if (directory / "study.json").exists():
        return read_json(directory / "study.json"), directory
    for name, path in files.items():
        target = directory / "tool" / name
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(path, target)
    study = {"id": study_id, "spec": spec, "arms": arms, "vendor": vendor, "tool": tool,
             "config": {"clusters": config["clusters"]}, "created": time.time(),
             "spec_path": str(Path(spec_path).resolve().relative_to(REPO))
             if Path(spec_path).resolve().is_relative_to(REPO) else str(spec_path)}
    write_json(directory / "study.json", study)
    return study, directory


def expected_minutes(spec, arms, task):
    throughput = spec["throughput"]
    if task["kind"] == "throughput":
        return (arms * (15 + throughput["pairs"] * (throughput["run_seconds"] + 6)) + 40) / 60
    return (arms * len(spec["memory"]["depths"]) * 25 + 40) / 60


def describe(study, clusters, config):
    from .deploy import make_plan

    spec = study["spec"]
    print(f"Study {study['id']}")
    if spec.get("question"):
        print(f"  {spec['question']}")
    for index, arm in enumerate(study["arms"]):
        if arm["commit"] == specs.WORKTREE:
            where = f"WORKTREE on {arm['head'][:9]}" + (f", {len(arm['dirty'])} changed" if arm["dirty"] else "")
        else:
            where = f"{arm['ref']} ({arm['commit'][:9]})"
        features = f" +{','.join(arm['features'])}" if arm["features"] else ""
        print(f"  {'control' if index == 0 else 'arm':<7} {arm['name']:<10} {where:<34} "
              f"{arm['workload']['queue']}{features}  source {arm['source'][:10]}")
    families = len({arm.get("family") or specs.arm_family(spec, arm) for arm in study["arms"]})
    deep = spec["deep"]
    for cluster in clusters:
        cluster_config = dict(config["clusters"][cluster])
        cluster_config["cores"] = cluster_config.get("cores") or os.cpu_count()
        plan = make_plan(study, cluster, cluster_config)
        tasks = plan["tasks"]
        expected = sum(expected_minutes(spec, len(study["arms"]), task) for task in tasks) / 60
        limit = sum(task["minutes"] for task in tasks) / 60
        cells = len({task["cell"] for task in tasks})
        collectors = specs.collectors_for(spec, cluster, cluster_config)
        deep_cells = len(deep["cells"]) or families * (deep["winners"] + deep["losers"])
        print(f"  {cluster:<6} stage 1: {cells} cells x {spec['throughput']['replicates']} replicates = "
              f"{len(tasks)} exclusive allocations, ~{expected:.1f} node-h (limits {limit:.1f})")
        if collectors and deep["auto"] or deep["cells"]:
            print(f"         stage 2: up to {deep_cells} cells x {len(collectors)} collectors "
                  f"x {deep['replicates']} replicate(s): {', '.join(collectors)}")
        if plan["skipped_scenarios"]:
            print(f"         skipped (more threads than {cluster_config['cores']} cores): "
                  f"{', '.join(plan['skipped_scenarios'])}")


def remote_command(study_dir, *args):
    words = [f"{study_dir}/tool/ab.py", "remote", *args]
    return "PYTHONDONTWRITEBYTECODE=1 python3 " + " ".join(shlex.quote(str(w)) for w in words)


def remote_study_dir(config, study):
    return f"{config['remote_root']}/studies/{study['id']}"


def upload(study, directory, config):
    """Copy vendor, arm sources and the frozen study to the shared GPFS, once each."""
    from .transport import rsync, ssh

    host, root = config["transfer_host"], config["remote_root"]
    needed = [f"vendor/{study['vendor']}"] + [f"sources/{s}" for s in sorted({a["source"] for a in study["arms"]})]
    probe = (f"cd {shlex.quote(root)} 2>/dev/null && for p in {' '.join(needed)}; do "
             f"[ -e \"$p/.complete\" ] && echo \"$p\"; done; true")
    present = set(ssh(host, probe).split())
    for path in needed:
        if path in present:
            continue
        local = REPO / "vendor" if path.startswith("vendor/") else LOCAL_ROOT / path
        print(f"  uploading {path}")
        rsync(f"{local}/", f"{host}:{root}/{path}/", filters=["--mkpath"])
    target = remote_study_dir(config, study)
    rsync(f"{directory / 'tool'}/", f"{host}:{target}/tool/", filters=["--mkpath"])
    rsync(f"{directory / 'study.json'}", f"{host}:{target}/study.json", filters=["--mkpath"])
    # Completion markers last: a directory without one is re-sent next time.
    markers = [f"{root}/{path}/.complete" for path in needed if path not in present]
    if markers:
        ssh(host, "touch " + " ".join(shlex.quote(m) for m in markers))


def call_cluster(study, directory, cluster, config, command, *extra):
    """Run a deployment command for `cluster` and return its status document."""
    if cluster == "local":
        from . import deploy
        dep = deploy.Deployment(directory / "local")
        if command == "advance":
            return deploy.advance(dep)
        if command == "cancel":
            return deploy.cancel(dep)
        return deploy.request_deep(dep, *extra)
    from .transport import remote_json
    study_dir = remote_study_dir(config, study)
    args = [command, f"{study_dir}/{cluster}"]
    if command == "deep":
        cells, collectors = extra
        args += [word for cell in cells for word in ("--cell", cell)]
        args += [word for collector in collectors or () for word in ("--collector", collector)]
    return remote_json(config["clusters"][cluster]["host"], remote_command(study_dir, *args))


def in_parallel(function, clusters):
    """{cluster: function(cluster)}; a failure becomes that cluster's error status."""
    def guarded(cluster):
        try:
            return function(cluster)
        except (TransportError, LabError, SubmitError, specs.SpecError, OSError, TimeoutError) as exc:
            return {"cluster": cluster, "error": str(exc)}
    with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, len(clusters))) as pool:
        return dict(zip(clusters, pool.map(guarded, clusters)))


def record(directory, study, statuses):
    launch = read_json_or(directory / "launch.json", {"id": study["id"], "created": time.time(), "clusters": {}})
    for cluster, status in statuses.items():
        entry = launch["clusters"].setdefault(cluster, {"deployed": time.time()})
        if status.get("phase"):
            entry.update(phase=status["phase"], updated=time.time())
    write_json(directory / "launch.json", launch)
    LATEST.write_text(study["id"] + "\n")


def resolve_study(name):
    if name is None:
        if not LATEST.exists():
            raise LabError("no study has been launched from this checkout; see ./ab launch")
        name = LATEST.read_text().strip()
    directory = RESULTS / name
    if not (directory / "study.json").exists():
        matches = sorted(p.name for p in RESULTS.glob(f"{name}*") if (p / "study.json").exists())
        if len(matches) != 1:
            raise LabError(f"{len(matches)} studies match {name!r}" + (f": {', '.join(matches)}" if matches else ""))
        directory = RESULTS / matches[0]
    return read_json(directory / "study.json"), directory


def launched_clusters(directory, only=None):
    clusters = list(read_json_or(directory / "launch.json", {"clusters": {}})["clusters"])
    if only:
        unknown = set(only) - set(clusters)
        if unknown:
            raise LabError(f"not deployed on {sorted(unknown)}; deployed on {clusters}")
        return list(only)
    if not clusters:
        raise LabError("this study was prepared but never deployed; run ./ab launch again")
    return clusters


def fmt_effect(row):
    if row.get("effect") is None:
        return "no pairs yet"
    text = f"{row['effect']:.3f}x"
    if row.get("ci_low") is not None:
        text += f" [{row['ci_low']:.3f}, {row['ci_high']:.3f}]"
    return text


MAX_STATUS_ROWS = 48


def print_status(cluster, status, study=None):
    if "error" in status:
        print(f"{cluster}: ERROR {status['error']}")
        return
    marker = status.get("marker")
    headline = f"{status['study']} · {cluster} · {status['phase']}"
    if marker:
        headline += f" ({marker.get('by') or marker.get('reason', '')})"
    print(headline)
    build = status["build"]
    built = "cached" if build["job"] is None and status["phase"] != "building" else \
        f"{build['status']} (job {build['job']}, attempt {build['attempts']})"
    print(f"  build    {built}")
    order = ("done", "running", "pending", "new", "failed")
    for stage, counts in status["counts"].items():
        parts = [f"{counts[s]} {s}" for s in order if counts.get(s)]
        print(f"  {stage:<8} {' · '.join(parts)}")
    summary = status.get("summary") or {}
    effects = [row for row in summary.get("effects", []) if row.get("effect") is not None or row["verdict"] == "integrity"]
    best = summary.get("best") or []
    if len(best) > 1 or any(row.get("cells") for row in best):
        print("  best variation per family (geometric mean effect over measured cells):")
        for row in best:
            if row.get("cells") is None:
                print(f"    {row['family']:<18} {row['arm']:<24} control")
            else:
                print(f"    {row['family']:<18} {row['arm']:<24} {row['geomean_effect']:.3f}x  "
                      f"{row['wins']} wins, {row['losses']} losses of {row['cells']} cells")
    if effects:
        shown = effects
        if study is not None and len(study["arms"]) > 3:
            # Many arms: one line per cell and family, its best variation.
            from . import analysis
            shown = [row for per_cell in analysis.best_per_cell(study, effects).values() for row in per_cell.values()]
            shown += [row for row in effects if row["verdict"] == "integrity"]
            print("  throughput, each family's best variation / control (95% CI):")
        else:
            print("  throughput, candidate / control (95% CI):")
        shown = sorted(shown, key=lambda r: -(r["effect"] or 0))
        for row in shown[:MAX_STATUS_ROWS]:
            print(f"    {row['candidate']:<24} {row['cell']:<16} {fmt_effect(row):<28} {row['verdict']}")
        if len(shown) > MAX_STATUS_ROWS:
            print(f"    ... {len(shown) - MAX_STATUS_ROWS} more cells; see ./ab report")
    for entry in summary.get("selection") or []:
        print(f"  deep     {entry['cell']}: {'; '.join(entry['reasons'])}")
    for failure in status.get("failed", []):
        print(f"  failed   {failure['task']}: {failure['error'][:160]}")
    for note in status.get("notes", [])[-3:]:
        print(f"  note     {note}")
    if status.get("superseded"):
        print(f"  cancelled earlier work: {', '.join(status['superseded'])}")


def run_local(study, directory):
    """Deploy on this machine and drive it to completion in the foreground."""
    from . import deploy

    vendor = LOCAL_ROOT / "vendor" / study["vendor"]
    if not vendor.exists():
        vendor.parent.mkdir(parents=True, exist_ok=True)
        vendor.symlink_to(REPO / "vendor")
    status = deploy.deploy(directory, "local", keep_running=True, root=LOCAL_ROOT)
    dep = deploy.Deployment(directory / "local")
    previous = None
    while status["phase"] not in ENDED:
        progress = canonical([status["phase"], status["counts"], status["build"]])
        if progress == previous:
            raise LabError(f"the local deployment stopped making progress; see {dep.path / 'logs'}")
        previous = progress
        status = deploy.advance(dep)
    return status


def cmd_sync(args):
    from .transport import rsync, ssh

    config = load_config()
    destination = config["sync"].get(args.host)
    if destination is None:
        raise LabError(f"no sync destination for {args.host!r}; add it to lab/config.json: {sorted(config['sync'])}")
    # .gitignore decides what is mirrored, except that the vendored crates and
    # the Cargo config pointing at them are needed to build on the far side.
    # Excluded remote files (results, targets) are never deleted.
    filters = ["--include=/.cargo/", "--include=/.cargo/**", "--include=/vendor/", "--include=/vendor/**",
               "--exclude=/.git/", "--filter=:- .gitignore"]
    ssh(args.host, f"mkdir -p {destination}")
    output = rsync(f"{REPO}/", f"{args.host}:{destination}/", delete=True, filters=filters, dry_run=args.dry_run)
    print(output.strip())
    print(f"{'would mirror' if args.dry_run else 'mirrored'} {REPO} -> {args.host}:{destination}")


def cmd_new(args):
    if args.src:
        if args.base is not None or args.cand != specs.WORKTREE:
            raise LabError("--src compares two WORKTREE modules; omit --base and --cand")
        target, path = variants.create(REPO, args.name, args.src, queue=args.queue, force=args.force)
        print(f"created {target.relative_to(REPO)} and {path.relative_to(REPO)}")
        print(f"edit {target.relative_to(REPO)}/, then ./ab launch {args.name} --local --quick")
        return
    path = LAB / "studies" / f"{args.name}.json"
    if path.exists() and not args.force:
        raise LabError(f"{path} exists; pass --force to overwrite")
    raw = {"name": args.name,
           "question": "What does the candidate change, and where should it help?",
           "arms": [{"name": "base", "ref": args.base or "HEAD"}, {"name": "cand", "ref": args.cand}],
           "workload": {"queue": args.queue or "ubq-v8-s4096"}}
    specs.validate(raw, fifo_class())
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(raw, indent=2) + "\n")
    print(f"wrote {path.relative_to(REPO)}; every other field takes its default (lab/README.md)")


def cmd_queues(args):
    for entry in variants.entries(REPO):
        geometry = "[-s<SLOTS>[-v<BLOCKS>]]" if entry["adapter"] == "v8" else ""
        print(f"{variants.queue_name(entry)}{geometry}\t{entry['source']}/\t{entry['adapter']}\t{entry['fifo']}")


def cmd_launch(args):
    config = load_config()
    study, directory = prepare(find_spec(args.spec), config, quick=args.quick, again=args.again)
    clusters = ["local"] if args.local else (args.cluster or study["spec"]["clusters"])
    unknown = [c for c in clusters if c not in config["clusters"]]
    if unknown:
        raise LabError(f"unknown clusters {unknown}; configured: {sorted(config['clusters'])}")
    if "local" in clusters and len(clusters) > 1:
        raise LabError("run --local studies on their own")
    describe(study, clusters, config)
    if args.dry_run:
        print(f"dry run: frozen in {directory.relative_to(REPO)}; nothing launched")
        return 0
    if clusters == ["local"]:
        statuses = {"local": run_local(study, directory)}
    else:
        print(f"uploading to {config['transfer_host']}:{config['remote_root']}")
        upload(study, directory, config)
        study_dir = remote_study_dir(config, study)
        from .transport import remote_json
        extra = ["--keep-running"] if args.keep_running else []
        statuses = in_parallel(lambda c: remote_json(
            config["clusters"][c]["host"], remote_command(study_dir, "deploy", study_dir, c, *extra)), clusters)
    record(directory, study, {c: s for c, s in statuses.items() if "error" not in s})
    for cluster, status in statuses.items():
        print_status(cluster, status, study)
    if any("error" in s for s in statuses.values()):
        print("ab: some clusters failed; rerun the same command to retry them", file=sys.stderr)
        return 1
    if clusters == ["local"]:
        return cmd_report(argparse.Namespace(study=study["id"]))
    print(f"\nFollow with ./ab status {study['id']} --watch")
    return 0


def cmd_status(args):
    config = load_config()
    study, directory = resolve_study(args.study)
    clusters = launched_clusters(directory)
    while True:
        statuses = in_parallel(lambda c: call_cluster(study, directory, c, config, "advance"), clusters)
        record(directory, study, {c: s for c, s in statuses.items() if "error" not in s})
        if args.watch:
            print(f"\n--- {time.strftime('%H:%M:%S')}")
        for cluster, status in statuses.items():
            print_status(cluster, status, study)
        finished = all(status.get("phase") in ENDED for status in statuses.values())
        if not args.watch or finished:
            break
        time.sleep(args.watch * 60)
    if args.watch and finished:
        cmd_fetch(argparse.Namespace(study=study["id"], all=False))
        cmd_report(argparse.Namespace(study=study["id"]))
        try:
            cmd_plot(argparse.Namespace(study=study["id"], cluster=None))
        except LabError as exc:
            print(f"plots skipped: {exc}")
    return 0


def cmd_fetch(args):
    from .transport import rsync

    config = load_config()
    study, directory = resolve_study(args.study)
    for cluster in launched_clusters(directory):
        if cluster == "local":
            continue
        source = f"{config['transfer_host']}:{remote_study_dir(config, study)}/{cluster}/"
        rsync(source, f"{directory / cluster}/", filters=[] if args.all else FETCH_EXCLUDES)
        print(f"fetched {cluster} -> {(directory / cluster).relative_to(REPO)}")
    return 0


def cmd_report(args):
    from . import report

    study, directory = resolve_study(args.study)
    path, headline = report.write(study, directory, launched_clusters(directory))
    print("\n".join(headline))
    print(f"\nreport: {path.relative_to(REPO)}")
    return 0


def cmd_plot(args):
    import subprocess

    study, directory = resolve_study(args.study)
    clusters = launched_clusters(directory, args.cluster)
    try:
        import matplotlib  # noqa: F401
    except ImportError:
        venv = REPO / ".venv" / "bin" / "python"
        if venv.exists() and Path(sys.executable).resolve() != venv.resolve():
            argv = ["plot", study["id"], *(f"--cluster={c}" for c in args.cluster or [])]
            return subprocess.call([str(venv), str(LAB / "ab.py"), *argv])
        raise LabError("matplotlib is not installed for this python; pip install -r requirements-plot.txt")
    from . import plot

    paths = plot.write(study, directory, clusters)
    if not paths:
        raise LabError("no fetched throughput results to plot; run ./ab fetch first")
    for path in paths:
        print(f"wrote {path.relative_to(REPO)}")
    return 0


def cmd_deep(args):
    config = load_config()
    study, directory = resolve_study(args.study)
    clusters = launched_clusters(directory, args.cluster)
    statuses = in_parallel(lambda c: call_cluster(study, directory, c, config, "deep", args.cell,
                                                  args.collector), clusters)
    for cluster, status in statuses.items():
        print_status(cluster, status)
    return 0


def cmd_cancel(args):
    config = load_config()
    study, directory = resolve_study(args.study)
    clusters = launched_clusters(directory, args.cluster)
    statuses = in_parallel(lambda c: call_cluster(study, directory, c, config, "cancel"), clusters)
    record(directory, study, {c: s for c, s in statuses.items() if "error" not in s})
    for cluster, status in statuses.items():
        print_status(cluster, status)
    return 0


def cmd_list(args):
    launches = sorted(RESULTS.glob("*/launch.json"), key=lambda p: read_json(p).get("created", 0))
    if not launches:
        print("no studies yet; see ./ab new and ./ab launch")
    latest = LATEST.read_text().strip() if LATEST.exists() else None
    for path in launches:
        launch = read_json(path)
        when = time.strftime("%Y-%m-%d %H:%M", time.localtime(launch.get("created", 0)))
        clusters = ", ".join(f"{c}: {e.get('phase', '?')}" for c, e in launch["clusters"].items())
        print(f"{'*' if launch['id'] == latest else ' '} {launch['id']:<44} {when}  {clusters}")
    return 0


def cmd_performix(args):
    from . import performix

    study, directory = resolve_study(args.study)
    performix.import_study(study, directory, launched_clusters(directory), LOCAL_ROOT,
                           apx=args.apx, dry_run=args.dry_run)
    return 0


def build_parser():
    parser = argparse.ArgumentParser(prog="ab", description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True, metavar="COMMAND")

    p = sub.add_parser("sync", help="mirror this worktree to a host (default: stg)")
    p.add_argument("host", nargs="?", default="stg")
    p.add_argument("--dry-run", action="store_true", help="list what would change")
    p.set_defaults(func=cmd_sync)

    p = sub.add_parser("new", help="create a study, optionally forking a queue module")
    p.add_argument("name")
    p.add_argument("--src", help="copy this registered source directory to src/NAME and compare both modules")
    p.add_argument("--base", help="control arm's git revision (default: HEAD without --src)")
    p.add_argument("--cand", default=specs.WORKTREE, help="candidate arm (default: the working tree)")
    p.add_argument("--queue", help="queue/geometry to compare (with --src, must belong to the source)")
    p.add_argument("--force", action="store_true", help="replace an existing study spec; never overwrite a source module")
    p.set_defaults(func=cmd_new)

    p = sub.add_parser("queues", help="list registered queue modules, adapters and FIFO classes")
    p.set_defaults(func=cmd_queues)

    p = sub.add_parser("launch", help="freeze the arms, build, measure, then deep-dive")
    p.add_argument("spec", help="a spec file, or the name of one in lab/studies/")
    p.add_argument("--cluster", action="append", help="deploy only here (repeatable); default: the spec's")
    p.add_argument("--local", action="store_true", help="run on this machine, without Slurm")
    p.add_argument("--quick", action="store_true", help="a seconds-long version of the spec")
    p.add_argument("--again", action="store_true", help="relaunch identical inputs as a new study")
    p.add_argument("--keep-running", action="store_true", help="do not cancel earlier studies")
    p.add_argument("--dry-run", action="store_true", help="freeze and show the plan, launch nothing")
    p.set_defaults(func=cmd_launch)

    p = sub.add_parser("status", help="advance and show a study's deployments")
    p.add_argument("study", nargs="?")
    p.add_argument("--watch", nargs="?", const=10.0, type=float, metavar="MINUTES",
                   help="poll until done (default every 10 minutes), then fetch and report")
    p.set_defaults(func=cmd_status)

    p = sub.add_parser("fetch", help="copy results home (raw perf.data stays remote)")
    p.add_argument("study", nargs="?")
    p.add_argument("--all", action="store_true", help="include raw perf.data captures")
    p.set_defaults(func=cmd_fetch)

    p = sub.add_parser("report", help="write results/<study>/report.md and CSVs")
    p.add_argument("study", nargs="?")
    p.set_defaults(func=cmd_report)

    p = sub.add_parser("plot", help="write results/<study>/plots/*.png from fetched results")
    p.add_argument("study", nargs="?")
    p.add_argument("--cluster", action="append")
    p.set_defaults(func=cmd_plot)

    p = sub.add_parser("deep", help="deep-dive more cells after stage 1")
    p.add_argument("study", nargs="?")
    p.add_argument("--cell", action="append", required=True, help="e.g. 72p72c-b256 (repeatable)")
    p.add_argument("--cluster", action="append")
    p.add_argument("--collector", action="append", help="default: the cluster's collectors")
    p.set_defaults(func=cmd_deep)

    p = sub.add_parser("cancel", help="cancel a study's jobs")
    p.add_argument("study", nargs="?")
    p.add_argument("--cluster", action="append")
    p.set_defaults(func=cmd_cancel)

    p = sub.add_parser("list", help="studies launched from this checkout")
    p.set_defaults(func=cmd_list)

    p = sub.add_parser("performix", help="import fetched APX captures into Arm Performix")
    p.add_argument("study", nargs="?")
    p.add_argument("--apx", help="path to the apx CLI (default: PATH, then the Performix app)")
    p.add_argument("--dry-run", action="store_true")
    p.set_defaults(func=cmd_performix)
    return parser


def remote_main(argv):
    """Cluster-side commands; each prints one result line for the local CLI."""
    # Finish even if the SSH session that started us drops mid-command.
    signal.signal(signal.SIGHUP, signal.SIG_IGN)
    from . import deploy

    parser = argparse.ArgumentParser(prog="ab.py remote")
    sub = parser.add_subparsers(dest="command", required=True)
    p = sub.add_parser("deploy")
    p.add_argument("study_dir")
    p.add_argument("cluster")
    p.add_argument("--keep-running", action="store_true")
    for name in ("advance", "cancel"):
        sub.add_parser(name).add_argument("deployment")
    p = sub.add_parser("deep")
    p.add_argument("deployment")
    p.add_argument("--cell", action="append", required=True)
    p.add_argument("--collector", action="append")
    args = parser.parse_args(argv)
    try:
        if args.command == "deploy":
            result = deploy.deploy(args.study_dir, args.cluster, keep_running=args.keep_running)
        elif args.command == "advance":
            result = deploy.advance(deploy.Deployment(args.deployment))
        elif args.command == "cancel":
            result = deploy.cancel(deploy.Deployment(args.deployment))
        else:
            result = deploy.request_deep(deploy.Deployment(args.deployment), args.cell, args.collector)
    except Exception as exc:
        result = {"error": f"{type(exc).__name__}: {exc}"}
    emit(result)
    return 0


def main(argv=None):
    argv = list(sys.argv[1:] if argv is None else argv)
    if argv[:1] == ["job"]:
        from .task import job_main
        return job_main(argv[1:])
    if argv[:1] == ["remote"]:
        return remote_main(argv[1:])
    args = build_parser().parse_args(argv)
    try:
        return args.func(args) or 0
    except (LabError, specs.SpecError, SnapshotError, TransportError, SubmitError, OSError, TimeoutError) as exc:
        print(f"ab: {exc}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        print("\nab: interrupted; rerun the same command to continue", file=sys.stderr)
        return 130
