#!/usr/bin/env python3
"""Frozen, bounded queue hardware campaign; Python standard library only.

start runs a lightweight detached submitter on the login node. All builds and
measurements run in Slurm. Submission quota rejection causes backoff, never a
failed dependency or an array concurrency throttle. No remote access is used.
"""
from __future__ import annotations
import argparse
import collections
import csv
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import re
import shlex
import shutil
import signal
import subprocess
import sys
import time
import zipfile

ROOT = Path(__file__).resolve().parents[1]
SCHEMA = "ubq.hardware_campaign.v1"
FEATURES = "bench_rbbq,bench_lfqueue,bench_moodycamel"
CLUSTERS = {"grace": (144, "ngpp", "ngp_bsccs"), "mn5": (112, "gpp", "gp_bsccs")}
TERMINAL = {"COMPLETED", "FAILED", "CANCELLED", "TIMEOUT", "OUT_OF_MEMORY", "NODE_FAIL", "PREEMPTED", "BOOT_FAIL", "DEADLINE", "REVOKED"}
QUOTA = ("QOSMaxSubmit", "AssocMaxSubmit", "JobSubmit", "job submit limit", "maximum number of jobs")
STAT = {"stat-cpu": ["cycles:u", "instructions:u"], "stat-cache": ["cache-references:u", "cache-misses:u"],
        "stat-tlb": ["dTLB-loads:u", "dTLB-load-misses:u"]}
# `memory_access` is deliberately absent. It needs Arm SPE, which APX can only
# program after elevating to root on the compute node; BSC does not grant that,
# and the v4 pilot failed with engine.tool.service.ELEVATE_PRIVILEGES_FAILED
# after v2 and v3 spent pilot time on the same gap. Record it as an explicit
# coverage gap instead of re-attempting it every campaign.
APX = {name: name for name in ("code_hotspots", "cpu_microarchitecture", "instruction_mix",
                                "system_utilization")}
TERMINAL_JOB_STATES = {"completed", "failed", "skipped", "not_admitted"}
ACTIVE = None
DEADLINE = float("inf")


def apx_timeline_rows(archive_path):
    """Data rows in an APX archive's system-utilization timeline, if it has one."""
    rows = 0
    with zipfile.ZipFile(archive_path) as archive:
        for name in archive.namelist():
            if name.endswith("/timeline.csv"):
                text = archive.read(name).decode("utf-8", errors="replace")
                rows = max(rows, max(0, len(text.splitlines()) - 1))
    return rows


def read(path):
    return json.loads(Path(path).read_text())


def save(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + f".{os.getpid()}.tmp")
    tmp.write_text(json.dumps(value, sort_keys=True, indent=2) + "\n")
    tmp.replace(path)


def digest(path):
    h = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def run(command, *, cwd=ROOT, env=None, timeout=60, stdout_path=None, stderr_path=None):
    """Stream to disk, bound each command by the enclosing job budget, kill descendants."""
    global ACTIVE
    timeout = min(timeout, DEADLINE - time.time())
    if timeout <= 0:
        raise RuntimeError("bundle time budget exhausted")
    if stdout_path is None:
        return subprocess.run(list(map(str, command)), cwd=cwd, env=env, text=True,
                              capture_output=True, timeout=timeout, check=False)
    started = time.time()
    timed_out = False
    with Path(stdout_path).open("w") as out, Path(stderr_path).open("w") as err:
        ACTIVE = subprocess.Popen(list(map(str, command)), cwd=cwd, env=env, text=True,
                                  stdout=out, stderr=err, start_new_session=True)
        try:
            ACTIVE.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            timed_out = True
            stop_child()
        finally:
            code = ACTIVE.returncode
            ACTIVE = None
    save(str(stdout_path) + ".meta.json", {"command": list(map(str, command)), "returncode": code,
         "elapsed_secs": time.time() - started, "timed_out": timed_out})
    if timed_out:
        raise RuntimeError(f"command timed out after {timeout:.1f}s: {shlex.join(list(map(str, command)))}")
    return subprocess.CompletedProcess(command, code, Path(stdout_path).read_text(errors="replace"),
                                       Path(stderr_path).read_text(errors="replace"))


def stop_child():
    if ACTIVE and ACTIVE.poll() is None:
        try:
            os.killpg(ACTIVE.pid, signal.SIGTERM)
            ACTIVE.wait(timeout=5)
        except subprocess.TimeoutExpired:
            os.killpg(ACTIVE.pid, signal.SIGKILL)
            ACTIVE.wait()
        except ProcessLookupError:
            pass


# Offered items per empty-to-empty round.
#
# v3 used 8,388,608 (64 MiB of u64 payload). With the round-control cost now
# near 90 us on a 144-core Grace node, that cap still left the fastest batch
# configurations spending ~8-12% of a round in the harness, because a LUBQ
# batch-256 round at 72p72c completes in about 1.1 ms. 16,777,216 items roughly
# doubles the queue work per rendezvous without approaching the 8 GiB workload
# watchdog: 128 MiB of payload, and about 3.2 GiB at the worst per-item
# footprint the v3 lifecycle panel measured.
ROUND_ITEMS = 16_777_216

# Attempts allowed for a collector capability pilot before its recipe is
# declared unavailable. Each attempt is a fresh capture in the same allocation.
CAPABILITY_ATTEMPTS = 3

# Independent allocations attempted per collector pilot. Retrying inside one
# allocation cannot escape a node whose collector deployment is broken: in v5
# all three in-allocation instruction_mix attempts failed identically on one
# node, costing all 186 of that recipe's captures, while v3 completed 128 of
# 130 instruction_mix captures across many nodes. Separate allocations land on
# different nodes, and a recipe counts as available if any of them succeeds.
CAPABILITY_ALLOCATIONS = 3


def required_ids(job):
    """Every job id a job depends on, flattening any-of groups."""
    ids = []
    for required in job["requires"]:
        ids.extend(required if isinstance(required, list) else [required])
    return ids


def cap_ids(collector):
    """Pilot job ids for one collector, in submission order."""
    return ["cap-" + collector] + [f"cap-{collector}-a{n}" for n in range(1, CAPABILITY_ALLOCATIONS)]

# Sampling period for `perf mem record`. Measured on a Grace node: the SPE
# default writes 510 MB and 24.3M samples for a single six-round capture, while
# 100003 writes 19 MB and 239k samples with no measurable change in workload
# throughput (171.8 vs 175 M handoffs/s unprofiled). Left at the tool default
# on MN5, whose PEBS sampling has not been calibrated here.
SPE_PERIOD = ["-c", "100003"] if platform.machine().startswith("aarch64") else []


def workload(config, scenario, **overrides):
    result = {"queue": config["queue"], "scenario": scenario,
              "batch_size": config.get("batch_size"), "backoff": config.get("backoff") or "crossbeam",
              "mode": "handoff", "round_items": ROUND_ITEMS, "rounds": None, "duration_secs": 3.,
              "warmup_rounds": 2, "memory_depth": 65536, "idle_secs": [1, 10],
              "rss_limit_mib": 8192, "allow_unpinned": False,
              "block_size": 256, "capacity": 1048576, "segment_size": 256}
    result.update(overrides)
    return result


def make_plan(cluster):
    design = read(ROOT / "profiling/weekend_hardware_probe.design.json")
    cores = CLUSTERS[cluster][0]
    balanced = f"{cores//2}p{cores//2}c"
    configs = {c["config_id"]: c for c in design["configs"]}
    with (ROOT / "profiling/weekend_hardware_probe.matrix.csv").open() as stream:
        rows = [r for r in csv.DictReader(stream) if r["machine"] == cluster]
    scenarios = list(dict.fromkeys(r["scenario"] for r in rows))
    jobs = []
    def add(id, kind, **kw):
        jobs.append(dict(id=id, kind=kind, requires=[], seconds=3300, **kw))
    for cid, config in configs.items():
        add("gate-" + cid, "gate", config=config, scenarios=scenarios)
    for cid in ("ubq-b256", "lubq-b256"):
        add("gate-" + cid + "-yield", "gate", config={**configs[cid], "backoff": "yield"},
            scenarios=["1p32c", balanced])
    add("gate-ubq-item", "gate", config=configs["ubq-b256"], scenarios=["1p32c", balanced], variant="item")
    # Grace gets the SPE load-sampling panel through plain `perf` as well as the
    # APX recipes. APX's own `memory_access` recipe insists on elevating to root
    # and fails on this site, but a `perfparanoid` node exposes arm_spe_0 with
    # perf_event_paranoid=-1, and `perf mem record` captures it unprivileged.
    collectors = list(STAT) + (list(APX) + ["perf_mem"] if cluster == "grace"
                               else ["perf_record", "perf_mem", "perf_c2c"])
    for collector in collectors:
        for attempt in range(CAPABILITY_ALLOCATIONS):
            add(cap_ids(collector)[attempt], "capability", collector=collector)
    # Interleave algorithms and endpoint shapes; never order a whole panel by queue speed.
    for row in rows:
        cid, sc = row["config_id"], row["scenario"]
        add(f"core-{cid}-{sc}", "core", config=configs[cid], scenario=sc)
        jobs[-1]["requires"] = ["gate-" + cid] + [cap_ids(c) for c in STAT]
    memory_configs = [c for c in configs.values() if c["batch_size"] is None or c["config_id"] == "segqueue-b256"]
    for config in memory_configs:
        for p in (1, 16, cores - 1):
            add(f"memory-{config['config_id']}-{p}p1c", "memory", config=config, scenario=f"{p}p1c")
            jobs[-1]["requires"] = ["gate-" + config["config_id"]]
    for p in (1, 16, cores - 1):
        add(f"memory-scaffold-{p}p1c", "memory", config={"queue": "memory-scaffold", "batch_size": None},
            scenario=f"{p}p1c", scaffolding_control=True)
    for queue in ("ubq", "lubq"):
        for sc in ("1p32c", balanced):
            cid = queue + "-b256"
            add(f"ab-backoff-{queue}-{sc}", "ab", config=configs[cid], scenario=sc,
                arms=[{"name": "crossbeam"}, {"name": "yield", "backoff": "yield"}])
            jobs[-1]["requires"] = ["gate-" + cid, "gate-" + cid + "-yield"] + [cap_ids(c) for c in STAT]
    for sc in ("1p32c", balanced):
        add(f"ab-completion-{sc}", "ab", config=configs["ubq-b256"], scenario=sc,
            arms=[{"name": "item", "variant": "item"}, {"name": "segment"}])
        jobs[-1]["requires"] = ["gate-ubq-b256", "gate-ubq-item"] + [cap_ids(c) for c in STAT]
    parents = [j for j in jobs if j["kind"] == "ab" or
               j["kind"] == "core" and j["scenario"] in ("1p1c", "16p1c", "1p32c", balanced)]
    for parent in parents:
        for collector in collectors:
            if collector in STAT:
                continue
            add(f"deep-{parent['id']}-{collector}", "deep", parent=parent["id"], collector=collector)
            jobs[-1].update(requires=[parent["id"], cap_ids(collector)], seconds=6900)
    repeated = []
    for job in jobs:
        if job["kind"] in {"gate", "capability"}:
            repeated.append(job)
            continue
        for rep in range(3):
            copy = {**job, "repeat": rep, "requires": list(job["requires"])}
            if rep:
                copy["id"] += f"-r{rep}"
                if job["kind"] in {"core", "ab"}:
                    copy["schedule_parent"] = job["id"]
                    copy["requires"].append(job["id"])
            repeated.append(copy)
    return {"schema": SCHEMA, "cluster": cluster, "worker_cores": cores, "repeats": 3,
            "duration_secs": 30, "jobs": repeated,
            "omitted": {"retention_ab": "No correctness-approved excess-block reclamation implementation",
                        "numa_intervention": "Deferred; this campaign records deterministic physical CPU placement",
                        "allocation_calls": "Process footprint only; no allocator interception or exact retry counters"}}


def prepare(args):
    campaign = args.campaign.resolve()
    if campaign.exists():
        raise RuntimeError(f"campaign already exists: {campaign}; use resume")
    campaign.mkdir(parents=True)
    plan = make_plan(args.cluster)
    plan.update(created_epoch=time.time(), deadline_epoch=time.time() + args.hours * 3600,
                account=args.account, partition=args.partition or CLUSTERS[args.cluster][1],
                qos=args.qos or CLUSTERS[args.cluster][2], constraint=args.constraint,
                build_qos=getattr(args, "build_qos", "") or None,
                cluster=args.cluster, origin=str(ROOT))
    source = campaign / "source"
    source.mkdir()
    # Copy dirty worktree too. No symlinks to mutable build inputs, including vendor.
    # Cargo resolves all declared targets, even when testing only --lib or
    # building --bin bench_hardware. Preserve the other standard target trees
    # and package/toolchain inputs as well as this campaign's direct sources.
    for name in ("Cargo.toml", "Cargo.lock", "build.rs", "README.md", "LICENSE",
                 "rust-toolchain", "rust-toolchain.toml", "src", "benches", "tests",
                 "examples", "third_party", "vendor", ".cargo", "scripts", "slurm", "profiling"):
        original, target = ROOT / name, source / name
        if original.is_dir():
            shutil.copytree(original, target, ignore=shutil.ignore_patterns("__pycache__", "*.pyc"))
        elif original.exists():
            shutil.copy2(original, target)
    manifest = {str(p.relative_to(source)): digest(p) for p in sorted(source.rglob("*")) if p.is_file()}
    save(campaign / "source-manifest.json", manifest)
    plan["source_manifest_sha256"] = digest(campaign / "source-manifest.json")
    save(campaign / "campaign.json", plan)
    save(campaign / "identity.json", {"campaign_sha256": digest(campaign / "campaign.json")})
    (campaign / "logs").mkdir()
    (campaign / "results").mkdir()
    save(campaign / "state.json", {"jobs": {}, "build": None})
    return campaign


def verify_identity(campaign):
    if digest(campaign / "campaign.json") != read(campaign / "identity.json")["campaign_sha256"]:
        raise RuntimeError("campaign definition changed; use a new campaign label")
    if digest(campaign / "source-manifest.json") != read(campaign / "campaign.json")["source_manifest_sha256"]:
        raise RuntimeError("source manifest changed")


def topology(cores):
    result = run(["lscpu", "-p=CPU,CORE,SOCKET,NODE"])
    if result.returncode:
        raise RuntimeError("cannot discover physical cores")
    allowed = os.sched_getaffinity(0)
    selected = {}
    for row in result.stdout.splitlines():
        if row.startswith("#") or not row.strip():
            continue
        cpu, core, socket, node = map(int, row.split(","))
        if cpu in allowed:
            selected.setdefault((socket, core), (node, socket, core, cpu))
    ordered = sorted(selected.values())
    if len(ordered) != cores:
        raise RuntimeError(f"expected standard {cores}-physical-core node, found {len(ordered)} in cpuset")
    return [entry[-1] for entry in ordered]


def validate_result(result, config, variant="base"):
    if result.get("config") != config or not result.get("affinity_ok") and not config["allow_unpinned"]:
        raise RuntimeError("workload configuration or affinity mismatch")
    if config["mode"] == "memory":
        if result.get("memory_depth") != config["memory_depth"] or result["checkpoints"][-1]["phase"] != "destroyed":
            raise RuntimeError("incomplete memory lifecycle")
        return
    p = int(config["scenario"].split("p")[0])
    alignment = p * (config["batch_size"] or 1)
    expected = config["round_items"] // alignment * alignment
    if result.get("round_items") != expected or result.get("items") != expected * result.get("rounds", 0):
        raise RuntimeError("fixed-work item mismatch")
    if config["rounds"] is not None and (result.get("rounds") != config["rounds"] or not result.get("fixed_work")):
        raise RuntimeError("fixed-work round mismatch")
    if result.get("elapsed_ns", 0) <= 0 or result.get("completion_accounting") != ("item" if variant == "item" else "segment"):
        raise RuntimeError("invalid duration or binary variant")
    if config["mode"] == "verify" and result.get("exact_once_checked") is not True:
        raise RuntimeError("exact-once gate was not executed")


def parse_workload(raw):
    objects = []
    for line in raw.splitlines():
        try:
            value = json.loads(line)
        except ValueError:
            continue
        if isinstance(value, dict) and "benchmark" in value:
            objects.append(value)
    if len(objects) != 1:
        raise RuntimeError(f"expected one workload result, found {len(objects)}")
    return objects[0]


def validate_perf_csv(path, events):
    # perf's `-x` CSV output can print an event name without its requested
    # modifier suffix (e.g. "dTLB-loads" instead of "dTLB-loads:u"); match on
    # the modifier-free base so a real, fully time-running counter is not
    # discarded as an unrecognized row.
    def base_name(name):
        return name.split(":", 1)[0]

    wanted = {base_name(e): e for e in events}
    found = {}
    for row in csv.reader(Path(path).read_text().splitlines(), delimiter=";"):
        if len(row) < 3:
            continue
        event = wanted.get(base_name(row[2].strip()))
        if event is None:
            continue
        try:
            value = float(row[0])
            fraction = float(row[4].strip().rstrip("%")) / 100 if len(row) > 4 and row[4].strip() else 0.
        except ValueError:
            raise RuntimeError(f"unsupported or uncounted event: {event}")
        if not math.isfinite(value) or value < 0 or fraction < .90:
            raise RuntimeError(f"invalid counter or running fraction below 90%: {event}")
        found[event] = {"value": value, "running_fraction": fraction}
    if set(found) != set(events):
        raise RuntimeError("perf result missing requested events")
    return found


def measure(campaign, directory, config, variant="base", collector=None, timeout=300):
    directory.mkdir(parents=True, exist_ok=True)
    save(directory / "workload.json", config)
    binary = campaign / "artifacts" / ("bench_hardware-" + variant)
    command = [str(binary), "--config", str(directory / "workload.json")]
    env = dict(os.environ, RUST_BACKTRACE="1")
    prefix = []
    if collector in STAT:
        prefix = ["perf", "stat", "--no-big-num", "-x", ";", "-o", str(directory / "perf.csv")]
        # Small simultaneous groups: ratios refer to the same scheduled intervals.
        prefix += ["-e", "{" + ",".join(STAT[collector]) + "}", "--"]
    elif collector in ("perf_record", "perf_mem", "perf_c2c"):
        prefix = ["perf"] + ({"perf_record": ["record", "-e", "cycles:u", "-F", "99", "--call-graph", "fp"],
                              "perf_mem": ["mem", "record", "-t", "load"] + SPE_PERIOD,
                              "perf_c2c": ["c2c", "record"]}[collector])
        prefix += ["-o", str(directory / "perf.data"), "--"]
    if collector in APX:
        import run_profile_case as legacy
        from aggregate_profile_results import parse_apx_archive
        legacy.run_command = run  # Streaming and shared bundle deadline, also for preparation/export.
        recipe_params = {}
        if collector == "system_utilization":
            # Unlike the PMU-sampling recipes, this recipe samples on a plain
            # wall-clock interval (default 1 Hz). At the workload's actual
            # capture length that default can fall well under the archive
            # validation floor below, so target roughly 200 samples instead
            # of relying on the recipe's own default cadence.
            duration = float(config.get("duration_secs") or 30.)
            interval = min(60., max(0.01, duration / 200.))
            recipe_params = {"interval": f"{interval:.3f}", "thread_scan_interval": f"{interval:.3f}"}
        result, run_id, run_name, renamed = legacy.run_apx(
            {"recipe": APX[collector], "collector_timeout_secs": timeout,
             "case_id": directory.parent.name + "-" + directory.name,
             "apx_run_name": campaign.name + "-" + directory.parent.name + "-" + directory.name,
             "recipe_params": recipe_params},
            command, directory, campaign / "source", env)
        archive = next(directory.glob("*.zip"))
        parsed = parse_apx_archive(archive)
        samples = [m["value"] for m in parsed.metrics if m["metric_id"] == "apx.sample_count"]
        observations = max(samples) if samples else 0
        # The system-utilization recipe emits a per-core timeline CSV rather
        # than neoprof sample metrics, so its evidence is that timeline's row
        # count. v4 discarded a complete 200-row, 144-core capture purely
        # because it carried no `apx.sample_count`.
        timeline = apx_timeline_rows(archive)
        observations = max(observations, timeline)
        if observations < 100 or any(i.get("severity") == "error" for i in parsed.issues):
            raise RuntimeError("APX archive lacks 100 valid samples or has parser errors")
        save(directory / "hardware.json", {"metrics": parsed.metrics, "hotspots": parsed.hotspots,
                                           "issues": parsed.issues, "timeline_rows": timeline})
        if result is None:
            raise RuntimeError("APX archive has no completed workload result")
    else:
        completed = run(prefix + command, cwd=campaign / "source", env=env, timeout=timeout,
                        stdout_path=directory / "stdout.log", stderr_path=directory / "stderr.log")
        if completed.returncode:
            raise RuntimeError(f"workload/collector exit {completed.returncode}: {completed.stderr[-1500:]}")
        result = parse_workload(completed.stdout)
        if collector in STAT:
            save(directory / "hardware.json", validate_perf_csv(directory / "perf.csv", STAT[collector]))
        elif collector:
            reported = run(["perf", "script", "-i", str(directory / "perf.data"), "-F", "event,ip"],
                           timeout=120, stdout_path=directory / "samples.log", stderr_path=directory / "decode.stderr.log")
            samples = sum(bool(line.strip()) and not line.startswith("#") for line in reported.stdout.splitlines())
            if reported.returncode or samples < 100:
                raise RuntimeError("perf capture lacks 100 decodable samples")
            save(directory / "hardware.json", {"sample_count": samples})
    validate_result(result, config, variant)
    save(directory / "benchmark.json", result)
    save(directory / "status.json", {"status": "completed", "collector": collector or "shadow", "variant": variant})
    return result


def schedule(campaign, directory, config, arms):
    pilots = []
    for arm in arms:
        cfg = {**config, "rounds": None, "duration_secs": 3., "backoff": arm.get("backoff", config["backoff"])}
        pilots.append(measure(campaign, directory / ("pilot-" + arm["name"]), cfg, arm.get("variant", "base")))
    rounds = max(1, max(math.ceil(p["rounds"] * 30e9 / p["elapsed_ns"]) for p in pilots))
    fixed = {**config, "rounds": rounds, "duration_secs": 30.}
    save(directory / "schedule.json", {"config": fixed, "arms": arms})
    return fixed


def worker(campaign, jobid):
    global DEADLINE
    campaign = campaign.resolve()
    verify_identity(campaign)
    plan = read(campaign / "campaign.json")
    job = next(j for j in plan["jobs"] if j["id"] == jobid)
    directory = campaign / "results" / jobid
    if (directory / "status.json").exists():
        raise RuntimeError("refusing to overwrite a prior attempt; use a new campaign label")
    directory.mkdir(parents=True, exist_ok=True)
    DEADLINE = time.time() + job["seconds"]
    def interrupted(sig, frame):
        stop_child()
        raise RuntimeError(f"received signal {sig}")
    for sig in (signal.SIGTERM, signal.SIGINT, signal.SIGUSR1):
        signal.signal(sig, interrupted)
    save(directory / "case.json", job)
    save(directory / "status.json", {"status": "running", "started_epoch": time.time()})
    try:
        binaries = read(campaign / "artifacts/binaries.json")
        for variant, expected in binaries.items():
            if digest(campaign / "artifacts" / ("bench_hardware-" + variant)) != expected:
                raise RuntimeError("frozen binary hash mismatch")
        cpus = topology(plan["worker_cores"])
        os.environ["UBQ_BENCH_CORE_IDS"] = ",".join(map(str, cpus))
        provenance = {"node": platform.node(), "platform": platform.platform(), "cpus": cpus,
                      "page_size": os.sysconf("SC_PAGE_SIZE"), "binaries": binaries,
                      "source_manifest_sha256": plan["source_manifest_sha256"],
                      "slurm": {k: v for k, v in os.environ.items() if k.startswith("SLURM_")}}
        for name, command in {"lscpu": ["lscpu"], "numa": ["numactl", "--hardware"],
                              "perf": ["perf", "--version"], "perf_events": ["perf", "list"]}.items():
            try:
                r = run(command)
                provenance[name] = {"stdout": r.stdout, "stderr": r.stderr, "returncode": r.returncode}
            except OSError as e:
                provenance[name] = {"error": str(e)}
        for file in ("/proc/meminfo", "/proc/sys/kernel/perf_event_paranoid", "/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor"):
            if Path(file).exists():
                provenance[file] = Path(file).read_text()
        save(directory / "provenance.json", provenance)
        kind = job["kind"]
        if kind == "gate":
            for sc in job["scenarios"]:
                # Separate process per topology; reuse storage across repeated rounds.
                cfg = workload(job["config"], sc, mode="verify", round_items=262144, rounds=16, warmup_rounds=2)
                measure(campaign, directory / sc, cfg, job.get("variant", "base"))
            if job["config"]["queue"] == "ubq":
                # Exercise the original failure's large native batches as a diagnostic,
                # as well as the campaign's actual batch size, at the full burst cap.
                for batch in dict.fromkeys([job["config"].get("batch_size"), 1024]):
                    for sc in ("1p32c", f"{plan['worker_cores']//2}p{plan['worker_cores']//2}c"):
                        cfg = workload(job["config"], sc, mode="verify", batch_size=batch,
                                       round_items=ROUND_ITEMS, rounds=4, warmup_rounds=2)
                        measure(campaign, directory / f"large-{sc}-b{batch}", cfg, job.get("variant", "base"))
        elif kind == "capability":
            cfg = workload({"queue": "mutex-vecdeque", "batch_size": 256}, "1p1c")
            cfg = schedule(campaign, directory, cfg, [{"name": "base"}])
            # One flaky APX readiness or daemon start must not disable a whole
            # recipe. In v4 a single instruction_mix pilot failure skipped all
            # 186 of that recipe's deep captures, and v3 showed these failures
            # are transient rather than node capability: 33 distinct nodes with
            # exactly one failure each, 28 of which completed other captures.
            failure = None
            for attempt in range(CAPABILITY_ATTEMPTS):
                suffix = "" if attempt == 0 else f"-retry{attempt}"
                try:
                    measure(campaign, directory / ("capture" + suffix), cfg, collector=job["collector"])
                    failure = None
                    break
                except (RuntimeError, OSError, subprocess.TimeoutExpired, ValueError) as exc:
                    failure = exc
            if failure is not None:
                raise failure
        elif kind == "memory":
            for rep in [job["repeat"]]:
                for depth in (4096, 65536, 524288):
                    cfg = workload(job["config"], job["scenario"], mode="memory", memory_depth=depth)
                    measure(campaign, directory / f"r{rep}-depth{depth}", cfg)
        else:
            if kind == "deep":
                frozen = read(campaign / "results" / job["parent"] / "schedule.json")
                cfg, arms = frozen["config"], frozen["arms"]
                # Repeatability includes the *same ordered physical CPUs*, not their numeric IDs across nodes.
                save(directory / "schedule.json", frozen)
                collectors = [job["collector"]]
            else:
                arms = job.get("arms", [{"name": "base"}])
                if "schedule_parent" in job:
                    frozen = read(campaign / "results" / job["schedule_parent"] / "schedule.json")
                    cfg = frozen["config"]
                    save(directory / "schedule.json", frozen)
                else:
                    cfg = schedule(campaign, directory, workload(job["config"], job["scenario"]), arms)
                # Eligibility is accounting-reconciled; a successful sidecar alone
                # must not re-enable a collector whose pilot allocation OOMed.
                admitted = read(campaign / "state.json")["jobs"]
                collectors = [c for c in STAT
                              if any(admitted.get(i, {}).get("status") == "completed" for i in cap_ids(c))]
            for rep in [job["repeat"]]:
                ordered_arms = arms if rep % 2 == 0 else list(reversed(arms))
                for arm in ordered_arms:
                    arm_cfg = {**cfg, "backoff": arm.get("backoff", cfg["backoff"])}
                    # Each capture receives a same-node, same-schedule unprofiled shadow.
                    measure(campaign, directory / f"r{rep}-{arm['name']}-shadow", arm_cfg, arm.get("variant", "base"))
                    for collector in collectors:
                        measure(campaign, directory / f"r{rep}-{arm['name']}-{collector}", arm_cfg,
                                arm.get("variant", "base"), collector)
        save(directory / "status.json", {"status": "completed", "finished_epoch": time.time()})
        return 0
    except Exception as exc:
        stop_child()
        save(directory / "status.json", {"status": "failed", "error": str(exc), "finished_epoch": time.time()})
        print(f"{jobid}: {exc}", file=sys.stderr, flush=True)
        return 1


def artifact_status(campaign, id):
    path = campaign / "results" / id / "status.json"
    return read(path).get("status", "missing") if path.exists() else "missing"


def sbatch(campaign, plan, id, script, *args, array=None, seconds=3600, cpus=None, qos=None):
    command = ["sbatch", "--parsable", "--account=" + plan["account"], "--partition=" + plan["partition"],
               "--qos=" + (qos or plan["qos"]), "--nodes=1", "--ntasks=1", "--exclusive",
               "--cpus-per-task=" + str(cpus or plan["worker_cores"]), "--hint=nomultithread",
               "--time=" + str(math.ceil(seconds / 60)), "--signal=B:USR1@90",
               "--job-name=hw-" + id,
               "--output=" + str(campaign / "logs" / (id + "-%A_%a.out")),
               "--error=" + str(campaign / "logs" / (id + "-%A_%a.err"))]
    # Grace assigns full-node memory to an exclusive allocation via --mem=0.
    # MN5 derives memory from requested CPUs and rejects an explicit --mem.
    if plan.get("cluster") != "mn5":
        command.append("--mem=0")
    if plan["constraint"]:
        command += ["--constraint=" + plan["constraint"]]
    if array:
        command += ["--array=" + array]  # Deliberately no % running-task throttle.
    result = run(command + [str(script), *map(str, args)], timeout=60)
    if result.returncode:
        if any(q.lower() in result.stderr.lower() for q in QUOTA):
            save(campaign / "submission-backpressure.json", {"epoch": time.time(),
                 "requested_array": array, "scheduler_error": result.stderr})
            return None
        raise RuntimeError("sbatch failed: " + result.stderr)
    job_id = result.stdout.strip().split(";")[0]
    if not job_id.isdigit():
        raise RuntimeError("unrecognized sbatch response: " + result.stdout)
    return job_id


def accounting(campaign, state):
    ids = sorted({v["slurm"].split("_")[0] for v in state["jobs"].values() if "slurm" in v} |
                 ({state["build"]} if state.get("build") else set()))
    if not ids:
        return {}
    result = {}
    for start in range(0, len(ids), 100):
        query = run(["sacct", "--array", "-n", "-P", "-j", ",".join(ids[start:start+100]),
                     "--format=JobID%80,State%32,ExitCode,NodeList"], timeout=60)
        if query.returncode:
            raise RuntimeError("sacct failed: " + query.stderr)
        for line in query.stdout.splitlines():
            fields = line.split("|")
            if len(fields) >= 4:
                result[fields[0]] = {"state": fields[1].split()[0].rstrip("+"), "exit_code": fields[2], "nodes": fields[3]}
    for id, record in list(result.items()):
        if "." in id and (record["state"] in TERMINAL - {"COMPLETED"} or record["exit_code"] not in {"0:0", ""}):
            parent = result.setdefault(id.split(".")[0], {})
            parent.setdefault("allocation_state", parent.get("state", "UNKNOWN"))
            parent["state"] = record["state"] if record["state"] != "COMPLETED" else "FAILED"
            parent["failed_step"] = id
    save(campaign / "accounting.json", result)
    return result


def reconcile(campaign, state, records):
    for id, entry in state["jobs"].items():
        if "slurm" not in entry:
            continue
        record = records.get(entry["slurm"], {})
        scheduler = record.get("state", "UNKNOWN")
        entry["scheduler"] = record
        if scheduler in TERMINAL:
            good = scheduler == "COMPLETED" and record.get("exit_code") == "0:0"
            entry["status"] = "completed" if good and artifact_status(campaign, id) == "completed" else "failed"
        else:
            # Never infer live state from a sidecar; absent accounting stays unknown.
            entry["status"] = "running" if scheduler in {"RUNNING", "COMPLETING"} else "pending" if scheduler == "PENDING" else "unknown"


def eligible(job, state, campaign=None):
    for required in job["requires"]:
        # A list is an any-of group: the collector pilots for one recipe. The
        # group is undecided until every attempt is terminal.
        if isinstance(required, list):
            statuses = [state["jobs"].get(i, {}).get("status") for i in required]
            if any(st not in TERMINAL_JOB_STATES for st in statuses):
                return "wait"
            if any(st == "completed" for st in statuses):
                continue
            # No pilot succeeded: the unprofiled measurement still stands.
            if job["kind"] in {"core", "ab"}:
                continue
            return "skip"
        status = state["jobs"].get(required, {}).get("status")
        if status not in TERMINAL_JOB_STATES:
            return "wait"
        if status == "completed":
            continue
        # A missing collector never invalidates the unprofiled measurement.
        if job["kind"] in {"core", "ab"} and required.startswith("cap-"):
            continue
        # Replicate zero only owes its siblings a calibrated round count. If it
        # wrote that schedule before its own measurement failed, the remaining
        # repetitions are still independent observations of the same frozen
        # work, so an intermittent r0 failure must not erase the whole cell.
        if (
            campaign is not None
            and required == job.get("schedule_parent")
            and (campaign / "results" / required / "schedule.json").exists()
        ):
            continue
        return "skip"
    return "ready"


def recover_intent(campaign, state):
    """A crash between sbatch and state persistence must never duplicate expensive work."""
    intent = campaign / "submit-intent.json"
    if not intent.exists():
        return
    request = read(intent)
    # Accounting may lag; uncertain acceptance requires manual reconciliation, not resubmission.
    r = run(["sacct", "-X", "--array", "-n", "-P", "--starttime", time.strftime("%Y-%m-%d", time.localtime(request["time"])),
             "--name=hw-" + request["token"], "--format=JobID%80,JobName%100"])
    ids = {line.split("|")[0].split("_")[0] for line in r.stdout.splitlines()
           if "|hw-" + request["token"] + "|" in line and "." not in line.split("|")[0]}
    if r.returncode or len(ids) != 1:
        raise RuntimeError("unresolved submission intent; inspect submit-intent.json and Slurm accounting before resume")
    accepted(campaign, state, request, ids.pop())


def accepted(campaign, state, request, slurm):
    if request["ids"] == ["build"]:
        state["build"] = slurm
    else:
        for index, id in enumerate(request["ids"]):
            state["jobs"][id] = {"slurm": f"{slurm}_{index}", "status": "pending"}
    save(campaign / "state.json", state)
    (campaign / "submit-intent.json").unlink()


def submit(campaign, plan, state, jobs):
    size = min(300, len(jobs))
    while size:
        selected = jobs[:size]
        token = f"{hashlib.sha256(str(campaign).encode()).hexdigest()[:8]}-{time.time_ns()}"
        request = {"ids": [j["id"] for j in selected], "token": token, "time": time.time()}
        save(campaign / "submit-intent.json", request)
        mapping = campaign / "submissions" / (token + ".json")
        save(mapping, request["ids"])
        slurm = sbatch(campaign, plan, token, campaign / "source/slurm/hardware_campaign.sbatch",
                       campaign, "worker-array", mapping, array=f"0-{size-1}", seconds=selected[0]["seconds"] + 300)
        if slurm:
            accepted(campaign, state, request, slurm)
            return size
        (campaign / "submit-intent.json").unlink()
        size //= 2
    return 0


def controller(campaign, once=False):
    import fcntl
    campaign = campaign.resolve()
    with (campaign / "controller.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        verify_identity(campaign)
        plan, state = read(campaign / "campaign.json"), read(campaign / "state.json")
        recover_intent(campaign, state)
        while True:
            now = time.time()
            records = accounting(campaign, state)
            reconcile(campaign, state, records)
            build = records.get(state.get("build"), {})
            build_ok = build.get("state") == "COMPLETED" and build.get("exit_code") == "0:0" and (campaign / "artifacts/binaries.json").exists()
            if not state.get("build") and now < plan["deadline_epoch"]:
                token = f"build-{hashlib.sha256(str(campaign).encode()).hexdigest()[:8]}-{time.time_ns()}"
                request = {"ids": ["build"], "token": token, "time": now}
                save(campaign / "submit-intent.json", request)
                slurm = sbatch(campaign, plan, token, campaign / "source/slurm/hardware_campaign.sbatch",
                               campaign, "build", seconds=3600, cpus=4, qos=plan.get("build_qos"))
                if slurm:
                    accepted(campaign, state, request, slurm)
                else:
                    (campaign / "submit-intent.json").unlink()
            build_failed = build.get("state") in TERMINAL and not build_ok
            for job in plan["jobs"]:
                if job["id"] in state["jobs"]:
                    continue
                if build_failed or now >= plan["deadline_epoch"] - job["seconds"] - 300:
                    state["jobs"][job["id"]] = {"status": "not_admitted", "reason": "build failed" if build_failed else "weekend deadline"}
                elif build_ok and eligible(job, state, campaign) == "skip":
                    state["jobs"][job["id"]] = {"status": "skipped", "reason": "failed prerequisite (correctness or capability)"}
            if build_ok:
                # Priority waves; within each wave submit until the site's actual quota rejects admission.
                quota_full = False
                for kind in ("gate", "capability", "core", "memory", "ab", "deep"):
                    while True:
                        ready = [j for j in plan["jobs"] if j["kind"] == kind and j["id"] not in state["jobs"] and eligible(j, state, campaign) == "ready"]
                        if not ready:
                            break
                        if not submit(campaign, plan, state, ready):
                            quota_full = True
                            break
                    if quota_full:
                        break
            save(campaign / "state.json", state)
            counts = collections.Counter(v["status"] for v in state["jobs"].values())
            save(campaign / "progress.json", {"updated_epoch": now, "jobs": len(plan["jobs"]), "counts": dict(counts),
                  "unsubmitted": len(plan["jobs"]) - len(state["jobs"]), "build": build})
            print(time.strftime("%F %T"), dict(counts), flush=True)
            # Pending tasks past the campaign deadline are explicitly cancelled; running tasks finish.
            if now >= plan["deadline_epoch"]:
                for entry in state["jobs"].values():
                    if entry.get("scheduler", {}).get("state") == "PENDING":
                        cancelled = run(["scancel", entry["slurm"]])
                        if cancelled.returncode:
                            print("scancel:", cancelled.stderr, file=sys.stderr, flush=True)
            live = any(v["status"] in {"running", "pending", "unknown"} for v in state["jobs"].values())
            if once or len(state["jobs"]) == len(plan["jobs"]) and not live:
                return
            time.sleep(30)


def launch(campaign):
    log = (campaign / "controller.log").open("a")
    proc = subprocess.Popen([sys.executable, str(campaign / "source/scripts/hardware_campaign.py"), "controller", str(campaign)],
                            stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT, start_new_session=True,
                            cwd=campaign / "source")
    save(campaign / "controller.json", {"pid": proc.pid, "host": platform.node(), "started_epoch": time.time()})
    print(f"Campaign: {campaign}\nController PID: {proc.pid}\nLog: {campaign / 'controller.log'}")


def status(campaign):
    state = read(campaign / "state.json")
    records = accounting(campaign, state)
    reconcile(campaign, state, records)
    progress = read(campaign / "progress.json") if (campaign / "progress.json").exists() else {}
    print(json.dumps({"build": records.get(state.get("build"), {}),
        "controller_heartbeat_age_secs": round(time.time() - progress["updated_epoch"]) if "updated_epoch" in progress else None,
        "counts": dict(collections.Counter(v["status"] for v in state["jobs"].values())),
        "unsubmitted": len(read(campaign / "campaign.json")["jobs"]) - len(state["jobs"]),
        "failed": [id for id, v in state["jobs"].items() if v["status"] == "failed"]}, indent=2))


def report(campaign):
    state = read(campaign / "state.json")
    cached = campaign / "accounting.json"
    if cached.exists():
        reconcile(campaign, state, read(cached))
    rows, memory, hardware, effects = [], [], [], []
    for job in read(campaign / "campaign.json")["jobs"]:
        directory = campaign / "results" / job["id"]
        entry = state["jobs"].get(job["id"], {"status": "unsubmitted"})
        valid = entry["status"] == "completed"
        for path in sorted(directory.glob("*/benchmark.json")):
            b = read(path)
            match = re.match(r"r(\d+)-([^-]+)-(.+)$", path.parent.name)
            base = {"job": job["id"], "kind": job["kind"], "run": path.parent.name, "valid_bundle": valid,
                    "queue": b["queue"], "scenario": b["scenario"], "batch_size": b["batch_size"],
                    "node": read(directory / "provenance.json")["node"],
                    "repeat": int(match[1]) if match else "", "arm": match[2] if match else "",
                    "collector": match[3] if match else "diagnostic_or_calibration"}
            if "checkpoints" in b:
                empty = next(c["memory"].get("Rss_bytes") for c in b["checkpoints"] if c["phase"] == "constructed_empty")
                for checkpoint in b["checkpoints"]:
                    rss = checkpoint["memory"].get("Rss_bytes")
                    phase_index = {"full": 0, "drained": 1, "refilled": 2, "redrained": 3}.get(checkpoint["phase"])
                    memory.append({**base, "depth": b["memory_depth"], "phase": checkpoint["phase"], **checkpoint["memory"],
                                   **(checkpoint.get("os") or {}),
                                   "phase_elapsed_ns": b["phase_elapsed_ns"][phase_index] if phase_index is not None else "",
                                   "rss_delta_from_empty": rss - empty if rss is not None and empty is not None else "",
                                   "delta_bytes_per_payload_byte": (rss - empty) / (8*b["memory_depth"]) if rss is not None and empty is not None else ""})
            else:
                rows.append({**base, **{k: b[k] for k in ("items", "rounds", "round_items", "elapsed_ns", "ops_per_sec", "backoff", "completion_accounting")},
                             "barrier_overhead_proxy_fraction": b.get("barrier_overhead_proxy_fraction", ""), **(b["os"] or {})})
                metrics = path.parent / "hardware.json"
                if metrics.exists():
                    for event, value in read(metrics).items():
                        if isinstance(value, dict) and "value" in value:
                            hardware.append({**base, "event": event, **value,
                                             "per_handoff": value["value"] / (b["items"] + b["warmup_rounds"] * b["round_items"]),
                                             "scope": "whole process including startup, warmup and teardown"})
    shadows = {(r["job"], r["repeat"], r["arm"]): r for r in rows if r["collector"] == "shadow"}
    for row in rows:
        shadow = shadows.get((row["job"], row["repeat"], row["arm"]))
        overhead = row["elapsed_ns"] / shadow["elapsed_ns"] if shadow else None
        row["collector_to_shadow_elapsed_ratio"] = overhead if overhead is not None else ""
        row["quality_ok"] = bool(row["valid_bundle"] and shadow and row["elapsed_ns"] >= 5e9 and
                                 row["barrier_overhead_proxy_fraction"] != "" and row["barrier_overhead_proxy_fraction"] <= .05 and overhead <= 3)
    for job in read(campaign / "campaign.json")["jobs"]:
        if job["kind"] != "ab":
            continue
        control, treatment = [arm["name"] for arm in job["arms"]]
        for rep in range(3):
            a, b = (shadows.get((job["id"], rep, arm)) for arm in (control, treatment))
            if not a or not b:
                continue
            effects.append({"job": job["id"], "repeat": rep, "control": control, "treatment": treatment,
                            "node": a["node"], "valid_pair": a["quality_ok"] and b["quality_ok"] and a["items"] == b["items"],
                            "throughput_treatment_over_control": b["ops_per_sec"] / a["ops_per_sec"],
                            "cpu_time_treatment_over_control": (b["user_cpu_ns"] + b["system_cpu_ns"]) / max(1, a["user_cpu_ns"] + a["system_cpu_ns"])})
    for name, values in (("trials", rows), ("memory", memory), ("counters", hardware), ("effects", effects)):
        path = campaign / (name + ".csv")
        keys = list(dict.fromkeys(k for row in values for k in row))
        with path.open("w") as stream:
            writer = csv.DictWriter(stream, fieldnames=keys)
            writer.writeheader()
            writer.writerows(values)
        print(path)
    print("Filter valid_bundle=True; throughput comparisons use *-shadow rows only. Counters include warmup/startup. Raw sample archives remain beside each run.")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    for command in ("plan", "start"):
        p = sub.add_parser(command)
        p.add_argument("cluster", choices=CLUSTERS)
        if command == "start":
            p.add_argument("campaign", type=Path, help="new results directory on the shared cluster filesystem")
            p.add_argument("--hours", type=float, default=48)
            p.add_argument("--account", default=os.environ.get("ACCOUNT", "bsc18"))
            p.add_argument("--partition")
            p.add_argument("--qos")
            p.add_argument("--constraint", default="perfparanoid", help="Slurm constraint; empty string disables")
            # The build gates every measurement, so a busy general queue can
            # leave a campaign at zero progress for many hours. A single short
            # build is exactly what a debug QOS is for; measurements still run
            # under the normal QOS.
            p.add_argument("--build-qos", default="", help="QOS for the one-off build job; defaults to --qos")
        else:
            p.add_argument("--json", action="store_true")
    for command in ("resume", "controller", "status", "report", "worker"):
        p = sub.add_parser(command)
        p.add_argument("campaign", type=Path)
        if command == "controller":
            p.add_argument("--once", action="store_true")
        if command == "worker":
            p.add_argument("job")
    args = parser.parse_args(argv)
    if args.command == "plan":
        plan = make_plan(args.cluster)
        print(json.dumps(plan if args.json else {"cluster": args.cluster, "jobs": len(plan["jobs"]),
            "kinds": dict(collections.Counter(j["kind"] for j in plan["jobs"])), "omitted": plan["omitted"]}, indent=2))
    elif args.command == "start":
        if not 0 < args.hours <= 168:
            raise RuntimeError("hours must be in (0, 168]")
        for tool in ("sbatch", "sacct", "scancel"):
            if not shutil.which(tool):
                raise RuntimeError(f"{tool} unavailable; run start on the cluster login node")
        launch(prepare(args))
    elif args.command == "resume":
        launch(args.campaign.resolve())
    elif args.command == "worker":
        return worker(args.campaign, args.job)
    elif args.command == "controller":
        controller(args.campaign, args.once)
    elif args.command == "status":
        status(args.campaign.resolve())
    else:
        report(args.campaign.resolve())
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (RuntimeError, OSError, subprocess.TimeoutExpired, ValueError) as exc:
        print(f"hardware_campaign: {exc}", file=sys.stderr)
        sys.exit(1)
