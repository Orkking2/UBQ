"""Job submission for a deployment: Slurm on the clusters, in-process locally."""
from __future__ import annotations

import os
import re
import sys
from pathlib import Path

from .common import read_json_or, run, tail, write_json

# Scheduler states after which a job will never run again.
TERMINAL = {"COMPLETED", "FAILED", "CANCELLED", "TIMEOUT", "OUT_OF_MEMORY", "NODE_FAIL",
            "PREEMPTED", "BOOT_FAIL", "DEADLINE", "REVOKED"}
# sbatch stderr fragments that mean "too many jobs queued", not "invalid request".
QUOTA = ("qosmaxsubmit", "assocmaxsubmit", "maxsubmit", "job submit limit",
         "maximum number of jobs")
# Every job the lab submits is named `ab.<study hash>.<kind>.<token>`, which is
# how a new launch finds the stale jobs of earlier studies to cancel.
JOB_PREFIX = "ab."


class SubmitError(RuntimeError):
    pass


def expand_array_ids(job_id):
    """'123_[0-2,5%4]' -> ['123_0', '123_1', '123_2', '123_5']; other ids pass through."""
    match = re.match(r"^(\d+)_\[([^\]]+)\]$", job_id)
    if not match:
        return [job_id]
    master, body = match.group(1), match.group(2).split("%")[0]
    ids = []
    for part in filter(None, body.split(",")):
        low, _, high = part.partition("-")
        ids.extend(f"{master}_{i}" for i in range(int(low), int(high or low) + 1))
    return ids


def master_ids(job_ids):
    return sorted({str(job).split("_")[0] for job in job_ids if job})


class Slurm:
    def __init__(self, config, study_dir):
        self.config = config
        self.study_dir = Path(study_dir)
        self.script = self.study_dir / "tool" / "ablab" / "job.sbatch"

    def submit(self, name, args, *, minutes, log_dir, kind, array=None, dependency=None):
        """Return the new job id, or None when the QOS submit quota is full.

        `kind` is "task" (an exclusive measurement node), "build", or
        "control" (a one-core job that only runs `advance`).
        """
        c = self.config
        if kind == "task":
            qos_options = [c["qos"]]
        elif kind == "build":
            # The build gates everything, so it prefers the debug QOS, whose
            # queue is short; a QOS the site rejects falls back to the normal one.
            qos_options = list(dict.fromkeys(filter(None, [c.get("build_qos"), c["qos"]])))
        else:
            qos_options = [c["qos"]]
        pattern = "%x-%A_%a" if array else "%x-%j"
        for qos in qos_options:
            command = ["sbatch", "--parsable", "--account=" + c["account"],
                       "--partition=" + c["partition"], "--qos=" + qos, "--nodes=1", "--ntasks=1",
                       f"--time={max(1, int(minutes))}", "--job-name=" + name,
                       f"--output={log_dir}/{pattern}.out", f"--error={log_dir}/{pattern}.err"]
            if kind == "task":
                command += ["--exclusive", f"--cpus-per-task={c['cores']}", "--hint=nomultithread",
                            "--signal=B:USR1@60"]
                # Grace hands an exclusive job the whole node's memory only with
                # --mem=0; MN5 derives memory from CPUs and rejects --mem.
                if c.get("exclusive_mem0"):
                    command.append("--mem=0")
                if c.get("constraint"):
                    command.append("--constraint=" + c["constraint"])
                modules = c.get("task_modules", [])
            elif kind == "build":
                command.append(f"--cpus-per-task={c.get('build_cpus', 16)}")
                modules = c.get("build_modules", [])
            else:
                command.append("--cpus-per-task=1")
                modules = []
            command.append("--export=ALL,AB_MODULES=" + ":".join(modules))
            if array:
                command.append(f"--array=0-{array - 1}")
            if dependency:
                command.append("--dependency=afterany:" + dependency)
            command += [str(self.script), str(self.study_dir), *map(str, args)]
            result = run(command, timeout=120)
            if result.returncode == 0:
                job_id = result.stdout.strip().split(";")[0].strip()
                if not job_id.isdigit():
                    raise SubmitError("unrecognised sbatch reply: " + result.stdout.strip())
                return job_id
            if any(fragment in result.stderr.lower() for fragment in QUOTA):
                return None
            error = f"sbatch rejected {name} (qos {qos}): {tail(result.stderr, 600)}"
        raise SubmitError(error)

    def states(self, job_ids):
        """Map each array element ('123_4') or plain job ('123') to its Slurm state."""
        masters = master_ids(job_ids)
        states = {}
        for start in range(0, len(masters), 100):
            result = run(["sacct", "-X", "-n", "-P", "--array", "-j", ",".join(masters[start:start + 100]),
                          "--format=JobID%60,State%40"], timeout=120)
            if result.returncode:
                raise SubmitError("sacct failed: " + tail(result.stderr, 600))
            for line in result.stdout.splitlines():
                fields = line.strip().split("|")
                if len(fields) < 2 or not fields[0]:
                    continue
                # "CANCELLED by 123" and "COMPLETED+" both reduce to the bare state.
                state = (fields[1].split() or ["UNKNOWN"])[0].rstrip("+")
                for element in expand_array_ids(fields[0]):
                    states[element] = state
        return states

    def find(self, name):
        """Ids of jobs named `name`, queued or finished in the last week."""
        ids = set()
        queued = run(["squeue", "--me", "-h", "-n", name, "-o", "%F"], timeout=60)
        ids.update(word for word in queued.stdout.split() if word.isdigit())
        finished = run(["sacct", "-X", "-n", "-P", "--name=" + name, "--starttime=now-7days",
                        "--format=JobID%60"], timeout=60)
        ids.update(word.split("_")[0] for word in finished.stdout.split() if word.split("_")[0].isdigit())
        return sorted(ids)

    def cancel(self, job_ids):
        masters = master_ids(job_ids)
        for start in range(0, len(masters), 100):
            # Jobs that already ended make scancel complain; that is the goal state.
            run(["scancel", *masters[start:start + 100]], timeout=120)

    def ours(self):
        """(job id, name) of every queued or running lab job this user owns here."""
        result = run(["squeue", "--me", "-h", "-o", "%F|%j"], timeout=60)
        if result.returncode:
            raise SubmitError("squeue failed: " + tail(result.stderr, 600))
        jobs = {}
        for line in result.stdout.splitlines():
            job_id, _, name = line.strip().partition("|")
            if name.startswith(JOB_PREFIX) and job_id.isdigit():
                jobs[job_id] = name
        return sorted(jobs.items())


class Local:
    """Run each submission to completion before returning: a laptop smoke test.

    Jobs use the same entry point Slurm would, with AB_LOCAL=1 so a finishing
    task does not re-enter `advance` while the caller still holds the lock.
    """

    def __init__(self, config, study_dir, deploy_dir):
        self.config = config
        self.study_dir = Path(study_dir)
        self.registry = Path(deploy_dir) / "local-jobs.json"

    def submit(self, name, args, *, minutes, log_dir, kind, array=None, dependency=None):
        if kind == "control":
            return None
        jobs = read_json_or(self.registry, {"next": 1, "states": {}})
        job_id = str(jobs["next"])
        jobs["next"] += 1
        write_json(self.registry, jobs)
        for index in range(array or 1):
            element = f"{job_id}_{index}" if array else job_id
            env = dict(os.environ, AB_LOCAL="1", SLURM_ARRAY_TASK_ID=str(index), SLURM_JOB_ID=element)
            log = Path(log_dir) / f"{name}-{element}"
            try:
                result = run([sys.executable, self.study_dir / "tool" / "ab.py", "job", *args],
                             env=env, timeout=minutes * 120, stdout=f"{log}.out", stderr=f"{log}.err")
                state = "COMPLETED" if result.returncode == 0 else "FAILED"
            except TimeoutError:
                state = "TIMEOUT"
            jobs["states"][element] = state
            write_json(self.registry, jobs)
        return job_id

    def states(self, job_ids):
        known = read_json_or(self.registry, {"states": {}})["states"]
        return {job: known[job] for job in job_ids if job in known}

    def find(self, name):
        return []

    def cancel(self, job_ids):
        pass

    def ours(self):
        return []


def scheduler_for(deployment):
    config = deployment.config
    if config.get("scheduler") == "local":
        return Local(config, deployment.study_dir, deployment.path)
    return Slurm(config, deployment.study_dir)
