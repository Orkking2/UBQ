"""Helpers shared by the local CLI, the login-node controller and Slurm jobs.

Standard library only and Python 3.9 compatible: that is what the BSC login
and compute nodes run.
"""
from __future__ import annotations

import contextlib
import fcntl
import hashlib
import json
import os
import signal
import subprocess
import time
from pathlib import Path

# Remote commands print their result on one line with this prefix, because
# BSC login shells write module banners to the same streams.
JSON_PREFIX = "AB-JSON "

# Two-sided 95% Student t quantiles by degrees of freedom. A df between two
# keys uses the smaller key, which is the wider (conservative) interval.
_T975 = {1: 12.706, 2: 4.303, 3: 3.182, 4: 2.776, 5: 2.571, 6: 2.447, 7: 2.365,
         8: 2.306, 9: 2.262, 10: 2.228, 11: 2.201, 12: 2.179, 13: 2.160, 14: 2.145,
         15: 2.131, 16: 2.120, 17: 2.110, 18: 2.101, 19: 2.093, 20: 2.086,
         25: 2.060, 30: 2.042, 40: 2.021, 60: 2.000}


def batch_of(row):
    """A task's or row's batch pair: (producer batch, consumer batch), None for scalar."""
    return (row["producer_batch"], row["consumer_batch"])


def batch_token(batch):
    """The pair's token in cell names: `scalar`, `b256`, or `b1x256` (1 is the scalar API on that side)."""
    producer, consumer = batch
    if producer is None and consumer is None:
        return "scalar"
    if producer == consumer:
        return f"b{producer}"
    return f"b{producer or 1}x{consumer or 1}"


def batch_title(batch):
    """The pair in prose: `scalar`, `batch 256`, or `push 1 / pop 256`."""
    producer, consumer = batch
    if producer is None and consumer is None:
        return "scalar"
    if producer == consumer:
        return f"batch {producer}"
    return f"push {producer or 1} / pop {consumer or 1}"


def batch_order(batch):
    """Sort key: scalar first, then by the larger side, then producer, then consumer."""
    producer, consumer = batch
    return (producer is not None or consumer is not None, max(producer or 1, consumer or 1),
            producer or 1, consumer or 1)


def t975(df):
    if df < 1:
        raise ValueError("a t quantile needs at least one degree of freedom")
    return _T975[max(key for key in _T975 if key <= df)] if df <= 60 else 1.96


class Interrupted(Exception):
    """A Slurm signal (time limit or cancellation) arrived during a job."""


def read_json(path):
    return json.loads(Path(path).read_text())


def read_json_or(path, default):
    try:
        return read_json(path)
    except (OSError, ValueError):
        return default


def write_json(path, value):
    """Replace `path` atomically, so readers never see a partial document."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    tmp.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
    tmp.replace(path)


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def digest_text(text):
    return hashlib.sha256(text.encode()).hexdigest()


def sha256_file(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


@contextlib.contextmanager
def locked(path, timeout=300.0):
    """Hold an exclusive POSIX lock on `path`; GPFS honours these across nodes."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a") as handle:
        deadline = time.time() + timeout
        while True:
            try:
                fcntl.lockf(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except OSError:
                if time.time() >= deadline:
                    raise TimeoutError(f"lock still busy after {timeout:.0f}s: {path}")
                time.sleep(0.5)
        try:
            yield
        finally:
            fcntl.lockf(handle, fcntl.LOCK_UN)


class _Child:
    """The job's one foreground child, so a signal handler can stop it."""
    process = None


def stop_child():
    """Terminate the foreground child's whole process group, then kill it."""
    process = _Child.process
    if process is None or process.poll() is not None:
        return
    for sig in (signal.SIGTERM, signal.SIGKILL):
        try:
            os.killpg(process.pid, sig)
        except ProcessLookupError:
            return
        try:
            process.wait(timeout=5)
            return
        except subprocess.TimeoutExpired:
            continue


def run(command, *, cwd=None, env=None, timeout=60.0, stdout=None, stderr=None):
    """Run `command` in its own process group, killing the group at `timeout`.

    Output streams to the `stdout`/`stderr` paths when given, and is returned
    either way. A timeout raises TimeoutError after the kill.
    """
    args = [str(part) for part in command]
    out = open(stdout, "w") if stdout else subprocess.PIPE
    err = open(stderr, "w") if stderr else subprocess.PIPE
    try:
        process = subprocess.Popen(args, cwd=cwd, env=env, stdin=subprocess.DEVNULL,
                                   stdout=out, stderr=err, text=True, start_new_session=True)
        _Child.process = process
        try:
            captured_out, captured_err = process.communicate(timeout=timeout)
        except subprocess.TimeoutExpired:
            stop_child()
            raise TimeoutError(f"timed out after {timeout:.0f}s: {' '.join(args)}")
        finally:
            _Child.process = None
    finally:
        for handle in (out, err):
            if handle is not subprocess.PIPE:
                handle.close()
    if stdout:
        captured_out = Path(stdout).read_text(errors="replace")
    if stderr:
        captured_err = Path(stderr).read_text(errors="replace")
    return subprocess.CompletedProcess(args, process.returncode, captured_out or "", captured_err or "")


def install_signal_handlers():
    """Turn Slurm's termination signals into Interrupted, stopping the child first."""
    def handler(signum, frame):
        stop_child()
        raise Interrupted(f"received signal {signum}")
    for sig in (signal.SIGTERM, signal.SIGINT, signal.SIGUSR1):
        signal.signal(sig, handler)


def emit(value):
    print(JSON_PREFIX + json.dumps(value, sort_keys=True), flush=True)


def parse_emitted(text):
    for line in reversed(text.splitlines()):
        if line.startswith(JSON_PREFIX):
            return json.loads(line[len(JSON_PREFIX):])
    raise ValueError("remote command printed no result line")


def tail(text, limit=1500):
    text = (text or "").strip()
    return text if len(text) <= limit else "..." + text[-limit:]
