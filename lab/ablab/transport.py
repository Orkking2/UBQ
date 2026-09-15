"""SSH and rsync to BSC, multiplexed over one persistent connection per host."""
from __future__ import annotations

import shlex
from pathlib import Path

from .common import parse_emitted, run, tail

# One master connection per host lives for ten minutes, so the many short
# commands of a launch or status poll skip the slow BSC handshake.
CONTROL_PATH = str(Path.home() / ".ssh" / "ab-%C")
SSH_OPTIONS = ["-o", "BatchMode=yes", "-o", "ConnectTimeout=20",
               "-o", "ServerAliveInterval=15", "-o", "ServerAliveCountMax=4",
               "-o", "ControlMaster=auto", "-o", "ControlPath=" + CONTROL_PATH,
               "-o", "ControlPersist=10m", "-o", "LogLevel=ERROR"]


class TransportError(RuntimeError):
    pass


def ssh(host, script, timeout=900):
    result = run(["ssh", *SSH_OPTIONS, host, script], timeout=timeout)
    if result.returncode:
        raise TransportError(f"ssh {host} exited {result.returncode}: "
                             f"{tail(result.stderr or result.stdout, 1500)}")
    return result.stdout


def remote_json(host, script, timeout=900):
    """Run `script` on `host` and return the JSON result line it prints."""
    output = ssh(host, script, timeout)
    try:
        return parse_emitted(output)
    except ValueError:
        raise TransportError(f"{host} returned no result for `{script}`:\n{tail(output)}") from None


def rsync(source, destination, *, delete=False, filters=(), dry_run=False, timeout=3600):
    """`rsync -a source destination` over the multiplexed connection."""
    command = ["rsync", "-a", "--partial", "--info=stats1",
               "-e", " ".join(["ssh", *map(shlex.quote, SSH_OPTIONS)])]
    if delete:
        command.append("--delete")
    if dry_run:
        command += ["--dry-run", "--itemize-changes"]
    command += [*filters, str(source), str(destination)]
    result = run(command, timeout=timeout)
    if result.returncode:
        raise TransportError(f"rsync {source} -> {destination} failed: {tail(result.stderr, 1500)}")
    return result.stdout
