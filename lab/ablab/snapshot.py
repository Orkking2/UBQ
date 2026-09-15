"""Freeze each arm's build inputs into a content-addressed source directory.

A source directory holds exactly what Cargo needs to build bench_hardware and
is named by the hash of its contents plus the vendored crates. An arm whose
code has not changed maps to the same directory, and so to the same cached
binary, in every later study.
"""
from __future__ import annotations

import io
import os
import shutil
import subprocess
import tarfile
from pathlib import Path

from .common import canonical, digest_text, sha256_file
from .spec import WORKTREE

# Cargo validates every declared target, so benches/ is an input even though
# only bench_hardware is built. tests/ and examples/ are auto-discovered; leaving
# them out keeps test-only edits from invalidating cached binaries.
BUILD_INPUTS = ("Cargo.toml", "Cargo.lock", "build.rs", "src", "benches", "third_party")
# Ignored by .gitignore, yet it is what points Cargo at vendor/.
CARGO_CONFIG = ".cargo/config.toml"
# Files that define the measurement itself. Arms that differ here are not
# measured by one harness, so their comparison is not an A/B of the queue.
HARNESS = ("src/bench_harness/", "src/bin/bench_hardware.rs", "build.rs")


class SnapshotError(RuntimeError):
    pass


def git(repo, *args, binary=False):
    result = subprocess.run(["git", "-C", str(repo), *args], capture_output=True, check=False)
    if result.returncode:
        message = result.stderr.decode(errors="replace").strip() or "failed"
        raise SnapshotError(f"git {' '.join(args)}: {message}")
    return result.stdout if binary else result.stdout.decode()


def resolve(repo, ref):
    """The commit `ref` names, or WORKTREE for the working tree as it stands."""
    if ref == WORKTREE:
        return WORKTREE
    try:
        return git(repo, "rev-parse", "--verify", "--quiet", ref + "^{commit}").strip()
    except SnapshotError:
        raise SnapshotError(f"{ref!r} is not a commit in this repository") from None


def vendor_hash(repo):
    """Identity of vendor/: each crate's directory name and Cargo checksum file."""
    vendor = Path(repo) / "vendor"
    if not vendor.is_dir():
        raise SnapshotError("vendor/ is missing; run `cargo vendor` before launching")
    entries = {crate.name: sha256_file(crate / ".cargo-checksum.json")
               for crate in sorted(vendor.iterdir()) if (crate / ".cargo-checksum.json").is_file()}
    return digest_text(canonical(entries))


def _write_tree(repo, commit, target):
    if commit == WORKTREE:
        listed = git(repo, "ls-files", "-z", "--cached", "--others", "--exclude-standard",
                     "--", *BUILD_INPUTS)
        for relative in filter(None, listed.split("\0")):
            source = Path(repo) / relative
            if source.is_file():  # Deleted but not yet committed files are gone.
                destination = target / relative
                destination.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(source, destination)
    else:
        present = set(git(repo, "ls-tree", "--name-only", commit).split())
        data = git(repo, "archive", "--format=tar", commit, "--",
                   *[p for p in BUILD_INPUTS if p in present], binary=True)
        with tarfile.open(fileobj=io.BytesIO(data)) as archive:
            try:
                archive.extractall(target, filter="data")
            except TypeError:  # Python < 3.12 has no extraction filters.
                archive.extractall(target)
    config = Path(repo) / CARGO_CONFIG
    if not config.is_file():
        raise SnapshotError(f"{CARGO_CONFIG} is missing; it points Cargo at vendor/")
    (target / ".cargo").mkdir(parents=True, exist_ok=True)
    shutil.copy2(config, target / CARGO_CONFIG)


def _manifest(target):
    manifest = {}
    for path in sorted(target.rglob("*")):
        if path.is_file() and not path.is_symlink():
            executable = "+x" if os.access(path, os.X_OK) else ""
            manifest[path.relative_to(target).as_posix()] = sha256_file(path) + executable
    return manifest


def snapshot(repo, ref, sources_dir, vendor):
    """Freeze `ref` as sources_dir/<hash>/ and describe it."""
    commit = resolve(repo, ref)
    sources_dir = Path(sources_dir)
    sources_dir.mkdir(parents=True, exist_ok=True)
    temporary = sources_dir / f".tmp-{os.getpid()}"
    shutil.rmtree(temporary, ignore_errors=True)
    temporary.mkdir()
    try:
        _write_tree(repo, commit, temporary)
        manifest = _manifest(temporary)
        source = digest_text(canonical({"files": manifest, "vendor": vendor}))
        harness = digest_text(canonical({k: v for k, v in manifest.items() if k.startswith(HARNESS)}))
        # Relative, so the link resolves under any root holding sources/ and
        # vendor/ side by side, on this machine or on the cluster.
        os.symlink(f"../../vendor/{vendor}", temporary / "vendor")
        final = sources_dir / source
        if final.exists():
            shutil.rmtree(temporary)
        else:
            temporary.rename(final)
    except BaseException:
        shutil.rmtree(temporary, ignore_errors=True)
        raise
    described = {"ref": ref, "commit": commit, "source": source, "harness": harness,
                 "files": len(manifest)}
    if commit == WORKTREE:
        described["head"] = git(repo, "rev-parse", "HEAD").strip()
        described["dirty"] = [line[3:] for line in git(
            repo, "status", "--porcelain=v1", "--", *BUILD_INPUTS).splitlines()]
    return described
