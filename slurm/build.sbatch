#!/usr/bin/env bash
set -euo pipefail

src_dir=${1:?source directory required}
artifact_dir=${2:?artifact directory required}

module purge
module load gcc
module load rust
# module load rust/REPLACE_WITH_PINNED_VERSION
# Or prepend a project-installed Rust 1.92 toolchain to PATH.
# `gcc` above must provide a working `c++`/`g++` on PATH: bench_moodycamel
# compiles third_party/moodycamel/shim.cpp via the `cc` crate at build time.

: "${TMPDIR:?Slurm did not provide TMPDIR}"

mkdir -p "$artifact_dir" "$TMPDIR/ubq-target"
export CARGO_TARGET_DIR="$TMPDIR/ubq-target"
export CARGO_NET_OFFLINE=true

# If enabled, keep this identical for every compared iteration.
# export RUSTFLAGS="-C target-cpu=native"

cd "$src_dir"

cargo build \
    --release \
    --locked \
    --offline \
    --features bench_registry,bench_rbbq,bench_lfqueue,bench_wcq,bench_moodycamel \
    --bin bench_grid

install -m 0755 \
    "$CARGO_TARGET_DIR/release/bench_grid" \
    "$artifact_dir/bench_grid"

# Keep profiling-only code generation separate from the comparative grid so
# frame pointers/debug symbols cannot perturb the benchmark artifact.
export CARGO_TARGET_DIR="$TMPDIR/ubq-profile-target"
profile_rustflags="${RUSTFLAGS:+$RUSTFLAGS }-C force-frame-pointers=yes"
RUSTFLAGS="$profile_rustflags" CARGO_PROFILE_RELEASE_DEBUG=1 cargo build \
    --release \
    --locked \
    --offline \
    --features bench_registry,bench_rbbq,bench_lfqueue,bench_moodycamel \
    --bin bench_profile \
    --bin bench_atomic_updates \
    --bin bench_head_reload

install -m 0755 \
    "$CARGO_TARGET_DIR/release/bench_profile" \
    "$artifact_dir/bench_profile"
install -m 0755 \
    "$CARGO_TARGET_DIR/release/bench_atomic_updates" \
    "$artifact_dir/bench_atomic_updates"
install -m 0755 \
    "$CARGO_TARGET_DIR/release/bench_head_reload" \
    "$artifact_dir/bench_head_reload"

# The allocator probe is a separate binary: its shared counter updates must
# never perturb the throughput shadow or the perf/APX-collected workload.
export CARGO_TARGET_DIR="$TMPDIR/ubq-allocator-profile-target"
RUSTFLAGS="$profile_rustflags" CARGO_PROFILE_RELEASE_DEBUG=1 cargo build \
    --release \
    --locked \
    --offline \
    --features bench_registry,bench_rbbq,bench_lfqueue,bench_moodycamel,profile_allocator_probe \
    --bin bench_profile
install -m 0755 \
    "$CARGO_TARGET_DIR/release/bench_profile" \
    "$artifact_dir/bench_profile_alloc"

{
    date --iso-8601=seconds
    hostname
    git rev-parse HEAD
    git status --porcelain=v1
    rustc -Vv
    cargo -V
    printf 'RUSTFLAGS=%s\n' "${RUSTFLAGS-}"
    printf 'BENCH_PROFILE_RUSTFLAGS=%s\n' "$profile_rustflags"
    module -t list 2>&1
    sha256sum "$artifact_dir/bench_grid"
    sha256sum "$artifact_dir/bench_profile"
    sha256sum "$artifact_dir/bench_profile_alloc"
    sha256sum "$artifact_dir/bench_atomic_updates"
    sha256sum "$artifact_dir/bench_head_reload"
} > "$artifact_dir/build-info-${SLURM_JOB_ID}.txt"
