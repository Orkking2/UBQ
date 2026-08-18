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

{
    date --iso-8601=seconds
    hostname
    git rev-parse HEAD
    git status --porcelain=v1
    rustc -Vv
    cargo -V
    printf 'RUSTFLAGS=%s\n' "${RUSTFLAGS-}"
    module -t list 2>&1
    sha256sum "$artifact_dir/bench_grid"
} > "$artifact_dir/build-info-${SLURM_JOB_ID}.txt"