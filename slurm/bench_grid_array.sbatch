#!/usr/bin/env bash
set -euo pipefail

src_dir=${1:?source directory required}
binary=${2:?benchmark binary required}
manifest=${3:?scenario manifest required}
runs_root=${4:?runs root required}
machine_label=${5:?machine label required}

module purge
module load gcc
module load rust
# Load the same Rust/runtime modules used during the build if needed.

mapfile -t scenarios < <(
    awk 'NF && $1 !~ /^#/' "$manifest"
)

index=${SLURM_ARRAY_TASK_ID:?not running as an array element}

if ((index >= ${#scenarios[@]})); then
    echo "Array index $index is outside the manifest" >&2
    exit 2
fi

scenario=${scenarios[$index]}

if [[ ! "$scenario" =~ ^[0-9]+p[0-9]+c$ ]]; then
    echo "Invalid scenario: $scenario" >&2
    exit 2
fi

shard_id="$(printf '%03d-%s' "$index" "$scenario")"
shard_dir="$runs_root/shards/$shard_id"
mkdir -p "$shard_dir"

: "${TMPDIR:?Slurm did not provide TMPDIR}"
install -m 0755 "$binary" "$TMPDIR/bench_grid"

{
    date --iso-8601=seconds
    hostname
    printf 'scenario=%s\n' "$scenario"
    sha256sum "$TMPDIR/bench_grid"
    lscpu
    env | LC_ALL=C sort | sed -n '/^SLURM_/p'
} > "$shard_dir/slurm-${SLURM_ARRAY_JOB_ID}_${index}.txt"

cd "$src_dir"

# BSC warns that srun does not reliably inherit this value.
export SRUN_CPUS_PER_TASK="${SLURM_CPUS_PER_TASK:?}"
export RUST_BACKTRACE=1

srun \
  --ntasks=1 \
  --cpus-per-task="$SLURM_CPUS_PER_TASK" \
  --cpu-bind=cores \
  --kill-on-bad-exit=1 \
  "$TMPDIR/bench_grid" \
    --machine-label "$machine_label" \
    --runs-dir "$shard_dir" \
    --parallelism "$SLURM_CPUS_PER_TASK" \
    --scenarios "$scenario" \
    --queues ubq,segqueue,concurrent-queue,rbbq,lfqueue,wcq \
    --modes throughput \
    --repeats 3 \
    --batch-sizes 2,8,16,64,256,1024,4096 \
    --fastfifo-block-sizes 64,256,1024,4096 \
    --fastfifo-capacities 1048576 \
    --lfqueue-segment-sizes 32,256,1024 \
    --wcq-capacities 4096,65536,1048576 \
    --schedule-seed 0x55425106 \
    --throughput-warmup-ms 250 \
    --throughput-pilot-ms 100 \
    --throughput-phase-ms 1000 \
    --throughput-max-round-items 8388608