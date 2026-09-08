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
queue_set=${UBQ_BENCH_QUEUES:-ubq,lubq,segqueue,concurrent-queue,rbbq,lfqueue,mutex-vecdeque,ms-queue,moodycamel-cq}
batch_sizes=${UBQ_BENCH_BATCH_SIZES:-2,8,16,64,256,1024,4096,16384,65536,131072,262144}
fastfifo_block_sizes=${UBQ_BENCH_FASTFIFO_BLOCK_SIZES:-64,256,1024,4096}
fastfifo_capacities=${UBQ_BENCH_FASTFIFO_CAPACITIES:-1048576}
lfqueue_segment_sizes=${UBQ_BENCH_LFQUEUE_SEGMENT_SIZES:-32,256,1024}
wcq_capacities=${UBQ_BENCH_WCQ_CAPACITIES:-4096,65536,1048576}
repeats=${UBQ_BENCH_REPEATS:-3}
throughput_warmup_ms=${UBQ_BENCH_WARMUP_MS:-250}
throughput_pilot_ms=${UBQ_BENCH_PILOT_MS:-100}
throughput_phase_ms=${UBQ_BENCH_PHASE_MS:-1000}
throughput_max_round_items=${UBQ_BENCH_MAX_ROUND_ITEMS:-8388608}

if [[ ! "$scenario" =~ ^[0-9]+p[0-9]+c$ ]]; then
    echo "Invalid scenario: $scenario" >&2
    exit 2
fi

# This array task owns exactly this one scenario end to end: it writes
# straight into the shared coalesced tree at
# runs_root/machine_label/scenario/record.json (bench_grid computes that
# path itself from --runs-dir/--machine-label/--scenarios) rather than a
# private per-task shard directory. A small sidecar next to it records what
# this node actually was.
scenario_dir="$runs_root/$machine_label/$scenario"
mkdir -p "$scenario_dir"

: "${TMPDIR:?Slurm did not provide TMPDIR}"
install -m 0755 "$binary" "$TMPDIR/bench_grid"

{
    date --iso-8601=seconds
    hostname
    printf 'scenario=%s\n' "$scenario"
    printf 'queues=%s\n' "$queue_set"
    printf 'batch_sizes=%s\n' "$batch_sizes"
    printf 'fastfifo_block_sizes=%s\n' "$fastfifo_block_sizes"
    printf 'fastfifo_capacities=%s\n' "$fastfifo_capacities"
    printf 'lfqueue_segment_sizes=%s\n' "$lfqueue_segment_sizes"
    printf 'repeats=%s\n' "$repeats"
    printf 'throughput_warmup_ms=%s\n' "$throughput_warmup_ms"
    printf 'throughput_pilot_ms=%s\n' "$throughput_pilot_ms"
    printf 'throughput_phase_ms=%s\n' "$throughput_phase_ms"
    sha256sum "$TMPDIR/bench_grid"
    lscpu
    env | LC_ALL=C sort | sed -n '/^SLURM_/p'
} > "$scenario_dir/slurm-info.txt"

cd "$src_dir"

# BSC warns that srun does not reliably inherit this value.
export SRUN_CPUS_PER_TASK="${SLURM_CPUS_PER_TASK:?}"
export RUST_BACKTRACE=1

# naive-faa-queue is deliberately excluded here: it's the pathological/DNF
# baseline (src/bench_harness/baselines/naive_faa_queue.rs), meant to
# demonstrate livelock under contention rather than produce a comparable
# throughput number, and its own timeout needs are much tighter than the
# rest of this grid's. Run it separately (own --job-timeout-secs) if wanted.
#
# --batch-sizes: 131072 and 262144 were added on top of the existing sweep
#   specifically to reach into massive-batch territory. Both substantially
#   exceed the effective page capacity, so each push_batch crosses many block
#   boundaries.
srun \
  --ntasks=1 \
  --cpus-per-task="$SLURM_CPUS_PER_TASK" \
  --cpu-bind=cores \
  --kill-on-bad-exit=1 \
  "$TMPDIR/bench_grid" \
    --machine-label "$machine_label" \
    --runs-dir "$runs_root" \
    --parallelism "$SLURM_CPUS_PER_TASK" \
    --scenarios "$scenario" \
    --queues "$queue_set" \
    --modes throughput \
    --repeats "$repeats" \
    --batch-sizes "$batch_sizes" \
    --fastfifo-block-sizes "$fastfifo_block_sizes" \
    --fastfifo-capacities "$fastfifo_capacities" \
    --lfqueue-segment-sizes "$lfqueue_segment_sizes" \
    --wcq-capacities "$wcq_capacities" \
    --schedule-seed 0x55425106 \
    --throughput-warmup-ms "$throughput_warmup_ms" \
    --throughput-pilot-ms "$throughput_pilot_ms" \
    --throughput-phase-ms "$throughput_phase_ms" \
    --throughput-max-round-items "$throughput_max_round_items"
