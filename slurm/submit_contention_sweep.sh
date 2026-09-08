#!/usr/bin/env bash
set -euo pipefail

usage() {
    cat <<'EOF'
Usage: slurm/submit_contention_sweep.sh {mn5|grace} <machine-label> [--after JOBID]

Submit the feasible Np1c/1pNc/NpNc contention frontier with short throughput
rounds. The sweep includes every correctness-eligible queue implementation,
both UBQ/LUBQ backoffs, native batch sizes 8/64/256/1024/4096, RBBQ block
sizes 64/256/1024/4096, and LSCQ segment sizes 32/256/1024.

The environment variables documented by submit_bench_grid.sh may override any
matrix or timing default below.
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
    usage
    exit 0
fi
if [[ $# -lt 2 ]]; then
    usage >&2
    exit 2
fi

cluster=$1
machine_label=$2
shift 2

case "$cluster" in
    grace) manifest=manifests/contention-grace-144.txt ;;
    mn5) manifest=manifests/contention-mn5-112.txt ;;
    *)
        printf 'Unknown cluster: %s\n' "$cluster" >&2
        exit 2
        ;;
esac

: "${ACCOUNT:=bsc18}"
: "${UBQ:=/gpfs/projects/$ACCOUNT/$USER/UBQ}"
: "${UBQ_BENCH_QUEUES:=ubq,lubq,segqueue,concurrent-queue,rbbq,lfqueue,mutex-vecdeque,ms-queue,moodycamel-cq}"
: "${UBQ_BENCH_BATCH_SIZES:=8,64,256,1024,4096}"
: "${UBQ_BENCH_FASTFIFO_BLOCK_SIZES:=64,256,1024,4096}"
: "${UBQ_BENCH_FASTFIFO_CAPACITIES:=1048576}"
: "${UBQ_BENCH_LFQUEUE_SEGMENT_SIZES:=32,256,1024}"
: "${UBQ_BENCH_REPEATS:=3}"
: "${UBQ_BENCH_WARMUP_MS:=100}"
: "${UBQ_BENCH_PILOT_MS:=50}"
: "${UBQ_BENCH_PHASE_MS:=500}"
: "${UBQ_BENCH_MAX_ROUND_ITEMS:=8388608}"

export ACCOUNT UBQ UBQ_BENCH_QUEUES UBQ_BENCH_BATCH_SIZES
export UBQ_BENCH_FASTFIFO_BLOCK_SIZES UBQ_BENCH_FASTFIFO_CAPACITIES
export UBQ_BENCH_LFQUEUE_SEGMENT_SIZES UBQ_BENCH_REPEATS
export UBQ_BENCH_WARMUP_MS UBQ_BENCH_PILOT_MS UBQ_BENCH_PHASE_MS
export UBQ_BENCH_MAX_ROUND_ITEMS

exec "$UBQ/slurm/submit_bench_grid.sh" \
    "$cluster" "$machine_label" "$UBQ/$manifest" "$@"
