#!/usr/bin/env bash
set -euo pipefail

repo_root=${1:?repository root required}
cluster=${2:?cluster required}
results_root=${3:?results root required}
duration_ms=${4:?duration required}
warmup_ms=${5:?warmup required}
repeats=${6:?repeats required}

module purge
module load gcc
module load rust

: "${TMPDIR:?Slurm did not provide TMPDIR}"
: "${SLURM_CPUS_PER_TASK:?Slurm CPU count is unavailable}"
case "$cluster" in
    mn5|grace) ;;
    *) echo "unsupported cluster: $cluster" >&2; exit 2 ;;
esac

atomic_source="$repo_root/artifacts/$cluster/bench_atomic_updates"
reload_source="$repo_root/artifacts/$cluster/bench_head_reload"
if [[ ! -x "$atomic_source" || ! -x "$reload_source" ]]; then
    echo "focused profiling binaries are missing; submit a fresh build first" >&2
    exit 2
fi

atomic_binary="$TMPDIR/bench_atomic_updates"
reload_binary="$TMPDIR/bench_head_reload"
install -m 0755 "$atomic_source" "$atomic_binary"
install -m 0755 "$reload_source" "$reload_binary"

job_dir="$results_root/focused/$cluster/${SLURM_JOB_ID:?}"
mkdir -p "$job_dir"
{
    date --iso-8601=seconds
    hostname
    uname -a
    lscpu
    cat /proc/sys/kernel/perf_event_paranoid
    module -t list 2>&1
    sha256sum "$atomic_source" "$reload_source"
} > "$job_dir/provenance.txt"

"$atomic_binary" \
    --machine-label "$cluster" \
    --output "$job_dir/atomic_updates.json" \
    --max-updaters "$SLURM_CPUS_PER_TASK" \
    --duration-ms "$duration_ms" \
    --warmup-ms "$warmup_ms" \
    --repeats "$repeats"

"$reload_binary" \
    --machine-label "$cluster" \
    --output "$job_dir/head_reload.json" \
    --max-threads "$SLURM_CPUS_PER_TASK" \
    --duration-ms "$duration_ms" \
    --warmup-ms "$warmup_ms" \
    --repeats "$repeats"

printf 'wrote focused contention results to %s\n' "$job_dir"
