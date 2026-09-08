#!/usr/bin/env bash
set -euo pipefail

repo_root=${1:?repository root required}
cluster=${2:?cluster required}
results_root=${3:?results root required}
scenarios_csv=${4:?comma-separated scenario list required}
batch_size=${5:?batch size required}
repeats=${6:?repeats required}
duration_secs=${7:?duration required}
warmup_secs=${8:?warmup required}

module purge
module load gcc
module load rust

: "${TMPDIR:?Slurm did not provide TMPDIR}"
case "$cluster" in
    mn5|grace) ;;
    *) echo "unsupported cluster: $cluster" >&2; exit 2 ;;
esac

source_binary="$repo_root/artifacts/$cluster/bench_profile"
if [[ ! -x "$source_binary" ]]; then
    echo "bench_profile is missing for $cluster; submit a fresh build first" >&2
    exit 2
fi

binary="$TMPDIR/bench_profile"
install -m 0755 "$source_binary" "$binary"

job_dir="$results_root/$cluster/${SLURM_JOB_ID:?}"
mkdir -p "$job_dir"
{
    date --iso-8601=seconds
    hostname
    uname -a
    lscpu
    module -t list 2>&1
    sha256sum "$source_binary"
} > "$job_dir/provenance.txt"

IFS=',' read -r -a scenarios <<< "$scenarios_csv"
results_file="$job_dir/results.jsonl"
: > "$results_file"

for policy in crossbeam yield; do
    for scenario in "${scenarios[@]}"; do
        for ((repeat = 1; repeat <= repeats; repeat++)); do
            echo "policy=$policy scenario=$scenario repeat=$repeat/$repeats" >&2
            args=(--queue lubq --scenario "$scenario" --batch-size "$batch_size"
                  --warmup-secs "$warmup_secs" --duration-secs "$duration_secs" --json)
            if [[ "$policy" == "yield" ]]; then
                args+=(--lubq-backoff yield)
            fi
            "$binary" "${args[@]}" | python3 -c "
import json, sys
obj = json.load(sys.stdin)
obj['lubq_backoff'] = '$policy'
obj['repeat_index'] = $repeat
print(json.dumps(obj))
" >> "$results_file"
        done
    done
done

echo "wrote $results_file"
