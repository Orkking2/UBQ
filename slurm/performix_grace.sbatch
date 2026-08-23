#!/usr/bin/env bash
set -euo pipefail

src_dir=${1:?source directory required}
binary=${2:?bench_profile binary required}
results_root=${3:?results root required}
queue=${4:?queue required}
scenario=${5:?scenario required}
batch_size=${6-}
ubq_label=${7-}
recipe=${8:?recipe required}
duration_secs=${9:?duration required}
warmup_secs=${10:?warmup required}
tag=${11:?result tag required}

module purge
module load arm_performix/2026.3.1

: "${TMPDIR:?Slurm did not provide TMPDIR}"
result_dir="$results_root/$tag/${SLURM_JOB_ID:?}"
mkdir -p "$result_dir"
install -m 0755 "$binary" "$TMPDIR/bench_profile"

trap 'apx daemon stop >/dev/null 2>&1 || true' EXIT

profile_args=(
    --queue "$queue"
    --scenario "$scenario"
    --duration-secs "$duration_secs"
    --warmup-secs "$warmup_secs"
    --json
)
if [[ -n "$batch_size" ]]; then
    profile_args+=(--batch-size "$batch_size")
fi
if [[ -n "$ubq_label" ]]; then
    profile_args+=(--ubq-label "$ubq_label")
fi

printf -v workload '%q ' "$TMPDIR/bench_profile" "${profile_args[@]}"
workload=${workload% }
apx_timeout=$((duration_secs + warmup_secs + 120))

{
    date --iso-8601=seconds
    hostname
    printf 'queue=%s\n' "$queue"
    printf 'scenario=%s\n' "$scenario"
    printf 'batch_size=%s\n' "${batch_size:-scalar}"
    printf 'ubq_label=%s\n' "${ubq_label:-n/a}"
    printf 'recipe=%s\n' "$recipe"
    printf 'duration_secs=%s\n' "$duration_secs"
    printf 'warmup_secs=%s\n' "$warmup_secs"
    printf 'workload=%s\n' "$workload"
    sha256sum "$TMPDIR/bench_profile"
    lscpu
    env | LC_ALL=C sort | sed -n '/^SLURM_/p'
    apx version
} > "$result_dir/run-info.txt"

# The module supplies APX itself, while the target agent is a separate tool
# deployment. APX 1.20 reports a missing agent as a recipe-readiness message
# with exit status zero, so prepare localhost first and inspect readiness text
# explicitly below.
apx target prepare --target localhost \
    2>&1 | tee "$result_dir/apx-prepare.log"

apx recipe ready "$recipe" \
    --workload "$workload" \
    --working-dir "$src_dir" \
    --target=localhost \
    2>&1 | tee "$result_dir/apx-ready.log"
if grep -Fq "recipe is not ready" "$result_dir/apx-ready.log"; then
    echo "Arm Performix reports that $recipe is not ready; attempting its supported --deploy-tools path"
fi

set +e
apx recipe run "$recipe" \
    --workload "$workload" \
    --working-dir "$src_dir" \
    --target=localhost \
    --timeout "$apx_timeout" \
    --source "$src_dir/src" \
    --deploy-tools \
    2>&1 | tee "$result_dir/apx-run.log"
apx_status=${PIPESTATUS[0]}
set -e
if ((apx_status != 0)); then
    printf 'Arm Performix failed with exit status %s; see %s\n' \
        "$apx_status" "$result_dir/apx-run.log" >&2
    exit "$apx_status"
fi

run_id=$(awk '/Run ID:/ {print $NF}' "$result_dir/apx-run.log" | tail -n 1)
if [[ -z "$run_id" ]]; then
    echo "Arm Performix completed but no Run ID was found in its output" >&2
    exit 1
fi
printf '%s\n' "$run_id" > "$result_dir/run-id.txt"

apx run info "$run_id" 2>&1 | tee "$result_dir/apx-run-info.log"
apx run export "$run_id" "$result_dir" 2>&1 | tee "$result_dir/apx-export.log"

printf 'Performix run %s exported under %s\n' "$run_id" "$result_dir"
