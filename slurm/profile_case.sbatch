#!/usr/bin/env bash
set -euo pipefail

repo_root=${1:?repository root required}
plan=${2:?JSONL plan required}
results_root=${3:?results root required}
retry_incomplete=${4:-0}
index=${SLURM_ARRAY_TASK_ID:?profile_case.sbatch must run as a Slurm array element}

mapfile -t case_fields < <(
    python3 - "$plan" "$index" <<'PY'
import json
import sys

plan, index = sys.argv[1], int(sys.argv[2])
with open(plan, encoding="utf-8") as stream:
    for line_index, raw in enumerate(stream):
        if line_index == index:
            case = json.loads(raw)
            break
    else:
        raise SystemExit(f"array index {index} is outside {plan}")
for name in ("backend", "binary", "cluster", "campaign", "case_id"):
    value = case.get(name)
    if not isinstance(value, str) or not value or "\n" in value:
        raise SystemExit(f"case field {name} must be a non-empty single-line string")
    print(value)
allocator_probe = bool(case.get("allocator_probe", False))
print("1" if allocator_probe else "0")
allocator_binary = case.get("allocator_binary") or ""
if allocator_probe and (
    not isinstance(allocator_binary, str)
    or not allocator_binary
    or "\n" in allocator_binary
):
    raise SystemExit("allocator_probe case requires a non-empty allocator_binary")
print(allocator_binary)
PY
)

backend=${case_fields[0]}
binary=${case_fields[1]}
cluster=${case_fields[2]}
campaign=${case_fields[3]}
case_id=${case_fields[4]}
allocator_probe=${case_fields[5]}
allocator_binary=${case_fields[6]}
if [[ "$binary" != /* ]]; then
    binary="$repo_root/$binary"
fi
if [[ ! -x "$binary" ]]; then
    echo "Profiling binary is missing or not executable: $binary" >&2
    exit 2
fi
if [[ "$allocator_probe" == "1" ]]; then
    if [[ "$allocator_binary" != /* ]]; then
        allocator_binary="$repo_root/$allocator_binary"
    fi
    if [[ ! -x "$allocator_binary" ]]; then
        echo "Allocator profiling binary is missing or not executable: $allocator_binary" >&2
        exit 2
    fi
fi

: "${TMPDIR:?Slurm did not provide TMPDIR}"
module_log="$TMPDIR/profile-modules-${SLURM_JOB_ID:?}_${index}.log"
module_status=ok
{
    module purge
    module load gcc
    module load rust
    if [[ "$backend" == "apx" ]]; then
        if [[ -n "${APX_BIN_DIR:-}" ]]; then
            export PATH="$APX_BIN_DIR:$PATH"
        fi
        apx_module=${APX_MODULE:-arm_performix/2026.3.1}
        if [[ "$apx_module" != "none" ]] && ! module load "$apx_module"; then
            module_status=unavailable
            echo "APX module could not be loaded; the runner will perform the final PATH capability check"
        fi
    fi
    module -t list
} >"$module_log" 2>&1 || module_status=partial

staged_binary="$TMPDIR/bench_profile-$case_id"
install -m 0755 "$binary" "$staged_binary"
export UBQ_PROFILE_BINARY_OVERRIDE="$staged_binary"
if [[ "$allocator_probe" == "1" ]]; then
    staged_allocator_binary="$TMPDIR/bench_profile_alloc-$case_id"
    install -m 0755 "$allocator_binary" "$staged_allocator_binary"
    export UBQ_PROFILE_ALLOCATOR_BINARY_OVERRIDE="$staged_allocator_binary"
fi
export UBQ_PROFILE_MODULE_LOG="$module_log"
export UBQ_PROFILE_APX_MODULE_STATUS="$module_status"
export SRUN_CPUS_PER_TASK="${SLURM_CPUS_PER_TASK:?}"
export PYTHONUNBUFFERED=1

printf 'profile case=%s campaign=%s cluster=%s backend=%s node=%s\n' \
    "$case_id" "$campaign" "$cluster" "$backend" "$(hostname)"

force_args=()
if [[ "$retry_incomplete" == "1" ]]; then
    force_args=(--force)
fi

exec python3 "$repo_root/scripts/run_profile_case.py" \
    --plan "$plan" \
    --index "$index" \
    --results-root "$results_root" \
    --repo-root "$repo_root" \
    "${force_args[@]+"${force_args[@]}"}"
