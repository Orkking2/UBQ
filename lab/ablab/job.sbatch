#!/usr/bin/env bash
# The one Slurm entry point for ./ab jobs:
#   job.sbatch <study-dir> build <deployment>
#   job.sbatch <study-dir> task <deployment> <array-mapping.json>
#   job.sbatch <study-dir> advance <deployment>
# AB_MODULES is a colon-separated list of environment modules to load first.
set -uo pipefail
study=$1
shift
export PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
if [[ -n "${AB_MODULES:-}" ]] && type module >/dev/null 2>&1; then
    module purge >/dev/null 2>&1 || true
    IFS=: read -r -a modules <<< "$AB_MODULES"
    for m in "${modules[@]}"; do
        module load "$m" >/dev/null 2>&1 || echo "ab: module $m is unavailable" >&2
    done
    module -t list || true
fi
exec python3 "$study/tool/ab.py" job "$@"
