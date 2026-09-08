#!/usr/bin/env bash
set -euo pipefail

repo_root=${1:?repository root required}
cluster=${2:?cluster required}
machine_label=${3:?machine label required}

export UBQ="$repo_root"
export PYTHONUNBUFFERED=1

cd "$repo_root"

printf 'continuing throughput-guided Performix campaign cluster=%s label=%s node=%s\n' \
    "$cluster" "$machine_label" "$(hostname)"

exec python3 "$repo_root/scripts/performix_campaign_workflow.py" \
    continue "$cluster" "$machine_label"
