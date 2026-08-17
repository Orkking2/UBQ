#!/usr/bin/env bash
set -euo pipefail

usage() {
    cat <<'EOF'
Usage: slurm/submit_bench_grid.sh {mn5|grace} <machine-label> [manifest] [--after <job-id>]

Submit slurm/bench_grid_array.sbatch as a Slurm array job: one array task per
scenario in the manifest, computed from the manifest itself (not hand-typed),
with no %K concurrency throttle and no node-count cap — Slurm's own
partition/QOS/fairshare limits decide how many run at once. Each task writes
straight into the shared coalesced tree at
$UBQ/bench_results/runs/<machine-label>/<scenario>/record.json.

<machine-label> is a required, explicit choice, not a default: bump it
(e.g. mn5-1 -> mn5-2) for a fresh sweep so this batch's data stays isolated
from a previous one, or reuse it deliberately to greedily resume/extend one.

  manifest       defaults to manifests/mn5-112.txt or manifests/grace-144.txt
  --after JOBID  adds --dependency=afterok:JOBID, e.g. to chain directly onto
                 the job id printed by submit_build.sh

Env overrides:
  ACCOUNT   BSC allocation account (default: bsc18)
  UBQ       deployed repo root on the cluster
            (default: /gpfs/projects/$ACCOUNT/$USER/UBQ)

Example:
  build_id=$(slurm/submit_build.sh mn5)
  slurm/submit_bench_grid.sh mn5 mn5-1 --after "$build_id"
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

case "$1" in
    mn5)
        cluster=mn5
        qos=gp_bsccs
        partition=gpp
        cpus=112
        extra_sbatch_args=(--hint=nomultithread)
        default_manifest=manifests/mn5-112.txt
        ;;
    grace)
        cluster=grace
        qos=ngp_bsccs
        partition=ngpp
        cpus=144
        extra_sbatch_args=()
        default_manifest=manifests/grace-144.txt
        ;;
    *)
        printf 'Unknown cluster: %s\n\n' "$1" >&2
        usage >&2
        exit 2
        ;;
esac
machine_label=${2:?machine label required}
shift 2

manifest=""
after_job_id=""
while [[ $# -gt 0 ]]; do
    case "$1" in
        --after)
            after_job_id=${2:?--after requires a job id}
            shift 2
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            if [[ -n "$manifest" ]]; then
                printf 'Unexpected argument: %s\n\n' "$1" >&2
                usage >&2
                exit 2
            fi
            manifest=$1
            shift
            ;;
    esac
done

: "${ACCOUNT:=bsc18}"
: "${UBQ:=/gpfs/projects/$ACCOUNT/$USER/UBQ}"
manifest=${manifest:-"$UBQ/$default_manifest"}

scenario_count=$(awk 'NF && $1 !~ /^#/' "$manifest" | wc -l)
if ((scenario_count == 0)); then
    echo "manifest $manifest has no scenarios" >&2
    exit 2
fi

mkdir -p "$UBQ/logs"

dependency_args=()
if [[ -n "$after_job_id" ]]; then
    dependency_args=(--dependency="afterok:$after_job_id")
fi

sbatch \
    --account="$ACCOUNT" \
    --qos="$qos" \
    --partition="$partition" \
    --nodes=1 \
    --ntasks=1 \
    --cpus-per-task="$cpus" \
    --exclusive \
    "${extra_sbatch_args[@]+"${extra_sbatch_args[@]}"}" \
    --time=02:00:00 \
    --array="0-$((scenario_count - 1))" \
    "${dependency_args[@]+"${dependency_args[@]}"}" \
    --job-name="ubq-$cluster" \
    --output="$UBQ/logs/$cluster-%A_%a.out" \
    --error="$UBQ/logs/$cluster-%A_%a.err" \
    "$UBQ/slurm/bench_grid_array.sbatch" \
    "$UBQ/src" \
    "$UBQ/artifacts/$cluster/bench_grid" \
    "$manifest" \
    "$UBQ/bench_results/runs" \
    "$machine_label"
