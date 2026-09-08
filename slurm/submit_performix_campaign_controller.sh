#!/usr/bin/env bash
set -euo pipefail

usage() {
    cat <<'EOF'
Usage: slurm/submit_performix_campaign_controller.sh {mn5|grace} <machine-label> --after JOBID

Submit the lightweight continuation job which selects APX targets after the
complete throughput frontier, then extends the dependency graph with frozen
Code Hotspots and hardware arrays. The automation settings are read from the
workflow state created by scripts/performix_campaign_workflow.py auto.
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
    usage
    exit 0
fi
if [[ $# -lt 4 ]]; then
    usage >&2
    exit 2
fi

cluster=$1
machine_label=$2
shift 2

case "$cluster" in
    mn5)
        qos=gp_debug
        partition=gpp
        ;;
    grace)
        qos=ngp_debug
        partition=ngpp
        ;;
    *)
        printf 'Unknown cluster: %s\n' "$cluster" >&2
        exit 2
        ;;
esac

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
            printf 'Unexpected argument: %s\n' "$1" >&2
            exit 2
            ;;
    esac
done
if [[ ! "$after_job_id" =~ ^[0-9]+$ ]]; then
    echo "--after requires a numeric Slurm job id" >&2
    exit 2
fi

: "${ACCOUNT:=bsc18}"
: "${UBQ:=/gpfs/projects/$ACCOUNT/$USER/UBQ}"
mkdir -p "$UBQ/logs"

submission_output=$(sbatch --parsable \
    --account="$ACCOUNT" \
    --qos="$qos" \
    --partition="$partition" \
    --nodes=1 \
    --ntasks=1 \
    --cpus-per-task=1 \
    --time=00:20:00 \
    --export=ALL \
    --dependency="afterok:$after_job_id" \
    --job-name="flow-$machine_label" \
    --output="$UBQ/logs/flow-$cluster-%j.out" \
    --error="$UBQ/logs/flow-$cluster-%j.err" \
    "$UBQ/slurm/performix_campaign_controller.sbatch" \
    "$UBQ" \
    "$cluster" \
    "$machine_label")
if [[ -z "$submission_output" ]]; then
    echo "sbatch succeeded without returning a job id" >&2
    exit 1
fi
slurm_job_id=${submission_output%%$'\n'*}
slurm_job_id=${slurm_job_id%%;*}
printf '%s\n' "$slurm_job_id"
