#!/usr/bin/env bash
set -euo pipefail

usage() {
    cat <<'EOF'
Usage: slurm/submit_focused_contention.sh {mn5|grace} [options]

Submit the focused atomic-update and head-reload thread-scaling experiments.

Options:
  --after JOBID       Depend on a successful build
  --duration-ms N     Measured time per cell (default: 250)
  --warmup-ms N       Warm-up time per cell (default: 50)
  --repeats N         Repeats per cell (default: 3)
  --qos NAME          Override the cluster debug QoS
  --time LIMIT        Slurm wall-clock limit (default: 00:30:00)
  --results-root DIR  Default: $UBQ/profiling_results
EOF
}

if [[ $# -lt 1 || "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
    usage
    [[ $# -ge 1 ]] && exit 0 || exit 2
fi
cluster=$1
shift
case "$cluster" in
    mn5)
        partition=gpp
        qos=gp_debug
        cpus=112
        hints=(--hint=nomultithread)
        ;;
    grace)
        partition=ngpp
        qos=ngp_debug
        cpus=144
        hints=()
        ;;
    *) echo "unsupported cluster: $cluster" >&2; usage >&2; exit 2 ;;
esac

after_job_id=""
duration_ms=250
warmup_ms=50
repeats=3
time_limit=00:30:00
results_root=""
while [[ $# -gt 0 ]]; do
    case "$1" in
        --after) after_job_id=${2:?--after requires a value}; shift 2 ;;
        --duration-ms) duration_ms=${2:?--duration-ms requires a value}; shift 2 ;;
        --warmup-ms) warmup_ms=${2:?--warmup-ms requires a value}; shift 2 ;;
        --repeats) repeats=${2:?--repeats requires a value}; shift 2 ;;
        --qos) qos=${2:?--qos requires a value}; shift 2 ;;
        --time) time_limit=${2:?--time requires a value}; shift 2 ;;
        --results-root) results_root=${2:?--results-root requires a value}; shift 2 ;;
        -h|--help) usage; exit 0 ;;
        *) echo "unexpected argument: $1" >&2; usage >&2; exit 2 ;;
    esac
done
for value in "$duration_ms" "$repeats"; do
    if [[ ! "$value" =~ ^[1-9][0-9]*$ ]]; then
        echo "duration and repeats must be positive integers" >&2
        exit 2
    fi
done
if [[ ! "$warmup_ms" =~ ^[0-9]+$ ]]; then
    echo "warmup must be a non-negative integer" >&2
    exit 2
fi

: "${ACCOUNT:=bsc18}"
: "${UBQ:=/gpfs/projects/$ACCOUNT/$USER/UBQ}"
results_root=${results_root:-"${PROFILE_RESULTS_ROOT:-$UBQ/profiling_results}"}
mkdir -p "$UBQ/logs" "$results_root/focused/$cluster"
dependency=()
if [[ -n "$after_job_id" ]]; then
    dependency=(--dependency="afterok:$after_job_id")
fi

sbatch --parsable \
    --account="$ACCOUNT" \
    --qos="$qos" \
    --partition="$partition" \
    --nodes=1 \
    --ntasks=1 \
    --cpus-per-task="$cpus" \
    --exclusive \
    --constraint=perfparanoid \
    "${hints[@]+"${hints[@]}"}" \
    --time="$time_limit" \
    "${dependency[@]+"${dependency[@]}"}" \
    --job-name="focused-$cluster" \
    --output="$UBQ/logs/focused-$cluster-%j.out" \
    --error="$UBQ/logs/focused-$cluster-%j.err" \
    "$UBQ/slurm/focused_contention.sbatch" \
    "$UBQ" "$cluster" "$results_root" "$duration_ms" "$warmup_ms" "$repeats"
