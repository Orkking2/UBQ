#!/usr/bin/env bash
set -euo pipefail

usage() {
    cat <<'EOF'
Usage: slurm/submit_lubq_backoff_ab.sh {mn5|grace} [options]

A/B compares LUBQ's shard reservation backoff policy (crossbeam default vs.
yield, i.e. no spin on CAS failure) with repeated bench_profile throughput
measurements, no APX involved. Runs both policies, back to back, for every
requested scenario, on one exclusive node.

Options:
  --scenarios LIST     Comma-separated scenarios (default: 1p16c,1p32c)
  --batch-size N        Native queue batch size (default: 256)
  --repeats N           Repeats per policy/scenario cell (default: 5)
  --duration-secs N     Measured duration per run (default: 10)
  --warmup-secs N       Warmup before each measured run (default: 2)
  --after JOBID         Depend on a successful build
  --time LIMIT          Slurm wall-clock limit (default: 00:20:00)
  --results-root DIR    Default: $UBQ/profiling_results/lubq_backoff_ab

Env overrides:
  ACCOUNT   BSC allocation account (default: bsc18)
  UBQ       deployed repo root (default: /gpfs/projects/$ACCOUNT/$USER/UBQ)

Example:
  build_id=$(slurm/submit_build.sh grace)
  slurm/submit_lubq_backoff_ab.sh grace --after "$build_id"
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
        ;;
    grace)
        partition=ngpp
        qos=ngp_debug
        cpus=144
        ;;
    *) echo "unsupported cluster: $cluster" >&2; usage >&2; exit 2 ;;
esac

scenarios="1p16c,1p32c"
batch_size=256
repeats=5
duration_secs=10
warmup_secs=2
after_job_id=""
time_limit="00:20:00"
results_root=""
while [[ $# -gt 0 ]]; do
    case "$1" in
        --scenarios) scenarios=${2:?--scenarios requires a value}; shift 2 ;;
        --batch-size) batch_size=${2:?--batch-size requires a value}; shift 2 ;;
        --repeats) repeats=${2:?--repeats requires a value}; shift 2 ;;
        --duration-secs) duration_secs=${2:?--duration-secs requires a value}; shift 2 ;;
        --warmup-secs) warmup_secs=${2:?--warmup-secs requires a value}; shift 2 ;;
        --after) after_job_id=${2:?--after requires a job id}; shift 2 ;;
        --time) time_limit=${2:?--time requires a value}; shift 2 ;;
        --results-root) results_root=${2:?--results-root requires a value}; shift 2 ;;
        -h|--help) usage; exit 0 ;;
        *) echo "Unexpected argument: $1" >&2; usage >&2; exit 2 ;;
    esac
done

: "${ACCOUNT:=bsc18}"
: "${UBQ:=/gpfs/projects/$ACCOUNT/$USER/UBQ}"
: "${results_root:=$UBQ/profiling_results/lubq_backoff_ab}"

mkdir -p "$UBQ/logs" "$results_root"
dependency_args=()
if [[ -n "$after_job_id" ]]; then
    dependency_args=(--dependency="afterok:$after_job_id")
fi

sbatch --parsable \
    --account="$ACCOUNT" \
    --qos="$qos" \
    --partition="$partition" \
    --nodes=1 \
    --ntasks=1 \
    --cpus-per-task="$cpus" \
    --exclusive \
    --time="$time_limit" \
    "${dependency_args[@]+"${dependency_args[@]}"}" \
    --job-name="lubq-backoff-ab-$cluster" \
    --output="$UBQ/logs/lubq-backoff-ab-%j.out" \
    --error="$UBQ/logs/lubq-backoff-ab-%j.err" \
    "$UBQ/slurm/lubq_backoff_ab.sbatch" \
    "$UBQ" \
    "$cluster" \
    "$results_root" \
    "$scenarios" \
    "$batch_size" \
    "$repeats" \
    "$duration_secs" \
    "$warmup_secs"
