#!/usr/bin/env bash
set -euo pipefail

usage() {
    cat <<'EOF'
Usage: slurm/submit_build.sh {mn5|grace}

Submit slurm/build.sbatch on the given BSC cluster: compiles bench_grid and a
symbolized/frame-pointer bench_profile into $UBQ/artifacts/<cluster>/. Prints the submitted job id (--parsable)
so it can be chained into submit_bench_grid.sh via --after.

Env overrides:
  ACCOUNT   BSC allocation account (default: bsc18)
  UBQ       deployed repo root on the cluster
            (default: /gpfs/projects/$ACCOUNT/$USER/UBQ)

Example:
  build_id=$(slurm/submit_build.sh mn5)
  slurm/submit_bench_grid.sh mn5 mn5-1 --after "$build_id"
EOF
}

if [[ $# -ne 1 ]]; then
    usage >&2
    exit 2
fi

case "$1" in
    mn5)
        cluster=mn5
        qos=gp_debug
        partition=gpp
        ;;
    grace)
        cluster=grace
        qos=ngp_debug
        partition=ngpp
        ;;
    -h|--help)
        usage
        exit 0
        ;;
    *)
        printf 'Unknown cluster: %s\n\n' "$1" >&2
        usage >&2
        exit 2
        ;;
esac

: "${ACCOUNT:=bsc18}"
: "${UBQ:=/gpfs/projects/$ACCOUNT/$USER/UBQ}"

mkdir -p "$UBQ/logs"

sbatch --parsable \
    --account="$ACCOUNT" \
    --qos="$qos" \
    --partition="$partition" \
    --nodes=1 \
    --ntasks=1 \
    --cpus-per-task=4 \
    --time=00:30:00 \
    --job-name="ubq-build-$cluster" \
    --output="$UBQ/logs/build-$cluster-%j.out" \
    --error="$UBQ/logs/build-$cluster-%j.err" \
    "$UBQ/slurm/build.sbatch" \
    "$UBQ" \
    "$UBQ/artifacts/$cluster"
