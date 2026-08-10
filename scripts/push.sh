#!/usr/bin/env bash

set -euo pipefail

usage() {
    cat <<'EOF'
Usage: scripts/push.sh {hebrides|lab|-H|-L}

Push the UBQ workspace to ~/UBQ on the selected host.

  hebrides, -H  Push to the "hebrides" SSH host
  lab,      -L  Push to the "lab" SSH host
EOF
}

if [[ $# -ne 1 ]]; then
    usage >&2
    exit 2
fi

case "$1" in
    hebrides|-H|--hebrides)
        host=hebrides
        ;;
    lab|-L|--lab)
        host=lab
        ;;
    -h|--help)
        usage
        exit 0
        ;;
    *)
        printf 'Unknown host: %s\n\n' "$1" >&2
        usage >&2
        exit 2
        ;;
esac

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd -- "$script_dir/.." && pwd)"

rsync -avh \
    --exclude=target \
    --exclude=.git \
    --exclude=bench_results \
    --exclude=bench_results_old \
    "$repo_root/" \
    "${host}:~/UBQ/"
