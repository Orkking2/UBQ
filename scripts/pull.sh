#!/usr/bin/env bash

set -euo pipefail

usage() {
    cat <<'EOF'
Usage: scripts/pull.sh {hebrides|lab|-H|-L}

Pull bench results from ~/UBQ on the selected host.

  hebrides, -H  Pull from the "hebrides" SSH host
  lab,      -L  Pull from the "lab" SSH host
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

mkdir -p "$repo_root/bench_results"
rsync -avh \
    "${host}:~/UBQ/bench_results/" \
    "$repo_root/bench_results/"
