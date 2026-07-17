#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
out_root="${OUT_ROOT:-bench_results/io_uring_queue}"
timestamp=$(date -u +%Y%m%dT%H%M%SZ)
host=$(hostname | tr -c '[:alnum:]_.-' '-')
run_dir="$out_root/$timestamp-$host"
features="${CARGO_FEATURES:-bench_rbbq}"

has_out_dir=0
for arg in "$@"; do
  case "$arg" in
    --out-dir|--out-dir=*)
      has_out_dir=1
      ;;
  esac
done

if [[ "$has_out_dir" -eq 0 ]]; then
  set -- --out-dir "$run_dir" "$@"
fi

cd "$repo_root"
cargo run --release --features "$features" --bin io_uring_queue_bench -- "$@"
