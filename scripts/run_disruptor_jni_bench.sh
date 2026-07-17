#!/usr/bin/env bash
set -euo pipefail

DISRUPTOR_REPO="${DISRUPTOR_REPO:-https://github.com/LMAX-Exchange/disruptor.git}"
DISRUPTOR_REV="${DISRUPTOR_REV:-c871ca49826a6be7ada6957f6fbafcfecf7b1f87}"
QUEUE_SET="${QUEUE_SET:-ubq,rbbq}"
OUT_DIR="${OUT_DIR:-bench_results/disruptor_jni}"
WORK_ROOT="${WORK_ROOT:-${TMPDIR:-/tmp}/ubq_disruptor_jni}"
RBBQ_BLOCK_SIZE="${RBBQ_BLOCK_SIZE:-64}"
UBQ_VARIANTS="${UBQ_VARIANTS:-default}"
KEEP_WORKDIR=0

DEFAULT_UBQ_VARIANTS="balanced,8,127,crossbeam"
SWEEP_UBQ_VARIANTS="balanced,0,127,crossbeam;balanced,4,127,crossbeam;balanced,8,63,crossbeam;balanced,8,127,crossbeam;balanced,8,255,crossbeam;balanced,16,127,crossbeam;balanced,32,127,crossbeam;balanced,8,31,crossbeam;balanced,8,511,crossbeam;balanced,8,127,yield"

log() {
  printf '[disruptor-jni] %s\n' "$*" >&2
}

die() {
  printf '[disruptor-jni] ERROR: %s\n' "$*" >&2
  exit 1
}

on_error() {
  local status=$?
  printf '[disruptor-jni] ERROR: failed at line %s with exit code %s\n' "${BASH_LINENO[0]:-?}" "$status" >&2
  if [[ -n "${run_dir:-}" ]]; then
    printf '[disruptor-jni] Partial output, if any: %s\n' "$run_dir" >&2
  fi
  exit "$status"
}

trap on_error ERR

bootstrap_path() {
  local cargo_env=""
  if [[ -n "${HOME:-}" ]]; then
    export PATH="$HOME/.cargo/bin:$PATH"
    cargo_env="$HOME/.cargo/env"
  fi

  export PATH="/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin:$PATH"

  if ! command -v cargo >/dev/null 2>&1 && [[ -r "$cargo_env" ]]; then
    # shellcheck disable=SC1090
    source "$cargo_env"
  fi
}

require_cmd() {
  local name="$1"
  if ! command -v "$name" >/dev/null 2>&1; then
    die "required command '$name' was not found. PATH=$PATH"
  fi
}

usage() {
  cat <<'USAGE'
Usage: scripts/run_disruptor_jni_bench.sh [options]

Options:
  --queue ubq|rbbq|disruptor|both|all|a,b  Queue set to run (default: ubq,rbbq)
  --out-dir DIR                       Output root (default: bench_results/disruptor_jni)
  --work-root DIR                     Temporary checkout root
  --disruptor-repo URL                Disruptor git URL
  --disruptor-rev REV                 Disruptor commit/tag
  --rbbq-block-size N                 RBBQ block size (default: 64)
  --ubq-variants SET                  default|sweep|all|semicolon-separated labels
  --keep-workdir                      Keep temporary Disruptor checkouts
  -h, --help                          Show this help

UBQ variant labels use commas, so explicit lists must be separated with
semicolons, for example:
  --ubq-variants 'balanced,8,63,crossbeam;balanced,8,127,crossbeam'
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --queue)
      QUEUE_SET="$2"
      shift 2
      ;;
    --out-dir)
      OUT_DIR="$2"
      shift 2
      ;;
    --work-root)
      WORK_ROOT="$2"
      shift 2
      ;;
    --disruptor-repo)
      DISRUPTOR_REPO="$2"
      shift 2
      ;;
    --disruptor-rev)
      DISRUPTOR_REV="$2"
      shift 2
      ;;
    --rbbq-block-size)
      RBBQ_BLOCK_SIZE="$2"
      shift 2
      ;;
    --ubq-variants)
      UBQ_VARIANTS="$2"
      shift 2
      ;;
    --keep-workdir)
      KEEP_WORKDIR=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "unknown option: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
bootstrap_path
timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
host_label="$(hostname -s 2>/dev/null || hostname)"
case "$OUT_DIR" in
  /*) run_dir="$OUT_DIR/$timestamp-$host_label" ;;
  *) run_dir="$repo_root/$OUT_DIR/$timestamp-$host_label" ;;
esac
mkdir -p "$run_dir/logs" "$WORK_ROOT"

case "$QUEUE_SET" in
  all) QUEUE_SET="ubq,rbbq,disruptor" ;;
  both) QUEUE_SET="ubq,rbbq" ;;
esac

case "$UBQ_VARIANTS" in
  default)
    UBQ_VARIANTS="$DEFAULT_UBQ_VARIANTS"
    ;;
  sweep|all)
    UBQ_VARIANTS="$SWEEP_UBQ_VARIANTS"
    ;;
esac

IFS=',' read -r -a queues <<< "$QUEUE_SET"
IFS=';' read -r -a ubq_variants <<< "$UBQ_VARIANTS"

log "Starting run"
log "Repo root: $repo_root"
log "Queue set: $QUEUE_SET"
log "UBQ variants: $UBQ_VARIANTS"
log "Disruptor: $DISRUPTOR_REPO @ $DISRUPTOR_REV"
log "Output: $run_dir"

needs_rbbq=0
needs_native=0
include_ubq=0
for queue in "${queues[@]}"; do
  case "$queue" in
    ubq)
      needs_native=1
      include_ubq=1
      ;;
    rbbq)
      needs_native=1
      needs_rbbq=1
      ;;
    disruptor)
      ;;
    *)
      echo "unsupported queue selector: $queue" >&2
      exit 2
      ;;
  esac
done

if [[ "$include_ubq" -eq 1 ]]; then
  if [[ "${#ubq_variants[@]}" -eq 0 ]]; then
    die "at least one UBQ variant is required when queue set includes ubq"
  fi
  for ubq_variant in "${ubq_variants[@]}"; do
    if [[ -z "$ubq_variant" ]]; then
      die "empty UBQ variant in --ubq-variants"
    fi
  done
fi

require_cmd git
require_cmd java
require_cmd tee

if [[ "$needs_native" -eq 1 ]]; then
  require_cmd cargo
  features="jni"
  if [[ "$needs_rbbq" -eq 1 ]]; then
    features="jni,bench_fastfifo"
  fi
  log "Building libubq with features: $features"
  cargo rustc --release --lib --crate-type cdylib --features "$features"
fi

cat > "$run_dir/metadata.txt" <<META
timestamp_utc=$timestamp
host=$host_label
repo_root=$repo_root
disruptor_repo=$DISRUPTOR_REPO
disruptor_rev=$DISRUPTOR_REV
queue_set=$QUEUE_SET
rbbq_block_size=$RBBQ_BLOCK_SIZE
ubq_variants=$UBQ_VARIANTS
META

run_classes_for_queue() {
  local queue="$1"
  local checkout="$WORK_ROOT/disruptor-$queue-$timestamp"
  local cp="build/classes/java/main:build/classes/java/test:build/classes/java/perftest"

  log "Preparing Disruptor checkout for $queue: $checkout"
  git clone "$DISRUPTOR_REPO" "$checkout"
  git -C "$checkout" checkout "$DISRUPTOR_REV"

  cp -R "$repo_root/bindings/disruptor-jni/src/main/java/ubq" "$checkout/src/perftest/java/"

  case "$queue" in
    ubq)
      git -C "$checkout" apply "$repo_root/bindings/disruptor-jni/lmax-native-queue-adapter.patch"
      ;;
    rbbq)
      git -C "$checkout" apply "$repo_root/bindings/disruptor-jni/lmax-native-rbbq-adapter.patch"
      ;;
    disruptor)
      ;;
  esac

  log "Compiling Disruptor perftest classes for $queue"
  (cd "$checkout" && ./gradlew perftestClasses)

  if [[ "$queue" == "ubq" ]]; then
    for ubq_variant in "${ubq_variants[@]}"; do
      if [[ -z "$ubq_variant" ]]; then
        die "empty UBQ variant in --ubq-variants"
      fi
      local ubq_queue_label="ubq_$ubq_variant"
      run_one "$queue" "$ubq_queue_label" "$ubq_variant" "1p1c" "$checkout" "$cp" \
        com.lmax.disruptor.queue.OneToOneQueueThroughputTest
      run_one "$queue" "$ubq_queue_label" "$ubq_variant" "3p1c" "$checkout" "$cp" \
        com.lmax.disruptor.queue.ThreeToOneQueueThroughputTest
      run_one "$queue" "$ubq_queue_label" "$ubq_variant" "1p3c" "$checkout" "$cp" \
        com.lmax.disruptor.queue.OneToThreeQueueThroughputTest
    done
  elif [[ "$queue" == "disruptor" ]]; then
    run_one "$queue" "$queue" "" "1p1c" "$checkout" "$cp" \
      com.lmax.disruptor.sequenced.OneToOneSequencedThroughputTest
    run_one "$queue" "$queue" "" "3p1c" "$checkout" "$cp" \
      com.lmax.disruptor.sequenced.ThreeToOneSequencedThroughputTest
    run_one "$queue" "$queue" "" "1p3c" "$checkout" "$cp" \
      com.lmax.disruptor.sequenced.OneToThreeSequencedThroughputTest
  else
    run_one "$queue" "$queue" "" "1p1c" "$checkout" "$cp" \
      com.lmax.disruptor.queue.OneToOneQueueThroughputTest
    run_one "$queue" "$queue" "" "3p1c" "$checkout" "$cp" \
      com.lmax.disruptor.queue.ThreeToOneQueueThroughputTest
    run_one "$queue" "$queue" "" "1p3c" "$checkout" "$cp" \
      com.lmax.disruptor.queue.OneToThreeQueueThroughputTest
  fi

  if [[ "$KEEP_WORKDIR" -eq 0 ]]; then
    log "Removing temporary checkout: $checkout"
    rm -rf "$checkout"
  fi
}

run_one() {
  local queue="$1"
  local log_queue="$2"
  local ubq_variant="$3"
  local scenario="$4"
  local checkout="$5"
  local cp="$6"
  local class_name="$7"
  local log_path="$run_dir/logs/${log_queue}__${scenario}.log"

  log "Running $log_queue $scenario -> $log_path"
  if [[ "$queue" == "ubq" ]]; then
    (cd "$checkout" && java \
      -Djava.library.path="$repo_root/target/release" \
      -Dubq.jni.ubqVariant="$ubq_variant" \
      -cp "$cp" "$class_name") | tee "$log_path"
  elif [[ "$queue" == "rbbq" ]]; then
    (cd "$checkout" && java \
      -Djava.library.path="$repo_root/target/release" \
      -Dubq.jni.rbbq.blockSize="$RBBQ_BLOCK_SIZE" \
      -cp "$cp" "$class_name") | tee "$log_path"
  else
    (cd "$checkout" && java -cp "$cp" "$class_name") | tee "$log_path"
  fi
}

for queue in "${queues[@]}"; do
  run_classes_for_queue "$queue"
done

python_bin="python3"
if ! command -v "$python_bin" >/dev/null 2>&1; then
  python_bin="python"
fi
require_cmd "$python_bin"
"$python_bin" "$repo_root/scripts/summarize_disruptor_jni.py" "$run_dir" --out-dir "$run_dir"

log "Results written to $run_dir"
log "Summary CSV: $run_dir/summary.csv"
log "Samples CSV: $run_dir/samples.csv"
