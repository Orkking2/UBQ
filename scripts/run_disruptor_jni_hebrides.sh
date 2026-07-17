#!/usr/bin/env bash
set -euo pipefail

HEBRIDES_HOST="${HEBRIDES_HOST:-hebrides}"
REMOTE_REPO="${REMOTE_REPO:-~/UBQ}"
REMOTE_OUT_DIR="${REMOTE_OUT_DIR:-bench_results/disruptor_jni}"
QUEUE_SET="${QUEUE_SET:-ubq,rbbq}"
RBBQ_BLOCK_SIZE="${RBBQ_BLOCK_SIZE:-64}"
UBQ_VARIANTS="${UBQ_VARIANTS:-default}"
DISRUPTOR_REPO="${DISRUPTOR_REPO:-https://github.com/LMAX-Exchange/disruptor.git}"
DISRUPTOR_REV="${DISRUPTOR_REV:-c871ca49826a6be7ada6957f6fbafcfecf7b1f87}"

log() {
  printf '[disruptor-jni-hebrides] %s\n' "$*" >&2
}

die() {
  printf '[disruptor-jni-hebrides] ERROR: %s\n' "$*" >&2
  exit 1
}

on_error() {
  local status=$?
  printf '[disruptor-jni-hebrides] ERROR: failed at line %s with exit code %s\n' "${BASH_LINENO[0]:-?}" "$status" >&2
  exit "$status"
}

trap on_error ERR

require_cmd() {
  local name="$1"
  if ! command -v "$name" >/dev/null 2>&1; then
    die "required command '$name' was not found. PATH=$PATH"
  fi
}

usage() {
  cat <<'USAGE'
Usage: scripts/run_disruptor_jni_hebrides.sh [options]

Options:
  --host HOST             SSH host (default: hebrides)
  --remote-repo DIR       Remote repo/worktree directory (default: ~/UBQ)
  --queue SET             ubq|rbbq|disruptor|both|all|a,b (default: ubq,rbbq)
  --rbbq-block-size N     RBBQ block size (default: 64)
  --ubq-variants SET      default|sweep|all|semicolon-separated labels
  --disruptor-repo URL    Disruptor git URL
  --disruptor-rev REV     Disruptor commit/tag
  -h, --help              Show this help

The script syncs the source files needed for this benchmark to the remote
directory, runs scripts/run_disruptor_jni_bench.sh over SSH, then pulls the
bench_results/disruptor_jni folder back into the local workspace.
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --host)
      HEBRIDES_HOST="$2"
      shift 2
      ;;
    --remote-repo)
      REMOTE_REPO="$2"
      shift 2
      ;;
    --queue)
      QUEUE_SET="$2"
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
    --disruptor-repo)
      DISRUPTOR_REPO="$2"
      shift 2
      ;;
    --disruptor-rev)
      DISRUPTOR_REV="$2"
      shift 2
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
cd "$repo_root"

require_cmd ssh
require_cmd rsync

log "Host: $HEBRIDES_HOST"
log "Remote repo: $REMOTE_REPO"
log "Queue set: $QUEUE_SET"
log "UBQ variants: $UBQ_VARIANTS"
log "Disruptor: $DISRUPTOR_REPO @ $DISRUPTOR_REV"

log "Ensuring remote directory exists"
ssh "$HEBRIDES_HOST" "mkdir -p $REMOTE_REPO"

log "Syncing benchmark source to $HEBRIDES_HOST:$REMOTE_REPO"
rsync -az \
  Cargo.toml Cargo.lock build.rs README.md LICENSE \
  src bindings docs scripts tests benches \
  "$HEBRIDES_HOST:$REMOTE_REPO/"

log "Running remote benchmark"
ssh "$HEBRIDES_HOST" \
  "cd $REMOTE_REPO && DISRUPTOR_REPO='$DISRUPTOR_REPO' DISRUPTOR_REV='$DISRUPTOR_REV' QUEUE_SET='$QUEUE_SET' RBBQ_BLOCK_SIZE='$RBBQ_BLOCK_SIZE' UBQ_VARIANTS='$UBQ_VARIANTS' scripts/run_disruptor_jni_bench.sh --queue '$QUEUE_SET' --rbbq-block-size '$RBBQ_BLOCK_SIZE' --ubq-variants '$UBQ_VARIANTS' --disruptor-repo '$DISRUPTOR_REPO' --disruptor-rev '$DISRUPTOR_REV'"

mkdir -p bench_results/disruptor_jni
log "Pulling results back from $HEBRIDES_HOST:$REMOTE_REPO/$REMOTE_OUT_DIR"
rsync -az "$HEBRIDES_HOST:$REMOTE_REPO/$REMOTE_OUT_DIR/" bench_results/disruptor_jni/

log "Pulled hebrides results into $repo_root/bench_results/disruptor_jni"
