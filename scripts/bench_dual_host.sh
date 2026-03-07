#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  ./scripts/bench_dual_host.sh --ubq-label LABEL [options]
  ./scripts/bench_dual_host.sh --ubq-labels 'LABEL1;LABEL2;...' [options]

Required:
  --ubq-label LABEL         UBQ label (v(3|4|5|7),N,L[,b] or v6,0,L[,b]; repeatable; N in 1|2|4|8|16|32|64)
  --ubq-labels LIST         Semicolon-separated UBQ labels (same format)

Options:
  --remote-host HOST        SSH host alias (default: lab)
  --local-machine-label L   Machine label to write for local runs (default: local)
  --remote-dir DIR          Remote repo directory (default: ~/UBQ)
  --out-root DIR            Output root (default: bench_results)
  --n N                     Repeat benchmark N times per label (default: 1)
  --runs N                  Alias for --n
  --purge-losers            Remove non-winning UBQ labels after each run
  --items-per-producer N    Pass-through to bench harness
  --queues LIST             Pass-through to bench harness
  --scenarios LIST          Pass-through to bench harness (xpxc tokens)
  --modes LIST              Pass-through to bench harness
  --cargo-jobs N            Pass -j N to cargo bench (caps build parallelism)
  --only-ubq                Pass-through to bench harness
  --throughput-only         Pass-through to bench harness
  --skip-plot              Skip plot generation at end (for iterative drivers)
  --tmux                    Run local/remote benchmarks in tmux panes
  --tmux-session NAME       tmux session name (default: auto-generated)
  --skip-remote             Local-only run for development checks
  --skip-local              Remote-only run (skip local benchmark execution)
  -h, --help                Show this help
EOF
}

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 1
  fi
}

resolve_python_cmd() {
  if [[ -n "${PYTHON:-}" ]]; then
    if command -v "$PYTHON" >/dev/null 2>&1; then
      printf '%s' "$PYTHON"
      return 0
    fi
    echo "configured python command not found: $PYTHON" >&2
    exit 1
  fi

  local candidate
  for candidate in python3 python; do
    if command -v "$candidate" >/dev/null 2>&1; then
      printf '%s' "$candidate"
      return 0
    fi
  done

  echo "missing required command: python3 or python" >&2
  exit 1
}

sanitize_name() {
  local raw="$1"
  local sanitized
  sanitized="$(printf '%s' "$raw" | tr '[:upper:]' '[:lower:]' | sed 's/[^a-z0-9._-]/_/g')"
  sanitized="${sanitized##_}"
  sanitized="${sanitized%%_}"
  if [[ -z "$sanitized" ]]; then
    echo "name"
  else
    echo "$sanitized"
  fi
}

label_features_csv() {
  local label="$1"
  local features=""
  local backoff_tag=""

  if [[ "$label" =~ ^v(3|4|5|7)[,_](1|2|4|8|16|32|64)[,_](31|63|127|255|511|1023|2047|4095)([,_](b)?)?$ ]]; then
    local version="${BASH_REMATCH[1]}"
    local pool_size="${BASH_REMATCH[2]}"
    local block_length="${BASH_REMATCH[3]}"
    backoff_tag="${BASH_REMATCH[5]}"
    features="ubq_v${version},ubq_pool_${pool_size},ubq_block_${block_length}"
    if [[ "$backoff_tag" == "b" ]]; then
      features+=",ubq_backoff_cq"
    fi
    printf '%s' "$features"
    return 0
  fi

  if [[ "$label" =~ ^v6[,_]0[,_](31|63|127|255|511|1023|2047|4095)([,_](b)?)?$ ]]; then
    local block_length="${BASH_REMATCH[1]}"
    backoff_tag="${BASH_REMATCH[3]}"
    features="ubq_v6,ubq_block_${block_length}"
    if [[ "$backoff_tag" == "b" ]]; then
      features+=",ubq_backoff_cq"
    fi
    printf '%s' "$features"
    return 0
  fi

  return 1
}

ubq_labels=()
remote_host="lab"
local_machine_label="local"
remote_dir="~/UBQ"
out_root="bench_results"
skip_remote=0
skip_local=0
use_tmux=0
tmux_session=""
run_count=1
purge_losers=0
skip_plot=0
cargo_jobs=""
bench_args=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --ubq-label)
      [[ $# -ge 2 ]] || { echo "--ubq-label requires a value" >&2; exit 1; }
      ubq_labels+=("$2")
      shift 2
      ;;
    --ubq-label=*)
      ubq_labels+=("${1#*=}")
      shift
      ;;
    --ubq-labels)
      [[ $# -ge 2 ]] || { echo "--ubq-labels requires a value" >&2; exit 1; }
      IFS=';' read -r -a parsed_labels <<< "$2"
      ubq_labels+=("${parsed_labels[@]}")
      shift 2
      ;;
    --ubq-labels=*)
      IFS=';' read -r -a parsed_labels <<< "${1#*=}"
      ubq_labels+=("${parsed_labels[@]}")
      shift
      ;;
    --remote-host)
      [[ $# -ge 2 ]] || { echo "--remote-host requires a value" >&2; exit 1; }
      remote_host="$2"
      shift 2
      ;;
    --remote-host=*)
      remote_host="${1#*=}"
      shift
      ;;
    --local-machine-label)
      [[ $# -ge 2 ]] || { echo "--local-machine-label requires a value" >&2; exit 1; }
      local_machine_label="$2"
      shift 2
      ;;
    --local-machine-label=*)
      local_machine_label="${1#*=}"
      shift
      ;;
    --remote-dir)
      [[ $# -ge 2 ]] || { echo "--remote-dir requires a value" >&2; exit 1; }
      remote_dir="$2"
      shift 2
      ;;
    --remote-dir=*)
      remote_dir="${1#*=}"
      shift
      ;;
    --out-root)
      [[ $# -ge 2 ]] || { echo "--out-root requires a value" >&2; exit 1; }
      out_root="$2"
      shift 2
      ;;
    --out-root=*)
      out_root="${1#*=}"
      shift
      ;;
    --n|--runs)
      [[ $# -ge 2 ]] || { echo "$1 requires a value" >&2; exit 1; }
      run_count="$2"
      shift 2
      ;;
    --n=*|--runs=*)
      run_count="${1#*=}"
      shift
      ;;
    --cargo-jobs)
      [[ $# -ge 2 ]] || { echo "--cargo-jobs requires a value" >&2; exit 1; }
      cargo_jobs="$2"
      shift 2
      ;;
    --cargo-jobs=*)
      cargo_jobs="${1#*=}"
      shift
      ;;
    --purge-losers)
      purge_losers=1
      shift
      ;;
    --skip-plot)
      skip_plot=1
      shift
      ;;
    --items-per-producer|--queues|--scenarios|--modes)
      [[ $# -ge 2 ]] || { echo "$1 requires a value" >&2; exit 1; }
      bench_args+=("$1" "$2")
      shift 2
      ;;
    --items-per-producer=*|--queues=*|--scenarios=*|--modes=*)
      bench_args+=("$1")
      shift
      ;;
    --only-ubq|--throughput-only)
      bench_args+=("$1")
      shift
      ;;
    --tmux)
      use_tmux=1
      shift
      ;;
    --tmux-session)
      [[ $# -ge 2 ]] || { echo "--tmux-session requires a value" >&2; exit 1; }
      tmux_session="$2"
      shift 2
      ;;
    --tmux-session=*)
      tmux_session="${1#*=}"
      shift
      ;;
    --skip-remote)
      skip_remote=1
      shift
      ;;
    --skip-local)
      skip_local=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "unknown argument: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if [[ ${#ubq_labels[@]} -eq 0 ]]; then
  echo "at least one --ubq-label (or --ubq-labels) value is required" >&2
  usage >&2
  exit 1
fi

filtered_labels=()
for label in "${ubq_labels[@]}"; do
  trimmed="$label"
  trimmed="${trimmed#"${trimmed%%[![:space:]]*}"}"
  trimmed="${trimmed%"${trimmed##*[![:space:]]}"}"
  if [[ -n "$trimmed" ]]; then
    filtered_labels+=("$trimmed")
  fi
done
ubq_labels=("${filtered_labels[@]}")
if [[ ${#ubq_labels[@]} -eq 0 ]]; then
  echo "all provided labels were empty" >&2
  exit 1
fi

if ! [[ "$run_count" =~ ^[1-9][0-9]*$ ]]; then
  echo "--n/--runs must be a positive integer (got: $run_count)" >&2
  exit 1
fi

if [[ -n "$cargo_jobs" ]] && ! [[ "$cargo_jobs" =~ ^[1-9][0-9]*$ ]]; then
  echo "--cargo-jobs must be a positive integer (got: $cargo_jobs)" >&2
  exit 1
fi

if [[ "$skip_remote" -eq 1 && "$skip_local" -eq 1 ]]; then
  echo "cannot combine --skip-remote and --skip-local (no benchmarks would run)" >&2
  exit 1
fi

local_machine_label="${local_machine_label#"${local_machine_label%%[![:space:]]*}"}"
local_machine_label="${local_machine_label%"${local_machine_label##*[![:space:]]}"}"
if [[ -z "$local_machine_label" ]]; then
  echo "--local-machine-label cannot be empty" >&2
  exit 1
fi

require_cmd cargo
python_bin="$(resolve_python_cmd)"
if [[ "$skip_remote" -eq 0 ]]; then
  require_cmd rsync
  require_cmd ssh
fi
if [[ "$use_tmux" -eq 1 ]]; then
  require_cmd tmux
fi

remote_machine_dir="$(sanitize_name "$remote_host")"
local_machine_dir="$(sanitize_name "$local_machine_label")"
purged_labels_file="$out_root/purged_ubq_labels.txt"
remote_dir_cmd="$remote_dir"

if [[ "$remote_dir_cmd" == "~/"* ]]; then
  remote_dir_cmd="\$HOME/${remote_dir_cmd#"~/"}"
fi

if [[ "$skip_remote" -eq 0 ]]; then
  echo "Syncing repo to ${remote_host}:${remote_dir}..."
  rsync -avz --delete --prune-empty-dirs \
    --include='/Cargo.toml' \
    --include='/Cargo.lock' \
    --include='/README.md' \
    --include='/LICENSE' \
    --include='/src/' --include='/src/**' \
    --include='/benches/' --include='/benches/**' \
    --include='/tests/' --include='/tests/**' \
    --include='/scripts/' --include='/scripts/**' \
    --exclude='*' \
    ./ "${remote_host}:${remote_dir}/"
fi

last_safe_label=""
for ubq_label in "${ubq_labels[@]}"; do
  if ! feature_csv="$(label_features_csv "$ubq_label")"; then
    echo "invalid label '${ubq_label}'. Expected v(3|4|5|7),(1|2|4|8|16|32|64),L[,b] or v6,0,L[,b] where L in (31|63|127|255|511|1023|2047|4095)" >&2
    exit 1
  fi

  safe_label="$(sanitize_name "$ubq_label")"
  last_safe_label="$safe_label"

  if [[ "$purge_losers" -eq 1 && -f "$purged_labels_file" ]] && grep -Fqx "$safe_label" "$purged_labels_file"; then
    echo "WARNING: UBQ label '$ubq_label' (sanitized: '$safe_label') was previously purged as a non-winner." >&2
    echo "WARNING: See $purged_labels_file to review labels that were not best in any machine/scenario." >&2
  fi

  # Keep one canonical directory per UBQ label.
  run_dir="$out_root/runs/${safe_label}"
  mkdir -p "$run_dir"

  for ((run_index = 1; run_index <= run_count; run_index++)); do
    run_timestamp="$(date -u +"%Y%m%dT%H%M%SZ")"
    run_id="${run_timestamp}_r${run_index}_$RANDOM"
    local_json="$run_dir/${local_machine_dir}_${run_id}.json"
    remote_json="$run_dir/${remote_machine_dir}_${run_id}.json"
    remote_tmp_json="$remote_dir/bench_results/.dual_host_${run_id}_${safe_label}.json"
    remote_tmp_json_cmd="$remote_tmp_json"
    if [[ "$remote_tmp_json_cmd" == "~/"* ]]; then
      remote_tmp_json_cmd="\$HOME/${remote_tmp_json_cmd#"~/"}"
    fi

    local_cmd=()
    if [[ "$skip_local" -eq 0 ]]; then
      local_cmd=(cargo bench)
      if [[ -n "$cargo_jobs" ]]; then
        local_cmd+=(-j "$cargo_jobs")
      fi
      local_cmd+=(
        --bench ubq_bench --features "$feature_csv" -- --ubq-label "$ubq_label" --machine-label "$local_machine_label" --out "$local_json"
      )
      if [[ ${#bench_args[@]} -gt 0 ]]; then
        local_cmd+=("${bench_args[@]}")
      fi
    fi

    if [[ "$skip_remote" -eq 0 ]]; then
      remote_cargo_jobs_cmd=""
      if [[ -n "$cargo_jobs" ]]; then
        remote_cargo_jobs_cmd="-j ${cargo_jobs} "
      fi
      remote_cmd="if [ -f \"\$HOME/.cargo/env\" ]; then . \"\$HOME/.cargo/env\"; fi; export PATH=\"\$HOME/.cargo/bin:\$PATH\"; if ! command -v cargo >/dev/null 2>&1; then echo \"cargo not found on remote host (${remote_host}). Install Rust/Cargo or update PATH.\" >&2; exit 127; fi; cd \"${remote_dir_cmd}\" && cargo bench ${remote_cargo_jobs_cmd}--bench ubq_bench --features $(printf '%q' "$feature_csv") -- --ubq-label $(printf '%q' "$ubq_label") --machine-label $(printf '%q' "$remote_host") --out \"${remote_tmp_json_cmd}\""
      if [[ ${#bench_args[@]} -gt 0 ]]; then
        for arg in "${bench_args[@]}"; do
          remote_cmd+=" $(printf '%q' "$arg")"
        done
      fi

      local_status=0
      remote_status=0

      if [[ "$skip_local" -eq 1 ]]; then
        echo "Label ${ubq_label} run ${run_index}/${run_count}: running remote benchmark only (--skip-local)..."
        if ssh "$remote_host" "$remote_cmd"; then
          remote_status=0
        else
          remote_status=$?
        fi
      elif [[ "$use_tmux" -eq 1 ]]; then
        session_name="$tmux_session"
        if [[ -z "$session_name" ]]; then
          session_name="ubq_bench_${safe_label}_${run_id}"
        fi
        session_name="$(sanitize_name "$session_name")"

        tmux_tmp="$(mktemp -d "${TMPDIR:-/tmp}/ubq_tmux.XXXXXX")"
        local_status_file="$tmux_tmp/local.status"
        remote_status_file="$tmux_tmp/remote.status"
        local_wait_key="${session_name}_local_done"
        remote_wait_key="${session_name}_remote_done"

        local_cmd_escaped="$(printf '%q ' "${local_cmd[@]}")"
        local_tmux_payload="${local_cmd_escaped}; status=\$?; printf '%s' \"\$status\" > $(printf '%q' "$local_status_file"); tmux wait-for -S $(printf '%q' "$local_wait_key")"
        remote_tmux_payload="ssh $(printf '%q' "$remote_host") $(printf '%q' "$remote_cmd"); status=\$?; printf '%s' \"\$status\" > $(printf '%q' "$remote_status_file"); tmux wait-for -S $(printf '%q' "$remote_wait_key")"

        echo "Label ${ubq_label} run ${run_index}/${run_count}: running local+remote in tmux session ${session_name}"
        echo "Attach with: tmux attach -t ${session_name}"
        tmux new-session -d -s "$session_name" "bash -lc $(printf '%q' "$local_tmux_payload")"
        tmux split-window -t "${session_name}:0" -h "bash -lc $(printf '%q' "$remote_tmux_payload")"
        tmux select-layout -t "${session_name}:0" even-horizontal >/dev/null 2>&1 || true

        tmux wait-for "$local_wait_key"
        tmux wait-for "$remote_wait_key"

        if [[ -f "$local_status_file" ]]; then
          local_status="$(<"$local_status_file")"
        else
          local_status=1
        fi
        if [[ -f "$remote_status_file" ]]; then
          remote_status="$(<"$remote_status_file")"
        else
          remote_status=1
        fi

        tmux kill-session -t "$session_name" >/dev/null 2>&1 || true
        rm -rf "$tmux_tmp"
      else
        echo "Label ${ubq_label} run ${run_index}/${run_count}: running local and remote benchmarks in parallel..."
        "${local_cmd[@]}" &
        local_pid=$!
        ssh "$remote_host" "$remote_cmd" &
        remote_pid=$!

        if wait "$local_pid"; then
          local_status=0
        else
          local_status=$?
        fi
        if wait "$remote_pid"; then
          remote_status=0
        else
          remote_status=$?
        fi
      fi

      if [[ "$local_status" -ne 0 || "$remote_status" -ne 0 ]]; then
        echo "benchmark failed for label ${ubq_label} run ${run_index}/${run_count} (local=${local_status}, remote=${remote_status})" >&2
        exit 1
      fi

      echo "Label ${ubq_label} run ${run_index}/${run_count}: copying remote result back..."
      rsync -avz "${remote_host}:${remote_tmp_json}" "$remote_json"
    else
      echo "Label ${ubq_label} run ${run_index}/${run_count}: running local benchmark..."
      "${local_cmd[@]}"
      echo "Label ${ubq_label} run ${run_index}/${run_count}: skipping remote benchmark (--skip-remote)."
    fi
  done
done

if [[ "$purge_losers" -eq 1 ]]; then
  echo "Pruning run history (drop overwritten and non-leading UBQ labels)..."
  "$python_bin" scripts/prune_bench_runs.py \
    --runs-dir "$out_root/runs" \
    --latest-label "$last_safe_label" \
    --purge-losers \
    --purged-labels-file "$purged_labels_file"
else
  echo "Normalizing run history (keep newest directory per UBQ label; loser purge disabled)..."
  "$python_bin" scripts/prune_bench_runs.py \
    --runs-dir "$out_root/runs" \
    --latest-label "$last_safe_label"
fi

if [[ "$skip_plot" -eq 1 ]]; then
  echo "Skipping plot generation (--skip-plot)."
else
  plot_inputs=()
  if compgen -G "$out_root/runs/*" >/dev/null; then
    while IFS= read -r run_path; do
      [[ -d "$run_path" ]] || continue
      if compgen -G "$run_path/*.json" >/dev/null; then
        for path in "$run_path"/*.json; do
          [[ -f "$path" ]] || continue
          plot_inputs+=("$path")
        done
      fi
    done < <(printf '%s\n' "$out_root"/runs/* | sort)
  fi

  if [[ ${#plot_inputs[@]} -eq 0 ]]; then
    echo "No benchmark files found for plotting under: $out_root/runs" >&2
    exit 1
  fi

  echo "Generating plots from curated runs in runs/ ..."
  "$python_bin" scripts/plot_bench.py --out-dir "$out_root/plots" "${plot_inputs[@]}"
fi

echo "Done."
