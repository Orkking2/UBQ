#!/usr/bin/env bash
set -euo pipefail

usage() {
    cat <<'EOF'
Usage: slurm/submit_profile_plan.sh {mn5|grace} <plan.jsonl> [options]

Validate and submit a JSONL profiling plan as one Slurm array. Every case must
target the requested cluster. Plans containing APX cases default to one running
array element at a time; perf-only plans default to four.

Options:
  --after JOBID          Depend on a successful build or earlier job
  --qos NAME             Override the cluster's debug QoS
  --time DD-HH:MM:SS     Per-case wall limit (default: plan timeout + cushion)
  --max-concurrent N     Maximum simultaneously running array elements. This
                         does not reduce the number of submitted array tasks.
  --max-submit-tasks N   Refuse plans with more than N submitted array tasks
                         (unset by default; useful for site/QoS limits)
  --results-root DIR     Result root (default: $UBQ/profiling_results/raw)
  --retry-incomplete     Overwrite sidecars from an incomplete prior attempt
                         instead of failing the case (does not touch cases
                         already recorded as completed)

Env overrides:
  ACCOUNT                BSC allocation account (default: bsc18)
  UBQ                    Deployed repository root
                         (default: /gpfs/projects/$ACCOUNT/$USER/UBQ)
  PROFILE_RESULTS_ROOT   Result root when --results-root is omitted
  PROFILE_MAX_SUBMIT_TASKS
                         Submitted-task ceiling when --max-submit-tasks is omitted
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
    usage
    exit 0
fi
if [[ $# -lt 2 ]]; then
    usage >&2
    exit 2
fi

cluster=$1
plan=$2
shift 2

case "$cluster" in
    mn5)
        default_qos=gp_debug
        partition=gpp
        cpus=112
        extra_sbatch_args=(--hint=nomultithread)
        ;;
    grace)
        default_qos=ngp_debug
        partition=ngpp
        cpus=144
        extra_sbatch_args=()
        ;;
    *)
        printf 'Unknown cluster: %s\n\n' "$cluster" >&2
        usage >&2
        exit 2
        ;;
esac

after_job_id=""
qos=$default_qos
time_limit=""
max_concurrent=""
max_submit_tasks=""
results_root=""
retry_incomplete=0
while [[ $# -gt 0 ]]; do
    case "$1" in
        --after)
            after_job_id=${2:?--after requires a job id}
            shift 2
            ;;
        --qos)
            qos=${2:?--qos requires a value}
            shift 2
            ;;
        --time)
            time_limit=${2:?--time requires a value}
            shift 2
            ;;
        --max-concurrent)
            max_concurrent=${2:?--max-concurrent requires a value}
            shift 2
            ;;
        --max-submit-tasks)
            max_submit_tasks=${2:?--max-submit-tasks requires a value}
            shift 2
            ;;
        --results-root)
            results_root=${2:?--results-root requires a value}
            shift 2
            ;;
        --retry-incomplete)
            retry_incomplete=1
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            printf 'Unexpected argument: %s\n\n' "$1" >&2
            usage >&2
            exit 2
            ;;
    esac
done

if [[ ! -f "$plan" ]]; then
    echo "profiling plan does not exist: $plan" >&2
    exit 2
fi
plan_dir=$(cd -- "$(dirname -- "$plan")" && pwd)
plan="$plan_dir/$(basename -- "$plan")"

plan_summary=$(
    python3 - "$plan" "$cluster" <<'PY'
import json
import re
import sys
from pathlib import Path

path = Path(sys.argv[1])
requested_cluster = sys.argv[2]
required = (
    "schema_version",
    "case_id",
    "campaign",
    "stage",
    "cluster",
    "backend",
    "binary",
    "queue",
    "scenario",
    "repeat",
    "duration_secs",
    "warmup_secs",
)
nonempty_strings = ("case_id", "campaign", "stage", "binary", "queue")
scenario_pattern = re.compile(r"^[1-9][0-9]*p[1-9][0-9]*c$")
backends = []
max_collector_timeout = 0

with path.open(encoding="utf-8") as stream:
    for line_number, raw in enumerate(stream, 1):
        if not raw.strip():
            raise SystemExit(f"{path}:{line_number}: blank lines are not valid plan cases")
        try:
            case = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise SystemExit(f"{path}:{line_number}: invalid JSON: {exc.msg}") from exc
        if not isinstance(case, dict):
            raise SystemExit(f"{path}:{line_number}: each plan case must be a JSON object")

        missing = [name for name in required if name not in case]
        if missing:
            raise SystemExit(
                f"{path}:{line_number}: missing required field(s): {', '.join(missing)}"
            )
        for name in nonempty_strings:
            value = case[name]
            if not isinstance(value, str) or not value.strip() or "\n" in value:
                raise SystemExit(f"{path}:{line_number}: {name} must be a non-empty string")
        if not isinstance(case["schema_version"], int) or case["schema_version"] < 1:
            raise SystemExit(f"{path}:{line_number}: schema_version must be a positive integer")
        if case["cluster"] != requested_cluster:
            raise SystemExit(
                f"{path}:{line_number}: cluster {case['cluster']!r} does not match "
                f"requested cluster {requested_cluster!r}"
            )
        backend = case["backend"]
        if backend not in ("perf", "apx"):
            raise SystemExit(f"{path}:{line_number}: backend must be 'perf' or 'apx'")
        if not isinstance(case["scenario"], str) or not scenario_pattern.fullmatch(case["scenario"]):
            raise SystemExit(f"{path}:{line_number}: invalid scenario {case['scenario']!r}")
        if not isinstance(case["repeat"], int) or isinstance(case["repeat"], bool) or case["repeat"] < 1:
            raise SystemExit(f"{path}:{line_number}: repeat must be a positive integer")
        if (
            not isinstance(case["duration_secs"], int)
            or isinstance(case["duration_secs"], bool)
            or case["duration_secs"] < 1
        ):
            raise SystemExit(f"{path}:{line_number}: duration_secs must be a positive integer")
        if (
            not isinstance(case["warmup_secs"], int)
            or isinstance(case["warmup_secs"], bool)
            or case["warmup_secs"] < 0
        ):
            raise SystemExit(f"{path}:{line_number}: warmup_secs must be a non-negative integer")
        if case.get("allocator_probe", False):
            allocator_binary = case.get("allocator_binary")
            if (
                not isinstance(allocator_binary, str)
                or not allocator_binary.strip()
                or "\n" in allocator_binary
            ):
                raise SystemExit(
                    f"{path}:{line_number}: allocator_probe requires allocator_binary"
                )

        if backend == "apx":
            recipe = case.get("recipe")
            if not isinstance(recipe, str) or not recipe.strip():
                raise SystemExit(f"{path}:{line_number}: APX cases require a non-empty recipe")
        else:
            events = case.get("perf_events")
            valid_string = isinstance(events, str) and bool(events.strip())
            valid_list = (
                isinstance(events, list)
                and bool(events)
                and all(isinstance(event, str) and event.strip() for event in events)
            )
            if not (valid_string or valid_list):
                raise SystemExit(
                    f"{path}:{line_number}: perf cases require non-empty perf_events"
                )
        backends.append(backend)
        timeout = case.get("collector_timeout_secs")
        if not isinstance(timeout, int) or isinstance(timeout, bool) or timeout < 1:
            raise SystemExit(
                f"{path}:{line_number}: collector_timeout_secs must be a positive integer"
            )
        max_collector_timeout = max(max_collector_timeout, timeout)

if not backends:
    raise SystemExit(f"{path}: plan contains no cases")

print(
    f"{len(backends)}\t{backends.count('apx')}\t{backends.count('perf')}\t"
    f"{max_collector_timeout}"
)
PY
)
IFS=$'\t' read -r case_count apx_count perf_count max_collector_timeout <<< "$plan_summary"

max_submit_tasks=${max_submit_tasks:-${PROFILE_MAX_SUBMIT_TASKS:-}}
if [[ -n "$max_submit_tasks" && ! "$max_submit_tasks" =~ ^[1-9][0-9]*$ ]]; then
    echo "--max-submit-tasks (or PROFILE_MAX_SUBMIT_TASKS) must be a positive integer" >&2
    exit 2
fi

if [[ -z "$max_concurrent" ]]; then
    if ((apx_count > 0)); then
        max_concurrent=1
    else
        max_concurrent=4
    fi
fi
if [[ ! "$max_concurrent" =~ ^[1-9][0-9]*$ ]]; then
    echo "--max-concurrent must be a positive integer" >&2
    exit 2
fi
if [[ -n "$max_submit_tasks" ]]; then
    if ((case_count > max_submit_tasks)); then
        printf 'profiling plan has %d submitted array tasks, exceeding the explicit ceiling of %d\n' \
            "$case_count" "$max_submit_tasks" >&2
        printf '%s\n' \
            "--max-concurrent limits simultaneously running tasks only; it does not reduce submitted tasks counted by Slurm/QoS" >&2
        exit 2
    fi
    printf 'submission preflight: %d array tasks within explicit ceiling %d (%d maximum concurrent)\n' \
        "$case_count" "$max_submit_tasks" "$max_concurrent" >&2
fi
if [[ -z "$time_limit" ]]; then
    if ((apx_count > 0)); then
        wall_seconds=$((max_collector_timeout + 600))
    else
        wall_seconds=$((max_collector_timeout + 300))
    fi
    if ((wall_seconds >= 86400)); then
        days=$((wall_seconds / 86400))
        remainder=$((wall_seconds % 86400))
        printf -v time_limit '%d-%02d:%02d:%02d' \
            "$days" "$((remainder / 3600))" "$(((remainder % 3600) / 60))" "$((remainder % 60))"
    else
        printf -v time_limit '%02d:%02d:%02d' \
            "$((wall_seconds / 3600))" "$(((wall_seconds % 3600) / 60))" "$((wall_seconds % 60))"
    fi
fi
if [[ -z "$qos" || "$qos" == *$'\n'* ]]; then
    echo "--qos must be a non-empty single-line value" >&2
    exit 2
fi
if [[ -z "$time_limit" || "$time_limit" == *$'\n'* ]]; then
    echo "--time must be a non-empty single-line value" >&2
    exit 2
fi

: "${ACCOUNT:=bsc18}"
: "${UBQ:=/gpfs/projects/$ACCOUNT/$USER/UBQ}"
results_root=${results_root:-"${PROFILE_RESULTS_ROOT:-$UBQ/profiling_results/raw}"}
mkdir -p "$UBQ/logs" "$results_root"

dependency_args=()
if [[ -n "$after_job_id" ]]; then
    dependency_args=(--dependency="afterok:$after_job_id")
fi

submission_output=$(sbatch --parsable \
    --account="$ACCOUNT" \
    --qos="$qos" \
    --partition="$partition" \
    --nodes=1 \
    --ntasks=1 \
    --cpus-per-task="$cpus" \
    --exclusive \
    --constraint=perfparanoid \
    "${extra_sbatch_args[@]+"${extra_sbatch_args[@]}"}" \
    --time="$time_limit" \
    --array="0-$((case_count - 1))%$max_concurrent" \
    "${dependency_args[@]+"${dependency_args[@]}"}" \
    --job-name="profile-$cluster" \
    --output="$UBQ/logs/profile-$cluster-%A_%a.out" \
    --error="$UBQ/logs/profile-$cluster-%A_%a.err" \
    "$UBQ/slurm/profile_case.sbatch" \
    "$UBQ" \
    "$plan" \
    "$results_root" \
    "$retry_incomplete")
if [[ -z "$submission_output" ]]; then
    echo "sbatch succeeded without returning a job id" >&2
    exit 1
fi
slurm_job_id=${submission_output%%$'\n'*}
slurm_job_id=${slurm_job_id%%;*}

# Record case identities only after Slurm accepts the array. The aggregator
# uses the union across accepted submissions to distinguish missing cases from
# cases that were never submitted.
manifest_status=0
python3 - "$plan" "$results_root" "$slurm_job_id" <<'PY' || manifest_status=$?
import datetime as dt
import fcntl
import hashlib
import json
import os
import sys
from collections import defaultdict
from pathlib import Path

plan_path = Path(sys.argv[1])
results_root = Path(sys.argv[2])
job_id = sys.argv[3]
raw = plan_path.read_bytes()
groups = defaultdict(list)
for line in raw.decode("utf-8").splitlines():
    case = json.loads(line)
    groups[case["campaign"]].append(case)

for campaign, cases in groups.items():
    campaign_dir = results_root / campaign
    campaign_dir.mkdir(parents=True, exist_ok=True)
    lock_path = campaign_dir / ".campaign.lock"
    with lock_path.open("a+", encoding="utf-8") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        manifest_path = campaign_dir / "campaign.json"
        if manifest_path.is_file():
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        else:
            manifest = {"schema_version": 1, "campaign": campaign, "case_fingerprints": {}, "submissions": []}
        fingerprints = manifest.setdefault("case_fingerprints", {})
        for case in cases:
            case_id = case["case_id"]
            fingerprint = hashlib.sha256(
                json.dumps(case, sort_keys=True, separators=(",", ":")).encode("utf-8")
            ).hexdigest()
            previous = fingerprints.get(case_id)
            if previous is not None and previous != fingerprint:
                raise SystemExit(f"case identity collision for {campaign}/{case_id}")
            fingerprints[case_id] = fingerprint
        plan_sha = hashlib.sha256(raw).hexdigest()
        if not any(entry.get("job_id") == job_id for entry in manifest["submissions"]):
            manifest["submissions"].append(
                {
                    "job_id": job_id,
                    "submitted_at": dt.datetime.now(dt.timezone.utc).isoformat(),
                    "plan": str(plan_path),
                    "plan_sha256": plan_sha,
                    "case_count": len(cases),
                    "stages": sorted({case["stage"] for case in cases}),
                    "cluster": sorted({case["cluster"] for case in cases}),
                }
            )
        manifest["expected_case_ids"] = sorted(fingerprints)
        manifest["expected_cases"] = len(fingerprints)
        temporary = manifest_path.with_suffix(".json.tmp")
        temporary.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        os.replace(temporary, manifest_path)
PY
if ((manifest_status != 0)); then
    printf 'Slurm accepted job %s, but its profiling manifest could not be updated\n' \
        "$slurm_job_id" >&2
    printf '%s\n' "$submission_output"
    exit "$manifest_status"
fi

printf '%s\n' "$submission_output"
