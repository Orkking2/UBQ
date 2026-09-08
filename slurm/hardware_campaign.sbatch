#!/usr/bin/env bash
set -euo pipefail
campaign=${1:?campaign directory required}
mode=${2:?build or worker-array required}
cd "$campaign/source"
export PYTHONUNBUFFERED=1
module purge
module load gcc
module load rust
if [[ "$mode" == build ]]; then
    # The source snapshot includes vendored dependencies. Never silently fetch new inputs.
    python3 - "$campaign" <<'PY'
import hashlib,json,sys
from pathlib import Path
root=Path(sys.argv[1])
for name, expected in json.loads((root/'source-manifest.json').read_text()).items():
    if hashlib.sha256((root/'source'/name).read_bytes()).hexdigest()!=expected:
        raise SystemExit('Source hash mismatch: '+name)
PY
    mkdir -p "$campaign/artifacts"
    export CARGO_TARGET_DIR="$campaign/target"
    export CARGO_PROFILE_RELEASE_DEBUG=2
    export CARGO_PROFILE_RELEASE_STRIP=none
    export RUSTFLAGS="-C force-frame-pointers=yes -C target-cpu=native"
    export CXXFLAGS="-fno-omit-frame-pointer -g"
    features=bench_rbbq,bench_lfqueue,bench_moodycamel
    cargo test --offline --locked --lib --features "$features" bench_harness::hardware::tests -- --test-threads=1
    cargo test --offline --locked --lib --features "$features" dropping_partially_read_batch -- --test-threads=1
    cargo build --offline --locked --release --bin bench_hardware --features "$features"
    install -m 0755 "$CARGO_TARGET_DIR/release/bench_hardware" "$campaign/artifacts/bench_hardware-base"
    cargo test --offline --locked --lib --features "$features,probe_item_completion" bench_harness::hardware::tests -- --test-threads=1
    cargo test --offline --locked --lib --features "$features,probe_item_completion" dropping_partially_read_batch -- --test-threads=1
    cargo build --offline --locked --release --bin bench_hardware --features "$features,probe_item_completion"
    install -m 0755 "$CARGO_TARGET_DIR/release/bench_hardware" "$campaign/artifacts/bench_hardware-item"
    python3 - "$campaign" <<'PY'
import hashlib,json,os,subprocess,sys
from pathlib import Path
root=Path(sys.argv[1])
(root/'artifacts/binaries.json').write_text(json.dumps({v:hashlib.sha256((root/'artifacts'/('bench_hardware-'+v)).read_bytes()).hexdigest() for v in ('base','item')},indent=2)+'\n')
(root/'artifacts/compiler.txt').write_text(subprocess.check_output(['rustc','-Vv'],text=True))
(root/'artifacts/build-provenance.json').write_text(json.dumps({
    'flags': {k:v for k,v in os.environ.items() if k in ('RUSTFLAGS','CXXFLAGS') or k.startswith('CARGO_PROFILE_')},
    'cxx_version': subprocess.check_output(['c++','--version'],text=True),
    'features': 'bench_rbbq,bench_lfqueue,bench_moodycamel; item adds probe_item_completion'
},indent=2)+'\n')
PY
elif [[ "$mode" == worker-array ]]; then
    mapping=${3:?array mapping required}
    job=$(python3 - "$mapping" "${SLURM_ARRAY_TASK_ID:?}" <<'PY'
import json,sys
print(json.load(open(sys.argv[1]))[int(sys.argv[2])])
PY
)
    cluster=$(python3 - "$campaign/campaign.json" <<'PY'
import json,sys
print(json.load(open(sys.argv[1]))['cluster'])
PY
)
    if [[ "$cluster" == grace ]]; then
        if [[ "${APX_MODULE:-arm_performix/2026.3.1}" != none ]]; then
            module load "${APX_MODULE:-arm_performix/2026.3.1}" || true
        fi
        if [[ -n "${APX_BIN_DIR:-}" ]]; then export PATH="$APX_BIN_DIR:$PATH"; fi
    fi
    module -t list
    exec python3 scripts/hardware_campaign.py worker "$campaign" "$job"
else
    echo "Unknown mode: $mode" >&2
    exit 2
fi
