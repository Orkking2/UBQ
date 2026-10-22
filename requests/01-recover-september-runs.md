# 01: recover the runs of September 2026

No jobs are submitted and no node-hours are used. Paste every output back.

## Purpose

Four runs were launched by Nicolas (user `bsc614227`) on Grace and MareNostrum 5 on 2026-09-15 and never collected, because his access ended. If his scratch is still readable, their results are two machines' worth of data for four queues and can be packaged in minutes.

## 1. Is the old lab directory readable?

```sh
ls -ld /gpfs/scratch/bsc18/bsc614227/ubq-lab
ls /gpfs/scratch/bsc18/bsc614227/ubq-lab
ls /gpfs/scratch/bsc18/bsc614227/ubq-lab/runs
```

If the first command answers "Permission denied" or "No such file or directory", paste the message and stop here: the directory is gone or closed, and `02-paper-matrix.md` carries its own copy of everything.

## 2. The state of each run

```sh
cd /gpfs/scratch/bsc18/bsc614227/ubq-lab/runs
python3 - <<'PY'
import json, os
from collections import Counter
for run in ["ubq-2605a602", "ubq-v8-e406b9e8", "ubq-bitmask-8d50a7a8", "segqueue-e92e2301"]:
    for cluster in ["grace", "mn5"]:
        path = f"{run}/{cluster}/state.json"
        if not os.path.exists(path):
            print(run, cluster, "no state.json"); continue
        state = json.load(open(path))
        tasks = state.get("tasks", {})
        print(run, cluster, "phase", state.get("phase"), dict(Counter(t.get("status") for t in tasks.values())))
PY
```

`state.json` is the lab's record of one run on one cluster: the phase (`building`, `stage1`, `stage2`, `done`) and every task's status. Paste the output whatever it says; a run that never finished is still worth collecting.

## 3. Package the results

```sh
cd /gpfs/scratch/bsc18/bsc614227/ubq-lab/runs
for run in ubq-2605a602 ubq-v8-e406b9e8 ubq-bitmask-8d50a7a8 segqueue-e92e2301; do
  tar czf "$HOME/$run.tar.gz" --exclude='perf.data*' --exclude=lock --exclude='.*.tmp' "$run"
done
ls -la "$HOME"/ubq*.tar.gz "$HOME"/segqueue*.tar.gz
sha256sum "$HOME"/ubq*.tar.gz "$HOME"/segqueue*.tar.gz
```

Expected: tens of megabytes each (the raw profiler captures are left out). Zip the four archives and send them to Nicolas over Zulip with the checksums.

## 4. Also useful, for the next request

```sh
du -sh /gpfs/scratch/bsc18/bsc614227/ubq-lab/vendor /gpfs/scratch/bsc18/bsc614227/ubq-lab/sources /gpfs/scratch/bsc18/bsc614227/ubq-lab/builds
sacctmgr show assoc user=$USER format=account,partition,qos -P
df -h /gpfs/scratch/bsc18/$USER 2>/dev/null || df -h "$HOME"
```

The first says whether the vendored crates already sit on scratch (they are 195 MB uncompressed; the bundle of `02` carries its own copy regardless); the second, under which account, partitions and QOS your jobs would run; the third, how much scratch you have for the runs.
