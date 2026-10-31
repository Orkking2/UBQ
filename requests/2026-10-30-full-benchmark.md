# The full benchmark, with UBQ's fix (2026-10-30)

> **When to reply.** Message Guillem **once**, when this request is complete:
> - every run below has ended on both clusters, as step 4 defines it, and step 5 has packed the archives; or
> - a step of the request itself failed and the request stopped.
>
> Send no progress messages and no partial results. Send nothing from earlier requests, now or later. If your session must end before the runs do, end it without messaging; the next session runs the status loop of step 4 and carries on from there.

Read `README.md` in this directory first.

## What this is

Ten runs, one per queue, all from one frozen source, on Grace and on MareNostrum 5.

The source carries a fix to `ubq`. In the October bundles `ubq` crashed, and sometimes delivered an item twice, at wide batched consumer shapes on Grace. This run is the fix's test as well as the benchmark: every cell checks that each item is delivered exactly once, and `ubq`'s results also count how often the fix's repair path ran.

The vendored `rbbq` carries its own fix for a duplicate delivery, the same one as in the bundle of 2026-10-23.

Beside the throughput matrix:
- a memory section records what each queue holds, at seven element sizes from 8 bytes to 1 KiB;
- the profiling stage captures the usual representative cells, plus a few where UBQ was slower than another queue.

## 0. The earlier runs: record them, cancel them, send nothing

Every run of the earlier bundles is no longer wanted, finished or not. Do not package or send any of their results, now or later, and leave their directories where they are. Do this step **before** extracting the new bundle.

On `grace1.bsc.es`, with `ROOT` the directory used before (if there was none, skip this step):

```sh
export ROOT=/gpfs/scratch/bsc18/$USER/ubq-lab
cd $ROOT/runs && python3 - grace <<'PY'
import json, os, subprocess, sys
from collections import Counter
cluster = sys.argv[1]
for run in sorted(os.listdir(".")):
    path = f"{run}/{cluster}/state.json"
    if not os.path.exists(path):
        continue
    state = json.load(open(path))
    phase = state.get("phase")
    counts = dict(Counter(t.get("status") for t in state.get("tasks", {}).values()))
    action = "left as it is"
    if phase not in ("done", "failed", "superseded", "cancelled"):
        tool, deployment = os.path.abspath(f"{run}/tool/ab.py"), os.path.abspath(f"{run}/{cluster}")
        reply = subprocess.run(["python3", tool, "remote", "cancel", deployment],
                               capture_output=True, text=True)
        action = "cancelled: " + (reply.stdout.strip() or reply.stderr.strip())[-300:]
    print(run, cluster, "phase", phase, counts, "->", action)
PY
```

Then do the same on `glogin1.bsc.es` for MareNostrum 5, with `mn5` in place of `grace` on the `python3 - grace` line. The script reads each run's `state.json`, which has no side effects, and cancels a run only if it is still building or measuring. Keep both outputs for the reply.

## 1. The budget

The lab's estimates for the measurement stage, in node-hours, with the Slurm limits that bound them:

| run | Grace | MareNostrum 5 |
|---|---|---|
| `segqueue-94b068a9` | 8 (limit 38) | 8 (limit 115) |
| `concurrent-queue-c97267e1` | 8 (38) | 8 (115) |
| `ms-queue-18b5c940` | 8 (38) | 8 (115) |
| `lscq-97e54331` | 17 (90) | 17 (271) |
| `rbbq-25736508` | 30 (166) | 30 (498) |
| `ubq-02479967` | 96 (486) | 96 (1,457) |
| `mutex-vecdeque-29cf7314` | 61 (299) | 61 (897) |
| `ubq-dynblock-9bdc7d82` | 309 (1,718) | 309 (5,152) |
| `ubq-bitmask-657c4ce6` | 451 (2,539) | 451 (7,616) |
| `ubq-v8-3831c8bd` | 735 (4,181) | 735 (12,543) |
| total | about 1,723 | about 1,723 |

The profiling stage adds a few node-hours per run and cluster. Nicolas put this budget to Guillem with the link to this file. If Guillem has not agreed to it, ask him before step 3. That question is the only message allowed before the reply.

## 2. The bundle

Download `bundle.tar.gz` from the release https://github.com/Orkking2/UBQ/releases/tag/bsc-requests-2026-10-30 (direct link: https://github.com/Orkking2/UBQ/releases/download/bsc-requests-2026-10-30/bundle.tar.gz; sha256 `7b69235aa3645b0217f0fdffca840330923c23815b36c85b7d100d80e9002a5d`). Fetch it on a login node if it has outbound HTTPS, otherwise on another machine, and copy it over. Extract it into `$ROOT`:

```sh
export ROOT=/gpfs/scratch/bsc18/$USER/ubq-lab
mkdir -p $ROOT && cd $ROOT
tar xzf /path/to/bundle.tar.gz
sha256sum /path/to/bundle.tar.gz
ls $ROOT/runs
df -h /gpfs/scratch/bsc18/$USER
```

It adds three things beside anything already there:
- `vendor/126100f9e85d1882504abce43c7c3f0d21c095b1360a57c4bcf428154556d814`, the vendored crates, so builds are offline;
- `sources/a0e150ae93c0be99…`, the frozen source, whose `vendor` entry links to the former;
- one `runs/<id>/` per run in the table.

Nothing else is needed. The build jobs load the `gcc` and `rust` modules themselves.

## 3. Launch

On `grace1.bsc.es`, then on `glogin1.bsc.es`; both see `$ROOT`. Launch in the table's order, cheapest first, on Grace with `grace` and on MareNostrum 5 with `mn5` as the last word:

```sh
export ROOT=/gpfs/scratch/bsc18/$USER/ubq-lab
C=grace   # on glogin1.bsc.es: C=mn5
for id in segqueue-94b068a9 concurrent-queue-c97267e1 ms-queue-18b5c940 lscq-97e54331 rbbq-25736508 \
          ubq-02479967 mutex-vecdeque-29cf7314 ubq-dynblock-9bdc7d82 ubq-bitmask-657c4ce6 ubq-v8-3831c8bd; do
  python3 $ROOT/runs/$id/tool/ab.py remote deploy $ROOT/runs/$id $C
done
```

Each deploy prints one JSON line: `"phase": "building"` and a build job id is the expected first reply. Keep the replies for the reply. The runs then advance themselves. Several runs share the QOS submit quota, and a run whose submission found the quota full is pushed along by the other runs' jobs and by any `advance` call.

## 4. Until every run has ended

From time to time, on either login node:

```sh
export ROOT=/gpfs/scratch/bsc18/$USER/ubq-lab
for id in segqueue-94b068a9 concurrent-queue-c97267e1 ms-queue-18b5c940 lscq-97e54331 rbbq-25736508 \
          ubq-02479967 mutex-vecdeque-29cf7314 ubq-dynblock-9bdc7d82 ubq-bitmask-657c4ce6 ubq-v8-3831c8bd; do
  for C in grace mn5; do python3 $ROOT/runs/$id/tool/ab.py remote advance $ROOT/runs/$id/$C; done
done
```

- **One retry for failed tasks.** When a run reaches `done` on a cluster and its reply lists failed tasks, run `python3 $ROOT/runs/<id>/tool/ab.py remote retry $ROOT/runs/<id>/<cluster>` once for that run and cluster, then wait for it to reach `done` again. Never retry a run twice.
- **When a run has ended.** A run has ended on a cluster when its phase is `done` after its one retry (or `done` with no failed tasks), or `failed`. A run in `failed` means its build failed; its log is `$ROOT/runs/<id>/<cluster>/logs/build-*.err`, and it goes in the reply while the other runs go on.
- **When the request is complete.** Every run in the table has ended on both clusters.
- **What is data, not failure.** A queue that crashes, hangs or miscounts inside a measurement is a result the lab records. Nothing is to be done about it, and it does not end the request.

## 5. Pack and reply

As each run ends on a cluster, archive it into a directory of its own (scratch may be purged; older archives elsewhere in `~` stay out):

```sh
mkdir -p ~/full-benchmark-2026-10-30
cd $ROOT/runs/<id>
tar czf ~/full-benchmark-2026-10-30/<id>-<cluster>.tar.gz --exclude='perf.data*' --exclude=lock --exclude='.*.tmp' <cluster>
```

When the request is complete, check that the twenty archives are there, then zip them with their checksums:

```sh
cd ~/full-benchmark-2026-10-30
for id in segqueue-94b068a9 concurrent-queue-c97267e1 ms-queue-18b5c940 lscq-97e54331 rbbq-25736508 \
          ubq-02479967 mutex-vecdeque-29cf7314 ubq-dynblock-9bdc7d82 ubq-bitmask-657c4ce6 ubq-v8-3831c8bd; do
  for C in grace mn5; do ls -la $id-$C.tar.gz || echo "MISSING $id-$C"; done
done
sha256sum *.tar.gz > checksums.sha256
cd ~ && zip -r full-benchmark-2026-10-30.zip full-benchmark-2026-10-30
```

A run that failed to build has no results worth archiving: say so in the message instead of sending its archive.

Then send Guillem one message, for Nicolas, with:
1. the zip;
2. the last output of the status loop of step 4;
3. the two outputs of step 0;
4. the replies of the deploys in step 3.

Nothing else: no results from earlier runs.
