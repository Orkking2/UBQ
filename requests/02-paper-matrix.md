# 02: the benchmark matrix on Grace and MareNostrum 5

One run per queue, every run from the same frozen source. Read `README.md` in this directory first, including its section "Changes": this file and its bundle replaced those of 2026-10-21 on 2026-10-23.

## The bundle

Download `bundle.tar.gz` from the release https://github.com/Orkking2/UBQ/releases/tag/bsc-requests-2026-10-23 (the asset's direct link: https://github.com/Orkking2/UBQ/releases/download/bsc-requests-2026-10-23/bundle.tar.gz) (sha256 `07a009502e70ad9e9ca0f174dd8b3b7b447d95ee8dcf7923e837a801e5111e16`, 21.6 MB), on a login node if it has outbound HTTPS, otherwise on another machine and copy it over. Extract it into a directory of your own on scratch:

```sh
export ROOT=/gpfs/scratch/bsc18/$USER/ubq-lab
mkdir -p $ROOT && cd $ROOT
tar xzf /path/to/bundle.tar.gz
ls $ROOT $ROOT/runs
sha256sum /path/to/bundle.tar.gz
```

It holds `vendor/cc082f8ee870c14f0395d4eb9aaacb61172d0578ccf0d45a8c0e15bd36f7c142` (the vendored crates, so builds are offline), `sources/3f212ad9a814c0c3e857e3e39ea50c7125d76c03b42b706f84c5546746a79935` (the frozen source; its `vendor` entry is a relative link to the former) and one `runs/<id>/` per run below, each with `run.json` and the lab's `tool/`. Nothing else is needed: the build job loads the `gcc` and `rust` modules itself.

## Before launching

```sh
sacctmgr show assoc user=$USER format=account,partition,qos -P
df -h /gpfs/scratch/bsc18/$USER
```

Every `run.json` names account `bsc18`, partitions `ngpp` and `gpp`, QOS `ngp_bsccs`/`gp_bsccs` for measurements and `ngp_debug`/`gp_debug` for the build, and the constraint `perfparanoid`. If your association differs, change those fields under `config.clusters.grace` and `config.clusters.mn5` in each `run.json` (the two QOS fields are `qos` and `build_qos`), and nothing else.

## The runs

| queue | run | arms | Cargo features | cells | allocations per cluster | node-h Grace | node-h MN5 | stage 2 (cells × collectors, Grace / MN5) |
|---|---|---|---|---|---|---|---|---|
| `segqueue` | `segqueue-a1d86daf` | 1 | — | 94 | 282 | 8 | 8 | 4×6, 4×4 |
| `concurrent-queue` | `concurrent-queue-0ff382fa` | 1 | — | 94 | 282 | 8 | 8 | 4×6, 4×4 |
| `ms-queue` | `ms-queue-61c6acaa` | 1 | — | 94 | 282 | 8 | 8 | 4×6, 4×4 |
| `lscq` | `lscq-e0d433e5` | 3 | bench_lfqueue | 94 | 282 | 16 | 16 | 4×6, 4×4 |
| `ubq` | `ubq-78607b26` | 2 | — | 746 | 2238 | 96 | 96 | 12×6, 12×4 |
| `mutex-vecdeque` | `mutex-vecdeque-d572eae6` | 1 | — | 746 | 2238 | 60 | 60 | 12×6, 12×4 |
| `ubq-dynblock` | `ubq-dynblock-d9d68fe8` | 8 | — | 746 | 2238 | 308 | 308 | 12×6, 12×4 |
| `ubq-bitmask` | `ubq-bitmask-9b4c6a0a` | 12 | — | 746 | 2238 | 449 | 449 | 12×6, 12×4 |
| `ubq-v8` | `ubq-v8-43d51c69` | 20 | — | 746 | 2238 | 732 | 732 | 12×6, 12×4 |

Totals: about 1685 node-hours on Grace and 1685 on MareNostrum 5 for the measurement stage.

Node-hours are the lab's own estimates for the measurement stage (every cell on an exclusive node, three replicates); the profiling stage adds about two to four node-hours per run and cluster. The order above is by value per node-hour: the one-arm queues and the baselines first, `ubq-v8`'s twenty-geometry grid last. RBBQ (`rbbq`) is not in this bundle: it delivered some items twice under heavy consumer contention, and its corrected run will come as a separate request. Confirm the total with Guillem before launching the expensive runs; launching a subset is fine, and a run can be launched later from the same bundle.

## Launch

On `grace1.bsc.es`, then on `glogin1.bsc.es` (both see `$ROOT`):

```sh
export ROOT=/gpfs/scratch/bsc18/$USER/ubq-lab
python3 $ROOT/runs/segqueue-a1d86daf/tool/ab.py remote deploy $ROOT/runs/segqueue-a1d86daf grace
python3 $ROOT/runs/concurrent-queue-0ff382fa/tool/ab.py remote deploy $ROOT/runs/concurrent-queue-0ff382fa grace
python3 $ROOT/runs/ms-queue-61c6acaa/tool/ab.py remote deploy $ROOT/runs/ms-queue-61c6acaa grace
python3 $ROOT/runs/lscq-e0d433e5/tool/ab.py remote deploy $ROOT/runs/lscq-e0d433e5 grace
python3 $ROOT/runs/ubq-78607b26/tool/ab.py remote deploy $ROOT/runs/ubq-78607b26 grace
python3 $ROOT/runs/mutex-vecdeque-d572eae6/tool/ab.py remote deploy $ROOT/runs/mutex-vecdeque-d572eae6 grace
python3 $ROOT/runs/ubq-dynblock-d9d68fe8/tool/ab.py remote deploy $ROOT/runs/ubq-dynblock-d9d68fe8 grace
python3 $ROOT/runs/ubq-bitmask-9b4c6a0a/tool/ab.py remote deploy $ROOT/runs/ubq-bitmask-9b4c6a0a grace
python3 $ROOT/runs/ubq-v8-43d51c69/tool/ab.py remote deploy $ROOT/runs/ubq-v8-43d51c69 grace
```

```sh
export ROOT=/gpfs/scratch/bsc18/$USER/ubq-lab
python3 $ROOT/runs/segqueue-a1d86daf/tool/ab.py remote deploy $ROOT/runs/segqueue-a1d86daf mn5
python3 $ROOT/runs/concurrent-queue-0ff382fa/tool/ab.py remote deploy $ROOT/runs/concurrent-queue-0ff382fa mn5
python3 $ROOT/runs/ms-queue-61c6acaa/tool/ab.py remote deploy $ROOT/runs/ms-queue-61c6acaa mn5
python3 $ROOT/runs/lscq-e0d433e5/tool/ab.py remote deploy $ROOT/runs/lscq-e0d433e5 mn5
python3 $ROOT/runs/ubq-78607b26/tool/ab.py remote deploy $ROOT/runs/ubq-78607b26 mn5
python3 $ROOT/runs/mutex-vecdeque-d572eae6/tool/ab.py remote deploy $ROOT/runs/mutex-vecdeque-d572eae6 mn5
python3 $ROOT/runs/ubq-dynblock-d9d68fe8/tool/ab.py remote deploy $ROOT/runs/ubq-dynblock-d9d68fe8 mn5
python3 $ROOT/runs/ubq-bitmask-9b4c6a0a/tool/ab.py remote deploy $ROOT/runs/ubq-bitmask-9b4c6a0a mn5
python3 $ROOT/runs/ubq-v8-43d51c69/tool/ab.py remote deploy $ROOT/runs/ubq-v8-43d51c69 mn5
```

Each command prints one JSON line: `"phase": "building"` and a build job id is the expected first reply. Paste the replies. The runs then advance themselves; several runs share the QOS submit quota, and a run whose submission found the quota full is pushed along by the other runs' jobs and by any `advance` call.

## While they run

Once a day, or whenever asked, on either login node:

```sh
export ROOT=/gpfs/scratch/bsc18/$USER/ubq-lab
for d in $ROOT/runs/*/grace $ROOT/runs/*/mn5; do python3 $(dirname $d)/tool/ab.py remote advance $d; done
```

Paste the replies. `"phase": "done"` ends a run on that cluster; `"failed"` entries name tasks whose last attempt failed (one retry is automatic); a build failure's log is `$ROOT/runs/<id>/<cluster>/logs/build-*.err`. `remote cancel <run>/<cluster>` stops a run.

## Collect

When a run reports `done` on a cluster, package that cluster's directory without the raw captures and send the archives to Nicolas over Zulip as they come, with the checksums:

```sh
export ROOT=/gpfs/scratch/bsc18/$USER/ubq-lab
cd $ROOT/runs/<id>
tar czf ~/<id>-<cluster>.tar.gz --exclude='perf.data*' --exclude=lock --exclude='.*.tmp' <cluster>
sha256sum ~/<id>-<cluster>.tar.gz
```

`03-perf-and-performix.md` says what the profiling stage leaves behind and how to send raw captures for particular cells.
