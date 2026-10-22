# 02: the benchmark matrix on Grace and MareNostrum 5

One run per queue, every run from the same frozen source. Read `README.md` in this directory first.

## The bundle

Download `bundle.tar.gz` from the release https://github.com/Orkking2/UBQ/releases/tag/bsc-requests-2026-10-21 (the asset's direct link: https://github.com/Orkking2/UBQ/releases/download/bsc-requests-2026-10-21/bundle.tar.gz) (sha256 `6b22753f633b135bf6cf466aaf6c9689a2216c983c30cdb7489ad4082ae1a31c`, 21.7 MB), on a login node if it has outbound HTTPS, otherwise on another machine and copy it over. Extract it into a directory of your own on scratch:

```sh
export ROOT=/gpfs/scratch/bsc18/$USER/ubq-lab
mkdir -p $ROOT && cd $ROOT
tar xzf /path/to/bundle.tar.gz
ls $ROOT $ROOT/runs
sha256sum /path/to/bundle.tar.gz
```

It holds `vendor/cc082f8ee870c14f0395d4eb9aaacb61172d0578ccf0d45a8c0e15bd36f7c142` (the vendored crates, so builds are offline), `sources/444e2a79ec68e512939d6922610df5f2a780e6d11ca2d2007729fa6b197823dd` (the frozen source; its `vendor` entry is a relative link to the former) and one `runs/<id>/` per run below, each with `run.json` and the lab's `tool/`. Nothing else is needed: the build job loads the `gcc` and `rust` modules itself.

## Before launching

```sh
sacctmgr show assoc user=$USER format=account,partition,qos -P
df -h /gpfs/scratch/bsc18/$USER
```

Every `run.json` names account `bsc18`, partitions `ngpp` and `gpp`, QOS `ngp_bsccs`/`gp_bsccs` for measurements and `ngp_debug`/`gp_debug` for the build, and the constraint `perfparanoid`. If your association differs, change those fields under `config.clusters.grace` and `config.clusters.mn5` in each `run.json` (the two QOS fields are `qos` and `build_qos`), and nothing else.

## The runs

| queue | run | arms | Cargo features | cells | allocations per cluster | node-h Grace | node-h MN5 | stage 2 (cells × collectors, Grace / MN5) |
|---|---|---|---|---|---|---|---|---|
| `segqueue` | `segqueue-faaa8296` | 1 | — | 94 | 282 | 8 | 8 | 4×6, 4×4 |
| `concurrent-queue` | `concurrent-queue-47aa68ba` | 1 | — | 94 | 282 | 8 | 8 | 4×6, 4×4 |
| `ms-queue` | `ms-queue-3716b767` | 1 | — | 94 | 282 | 8 | 8 | 4×6, 4×4 |
| `lscq` | `lscq-fb57c06c` | 3 | bench_lfqueue | 94 | 282 | 16 | 16 | 4×6, 4×4 |
| `rbbq` | `rbbq-982b56fc` | 6 | bench_fastfifo | 94 | 282 | 30 | 30 | 4×6, 4×4 |
| `ubq` | `ubq-fe1c6682` | 2 | — | 746 | 2238 | 96 | 96 | 12×6, 12×4 |
| `mutex-vecdeque` | `mutex-vecdeque-ac0369c0` | 1 | — | 746 | 2238 | 60 | 60 | 12×6, 12×4 |
| `ubq-dynblock` | `ubq-dynblock-734f78da` | 8 | — | 746 | 2238 | 308 | 308 | 12×6, 12×4 |
| `ubq-bitmask` | `ubq-bitmask-e73d7d63` | 12 | — | 746 | 2238 | 449 | 449 | 12×6, 12×4 |
| `ubq-v8` | `ubq-v8-aeb70aa3` | 20 | — | 746 | 2238 | 732 | 732 | 12×6, 12×4 |

Totals: about 1715 node-hours on Grace and 1715 on MareNostrum 5 for the measurement stage.

Node-hours are the lab's own estimates for the measurement stage (every cell on an exclusive node, three replicates); the profiling stage adds about two to four node-hours per run and cluster. The order above is by value per node-hour: the one-arm queues and the baselines first, `ubq-v8`'s twenty-geometry grid last. Confirm the total with Guillem before launching the expensive runs; launching a subset is fine, and a run can be launched later from the same bundle.

## Launch

On `grace1.bsc.es`, then on `glogin1.bsc.es` (both see `$ROOT`):

```sh
export ROOT=/gpfs/scratch/bsc18/$USER/ubq-lab
python3 $ROOT/runs/segqueue-faaa8296/tool/ab.py remote deploy $ROOT/runs/segqueue-faaa8296 grace
python3 $ROOT/runs/concurrent-queue-47aa68ba/tool/ab.py remote deploy $ROOT/runs/concurrent-queue-47aa68ba grace
python3 $ROOT/runs/ms-queue-3716b767/tool/ab.py remote deploy $ROOT/runs/ms-queue-3716b767 grace
python3 $ROOT/runs/lscq-fb57c06c/tool/ab.py remote deploy $ROOT/runs/lscq-fb57c06c grace
python3 $ROOT/runs/rbbq-982b56fc/tool/ab.py remote deploy $ROOT/runs/rbbq-982b56fc grace
python3 $ROOT/runs/ubq-fe1c6682/tool/ab.py remote deploy $ROOT/runs/ubq-fe1c6682 grace
python3 $ROOT/runs/mutex-vecdeque-ac0369c0/tool/ab.py remote deploy $ROOT/runs/mutex-vecdeque-ac0369c0 grace
python3 $ROOT/runs/ubq-dynblock-734f78da/tool/ab.py remote deploy $ROOT/runs/ubq-dynblock-734f78da grace
python3 $ROOT/runs/ubq-bitmask-e73d7d63/tool/ab.py remote deploy $ROOT/runs/ubq-bitmask-e73d7d63 grace
python3 $ROOT/runs/ubq-v8-aeb70aa3/tool/ab.py remote deploy $ROOT/runs/ubq-v8-aeb70aa3 grace
```

```sh
export ROOT=/gpfs/scratch/bsc18/$USER/ubq-lab
python3 $ROOT/runs/segqueue-faaa8296/tool/ab.py remote deploy $ROOT/runs/segqueue-faaa8296 mn5
python3 $ROOT/runs/concurrent-queue-47aa68ba/tool/ab.py remote deploy $ROOT/runs/concurrent-queue-47aa68ba mn5
python3 $ROOT/runs/ms-queue-3716b767/tool/ab.py remote deploy $ROOT/runs/ms-queue-3716b767 mn5
python3 $ROOT/runs/lscq-fb57c06c/tool/ab.py remote deploy $ROOT/runs/lscq-fb57c06c mn5
python3 $ROOT/runs/rbbq-982b56fc/tool/ab.py remote deploy $ROOT/runs/rbbq-982b56fc mn5
python3 $ROOT/runs/ubq-fe1c6682/tool/ab.py remote deploy $ROOT/runs/ubq-fe1c6682 mn5
python3 $ROOT/runs/mutex-vecdeque-ac0369c0/tool/ab.py remote deploy $ROOT/runs/mutex-vecdeque-ac0369c0 mn5
python3 $ROOT/runs/ubq-dynblock-734f78da/tool/ab.py remote deploy $ROOT/runs/ubq-dynblock-734f78da mn5
python3 $ROOT/runs/ubq-bitmask-e73d7d63/tool/ab.py remote deploy $ROOT/runs/ubq-bitmask-e73d7d63 mn5
python3 $ROOT/runs/ubq-v8-aeb70aa3/tool/ab.py remote deploy $ROOT/runs/ubq-v8-aeb70aa3 mn5
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
