# 02: the benchmark matrix on Grace and MareNostrum 5

One run per queue, every run from the same frozen source. Read `README.md` in this directory first, including its section "Changes": this file and its bundle replaced those of 2026-10-21 on 2026-10-23.

## The bundle

Download `bundle.tar.gz` from the release https://github.com/Orkking2/UBQ/releases/tag/bsc-requests-2026-10-23 (the asset's direct link: https://github.com/Orkking2/UBQ/releases/download/bsc-requests-2026-10-23/bundle.tar.gz) (sha256 `5d87447ac162aef423c8d397039ebbd73f54352cbba1ada16cfb1f10628f5f2c`, 21.7 MB), on a login node if it has outbound HTTPS, otherwise on another machine and copy it over. Extract it into a directory of your own on scratch:

```sh
export ROOT=/gpfs/scratch/bsc18/$USER/ubq-lab
mkdir -p $ROOT && cd $ROOT
tar xzf /path/to/bundle.tar.gz
ls $ROOT $ROOT/runs
sha256sum /path/to/bundle.tar.gz
```

It holds `vendor/126100f9e85d1882504abce43c7c3f0d21c095b1360a57c4bcf428154556d814` (the vendored crates, so builds are offline), `sources/30fffae70cd1703e76b174df1d97f868ae8381e55d555ebeffa3eadd02322b2e` (the frozen source; its `vendor` entry is a relative link to the former) and one `runs/<id>/` per run below, each with `run.json` and the lab's `tool/`. Nothing else is needed: the build job loads the `gcc` and `rust` modules itself.

## Before launching

```sh
sacctmgr show assoc user=$USER format=account,partition,qos -P
df -h /gpfs/scratch/bsc18/$USER
```

Every `run.json` names account `bsc18`, partitions `ngpp` and `gpp`, QOS `ngp_bsccs`/`gp_bsccs` for measurements and `ngp_debug`/`gp_debug` for the build, and the constraint `perfparanoid`. If your association differs, change those fields under `config.clusters.grace` and `config.clusters.mn5` in each `run.json` (the two QOS fields are `qos` and `build_qos`), and nothing else.

## The runs

| queue | run | arms | Cargo features | cells | allocations per cluster | node-h Grace | node-h MN5 | stage 2 (cells × collectors, Grace / MN5) |
|---|---|---|---|---|---|---|---|---|
| `segqueue` | `segqueue-8519d6e0` | 1 | — | 94 | 282 | 8 | 8 | 4×6, 4×4 |
| `concurrent-queue` | `concurrent-queue-c03158f2` | 1 | — | 94 | 282 | 8 | 8 | 4×6, 4×4 |
| `ms-queue` | `ms-queue-0a3e47c6` | 1 | — | 94 | 282 | 8 | 8 | 4×6, 4×4 |
| `lscq` | `lscq-79226bb3` | 3 | bench_lfqueue | 94 | 282 | 16 | 16 | 4×6, 4×4 |
| `rbbq` | `rbbq-080d3e08` | 6 | bench_fastfifo | 94 | 282 | 30 | 30 | 4×6, 4×4 |
| `ubq` | `ubq-ac246240` | 2 | — | 746 | 2238 | 96 | 96 | 12×6, 12×4 |
| `mutex-vecdeque` | `mutex-vecdeque-aa7d8851` | 1 | — | 746 | 2238 | 60 | 60 | 12×6, 12×4 |
| `ubq-dynblock` | `ubq-dynblock-d1ee9a05` | 8 | — | 746 | 2238 | 308 | 308 | 12×6, 12×4 |
| `ubq-bitmask` | `ubq-bitmask-62db9413` | 12 | — | 746 | 2238 | 449 | 449 | 12×6, 12×4 |
| `ubq-v8` | `ubq-v8-63bd8eec` | 20 | — | 746 | 2238 | 732 | 732 | 12×6, 12×4 |

Totals: about 1715 node-hours on Grace and 1715 on MareNostrum 5 for the measurement stage.

Node-hours are the lab's own estimates for the measurement stage (every cell on an exclusive node, three replicates); the profiling stage adds about two to four node-hours per run and cluster. The order above is by value per node-hour: the one-arm queues and the baselines first, `ubq-v8`'s twenty-geometry grid last. RBBQ (`rbbq`) runs with a fix to its consumer path (it delivered some items twice under heavy consumer contention); the bundle's vendored `rbbq` 0.1.0 carries that patch, which is why the vendor tree has a new hash. Confirm the total with Guillem before launching the expensive runs; launching a subset is fine, and a run can be launched later from the same bundle.

## Launch

On `grace1.bsc.es`, then on `glogin1.bsc.es` (both see `$ROOT`):

```sh
export ROOT=/gpfs/scratch/bsc18/$USER/ubq-lab
python3 $ROOT/runs/segqueue-8519d6e0/tool/ab.py remote deploy $ROOT/runs/segqueue-8519d6e0 grace
python3 $ROOT/runs/concurrent-queue-c03158f2/tool/ab.py remote deploy $ROOT/runs/concurrent-queue-c03158f2 grace
python3 $ROOT/runs/ms-queue-0a3e47c6/tool/ab.py remote deploy $ROOT/runs/ms-queue-0a3e47c6 grace
python3 $ROOT/runs/lscq-79226bb3/tool/ab.py remote deploy $ROOT/runs/lscq-79226bb3 grace
python3 $ROOT/runs/rbbq-080d3e08/tool/ab.py remote deploy $ROOT/runs/rbbq-080d3e08 grace
python3 $ROOT/runs/ubq-ac246240/tool/ab.py remote deploy $ROOT/runs/ubq-ac246240 grace
python3 $ROOT/runs/mutex-vecdeque-aa7d8851/tool/ab.py remote deploy $ROOT/runs/mutex-vecdeque-aa7d8851 grace
python3 $ROOT/runs/ubq-dynblock-d1ee9a05/tool/ab.py remote deploy $ROOT/runs/ubq-dynblock-d1ee9a05 grace
python3 $ROOT/runs/ubq-bitmask-62db9413/tool/ab.py remote deploy $ROOT/runs/ubq-bitmask-62db9413 grace
python3 $ROOT/runs/ubq-v8-63bd8eec/tool/ab.py remote deploy $ROOT/runs/ubq-v8-63bd8eec grace
```

```sh
export ROOT=/gpfs/scratch/bsc18/$USER/ubq-lab
python3 $ROOT/runs/segqueue-8519d6e0/tool/ab.py remote deploy $ROOT/runs/segqueue-8519d6e0 mn5
python3 $ROOT/runs/concurrent-queue-c03158f2/tool/ab.py remote deploy $ROOT/runs/concurrent-queue-c03158f2 mn5
python3 $ROOT/runs/ms-queue-0a3e47c6/tool/ab.py remote deploy $ROOT/runs/ms-queue-0a3e47c6 mn5
python3 $ROOT/runs/lscq-79226bb3/tool/ab.py remote deploy $ROOT/runs/lscq-79226bb3 mn5
python3 $ROOT/runs/rbbq-080d3e08/tool/ab.py remote deploy $ROOT/runs/rbbq-080d3e08 mn5
python3 $ROOT/runs/ubq-ac246240/tool/ab.py remote deploy $ROOT/runs/ubq-ac246240 mn5
python3 $ROOT/runs/mutex-vecdeque-aa7d8851/tool/ab.py remote deploy $ROOT/runs/mutex-vecdeque-aa7d8851 mn5
python3 $ROOT/runs/ubq-dynblock-d1ee9a05/tool/ab.py remote deploy $ROOT/runs/ubq-dynblock-d1ee9a05 mn5
python3 $ROOT/runs/ubq-bitmask-62db9413/tool/ab.py remote deploy $ROOT/runs/ubq-bitmask-62db9413 mn5
python3 $ROOT/runs/ubq-v8-63bd8eec/tool/ab.py remote deploy $ROOT/runs/ubq-v8-63bd8eec mn5
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
