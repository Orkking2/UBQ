# Benchmark requests for BSC

These files ask an agent with a shell on BSC's login nodes to run a benchmark lab on Grace and MareNostrum 5 and send the results back. They were written by Nicolas (the author of UBQ) for Guillem, who runs them under his own account; the agent that runs them is a Claude Code session, so each request is written for it: read the whole file, run what it says, paste back what it asks for, and stop when something fails rather than improvising.

## Changes

**2026-10-23: the bundle of 2026-10-21 is replaced by the release `bsc-requests-2026-10-23`, and `02-paper-matrix.md` now names its runs.** The runner in the old bundle built each queue from its main thread, which was not pinned, so on a two-socket node the queue's memory was sometimes placed on the socket away from the worker threads. On a 2 × 80-core Neoverse-N1 machine this happened in about one trial in four, and those trials ran up to thirty times slower; every queue was exposed to it.

The new runner builds the queue and coordinates the measurement from a fresh thread, which gets a memory arena of its own. It holds that thread on the first worker's core while the queue is built, then releases it before anything is measured.

RBBQ now runs with a fix to its consumer path: under heavy consumer contention it had delivered some items twice. Its vendored `rbbq` 0.1.0 carries the patch, so the vendor tree has a new hash.

Nothing else changed. The frozen source `30fffae70c…` differs from the old `444e2a79ec…` only in `src/bench_harness/hardware.rs`, the vendor tree only in `rbbq`, and the matrix, the specs and the cluster settings are the same.

The release's asset was replaced once on 2026-10-23. An earlier asset of the same name (sha256 `07a00950…1e16`, nine runs, without RBBQ) carried a weaker form of the runner fix, published under a `3f212ad9a8…` source. If you downloaded that one, download the bundle again and check its sha256 against `02-paper-matrix.md`.

- If nothing from an older bundle was launched, follow `02-paper-matrix.md` as it now stands.
- If runs from an older bundle were launched, do not send their results; if some were already sent, say which. Extract the new bundle into the same `$ROOT`: it adds a new source, a new vendor tree and new run directories beside the old ones. Then launch the new runs as `02-paper-matrix.md` says. Deploying a new run of a queue cancels that queue's older run on the same cluster automatically, and its state becomes `superseded`; no manual cancellation is needed.

## What is being measured

UBQ is an unbounded multi-producer, multi-consumer FIFO queue (this repository). The requests measure it, in several configurations, against other strict FIFO queues on two machines, for a paper. One run measures one queue over a fixed matrix: 23 producer/consumer shapes, several batch sizes, four item sizes, three replicates, each cell on an exclusive node; then a second stage profiles representative cells with `perf` and, on Grace, Arm Performix. The lab's own documentation is `lab/README.md` in this repository.

## The lab on the cluster side

Everything the cluster needs ships in a bundle, extracted into a directory of the agent's own on the shared scratch, called `$ROOT` below:

```text
$ROOT/
  vendor/<hash>/        the crate's vendored dependencies (builds are offline)
  sources/<hash>/       the frozen source of the crate; its `vendor` entry links to the above
  runs/<id>/run.json    one run's complete specification, arms, and cluster settings
  runs/<id>/tool/       the lab's cluster-side code: standard-library Python 3.9, nothing to install
  builds/<cluster>/     written by the build job: one runner binary per source and feature set
  cache/<cluster>/      the Cargo target directory of the builds
```

One command per run and cluster launches it:

```sh
python3 $ROOT/runs/<id>/tool/ab.py remote deploy $ROOT/runs/<id> grace   # on grace1.bsc.es
python3 $ROOT/runs/<id>/tool/ab.py remote deploy $ROOT/runs/<id> mn5     # on glogin1.bsc.es
```

It writes `$ROOT/runs/<id>/<cluster>/` (`plan.json`, `state.json`, `logs/`, later `tasks/`), submits the build job with `sbatch`, and prints one JSON line of status. After that the run advances itself: every job that ends submits what comes next, and a one-core watchdog job follows each array. To see where a run stands at any time, or to push it along after a quota refusal:

```sh
python3 $ROOT/runs/<id>/tool/ab.py remote advance $ROOT/runs/<id>/<cluster>
```

The reply names the phase (`building`, `stage1`, `stage2`, `done`, or an error), counts the tasks by status, lists failures, and repeats the latest notes. `remote cancel` with the same argument cancels a run's jobs.

The jobs are submitted with the account, partition, QOS and node constraint written in `run.json` under `config.clusters.<cluster>`: account `bsc18`, partitions `ngpp` (Grace) and `gpp` (MareNostrum 5), QOS `ngp_bsccs` and `gp_bsccs` for measurements, `ngp_debug` and `gp_debug` for the build, and the node constraint `perfparanoid`, which gives nodes where `perf` runs unprivileged. If `sacctmgr show assoc user=$USER format=account,partition,qos -P` shows other values for your account, change those fields in `run.json` before launching; nothing else in `run.json` should change.

A measurement task takes an exclusive node for a few minutes; the whole matrix of one run is hundreds of allocations, listed with their node-hour estimates in each request. Confirm the budget with Guillem before launching the expensive runs; the cheap ones can go first.

## What comes back

For every run and cluster, one archive of the cluster directory without the raw profiler captures:

```sh
cd $ROOT/runs/<id>
tar czf ~/<id>-<cluster>.tar.gz --exclude='perf.data*' --exclude=lock --exclude='.*.tmp' <cluster>
sha256sum ~/<id>-<cluster>.tar.gz
```

Put the archives in a zip and send it to Nicolas over Zulip, with the status replies and the checksums pasted into the message. The results are JSON and logs; tens of megabytes per run. Back on Nicolas's machine the archives drop into the lab and produce the reports and figures.

## Rules for the agent

- Run the commands as written, in order. Do not change `tool/`, `sources/`, `vendor/` or `run.json` except the account and QOS fields named above.
- Do not delete, move or change anything under `/gpfs/scratch/bsc18/bsc614227/` or `/gpfs/projects/bsc18/bsc614227/`, Nicolas's former directories: read them if they are readable, copy what a request asks for.
- If a command fails, paste its output and stop that request; the others may go on.
- Builds happen inside a Slurm job (the job script loads the `gcc` and `rust` modules); nothing is compiled on a login node.
- Scratch may be purged: send the archives as soon as a run is done.

## The requests, in order

1. `01-recover-september-runs.md`: no jobs; checks whether Nicolas's old scratch is readable and packages four runs that were never collected. It can run now.
2. `02-paper-matrix.md`: the benchmark proper, one run per queue, with the launch commands and the budget; its bundle is the release asset `bsc-requests-2026-10-23` of this repository (the earlier `bsc-requests-2026-10-21` is superseded; see "Changes"). The answers to `01` say whether the old scratch can be reused, but the bundle carries everything regardless.
3. `03-perf-and-performix.md`: the profiling data Guillem asked for: what the runs already capture, how to send it, and how to ask for more cells. It applies once `02` runs.
