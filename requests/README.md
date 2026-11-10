# Benchmark requests for BSC

These files ask an agent with a shell on BSC's login nodes to run a benchmark lab on Grace and MareNostrum 5, and to send the results back. Nicolas, the author of UBQ, wrote them for Guillem, who runs them under his own account. The agent that runs them is a Claude Code session, so each request is written for it: read the whole file, run what it says, and stop when something fails rather than improvising.

**This directory holds one request: `2026-11-10-step-overhead.md`.** It is a short diagnosis on debug nodes, and it ends the request of 2026-10-30: its step 2 archives everything that request's runs have finished, then cancels the rest. Launch nothing from earlier requests. The description of the lab below is the earlier request's; this request uses its directory, `$ROOT` on `/gpfs/projects`, and changes nothing in it.

## What is being measured

UBQ is an unbounded multi-producer, multi-consumer FIFO queue (this repository). The request measures it, in several configurations, against other strict FIFO queues on two machines, for a paper.

One run measures one queue over a fixed matrix: 23 producer/consumer shapes, several batch sizes and four item sizes, three replicates each, every cell on an exclusive node. Every cell also checks that each item is delivered exactly once. A memory section records what each queue holds at seven element sizes. A second stage then profiles chosen cells with `perf` and, on Grace, Arm Performix. The lab's own documentation is `lab/README.md` in this repository.

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

It writes `$ROOT/runs/<id>/<cluster>/` (`plan.json`, `state.json`, `logs/`, later `tasks/`), submits the build job with `sbatch`, and prints one JSON line of status. After that the run advances itself: every job that ends submits what comes next, and a one-core watchdog job follows each array. Deploying a run cancels any older, unfinished run of the same queue on that cluster.

To see where a run stands, or to push it along after a quota refusal:

```sh
python3 $ROOT/runs/<id>/tool/ab.py remote advance $ROOT/runs/<id>/<cluster>
```

The reply names the phase, counts the tasks by status, lists failures, and repeats the latest notes. The phase is one of:
- `building`;
- `measuring` (stage 1);
- `deep` (stage 2, the profiling);
- `done`;
- `failed`, `superseded` or `cancelled`.

Two more commands take the same argument:
- `remote cancel` cancels a run's jobs.
- `remote retry` gives every failed task of the run one more attempt. Each task already has one automatic retry; `remote retry` is the only other one.

The jobs are submitted with the account, partition, QOS and node constraint written in `run.json` under `config.clusters.<cluster>`:
- account `bsc18`;
- partitions `ngpp` (Grace) and `gpp` (MareNostrum 5);
- QOS `ngp_bsccs` and `gp_bsccs` for measurements, `ngp_debug` and `gp_debug` for the build;
- the node constraint `perfparanoid`, which gives nodes where `perf` runs unprivileged.

If `sacctmgr show assoc user=$USER format=account,partition,qos -P` shows other values for your account, change those fields in each `run.json` before launching; nothing else in `run.json` should change. MareNostrum 5's task time limits are three times Grace's (`time_factor` in its cluster settings), because its processes started up to a minute late in October.

## What comes back

For every run and cluster, one archive of the cluster directory without the raw profiler captures:

```sh
cd $ROOT/runs/<id>
tar czf ~/<id>-<cluster>.tar.gz --exclude='perf.data*' --exclude=lock --exclude='.*.tmp' <cluster>
sha256sum ~/<id>-<cluster>.tar.gz
```

The archives go in one zip, sent to Nicolas through Guillem over Zulip, with the status replies and the checksums in the message. The results are JSON and logs, tens of megabytes per run. Back on Nicolas's machine the archives drop into the lab and produce the reports and figures.

## Rules for the agent

- **Reply once per request, when the request is complete**, as the request defines it. Send no progress messages and no partial results.
- Run the commands as written, in order. Do not change `tool/`, `sources/`, `vendor/` or `run.json`, except the account and QOS fields named above.
- Do not delete, move or change anything under `/gpfs/scratch/bsc18/bsc614227/` or `/gpfs/projects/bsc18/bsc614227/`, Nicolas's former directories.
- If a command of the request itself fails, stop there and report it in the one reply. A queue that crashes, hangs or miscounts inside a measurement is a result the lab records, not a failure of the request.
- Builds happen inside a Slurm job (the job script loads the `gcc` and `rust` modules); nothing is compiled on a login node.
- Scratch may be purged, so copy each archive to `$HOME` as soon as its run is done, and send them all together at the end.
