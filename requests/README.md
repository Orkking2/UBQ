# Benchmark requests for BSC

These files ask an agent with a shell on BSC's login nodes to run benchmarks on Grace and MareNostrum 5, and to send the results back. Nicolas, the author of UBQ, wrote them for Guillem, who runs them under his own account. The agent that runs them is a Claude Code session, so each request is written for it.

**This directory holds one request: `2026-11-20-v11-focused.md`.** It measures v11, the newest version of Nicolas's queue, in two jobs per cluster. It ends the request of 2026-10-30, whose remaining runs on Grace are no longer needed: its step 0 cancels them and archives what they finished. It replaces the request of 2026-11-10 (a diagnosis of the step overhead), whose question it carries as an optional last step. Launch nothing from earlier requests.

## What is being measured

UBQ is a family of multi-producer, multi-consumer FIFO queues (this repository). v11 is version 11 of UBQ, the newest, meant to take the place of the earlier versions if it proves better: producers reserve with one fetch-and-add, a queue is built for a capacity but holds memory only for what is in it, and blocks it is done with can be given back to the system. The request asks four questions, one plan each: whether v11 delivers every element exactly once on these machines; where its throughput stands beside other strict FIFO queues; what moves v11 itself (the batch size, the block length, and what its own counters say); and what each queue holds in memory.

## How this request runs

It does not use the lab of the earlier requests (`runs/<id>/tool/ab.py`). It has a kit of four files:

```text
$ROOT/
  bundle.tar.gz         the frozen source of the crate, its vendored dependencies, and the kit
  kit/job.sh            the one job script: `run <cluster> <root> <rep>` does everything in one job
  kit/make_plans.py     writes the plans on the node, from the node's own cores
  kit/drive.py          runs one plan, a trial at a time, on the node's own disk
  kit/summarize.py      tables from the result files
  builds/<cluster>/     the runner binary, stored under its hash, and build.json, which names it
  results/<cluster>/    one result file per plan and job: a JSON line per trial
  logs/<cluster>/       each job's log, and a directory per job (node.txt, probe.tsv, the plans as run)
```

A job has a node to itself. If no job has built the runner for its cluster yet, it builds it on the node, tests it and runs a smoke test; then it runs the four plans in order. It keeps everything a trial touches on the node's own disk (`$TMPDIR`), and writes to GPFS between trials only: one result file, copied and renamed into place. A job cut short by its time limit is submitted again and continues from its result files. A trial that has not ended after 180 s is killed and recorded; a cell two of whose trials were killed is not run again in that job, and its other trials are recorded as `not run`.

A **plan** is a list of cells (a queue, a shape such as `16p16c` for sixteen producers and sixteen consumers, a batch size, a delay) and a number of trials. Every (cell, trial) pair is one run of the runner binary, of about a second, in a shuffled order. The shapes are computed on the node: `S` is the cores of one socket and `N` those of both.

The jobs are submitted with account `bsc18` and partitions `ngpp` (Grace) and `gpp` (MareNostrum 5). The measurement QOS are `ngp_bsccs` and `gp_bsccs`; the request tries the debug QOS (`ngp_debug`, `gp_debug`) for one of the two jobs, because its queue is short. If `sacctmgr show assoc user=$USER format=account,partition,qos -P` shows other values for your account, use those.

## What comes back

One zip, sent to Nicolas through Guillem over Zulip: the result files, the logs, the kit as it was run, and the archives of the earlier request's runs. Megabytes, not gigabytes.

## Rules for the agent

- **Reply once, when the request is complete on both clusters.** Send no progress messages and no partial results. Message earlier only if something stops the request and you cannot repair it.
- **Repair the machinery, not the measurement.** The request says what you may change (the kit's handling of files, directories, modules and the scheduler) and what you may not (the source, the binary, and what is measured). Record every change.
- **Get the jobs started by whatever QOS starts them soonest** among those your account may use for a one-node job of two hours or less. Nicolas needs the results before December 7.
- A queue that crashes, hangs or miscounts inside a trial is a result, not a failure of the request.
- Do not delete, move or change anything under `/gpfs/scratch/bsc18/bsc614227/` or `/gpfs/projects/bsc18/bsc614227/`, Nicolas's former directories, or under the earlier request's directory beyond what step 0 says.
- Builds happen inside a Slurm job; nothing is compiled on a login node.
- Copy the final zip to `$HOME` before you send it; scratch may be purged.
