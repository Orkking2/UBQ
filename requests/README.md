# Benchmark requests for BSC

These files ask an agent with a shell on BSC's login nodes to run benchmarks on Grace and MareNostrum 5, and to send the results back through the person who gave it the request. The agent that runs them is a Claude Code session, so each request is written for it.

**This directory holds one request: `2026-11-26-v11-lines.md`.** It is the third in the series that began on 2026-11-20, whose results are in hand; the earlier request's file has been removed from this directory (it is in this branch's history), and nothing of it is to be run again. Launch nothing from earlier requests.

## What is being measured

UBQ is a family of multi-producer, multi-consumer FIFO queues (this repository). v11 is version 11 of UBQ, the newest: producers reserve with one fetch-and-add, a queue is built for a capacity but holds memory only for what is in it, and blocks it is done with can be given back to the system. The request asks three questions, one plan each: whether v11 and BQ deliver every element exactly once on these machines; how each queue's throughput goes along three lines of shapes (X producers and one consumer, X a side, one producer and X consumers), with the batch sizes of the queues that take batches at the largest shape of each line; and what each machine charges for one shared word.

## How this request runs

It has a kit of four files, as the earlier issue had:

```text
$ROOT/
  bundle.tar.gz         the frozen source of the crate, its vendored dependencies, and the kit
  kit/job.sh            the one job script: `build <cluster> <root>` builds and tests the binaries; `run <cluster> <root> [<rep>]` runs one replicate of every plan (in a task of a Slurm array the replicate is the task's number; a fifth word names the plans to run, if not all)
  kit/make_plans.py     writes the plans on the node, from the node's own cores
  kit/drive.py          runs one plan, a trial at a time, on the node's own disk
  kit/summarize.py      tables from the result files
  builds/<cluster>/     the runner and the line benchmark, each stored under its hash, and build.json, which names them
  results/<cluster>/    one result file per plan and task, stored when the task ends: a JSON line per trial
  logs/<cluster>/       each job's log, and a directory per job, stored when the job ends (node.txt, probe.tsv, the plans as run)
```

Each cluster has one build job and an array of ten tasks, each with a node to itself. The build job builds the binaries on its node, tests the runner, runs a smoke test and times the shared filesystem; the tasks start when it has succeeded, copy the binaries and each run the three plans in order, every cell in an order of their own. Each keeps everything it writes on its node's own disk (`$TMPDIR`), its results and its logs alike, and stores them on GPFS once, when it ends; a job that Slurm ends at its limit stores what it has first. A job cut short by its time limit is submitted again and continues from its result files. A trial that has not ended after 180 s is killed and recorded; a cell two of whose trials were killed is not run again in that task, and its other trials are recorded as `not run`.

A **plan** is a list of cells (a queue, a shape such as `16p16c` for sixteen producers and sixteen consumers, a batch size, a delay) and a number of trials. Every (cell, trial) pair is one run of a binary, of about a second, in a shuffled order. The shapes are computed on the node: `S` is the cores of one socket and `N` those of both.

The jobs are submitted with the account, partitions and QOS that the earlier issue's jobs used; the request's step 2 shows how to read them from `sacct`. It tries the cluster's debug QOS for the build job, because its queue is short.

## What comes back

One zip: the result files, the logs, the kit as it was run, and the summaries. Megabytes, not gigabytes.

## Rules for the agent

- **Reply once, when the request is complete on both clusters.** Send no progress messages and no partial results. Message earlier only if something stops the request and you cannot repair it.
- **Repair the machinery, not the measurement.** The request says what you may change (the kit's handling of files, directories, modules and the scheduler) and what you may not (the source, the binaries, and what is measured). Record every change.
- **Get the jobs started by whatever QOS starts them soonest** among those your account may use for a one-node job of 90 minutes or less.
- A queue that crashes, hangs or miscounts inside a trial is a result, not a failure of the request.
- Do not delete, move or change anything in the earlier requests' directories.
- Builds happen inside a Slurm job; nothing is compiled on a login node.
- Copy the final zip to `$HOME` before you send it; scratch may be purged.
