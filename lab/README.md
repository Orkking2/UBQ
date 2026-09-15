# The lab: staged A/B studies of the queue

`./ab` takes a question about the queue from a spec file to a hardware
explanation, on BSC's Grace (Arm, 144 cores) and MareNostrum 5 GPP (x86,
112 cores), or on this machine.

1. **Fork.** `new NAME --src src/v8/` copies the queue into `src/NAME/`,
   registers it, and writes a study comparing the source and its independent
   copy. Original UBQ (`--src src/`) and existing variants use the same system.
2. **Freeze.** Each arm is a git revision, or `WORKTREE` (the working tree
   as it stands, uncommitted changes included), plus optional Cargo features
   and workload settings. `launch` copies each arm's build inputs into a
   content-addressed source directory. Code that has not changed maps to the
   same directory, and to the same cached binary, in every later study, so a
   baseline is built once per cluster.
3. **Stage 1: throughput.** One exclusive allocation per cell (scenario x
   batch size) per replicate. Inside it, every arm first passes an exact-once
   delivery check at that shape. A short pilot then sizes one fixed schedule
   (round size and count) for all arms, and the arms run that identical work
   in interleaved pairs, rotating which arm goes first. Both runs of a pair
   share a node, so node-to-node variation cancels out of their ratio.
4. **Stage 2: deep.** When stage 1 ends, each candidate's biggest wins and
   losses are selected, and both arms are captured there under every
   collector, each capture beside an unprofiled twin run on the same node:
   `perf stat` counter groups, `perf record` cycles, `perf mem` loads (Arm SPE
   on Grace, PEBS on MN5), `perf c2c` on MN5, and on Grace Arm Performix's
   Code Hotspots, CPU Microarchitecture and Instruction Mix recipes.
5. **Report.** `fetch` brings the results home, `report` writes
   `results/<study>/report.md` and CSVs, and `performix` imports the APX
   captures into the Performix app under names that pair the arms.

## Quick start

```sh
./ab new cas-retry --src src/v8/         # copy to src/cas-retry/; write the paired study
$EDITOR src/cas-retry/queue.rs          # edit the independent candidate
$EDITOR lab/studies/cas-retry.json      # optional: the defaults below are a full study
./ab launch cas-retry --local --quick   # a one-minute smoke test on this machine
./ab launch cas-retry                   # Grace and MN5
./ab status --watch                     # polls; fetches and reports when both finish
```

To iterate, change `src/cas-retry/` and run `./ab launch cas-retry` again. The new
study cancels the previous one wherever it is still running (typically MN5,
which queues longer than Grace) before it starts. Pass `--keep-running` to
let both run.

## Commands

| command | does |
|---|---|
| `./ab sync [HOST]` | Mirror this worktree to HOST (default `stg`, the transfer node, which serves both clusters through the shared GPFS). Remote copies of files you deleted are removed; ignored paths such as `results/` and `target/` are never touched. |
| `./ab new NAME --src DIR [--queue Q]` | Copy a queue to `src/NAME/`, register it, and write a source-versus-copy study. |
| `./ab new NAME [--base REF] [--cand REF] [--queue Q]` | Write a revision study (defaults: `HEAD` vs `WORKTREE`). |
| `./ab queues` | List registered source modules, queue selectors, adapters and FIFO classes. |
| `./ab launch SPEC` | Freeze, upload, build, then run both stages. `--cluster` limits where, `--local` runs here without Slurm, `--quick` shrinks the study to seconds, `--dry-run` freezes and prints the plan, `--again` launches identical inputs as a new replicate study. |
| `./ab status [STUDY] [--watch [MIN]]` | Advance every deployment and show progress and effects so far; `--watch` polls until done, then fetches, reports and plots. |
| `./ab fetch [STUDY] [--all]` | Copy results home; raw `perf.data` stays remote unless `--all`. |
| `./ab report [STUDY]` | Write `report.md` and `csv/`, and print the effects; with several arms, also each family's best variation overall and per cell, and a ranking of every arm per cell. |
| `./ab plot [STUDY] [--cluster C]` | Write `results/<study>/plots/<cluster>-*.png` from fetched results, in absolute units (items/s, bytes) with every series named: scaling curves of each family at its best variation for each batch size, one throughput-by-geometry figure per family, and the geometry, memory and layout matrices (see "Reading the plots"). Uses `.venv` when the system Python lacks matplotlib. |
| `./ab deep [STUDY] --cell C [--collector X]` | Deep-dive further cells after stage 1, reopening a finished study. |
| `./ab cancel [STUDY] [--cluster C]` | Cancel a study's jobs. |
| `./ab list` | Studies launched from this checkout; `*` marks the latest. |
| `./ab performix [STUDY]` | Import fetched APX captures into Arm Performix. |

STUDY defaults to the latest launch and accepts any unique prefix.

## Independent queue variants

```sh
./ab new my-change --src src/v8/
./ab launch my-change --local --quick
./ab new next-layout --src src/my-change/
./ab new original-fork --src src/
./ab launch v8-vs-ubq --local --quick
```

The first command creates `src/my-change/` with v8's code and unit tests,
rewriting explicit Rust self references to `ubq::ab_my_change`. The source
directory remains untouched. The study compares `ubq-v8-s4096` with
`ubq-my-change-s4096`, both from `WORKTREE`, so they share one frozen source,
binary, and measurement harness. Each launch freezes both modules as they
stand; keep edits in the candidate directory to preserve the control.
Snapshots include untracked variant files, so no commit is needed to launch.

`--queue ubq-v8-s64-v3` selects the source geometry and applies it to both
arms. Without a geometry suffix, owned-buffer queues use the implementation's
default geometry, 4096 slots and 32 VBlocks for v8 and its forks.
`lab/studies/block-size.json` sweeps both dimensions for v8 (see "Sweeps"
below). Every registered queue, original UBQ forks included, takes the
workload's `backoff` policy (`crossbeam` or `yield`); original UBQ forks use
the original iterator batch API.
`--src src/` copies only the original queue's implementation files and unit
tests, keeping its private block, head, page and slot modules independent;
the shared backoff policies remain crate-level dependencies.

Names start with a lowercase letter and contain lowercase letters, digits,
hyphens or underscores, up to 48 characters. Rust module names use an `ab_`
prefix and replace hyphens with underscores. A destination or registration
collision is an error. `--force` replaces an existing **study spec only**;
it never overwrites a source module. If the earlier `my-change.json` spec
already exists, use `./ab new my-change --src src/v8/ --force` to replace it.

`src/queue_variants.tsv` is the registration point for original UBQ, v8 and
forks: name, Rust module, source path, adapter (`ubq` or `v8`), FIFO class.
Cargo generates module declarations and static benchmark dispatch from it;
adding a fork needs no harness edits. The two shared adapters preserve each
API's native batch operations. Scaffolding inherits its source's adapter and
FIFO class; change the class to `relaxed` if a redesign weakens ordering.
The exact-once probe checks delivery, not the FIFO contract.
New APIs need an adapter before they can be registered. External baseline
queues remain available through the same `bench_hardware` runner.

Generated studies run the source and candidate's libtest filters before
benchmarking. The supplied `v8-vs-ubq` study brings the original v8 A/B into
the lab; `v8-geometry` remains a configuration comparison. The separate v8
viability script remains a correctness gate, not a second benchmark system.

## Sweeps: A/B/C/... studies and families

A study may have any number of arms. The first is the control and every
other arm is measured against it in the same paired way, in the same
allocation, so one study can compare a queue at many geometries, or several
queues at several geometries each. `grid` writes those arms for you:

```json
{
  "name": "block-size",
  "grid": {"queues": ["ubq-v8"],
           "geometries": ["s4096-v32", "s256-v32", "s1024-v32", "s16384-v32",
                          "s4096-v2", "s4096-v8", "s4096-v128"]},
  "clusters": ["grace"]
}
```

Each queue at each geometry becomes an arm named `<queue>-<geometry>`
(`v8-s256-v32`), from `WORKTREE` unless `grid.ref` says otherwise; explicit
`arms` come first and may name the control. Every arm belongs to a
**family**: its `family` field, else its queue name without the geometry
suffix, so all v8 geometries are `ubq-v8`. Adding a fork to `grid.queues`
gives it its own family. Families are how a sweep is read:

- The report's *best variation per family* table names, per family, the
  variation with the largest geometric-mean effect over the cells it was
  measured in, and per cell the variation with the largest paired effect.
  The *ranking per cell* table orders every arm in each cell, marking each
  family's best.
- `./ab status` and the report's headline show those family bests instead
  of every arm, once a study has more than three arms.
- The deep stage selects each family's biggest wins and losses at its best
  variation per cell. A sweep over two queues at seven geometries each
  selects deep cases for each of its two families.
- The report's *geometry matrix* tables and `./ab plot` compare every arm
  against the batch's best arm, never against the control ("Reading the
  plots" below).

`lab/studies/geometry-matrix.json` is the full matrix for v8:
slots per VBlock in {256, 1024, 4096, 16384} times VBlocks
per block in {2, 8, 32, 128}, 16 arms, on Grace and MN5, with the RSS guard
at 64 GiB so the widest batched shapes are measured rather than killed.
`block-size.json` is the earlier cross through the default geometry (one
dimension varied at a time). Every arm of a sweep runs in every allocation,
so 16 arms take about 2.3x the node time of 7; the dry run prints the
estimate.

### Beyond A/B: several queues in one study

Any `bench_hardware` queue name is an arm: `ubq` (the original), `segqueue`,
`lubq`, `mutex-vecdeque`, `moodycamel-cq` (with the `bench_moodycamel`
feature on that arm), and so on, next to any number of v8 forks and
geometries. `strict-baselines.json` puts v8 at its selected
geometry beside the original UBQ and SegQueue; `relaxed-baselines.json`
compares LUBQ with moodycamel. The two cannot share a study: `launch`
refuses arms that mix FIFO classes, because a relaxed queue buys its speed
by giving up the global order. A relaxed derivative of v8 gets `relaxed` in
`src/queue_variants.tsv` and is compared in a relaxed study.

Families keep such a study readable: the scaling figure draws one line per
family in each panel, so a study with 16 v8 geometries and two baselines has
three lines per panel, each named in the panel's legend with the geometry it
shows there, which may differ from one batch size to the next.

## Interruptions

Every command can be interrupted and rerun.

- Identical inputs (spec, frozen sources, tool, configuration) name the
  identical study, `<name>-<hash>`, so rerunning an interrupted `launch`
  resumes it. `--again` is the only way to get a second study from the same
  inputs.
- Nothing runs on a login node between commands. Every job ends by calling
  `advance`, which takes the study's lock and moves it to its next step
  (build, then stage 1, then selection and stage 2, then done). A one-core
  watchdog job runs `advance` after each array ends, in case all its tasks
  died, and `./ab status` runs it too.
- A submission is recorded before `sbatch` runs and reconciled by job name
  afterwards, so a dropped connection never duplicates an array.
- A task that crashed or hit its time limit is retried once. A task that ran
  and found a problem, such as an arm failing the exact-once check, is a
  result, not a crash.
- If the QOS submit quota is full, submission waits for the next `advance`.

Every job is named `ab.<study hash>.<stage>.<token>`. Superseding a study
cancels its recorded jobs and any other `ab.*` job of yours on that cluster.

## Specs

Only `name` and `arms` (or `grid`, see "Sweeps") are required. The first
arm is the control; the rest are candidates.

```json
{
  "name": "cas-retry",
  "question": "Does retrying the publish CAS locally help the balanced shapes?",
  "arms": [
    {"name": "base", "ref": "HEAD"},
    {"name": "cand", "ref": "WORKTREE", "features": [], "workload": {}}
  ],
  "workload": {"queue": "ubq-v8-s4096"},
  "clusters": ["grace", "mn5"],
  "throughput": {
    "scenarios": ["1p1c", "2p1c", "4p1c", "8p1c", "16p1c", "32p1c", "64p1c", "Np1c",
                  "1p2c", "1p4c", "1p8c", "1p16c", "1p32c", "1p64c", "1pNc",
                  "2p2c", "4p4c", "8p8c", "16p16c", "32p32c", "HpHc", "4p16c", "16p4c"],
    "batch_sizes": [null, 16, 256, 4096],
    "skewed_batch_sizes": [[16, 256], [16, 4096], [256, 4096]],
    "replicates": 3, "pairs": 3, "run_seconds": 8, "max_run_seconds": 60
  },
  "memory": {"scenarios": ["Np1c"], "depths": [524288]},
  "deep": {"auto": true, "winners": 2, "losers": 2, "cells": [], "replicates": 1,
           "run_seconds": 20, "collectors": null},
  "build": {"tests": ["v8::", "bench_harness::hardware"]},
  "allow_harness_mismatch": false
}
```

- **Scenarios** are `<P>p<C>c`, where P or C may be `N` (worker cores - 1)
  or `H` (half the cores), so `Np1c` is 143p1c on Grace and 111p1c on MN5.
  Shapes needing more threads than cores are skipped. The default matrix
  scales producers alone (`1p1c` to `Np1c`), consumers alone (`1p2c` to
  `1pNc`) and both together (`2p2c` to `HpHc`), plus `4p16c` and `16p4c`:
  23 shapes at scalar, 16, 256 and 4096-item batches, plus three uneven
  pairs at each of the 16 skewed shapes, 140 cells. A round is always at
  least one batch per producer, so large batches at wide shapes measure
  correctly. A `batch_sizes` entry is `null` for scalar `push`/`pop`, an
  integer for that batch on both sides, or a `[producer, consumer]` pair
  that sets the sides apart: `[null, 256]` pushes one item per call and pops
  256 per call. A side of `1` means the scalar API, so `[1, 256]` is the same
  pair; its cell is `143p1c-b1x256`. `batch-mismatch.json` crosses four push
  sizes with four pop sizes at the widest shapes to ask whether a producer
  batch has to match the consumer batch.
- **Skewed batch sizes** are measured only at shapes whose sides differ,
  where an uneven pair has a reason: the outnumbered thread amortises more
  per call. Each `skewed_batch_sizes` entry is `[producer, consumer]` as
  measured at a producer-heavy shape (`16p1c-b16x256`); a consumer-heavy
  shape takes the mirror image (`1p16c-b256x16`), so the outnumbered side
  gets the same batch at either skew, the larger one by default. Balanced
  shapes never measure them; set the list to `[]` to measure matched
  batches only. A pair already in `batch_sizes` is measured once.
- **Replicates** are independent allocations; **pairs** are control/candidate
  run pairs within one. The fastest arm runs about `run_seconds` per run;
  no arm runs longer than `max_run_seconds`.
- **Memory** cells run the lifecycle probe (fill, drain, idle, refill,
  redrain, destroy) at each depth and record process RSS at every
  checkpoint.
- **Deep** selects up to `winners` and `losers` cells per family, at the
  family's best variation in each cell, or exactly `cells` when given. `auto: false` stops after stage 1 until
  `./ab deep`. `collectors` may be a list or an object keyed by cluster;
  `null` means every collector the cluster offers.
- **Build** runs these libtest filters against every arm before anything is
  measured; an arm that fails them never reaches a node.

### Arms

`ref` is any git revision, or `WORKTREE`. `workload` may override `queue`,
`backoff`, `block_size`, `capacity`, `segment_size` and `rss_limit_mib`.
`family` groups arms for the sweep summaries (default: the queue name
without its geometry suffix).
v8 geometries are queue names: `ubq-v8-s<slots per VBlock>`, optionally
`-v<VBlocks per allocation>` (for example `ubq-v8-s16384-v8`), so a geometry
study needs no code change and both arms can share one source.
`backoff` is `crossbeam` (the default) or `yield` and every registered queue
and LUBQ take it. A backoff is a variation within a family like a geometry:
give a queue one arm per policy (`dynblock-vs-ubq.json` and `v8-vs-ubq.json`
do) and the report's family bests, the ranking's ◆ marks, the deep-stage
selection and the scaling figure's panel legends say which policy each family was
best with (per batch size, in the figure); the geometry matrices get one row per family and policy.

An A/B study compares queues, not harnesses. `launch` refuses arms whose
`src/bench_harness/`, `src/bin/bench_hardware.rs`, or `build.rs` differ. Commit a harness
change so every ref has it, or set `allow_harness_mismatch` if you mean it.
All arms must also share a FIFO class (`src/queue_variants.tsv` for modules,
`FIFO_ORDERING` in `scripts/plot_style.py` for other queues), since relaxed queues buy speed by giving up the
global order.

## Reading the report

- **Throughput by shape** opens each cluster's results: per batch size, one
  row per shape and one column per family, with the median throughput of
  the family's fastest arm there (its backoff or geometry named when the
  family has several arms) and the fastest family in bold. The summary above
  the tables gives each family's geometric mean over the shapes, its ratio
  to the control's family over the shapes both ran, the shapes where it was
  ahead of that family, and the shapes where it was the fastest of all.
  `./ab status` and `./ab report` print that summary;
  `csv/shape_matrix.csv` and `csv/family_summary.csv` carry the numbers.
- **Effect** is candidate throughput over control, the geometric mean over
  replicates of each replicate's mean pair ratio. The 95% interval is a t
  interval across replicates; with one replicate it falls back to the pairs
  on that one node. **win** and **loss** mean the interval excludes 1.
- **Quality** is the share of runs timed at least 2 s with the harness's
  barrier overhead at most 5%. A low share means the harness, not the queue,
  dominates the timing, and absolute numbers deserve suspicion; paired
  ratios suffer less.
- **Integrity** lists arms that lost or duplicated items, or failed to run.
  Their cells get no effect estimate and are never deep-dived. A run killed
  by the RSS guard (32 GiB by default; `rss_limit_mib` in a workload raises
  it) is a failed run, not an integrity problem.
- **Layouts at equal geometry** (a grid with several families) compares each
  family with the control's family at every geometry both ran: the ratio of
  their effects in each cell, which cancels the control, as a geometric mean
  over all cells and per batch size. `./ab plot` draws it as
  `<cluster>-layout-matrix.png`.
- **Ranking per cell** (several arms) orders every arm by its effect and
  names the arm with the largest median end-of-run RSS; `csv/ranking.csv`
  and `csv/throughput_runs.csv` carry the RSS of every arm and run, taken
  from the process at the end of each fixed-work run.
- **Batch matrix** (a study with several batch pairs) lists, per arm and
  shape, the median throughput at every push/pop pair and its share of the
  arm's fastest pair at that shape; `csv/batch_matrix.csv` has the numbers.
- **Geometry matrix** (several arms) lists, per batch size, every arm's
  geometric-mean throughput over its cells, its share of the batch's best
  arm (a geometric mean over the cells both ran, so 100% is one real arm,
  not a per-cell oracle), and its largest and typical median end-of-run
  RSS. The control plays no part; `csv/geometry_matrix.csv` has the numbers.

## Reading the plots

Every figure uses absolute units (items/s with K, M, B suffixes; bytes in
MiB and GiB) and names every series. The control arm is the pairing
reference for the statistics, not a baseline any figure divides by.

- `<cluster>-scaling.png`: throughput against thread count on the producer,
  consumer and balanced axes, per batch size, one line per family at its
  best variation for that batch (the geometry matrix's ring), so a family
  that wants small VBlocks at scalar and big blocks at batch 4096 is drawn
  at each; when a family has several arms, a small legend in every panel
  names the variation drawn there, otherwise one legend below the panels
  names the families. A skewed pair and its mirror share a row, push 16 /
  pop 256 on the producer axis and push 256 / pop 16 on the consumer axis;
  axes without cells are left out.
- `<cluster>-geometry-<family>.png`: for one family, throughput against
  slots per VBlock at representative shapes, one line per VBlock count
  (light to dark); the ring is the fastest geometry in that cell.
- `<cluster>-geometry-matrix.png`: per family and batch size, a VBlocks x
  slots grid whose cells give each geometry's throughput as a share of the
  batch's best arm; the ring is the family's best geometry.
- `<cluster>-memory-matrix.png`: the same grid with end-of-run RSS, the
  largest cell on top and the typical (geometric mean) cell below.
- `<cluster>-layout-matrix.png`: each other family over the control's
  family at equal geometry, from the paired effects.
- `<cluster>-cells.png`: every arm in every cell as a share of that cell's
  fastest arm; ◆ marks each family's fastest arm there.
- `<cluster>-batch-matrix.png` (several batch pairs): per arm and shape, a
  push-batch x pop-batch grid annotated with absolute throughput and
  coloured by its share of the arm's fastest pair at that shape (ringed).
- **Deep** tables average each metric over replicates per arm, with each
  candidate's ratio to the control. Counter values are per handoff and
  cover the whole process (startup and warmup included); ratios such as
  IPC are only formed from counters in the same group, which were counted
  over the same run. SPE rates are per sampled load within one address
  space, so they do not depend on the sampling period. Each collector's
  slowdown against its unprofiled twin is noted.

## Where things live

On this machine, `results/<study>/` holds `study.json` (the frozen inputs),
`tool/` (the exact lab code the jobs run), one directory per cluster
(fetched), `report.md` and `csv/`. `results/.lab/sources/` holds the frozen
arm sources, which also give Performix each arm's code.

On the shared GPFS (`remote_root` in `lab/config.json`, on scratch because
projects is near its quota):

```text
vendor/<hash>/     vendored crates, uploaded once
sources/<hash>/    frozen arm sources, uploaded once each
builds/<cluster>/  cached bench_hardware binaries, one per source and feature set
cache/<cluster>/   the shared Cargo target directory
studies/<study>/   study.json, tool/, and per cluster: plan, state, logs, tasks/
```

Scratch can be purged, so fetch what you want to keep.

## Configuration

`lab/config.json` names the transfer host, the remote root, sync
destinations, and for each cluster its login host, Slurm account, partition,
QOS (`build_qos` is tried first for builds), node constraint, modules,
worker cores, collectors and `perf stat` counter groups. Grace's
`array_limit` caps each task array (300 on both clusters, whose QOS allows
366 submitted jobs per user; a larger study is submitted in pieces as
earlier arrays drain). `exclusive_mem0` requests the whole node's memory, which MN5 derives from
the CPU count instead.

## Testing the lab

```sh
python3 -m unittest discover -s lab/tests            # state machine, statistics, parsers
AB_E2E=1 python3 -m unittest discover -s lab/tests   # plus a real study and compiled fork probes
./ab launch lab/studies/v8-geometry.json --local --quick
```

The cluster side must stay standard-library Python 3.9, which the BSC nodes
run; `/usr/bin/python3` on macOS is 3.9 too.
