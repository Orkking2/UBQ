# UBQ

[![Crates.io](https://img.shields.io/crates/v/ubq.svg)](https://crates.io/crates/ubq)
[![Docs.rs](https://docs.rs/ubq/badge.svg)](https://docs.rs/ubq)

UBQ is a **lock-free, unbounded, multi-producer/multi-consumer (MPMC) queue**
built from linked, page-sized blocks, intended for concurrent producers and
consumers.

## Features

- **Lock-free** — `push` and `pop` never park the calling thread.
- **Unbounded** — capacity grows automatically as new blocks are allocated.
- **MPMC** — any number of producers and consumers may operate concurrently.
- **Arc-friendly sharing** — `UBQ<T>` is meant to be wrapped in `Arc` for shared,
  concurrent ownership across threads.
- **FIFO ordering** — elements are returned in the order they were pushed, within
  each block.

## Usage

Add UBQ to your `Cargo.toml`:

```toml
[dependencies]
ubq = "5"
```

### Basic example

```rust
use ubq::UBQ;

fn main() {
    let q: UBQ<u64> = UBQ::new();
    q.push(1);
    q.push(2);
    assert_eq!(q.pop(), Some(1));
    assert_eq!(q.pop(), Some(2));
    assert_eq!(q.pop(), None);
}
```

### Multi-threaded (MPMC)

```rust
use ubq::UBQ;
use std::sync::Arc;
use std::thread;

let q: Arc<UBQ<u64>> = Arc::new(UBQ::new());

// Spawn 4 producers and 4 consumers.
let m = 100_000;
let handles: Vec<_> = (0..4)
    .flat_map(|_| {
        let pq = Arc::clone(&q);
        let cq = Arc::clone(&q);
        [
            thread::spawn(move || { for i in 0..m { pq.push(i); } }),
            thread::spawn(move || { for _ in 0..m { while cq.pop().is_none() {} } }),
        ]
    })
    .collect();

for h in handles { h.join().unwrap(); }
```

See the full API reference on [docs.rs](https://docs.rs/ubq).

### `no_std + alloc`

UBQ supports `no_std + alloc` on Unix, Windows, and WebAssembly targets that
provide native 8-bit and pointer-width atomics:

```toml
[dependencies]
ubq = { version = "5", default-features = false }
```

The final application must install a global allocator. UBQ remains unbounded,
so a push may allocate a new base-page-sized, base-page-aligned block;
applications with a fixed memory budget must enforce their own queue-depth
limit. In `no_std` builds the built-in backoff policies spin instead of yielding
to an operating-system scheduler.

## How it works

UBQ and LUBQ share one internal page-block substrate: every allocation is
exactly one operating-system base page and carries the same intrusive successor
link. Their synchronization remains specialized. UBQ fills the trailing area
with stateful MPMC `Slot<T>` cells; each LUBQ shard fills it with plain `T`
payload cells and publishes contiguous prefixes through block-level counters.
This keeps allocation geometry and pointer tagging homogeneous without adding
per-item atomic state to the SPMC path.

The const UBQ block parameter is an upper bound on slots used in one page. If
that many `Slot<T>` cells do not fit, UBQ uses the page's maximum capacity.
[`UBQ::block_length`](https://docs.rs/ubq/latest/ubq/struct.UBQ.html#method.block_length)
reports the effective value. Element types whose slot cannot fit in one base
page, or whose alignment exceeds the base-page alignment, are rejected when
the queue is constructed.

## Benchmarks

This repo includes a benchmark harness that compares static UBQ and linked-shard
LUBQ against established MPMC queue implementations (`segqueue`,
`concurrent-queue`, and optional RBBQ/BBQ, `lfqueue`/LSCQ, and wCQ variants).
Unless `--scenarios` is supplied,
each machine benchmarks the complete power-of-two producer/consumer grid:
`2^n p 2^m c` for every `n,m >= 0` whose producer and consumer thread sum does
not exceed detected available parallelism. For example, a 16-thread machine
runs all 16 combinations of `1,2,4,8` producers and consumers.

The Rust benchmark harness and binaries are isolated behind the `bench_tools`
feature. Benchmark-specific features such as `bench_registry`, `bench_rbbq`,
`bench_lfqueue`, and `bench_wcq` enable it automatically.

The schema-v7 comparative harness has two front ends:

- `bench_matrix`: direct matrix execution. It dispatches through the
  precompiled benchmark registry and writes schema-v7 JSON files under
  `bench_results/runs`.
- `bench_grid`: reproducible UBQ execution across both backoff policies. UBQ
  always uses the number of `Slot<T>` values that fit in one system base page;
  block size is no longer a benchmark dimension.

For throughput, each UBQ backoff policy and LUBQ measures a
scalar-compatible operation and batch-shaped operations at sizes `8,32,256`.
Static UBQ uses its native `push_batch` and `pop_batch` APIs; LUBQ uses
`Sender::send_batch` and native per-shard `Receiver::try_recv_batch_into`
claims with a reusable caller-owned `Vec`, one persistent private sender shard
per producer, and one cursor per consumer. LUBQ retains its stable shard list
until final root teardown and reuses inactive empty shards across sender churn.
When `segqueue` is selected, its normal
`SegQueue::push`/`pop` run remains scalar and the same batch-size grid is run
through the fork's separate `BatchQueue::push`/`pop` API. `--batch-sizes`
replaces that shared batch-size list while retaining scalar measurements.
Thus the default three batch sizes produce eight UBQ throughput jobs per
scenario and repeat: scalar plus three batched runs for each backoff. Scalar
baselines are measured once, while the Crossbeam batch queue is measured once
per requested batch size. Other benchmark modes remain scalar.

For workload-specific modes, when `--items-per-producer` is omitted,
`bench_grid` uses the versioned
`scenario_scaled_v1` workload: 1–8 producers get 1,000,000 items each, 9–16
get 250,000, 17–32 get 62,500, and larger producer counts get 15,625. Every
queue, UBQ backoff, batch size, mode, and repeat in a scenario receives
the same resolved count. Supplying one or more `--items-per-producer` values
selects the `explicit` policy and runs every supplied value in every scenario.
The selected policy and scenario mapping are printed before execution and
recorded in each output file.

Every scenario's results are coalesced into one mutable record at
`bench_results/runs/<machine-label>/<scenario>/record.json`, reopened and
updated in place across invocations rather than a fresh file per run. By
default `bench_grid` greedily reuses any sample already on record there:
reuse is keyed on the machine-label plus the exact sample identity (scenario,
queue/configuration, mode, batch size, repeat) and the measurement protocol
that produced it (available parallelism, core placement/pinning, throughput
timing budget, item-count policy). Changing scenario, queue, or repeat
coverage between runs never invalidates unrelated cached samples, and
narrower or wider plans reuse their overlap; changing the measurement
protocol only recomputes the samples that protocol actually affects, leaving
the rest of the record untouched. Source/build state (git commit, `rustc`
version, hostname) is recorded per scenario for provenance but never gates
reuse, so editing source between runs does not invalidate prior data — a
crash or interruption costs at most the in-flight sample. `--rerun` ignores
existing samples for this machine-label and recomputes everything.
Deliberately reuse a machine-label to extend or resume a batch, or bump it
(e.g. `grace-1` -> `grace-2`) to force a fully fresh sweep isolated from a
prior one. Jobs execute sequentially on the same core range so separate queue
measurements cannot contend with one another. Within each job, producer and
consumer threads are interleaved over the assigned core IDs until one role is
exhausted; the actual role-to-core map is printed before execution and
recorded as `core_placement = "interleaved"`. Authoritative throughput
requires every worker thread to pin successfully. `--core-ids 0-7,16-23`
selects an explicit ordered CPU set; `--allow-unpinned` is a diagnostic
escape hatch whose records are excluded from winner claims. Hard timeouts are
derived only from the declared measurement budget: at least 30 seconds and
otherwise five times the warmup plus three measured phases.
`--job-timeout-secs` overrides that value. Each job runs in a reusable worker
process; if it exceeds its hard timeout, the parent kills and reaps the
entire worker, checkpoints a timed-out sample, starts a fresh worker, and
continues. Stdout is a fixed-width job table with the queue, scenario, mode,
batch size, thread use, pending count, and percentage of the complete plan;
each row advances from `Pending...` to `Pending...DONE`.

`bench_matrix` uses the same reuse rule when `--reuse-existing` is supplied.

### Slurm

One array task runs one scenario end to end, writing straight into the
coalesced tree above — there's no separate per-task shard directory to
reconcile afterward. `slurm/submit_bench_grid.sh {mn5|grace} <machine-label>`
sizes the array from the manifest itself (`manifests/mn5-112.txt` /
`manifests/grace-144.txt`) rather than a hand-typed bound, and submits with
no `%K` concurrency throttle and no node-count cap: every scenario in the
manifest gets a task regardless of how many run concurrently, and Slurm's own
partition/QOS/fairshare limits decide actual concurrency.
`slurm/submit_build.sh {mn5|grace}` builds `bench_grid` plus the symbolized
`bench_profile` foreground profiler workload;
chain the two with `--after`:

```bash
build_id=$(slurm/submit_build.sh mn5)
slurm/submit_bench_grid.sh mn5 mn5-1 --after "$build_id"
```

The comparative array explicitly runs both scalar and every requested native
batch size for LUBQ alongside UBQ, SegQueue, concurrent-queue, BBQ, LSCQ, wCQ,
Mutex+VecDeque, MS-Queue, and moodycamel::CQ. Each task records that exact
queue set in its scenario's `slurm-info.txt` provenance file.

`<machine-label>` is a required, explicit choice, not a default — per the
reuse rule above, bump it for a fresh sweep or hold it steady to greedily
resume/extend a batch. Both scripts default `ACCOUNT=bsc18` and
`UBQ=/gpfs/projects/$ACCOUNT/$USER/UBQ` (the repo's deployed location on the
cluster), overridable via env; run `--help` on either for the full option
list.

### Arm Performix on Grace

`slurm/submit_performix.sh` profiles one exact `ubq`, `lubq`, or `segqueue`
handoff case on an exclusive Grace node and exports the portable Performix run.
For example, this submits the motivating `1p1c`, batch-256 SegQueue case:

```bash
build_id=$(slurm/submit_build.sh grace)
slurm/submit_performix.sh grace segqueue 1p1c \
  --batch-size 256 --after "$build_id"
```

Use the same scenario/batch for LUBQ and UBQ to compare their hotspots:

```bash
slurm/submit_performix.sh grace lubq 1p1c --batch-size 256 --after "$build_id"
slurm/submit_performix.sh grace ubq 1p1c --batch-size 256 \
  --ubq-label balanced,1,page,crossbeam --after "$build_id"
```

The default recipe is `code_hotspots`; `--recipe cpu_microarchitecture` and
`--recipe instruction_mix` select the other unprivileged optimization views.
Results are written below `performix_results/grace/<case>/<job-id>/`, including
the run ID, readiness/run logs, provenance, and exported archive. The wrapper
deliberately rejects MN5 because `arm_performix/2026.3.1` is not known to be
available on `mn5gpp`. See [docs/performix.md](docs/performix.md) for the full
workflow and interpretation order.

### Personal machines

`bench_grid` runs the same way with no Slurm involved: point `--runs-dir` at
a local directory and it schedules the full feasible grid for the machine's
detected parallelism in one process. Because reuse is greedy by default, a
personal machine benefits from the same resilience Slurm gets for free —
interrupting or crashing the run loses at most the in-flight sample, and
rerunning the identical command resumes from wherever it left off. Keep a
stable `--machine-label` (e.g. a hostname) across iterative runs to
accumulate coverage cheaply; only bump it when you deliberately want a clean
slate.

`throughput` is an adaptive sustained protocol. A disposable doubling pilot
selects an empty-to-empty round size (100 ms target, 8,388,608-item cap), then
excluded warmup rounds accumulate 250 ms. Measured handoff rounds accumulate
at least one second and stop timing at the last real dequeue; sentinel delivery
and joins cannot change the headline rate. Paired producer-only fill and
consumer-only drain cycles independently accumulate at least one second and
report enqueue and dequeue ceilings. Hot-loop counters are thread-local and
exact totals are validated after every round. The timing budgets can be changed
with `--throughput-warmup-ms`, `--throughput-phase-ms`,
`--throughput-pilot-ms`, and `--throughput-max-round-items`. Complex, fairness,
latency, and application modes remain workload-specific measurements rather
than upper-bound claims.

`data_latency` timestamps immediately before enqueue and immediately after
dequeue without credits or producer throttling. It therefore measures latency
under an unbounded saturated workload; backlog and item count can affect the
average, so compare subjects using the same scenario and resolved workload.

UBQ labels remain four-part identifiers for result compatibility:

- `preset,pool,page,backoff`
- Example: `balanced,1,page,crossbeam`

The pool field is retained for result-format compatibility and must be `1`;
`page` records that block capacity is derived from the host base page and value
type. Numeric block labels from older plans are accepted and normalized to
`page`. Backoff is the only remaining static UBQ benchmark dimension.

Publication-backed baseline labels are emitted with their sizing knob:

- RBBQ/BBQ: `fastfifo_b<block_size>_c<requested_capacity>`, for example
  `fastfifo_b256_c1048576` (default block grid `64,256,1024,4096` and default
  explicit capacity 1,048,576). Use `--fastfifo-capacities` to cross capacities
  with the selected block sizes.
- LSCQ via `lfqueue`: `lfqueue_<segment_size>`, for example `lfqueue_256`
  (default grid `32,256,1024`).
- wCQ: `wcq_<capacity>`, for example `wcq_65536`
  (default grid `4096,65536,1048576`). wCQ is bounded, so fill/drain samples
  are only scheduled when the selected capacity can hold the full pre-drain
  item set plus consumer sentinels.

The plotting scripts also emit `queue_metadata.csv` files that map queue labels
back to their implementation family and publication lineage, so paper-backed
baselines remain identifiable in aggregate plots.

For a presentation-oriented preview run and the full BSC-CNS paper run, see
[`docs/bsc_cns_presentation_runbook.md`](docs/bsc_cns_presentation_runbook.md).

Run an explicit direct matrix:

```bash
cargo run --release --features bench_registry,bench_rbbq,bench_lfqueue,bench_wcq --bin bench_matrix -- \
  --machine-label local \
  --queues ubq,segqueue,concurrent-queue,rbbq,lfqueue,wcq \
  --ubq-label balanced,1,page,crossbeam \
  --rbbq-block-sizes 64,256,1024,4096 \
  --lfqueue-segment-sizes 32,256,1024 \
  --wcq-capacities 4096,65536,1048576 \
  --scenarios 1p1c,8p8c \
  --modes throughput \
  --items-per-producer 1000000
```

For BBQ ATC 2022-style microbenchmarks, the scenario parser also accepts
`spsc`, `mpsc:N-M`, `spmc:N-M`, `mpmc:N-M`, `bbq-atc22-x86-88t`, and
`bbq-atc22-oversub-x86-12t`. The paper-style metric modes are
`throughput`, `complex_throughput`, `data_latency`, and `fairness`. See
[docs/bbq_atc22_reproduction.md](docs/bbq_atc22_reproduction.md) for the
ready-to-run suite.

The harness also includes synthetic application-level queue experiments. These
are still controlled benchmarks, not full production workload models, but they
exercise common application communication patterns. See
[docs/application_benchmarks.md](docs/application_benchmarks.md) for notes on
how to interpret them:

- `app_log_fan_in`: producers emit boxed log/event records into one shared
  queue while consumers hash and free them.
- `app_pipeline`: ingress threads feed a first queue, worker threads transform
  records into a second queue, and one collector drains completions.
- `app_task_roundtrip`: client threads submit one in-flight request at a time
  to worker threads and receive completions through a shared response queue.

Run the application-level suite:

```bash
cargo run --release --features bench_registry,bench_rbbq,bench_lfqueue --bin bench_matrix -- \
  --machine-label local \
  --queues ubq,segqueue,concurrent-queue,rbbq,lfqueue \
  --ubq-label balanced,1,page,crossbeam \
  --scenarios 1p1c,4p1c,1p4c,4p4c,8p8c,16p16c \
  --modes app_log_fan_in,app_pipeline,app_task_roundtrip \
  --items-per-producer 100000 \
  --repeats 3
```

Run the benchmark grid on one machine:

```bash
cargo run --release --features bench_registry,bench_rbbq,bench_lfqueue,bench_wcq --bin bench_grid -- \
  --machine-label local \
  --queues ubq,segqueue,concurrent-queue,rbbq,lfqueue,wcq \
  --batch-sizes 8,32,128,512 \
  --rbbq-block-sizes 64,256,1024,4096 \
  --lfqueue-segment-sizes 32,256,1024 \
  --wcq-capacities 4096,65536,1048576
```

Add `--rerun` to benchmark every job without using compatible existing
results. Batch sizes must be integers of at least 2; duplicates are removed.
The scalar-compatible variant is always included.

To benchmark only scalar SegQueue and the forked BatchQueue—without scheduling
any UBQ configurations—select `segqueue` by itself:

```bash
cargo run --release --features bench_registry --bin bench_grid -- \
  --machine-label local \
  --queues segqueue \
  --batch-sizes 8,32,256 \
  --scenarios 1p1c,4p1c,1p4c,4p4c \
  --repeats 3
```

Omit `--scenarios` to use the complete feasible machine grid. This schedules
one scalar `SegQueue` sample and one native `BatchQueue` sample per requested
batch size, scenario, and repeat.

Measure word-sized atomic update contention independently of the queue grid:

```bash
cargo run --release --features bench_tools --bin bench_atomic_updates -- \
  --machine-label local
```

`bench_atomic_updates` models UBQ's incremental traversal across block sizes
`31,127,511,2047,4095` by default. The updater reserving the final slot
publishes the next block while threads observing the full block snooze. It
compares FAA, CAS, and CAS with `crossbeam_utils::Backoff::spin()` for both a
plain `AtomicU64` and an experimental shared allocation encoded as a 64-bit
synthetic block pointer plus a 32-bit generation and 32-bit index. A seventh,
U64-only case models Crossbeam `SegQueue`'s reservation mechanism: indices are
shifted by one metadata bit, advanced with a CAS and spin backoff, and skip a
sentinel position between block laps. Mixed-layout RMWs use the low
`AtomicU64` view. Each worker initially loads the full `AtomicU128`, caches its
pointer and generation, then reloads the full value only when a narrow load or
failed CAS reports a different generation. Boundary publication remains a full
`AtomicU128` store.

FAA additionally records reservations invalidated by a full block. Workers are
pinned where supported; CAS retries, boundary waits, and wide loads/stores are
recorded; case and block order rotate between repeats; and updater counts are
every power of two through detected available parallelism. Use
`--block-sizes` and `--alignment` to change the sweep. The default `ubq`
ordering profile uses UBQ's current Acquire FAA, SeqCst CAS, Acquire
load/failure, and Release publication orderings; pass `--ordering relaxed` to
isolate raw atomic costs. Results are written under
`bench_results/runs/<machine>/atomic_updates`.

The mixed-width layout is a hardware experiment, not a Rust-memory-model-safe
implementation: Rust does not support concurrent overlapping atomics of
different sizes. The benchmark refuses targets where `AtomicU128` is not
lock-free and records this limitation in its JSON metadata.

To isolate the retry-time role of a low-word token, run the focused head reload
experiment:

```bash
cargo run --release --features bench_tools --bin bench_head_reload -- \
  --machine-label local
python3 scripts/plot_head_reload.py \
  --runs-dir bench_results/runs/local/head_reload \
  --out-dir bench_results/plots
```

This models `pop_batch`'s reservation CAS with a 64-bit pointer plus a low word
split into a 16-bit token and 48-bit index. Both strategies acquire the full
head once per reservation. After a failed low-word CAS, `always_wide` acquires
the full head again; `token_gated` reuses the returned low word while its token
matches. The synthetic pointer and token deliberately remain fixed, measuring
the steady-frontier upper bound of the optimization. The binary sweeps powers
of two thread counts and counter increments (batch sizes), records CAS failures
and wide-load rates, and rotates strategy order across paired repeats. The
plotter emits a ratio grid where purple favors token gating, blue favors
unconditional wide reloads, and white is equal.

Generate a light-to-dark single-hue block-size graph for every layout/method,
a comparison graph using each type's normalized best block weighted by updater
count, its selected CAS-retry graph, and CSVs containing all samples and
selected winners with:

```bash
python3 scripts/plot_atomic_updates.py \
  --runs-dir bench_results/runs \
  --out-dir bench_results/plots
```

To plot only selected runs, pass their JSON paths separately or as one
comma-separated argument:

```bash
python3 scripts/plot_atomic_updates.py \
  /path/to/file1.json,/path/to/file2.json \
  --out-dir bench_results/plots
```

When `--runs-dir` is also supplied, selected files can be written as basenames;
the directory is searched recursively and the discovered runs are filtered by
those names:

```bash
python3 scripts/plot_atomic_updates.py \
  file1.json,file2.json \
  --runs-dir /path/to/runs \
  --out-dir bench_results/plots
```

Omit the filenames to plot every JSON run discovered under `--runs-dir`.

Python is only needed for the plotting helpers.

Per-scenario CSVs retain every measured configuration, while plots display one
best variation per queue family. Scalar and batched SegQueue and UBQ are
separate families. When both method kinds are present, each scenario also gets
`*_scalar` and `*_batched` CSVs and bar graphs so scalar queue operations are
not compared in the same panel as native batch operations. The original
combined artifact remains available for compatibility.
Single-scenario plots maximize throughput and minimize elapsed/latency/fairness
metrics. Scaling plots choose each family's representative using normalized
relative performance weighted toward higher-contention scenarios. A green
badge reports that the declared grid is exhausted; a red badge reports
incomplete or legacy coverage while the plotted winners remain the best
measurements present.

The MPSC and SPMC throughput outputs also include `*_batchcomp` plots. These
show the best scalar-compatible configuration, the scalar counterpart of the
best batched configuration, and every measured batch size for that underlying
UBQ configuration. Duplicate scalar configurations are shown only once. Cool
shades group batches below the selected winner, warm shades group larger
batches, the winning batch uses a green star, the best scalar is black, and a
distinct scalar counterpart is dashed gray.

LUBQ also gets dedicated speedup grids rather than appearing only as one line
in the general queue-family plots. The scalar grid compares scalar LUBQ with
each available external baseline; the batched grid compares the best measured
LUBQ batch size only with baselines that have native batch measurements. This
keeps architectural gains separate from the gain due merely to batching.

Set up a minimal plotting environment:

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements-plot.txt
```

Generate plots manually (PNG + CSV when `matplotlib` is installed, CSV-only otherwise):

```bash
./.venv/bin/python scripts/plot_bench.py --out-dir bench_results/plots path/to/run.json

# Optional: choose non-default error bars (default: SEM).
./.venv/bin/python scripts/plot_bench.py --error-bars stddev --out-dir bench_results/plots path/to/run.json

# Render plots from all JSON files under bench_results/runs recursively.
./.venv/bin/python scripts/plot_runs_folder.py --runs-dir bench_results/runs --out-dir bench_results/plots

# Render PNGs from existing generated CSV machine folders.
./.venv/bin/python scripts/plot_bench.py \
  --csv-dir bench_results/plots/grace/csv \
  --csv-dir bench_results/plots/hebrides/csv \
  --csv-dir bench_results/plots/mn5/csv \
  --out-dir bench_results/plots
```

Outputs are grouped by `meta.machine_label` and mode, e.g.:

- `bench_results/plots/local/throughput/1p1c_throughput.png`
- `bench_results/plots/local/throughput/1p1c_throughput_scalar.png`
- `bench_results/plots/local/throughput/1p1c_throughput_batched.png`
- `bench_results/plots/local/throughput/scenarios_line_throughput.png`
- `bench_results/plots/local/throughput_enqueue_ceiling/1p1c_enqueue_ceiling.png`
- `bench_results/plots/local/throughput_dequeue_ceiling/1p1c_dequeue_ceiling.png`
- `bench_results/plots/lab/throughput/1p1c_throughput.png`
- `bench_results/plots/hebrides/csv/throughput/1p1c_throughput.csv`
- `bench_results/plots/hebrides/csv/throughput/scenarios_line_throughput.csv`
- `bench_results/plots/hebrides/csv/throughput/queue_metadata.csv`
- `bench_results/plots/hebrides/csv/throughput/pool_size_matched_throughput.csv`
- `bench_results/plots/hebrides/csv/throughput/pool_size_effect_throughput.csv`
- `bench_results/plots/hebrides/throughput/pool_size_effect_throughput.png`
- `bench_results/plots/hebrides/throughput/lubq_speedup_grid_throughput_scalar.png`
- `bench_results/plots/hebrides/throughput/lubq_speedup_grid_throughput_batched.png`
- `bench_results/plots/hebrides/csv/throughput/lubq_speedup_grid_throughput_scalar.csv`
- `bench_results/plots/hebrides/csv/throughput/lubq_speedup_grid_throughput_batched.csv`
- `bench_results/plots/grace/throughput/mpsc_line_throughput.png`
- `bench_results/plots/grace/throughput/mpsc_line_throughput_batchcomp.png`
- `bench_results/plots/grace/throughput/spmc_line_throughput_batchcomp.png`

Schema-v7 plotting deduplicates reruns, summarizes repeats by median, and
emits separate handoff, enqueue-ceiling, and dequeue-ceiling CSVs/plots.
Fewer than three repeats are marked provisional.

Pool-size CSVs and heatmaps are emitted only when plotting historical result
sets that contain the former static-UBQ pool sweep. New static-UBQ runs contain
only the compatibility value `pool=1`.

Per-scenario UBQ outputs also emit a companion CSV named
`<scenario>_immediate_variants_throughput.csv` that marks each required
winner-adjacent block and backoff variant as `present` or `missing`. Historical
pool-sweep result sets also include the matching `pool=0` comparison.

`bench_matrix` and `bench_grid` are the sole comparative harness. The separate
`push_batch` microbenchmark remains available for its focused API measurement.

## License

MIT
