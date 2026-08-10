# UBQ

[![Crates.io](https://img.shields.io/crates/v/ubq.svg)](https://crates.io/crates/ubq)
[![Docs.rs](https://docs.rs/ubq/badge.svg)](https://docs.rs/ubq)

UBQ is a **lock-free, unbounded, multi-producer/multi-consumer (MPMC) queue**
built from a linked ring of fixed-size blocks, intended for concurrent producers
and consumers.

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

UBQ supports `no_std` targets that provide heap allocation and native 8-bit
and pointer-width atomics:

```toml
[dependencies]
ubq = { version = "5", default-features = false }
```

The final application must install a global allocator. UBQ remains unbounded,
so a push may allocate a new aligned block; applications with a fixed memory
budget must enforce their own queue-depth limit. In `no_std` builds the built-in
backoff policies spin instead of yielding to an operating-system scheduler.

## How it works

TODO

## Benchmarks

This repo includes a benchmark harness that compares static UBQ, the opt-in
experimental dynamic implementation (`dubq`), and established
MPMC queue implementations (`segqueue`, `concurrent-queue`, and optional
RBBQ/BBQ, `lfqueue`/LSCQ, and wCQ variants). Unless `--scenarios` is supplied,
each machine benchmarks the complete power-of-two producer/consumer grid:
`2^n p 2^m c` for every `n,m >= 0` whose producer and consumer thread sum does
not exceed detected available parallelism. For example, a 16-thread machine
runs all 16 combinations of `1,2,4,8` producers and consumers.

The Rust benchmark harness and binaries are isolated behind the `bench_tools`
feature. Benchmark-specific features such as `bench_registry`, `bench_rbbq`,
`bench_lfqueue`, and `bench_wcq` enable it automatically.

The schema-v6 comparative harness has two front ends:

- `bench_matrix`: direct matrix execution. It dispatches through the
  precompiled benchmark registry and writes schema-v6 JSON files under
  `bench_results/runs`.
- `bench_grid`: reproducible UBQ/DUBQ grid execution. Its default sparse grid is
  `pool=[0,1,8,64]` × `block=[31,127,511,2047,4095]` × both backoffs (40
  configurations). `-d` selects the dense grid containing all 8 pool values ×
  all 8 block values × both backoffs (128 configurations). Configurations whose
  block is smaller than a scenario's producer count are excluded before jobs
  are counted for static UBQ. DUBQ interprets this dimension as a minimum block
  size and retains every grid point. Sparse and dense select configuration
  coverage only; both run the same complete scenario grid.

For throughput, every selected UBQ or DUBQ configuration measures a
scalar-compatible operation and, by default, paired `push_batch`/`pop_batch`
operations at batch sizes `8,32,256`. When `segqueue` is selected, its normal
`SegQueue::push`/`pop` run remains scalar and the same batch-size grid is run
through the fork's separate `BatchQueue::push`/`pop` API. `--batch-sizes`
replaces that shared batch-size list while retaining scalar measurements. DUBQ
has a batch-only API, so its scalar-compatible variant uses one-item batches
and is recorded without a batch size. Thus the static-UBQ default grid
has 160 sparse or 512 dense UBQ throughput jobs per unconstrained scenario and
repeat. Scalar baselines are measured once rather than once per UBQ
configuration, while the Crossbeam batch queue is measured once per requested
batch size. Other benchmark modes remain scalar.

For workload-specific modes, when `--items-per-producer` is omitted,
`bench_grid` uses the versioned
`scenario_scaled_v1` workload: 1–8 producers get 1,000,000 items each, 9–16
get 250,000, 17–32 get 62,500, and larger producer counts get 15,625. Every
queue, UBQ/DUBQ configuration, batch size, mode, and repeat in a scenario receives
the same resolved count. Supplying one or more `--items-per-producer` values
selects the `explicit` policy and runs every supplied value in every scenario.
The selected policy and scenario mapping are printed before execution and
recorded in each output file.

`bench_grid` reuses successful schema-v6 samples with a compatible measurement
fingerprint by default. Scenario, queue/configuration, mode, batch-size, and
repeat selection do not change that fingerprint; the exact sample key still
has to match, so narrower and wider plans reuse their overlap. It writes each
completed job through an atomic checkpoint, and retries failed or timed-out
jobs after an interruption. `--rerun` ignores existing samples. Jobs execute
sequentially on the same core range so separate queue measurements cannot
contend with one another. Within each job, producer and consumer threads are
interleaved over the assigned core IDs until one role is exhausted; the actual
role-to-core map is printed before execution and recorded as
`core_placement = "interleaved"`. Authoritative throughput requires every
worker thread to pin successfully. `--core-ids 0-7,16-23` selects an explicit
ordered CPU set; `--allow-unpinned` is a diagnostic escape hatch whose records
are excluded from winner claims. Hard timeouts are derived only from the
declared measurement budget: at least 30 seconds and otherwise five times the
warmup plus three measured phases. `--job-timeout-secs` overrides that value.
Each job runs in a reusable worker process; if it exceeds its hard
timeout, the parent kills and reaps the entire worker, checkpoints a timed-out
sample, starts a fresh worker, and continues. Schema-v5 and older results are
not reused or aggregated with schema-v6 data. Stdout is a fixed-width
job table with the queue, scenario, mode, batch size, thread use, pending count,
and percentage of the complete plan; each row advances from `Pending...` to
`Pending...DONE`.

`bench_matrix` uses the same overlap rule when `--reuse-existing` is supplied.

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

UBQ labels are 4-part identifiers:

- `preset,pool,block,backoff`
- Example: `balanced,8,127,crossbeam`

DUBQ labels are 3-part runtime configurations:

- `pool,min_block,backoff`
- Example: `8,127,crossbeam`

Select them directly with `--queues dubq --dubq-label 8,127,crossbeam`, or add
`dubq` to `bench_grid --queues` to sweep the selected sparse/dense grid. DUBQ is
opt-in and does not change either binary's default queue selection. Its current
mixed-width atomic head accesses are an experimental hardware design outside
Rust's supported overlapping-atomic memory model; results should be treated as
experimental rather than production-safety evidence.

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
  --queues ubq,dubq,segqueue,concurrent-queue,rbbq,lfqueue,wcq \
  --ubq-label balanced,8,127,crossbeam \
  --dubq-label 8,127,crossbeam \
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
  --ubq-label balanced,8,127,crossbeam \
  --scenarios 1p1c,4p1c,1p4c,4p4c,8p8c,16p16c \
  --modes app_log_fan_in,app_pipeline,app_task_roundtrip \
  --items-per-producer 100000 \
  --repeats 3
```

Run the sparse grid on one machine:

```bash
cargo run --release --features bench_registry,bench_rbbq,bench_lfqueue,bench_wcq --bin bench_grid -- \
  --machine-label local \
  --queues ubq,dubq,segqueue,concurrent-queue,rbbq,lfqueue,wcq \
  --batch-sizes 8,32,128,512 \
  --rbbq-block-sizes 64,256,1024,4096 \
  --lfqueue-segment-sizes 32,256,1024 \
  --wcq-capacities 4096,65536,1048576
```

Add `-d` for the dense grid or `--rerun` to benchmark every job without using
compatible existing results. Batch sizes must be integers of at least 2;
duplicates are removed. The scalar-compatible variant is always included.

To benchmark only scalar SegQueue and the forked BatchQueue—without scheduling
any UBQ/DUBQ configurations—select `segqueue` by itself:

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
best variation per queue family. Scalar and batched SegQueue, UBQ, and DUBQ are
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

The MPSC and SPMC throughput outputs also include `*_batchcomp` and
`*_dubq_batchcomp` plots. These show the best scalar-compatible configuration,
the scalar counterpart of the best batched configuration, and every measured
batch size for that underlying UBQ or DUBQ configuration. Duplicate scalar
configurations are shown only once. Cool
shades group batches below the selected winner, warm shades group larger
batches, the winning batch uses a green star, the best scalar is black, and a
distinct scalar counterpart is dashed gray.

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
- `bench_results/plots/grace/throughput/mpsc_line_throughput.png`
- `bench_results/plots/grace/throughput/mpsc_line_throughput_batchcomp.png`
- `bench_results/plots/grace/throughput/spmc_line_throughput_batchcomp.png`

Schema-v6 plotting deduplicates reruns, keeps fingerprints isolated, summarizes
repeats by median, and emits separate handoff, enqueue-ceiling, and
dequeue-ceiling CSVs/plots. Fewer than three repeats are marked provisional.

Per-scenario UBQ outputs also emit a companion CSV named
`<scenario>_immediate_variants_throughput.csv` that marks each required
winner-adjacent variant, including the matching `pool=0` no-pool comparison, as
`present` or `missing`.

`bench_matrix` and `bench_grid` are the sole comparative harness. The separate
`push_batch` microbenchmark remains available for its focused API measurement.

## License

MIT
