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

This repo includes a benchmark harness that compares UBQ against established
MPMC queue implementations (`segqueue`, `concurrent-queue`, and optional
RBBQ/BBQ, `lfqueue`/LSCQ, and wCQ variants) in
`1p1c`, `4p1c`, `1p4c`, `4p4c`, `8p1c`, `8p4c`, `8p8c`, `1p8c`, `4p8c`,
`16p1c`, `1p16c`, `8p16c`, `16p8c`, `16p16c`, `32p1c`, `1p32c`, `16p32c`,
`32p16c`, `32p32c`, `64p1c`, `1p64c`, `32p64c`, `64p32c`, and `64p64c`
scenarios.

The Rust benchmark harness and binaries are isolated behind the `bench_tools`
feature. Benchmark-specific features such as `bench_registry`, `bench_rbbq`,
`bench_lfqueue`, and `bench_wcq` enable it automatically.

The schema-v3 harness has two execution paths:

- `bench_matrix`: direct matrix execution. It dispatches through the
  precompiled benchmark registry and writes schema-v3 JSON files under
  `bench_results/runs`.
- `bench_grid`: reproducible UBQ grid execution. Its default sparse grid is
  `pool=[0,1,8,64]` × `block=[31,127,511,2047,4095]` × both backoffs (40
  configurations). `-d` selects the dense grid containing all 8 pool values ×
  all 8 block values × both backoffs (128 configurations). Configurations whose
  block is smaller than a scenario's producer count are excluded before jobs
  are counted.

For throughput, every UBQ configuration measures scalar `push` and
`push_batch` at batch sizes `2,4,8,16,32,64,128,256,512,1024,2048`. Thus an
unconstrained scenario has 480 sparse or 1,536 dense UBQ throughput jobs per
repeat; selected baseline queues are measured once rather than once per UBQ
configuration. Other benchmark modes remain scalar.

`bench_grid` reuses successful schema-v3 samples by default, writes each
completed job through an atomic checkpoint, and retries failed or timed-out
jobs after an interruption. `--rerun` ignores existing samples. Its stdout is a
fixed-width job table with the queue, scenario, mode, batch size, thread use,
pending count, and percentage of the complete plan.

UBQ labels are 4-part identifiers:

- `preset,pool,block,backoff`
- Example: `balanced,8,127,crossbeam`

Publication-backed baseline labels are emitted with their sizing knob:

- RBBQ/BBQ: `fastfifo_<block_size>`, for example `fastfifo_256`
  (default grid `64,256,1024,4096`).
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
  --ubq-label balanced,8,127,crossbeam \
  --rbbq-block-sizes 64,256,1024,4096 \
  --lfqueue-segment-sizes 32,256,1024 \
  --wcq-capacities 4096,65536,1048576 \
  --scenarios 1p1c,8p8c \
  --modes throughput,fill_drain \
  --items-per-producer 1000000
```

For BBQ ATC 2022-style microbenchmarks, the scenario parser also accepts
`spsc`, `mpsc:N-M`, `spmc:N-M`, `mpmc:N-M`, `bbq-atc22-x86-88t`, and
`bbq-atc22-oversub-x86-12t`. The paper-style metric modes are
`throughput`, `complex_throughput`, `data_latency`, and `fairness`. See
[docs/bbq_atc22_reproduction.md](docs/bbq_atc22_reproduction.md) and
`bench_fleet_bbq_atc22.toml` for the ready-to-run suite.

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
  --queues ubq,segqueue,concurrent-queue,rbbq,lfqueue,wcq \
  --rbbq-block-sizes 64,256,1024,4096 \
  --lfqueue-segment-sizes 32,256,1024 \
  --wcq-capacities 4096,65536,1048576
```

Add `-d` for the dense grid or `--rerun` to benchmark every job without using
compatible existing results.

Run the configured fleet grid:

```bash
cargo run --release --features bench_tools --bin full_bench_fleet -- \
  --machines local,lab,hebrides \
  --repeats 3
```

`full_bench_fleet` runs `bench_grid` per machine with the sparse grid by
default. Its own `-d` flag selects the dense grid, and `--rerun` is forwarded
to each machine. The typed TOML alternative is `ubq_grid = "dense"` under
`[defaults]`. It syncs
`bench_results/runs`, and refreshes plots under `bench_results/plots`.
`--repeats` overrides the `defaults.repeats` value from `bench_fleet.toml`.
Python is only needed for the plotting helpers.

Throughput plots retain every measured configuration and batch size in their
per-scenario CSV while displaying the best scalar UBQ configuration and the
best configuration/batch-size pair as distinct series. A green badge reports
that the declared grid is exhausted; a red badge reports incomplete or legacy
coverage while the plotted winners remain the best measurements present.

Set up a minimal plotting environment:

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements-plot.txt
```

Generate plots manually (PNG + CSV when `matplotlib` is installed, CSV-only otherwise):

```bash
./.venv/bin/python scripts/plot_bench.py --out-dir bench_results/plots path/to/run.json

# Optional: choose error bars from repeated samples (default: sem).
./.venv/bin/python scripts/plot_bench.py --error-bars stddev --out-dir bench_results/plots path/to/run.json

# Render plots from all JSON files under bench_results/runs recursively.
./.venv/bin/python scripts/plot_runs_folder.py --runs-dir bench_results/runs --out-dir bench_results/plots

# Render PNGs from existing generated CSV machine folders and emit merged paper figures.
./.venv/bin/python scripts/plot_bench.py \
  --csv-dir bench_results/plots/grace/csv \
  --csv-dir bench_results/plots/hebrides/csv \
  --csv-dir bench_results/plots/mn5/csv \
  --out-dir bench_results/plots

# Optional: cap how many configs appear in the per-machine scaling line chart.
./.venv/bin/python scripts/plot_runs_folder.py --runs-dir bench_results/runs --out-dir bench_results/plots --max-line-series 10
```

Outputs are grouped by `meta.machine_label` and mode, e.g.:

- `bench_results/plots/local/throughput/1p1c_throughput.png`
- `bench_results/plots/local/throughput/scenarios_line_throughput.png`
- `bench_results/plots/local/throughput_push_elapsed/1p1c_push_elapsed.png`
- `bench_results/plots/local/fill_drain_drain_elapsed/1p1c_drain_elapsed.png`
- `bench_results/plots/lab/throughput/1p1c_throughput.png`
- `bench_results/plots/hebrides/csv/throughput/1p1c_throughput.csv`
- `bench_results/plots/hebrides/csv/throughput/scenarios_line_throughput.csv`
- `bench_results/plots/hebrides/csv/throughput/queue_metadata.csv`
- `bench_results/plots/grace/throughput/mpsc_line_throughput.png`
- `bench_results/plots/paper/mpsc_producer_throughput.png`
- `bench_results/plots/paper/mpsc_push_elapsed_log.png`

When records contain the newer timing fields, the plotter also emits derived
metric folders such as `throughput_push_elapsed`,
`throughput_pop_elapsed`, `fill_drain_fill_elapsed`, and
`fill_drain_drain_elapsed`. Timing, latency, and fairness-ratio charts sort
lower values first; throughput charts still sort higher values first.

Per-scenario UBQ outputs also emit a companion CSV named
`<scenario>_immediate_variants_throughput.csv` that marks each required
winner-adjacent variant, including the matching `pool=0` no-pool comparison, as
`present` or `missing`.

The standalone benchmark target can be inspected with:

```bash
cargo bench --bench ubq_bench --features bench_tools -- --help
```

## License

MIT
