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
ubq = "2"
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

## How it works

TODO

## Benchmarks

This repo includes a benchmark harness that compares UBQ against established
MPMC queue implementations (`segqueue`, `concurrent-queue`, and optional
RBBQ/BBQ, `lfqueue`/LSCQ, and wCQ variants) in
`1p1c`, `4p1c`, `1p4c`, `4p4c`, `8p1c`, `8p4c`, `8p8c`, `1p8c`, `4p8c`,
`16p1c`, `1p16c`, `8p16c`, `16p8c`, `16p16c`, `32p1c`, `1p32c`, `16p32c`,
`32p16c`, `32p32c`, `64p1c`, `1p64c`, `32p64c`, `64p32c`, and `64p64c`
scenarios. The v2 harness has two layers:

- `bench_matrix`: direct matrix execution. It dispatches through the
  precompiled benchmark registry and writes v2 JSON files under
  `bench_results/runs`.
- `bench_frontier`: higher-level frontier search. It inspects existing v2 runs,
  expands the UBQ search graph scenario-by-scenario, and submits missing work to
  `bench_matrix`. RBBQ/BBQ uses a fixed block-size grid rather than adaptive
  frontier expansion.
  A run is `frontier-complete` when no pending frontier bundles remain; the
  frontier expands around the best fully-covered UBQ label per
  scenario/metric, including the matching `pool=0` no-pool variant, while
  propagating baseline-beating fully-covered winners across scenarios.

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

Run the frontier search on one machine:

```bash
cargo run --release --features bench_registry,bench_rbbq,bench_lfqueue,bench_wcq --bin bench_frontier -- \
  --machine-label local \
  --queues ubq,segqueue,concurrent-queue,rbbq,lfqueue,wcq \
  --seed-label balanced,8,127,crossbeam \
  --rbbq-block-sizes 64,256,1024,4096 \
  --lfqueue-segment-sizes 32,256,1024 \
  --wcq-capacities 4096,65536,1048576
```

Run the configured fleet search:

```bash
cargo run --release --bin full_bench_fleet -- \
  --machines local,lab,hebrides \
  --repeats 3
```

`full_bench_fleet` now runs `bench_frontier` per machine, syncs
`bench_results/runs`, and refreshes plots under `bench_results/plots`.
`--repeats` overrides the `defaults.repeats` value from `bench_fleet.toml`.
Python is only needed for the plotting helpers.

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

When records contain the newer timing fields, the plotter also emits derived
metric folders such as `throughput_push_elapsed`,
`throughput_pop_elapsed`, `fill_drain_fill_elapsed`, and
`fill_drain_drain_elapsed`. Timing, latency, and fairness-ratio charts sort
lower values first; throughput charts still sort higher values first.

Per-scenario UBQ outputs also emit a companion CSV named
`<scenario>_immediate_variants_throughput.csv` that marks each required
winner-adjacent variant, including the matching `pool=0` no-pool comparison, as
`present` or `missing`.

### UBQ label variants

UBQ variant knobs are compile-time feature flags:
- version: `ubq_v3`, `ubq_v4`, `ubq_v5`, `ubq_v6`, `ubq_v7`
- pool size: `ubq_pool_1`, `ubq_pool_2`, `ubq_pool_4`, `ubq_pool_8`, `ubq_pool_16`, `ubq_pool_32`, `ubq_pool_64` (`ubq_v6` is no-pool)
- block length: `ubq_block_31`, `ubq_block_63`, `ubq_block_127`, `ubq_block_255`, `ubq_block_511`, `ubq_block_1023`, `ubq_block_2047`, `ubq_block_4095`
- backoff mode: default crossbeam backoff, or `ubq_backoff_cq` (label suffix `,b`)

```bash
# Example: v5,8,1023
cargo bench --bench ubq_bench --features ubq_v5,ubq_pool_8,ubq_block_1023 -- \
  --ubq-label v5,8,1023 \
  --machine-label local \
  --out bench_results/ubq_v5_8_1023.json

# Example with concurrency-queue-style backoff: v5,8,1023,b
cargo bench --bench ubq_bench --features ubq_v5,ubq_pool_8,ubq_block_1023,ubq_backoff_cq -- \
  --ubq-label v5,8,1023,b \
  --machine-label local \
  --out bench_results/ubq_v5_8_1023_b.json
```

## Loom model checking

UBQ includes opt-in [loom](https://crates.io/crates/loom) tests for
deterministic interleaving exploration of high-contention block-boundary
scenarios:

```bash
LOOM_MAX_PREEMPTIONS=3 cargo test --features loom --test loom_ubq
```

By default, the scenario runner caps model exploration at 200 permutations for
practical runtime; override with `LOOM_MAX_PERMUTATIONS`.

## License

MIT
