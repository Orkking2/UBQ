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
unbounded MPMC queue implementations (`segqueue` and `concurrent-queue`) in
`1p1c`, `4p1c`, `1p4c`, `4p4c`, `8p1c`, `8p4c`, `8p8c`, `1p8c`, `4p8c`,
`16p1c`, `1p16c`, `8p16c`, `16p8c`, `16p16c`, `32p1c`, `1p32c`, `16p32c`,
`32p16c`, `32p32c`, `64p1c`, `1p64c`, `32p64c`, `64p32c`, and `64p64c`
scenarios. By default it runs `throughput`, `fill_drain`, and
`mutable_placeholder` modes.
Scenarios are auto-skipped when `producers + consumers` exceeds
`available_parallelism` on the host. Results are emitted as JSON; the plotting
helper generates machine/mode/scenario bar plots.

Run the default benchmark suite (release mode):

```bash
cargo bench --bench ubq_bench -- \
  --ubq-label main \
  --machine-label local \
  --out bench_results/ubq_default.json
```

Limit to specific queues or scenarios:

```bash
cargo bench --bench ubq_bench -- \
  --queues=ubq,segqueue,concurrent-queue \
  --scenarios=1p1c,8p8c
```

Run local + remote (`lab`) in one command (sync, bench, collect, and plot):

```bash
./scripts/bench_dual_host.sh --ubq-label v5,8,1023

# Same variant, but with UBQ's concurrency-queue-style backoff (`b` suffix).
./scripts/bench_dual_host.sh --ubq-label v5,8,1023,b

# Repeat each benchmark run 5 times to build sample size per bar.
./scripts/bench_dual_host.sh --ubq-label v5,8,1023 --n=5

# Run multiple UBQ labels in sequence; each label runs --n times.
./scripts/bench_dual_host.sh --ubq-labels 'v3,4,127;v5,8,2047;v7,4,1023' --n=3
```

Run the full nearest-neighbor search across the machines configured in
`bench_fleet.toml`, aggregate runs locally, and render plots once at the end:

```bash
cargo run --release --bin full_bench_fleet -- \
  --machines local,lab,hebrides
```

Search mode runs `complete_benches` once per machine (remote machines via SSH),
pulls each machine's `bench_results/runs` back to local, and then refreshes the
aggregated plots once. By default it allows incomplete per-machine scenario
sweeps; pass `--strict-complete` to fail on any incomplete scenario. Default
scenarios, remote paths, and seed fallback (`v4,8,127`) come from
`bench_fleet.toml`. Use `--complete-arg=...` to forward additional
`complete_benches` options. `complete_benches` now runs its search rounds
directly via `cargo bench`; Python is only needed for plotting helpers and the
fixed-label shell workflow.

To run a fixed set of UBQ variants instead of the search loop:

```bash
cargo run --release --bin full_bench_fleet -- \
  --machines local,lab \
  --ubq-labels 'v7,16,511;v6,0,511;v5,16,511' \
  --bench-arg --throughput-only \
  --bench-arg --scenarios=1p1c,8p8c
```

Fixed-label mode runs `scripts/bench_dual_host.sh` locally on each selected
machine, then pulls remote runs back into the local aggregated `runs` folder.

`bench_dual_host.sh` stores each UBQ label in `bench_results/runs/<ubq_label>`
and appends timestamped machine outputs (for example, `local_<run_id>.json` and
`lab_<run_id>.json`). `plot_bench.py` averages repeated samples per
machine/mode/scenario/queue and can render error bars from those samples.
By default the script keeps all labels and only normalizes to one directory per
label. Labels must match either
`v(3|4|5|7),(1|2|4|8|16|32|64),(31|63|127|255|511|1023|2047|4095)[,b]` or
`v6,0,(31|63|127|255|511|1023|2047|4095)[,b]` and map to cargo features:
`ubq_vX,ubq_pool_N,ubq_block_L[,ubq_backoff_cq]` (or
`ubq_v6,ubq_block_L[,ubq_backoff_cq]` for no-pool `v6`).
Use `--n` (or `--runs`) to repeat each label multiple times and append more
samples. To drop non-winning UBQ labels, pass `--purge-losers`. When enabled,
labels purged for being non-winners are tracked in
`bench_results/purged_ubq_labels.txt`.

Generate plots manually (PNG + CSV):

```bash
python3 scripts/plot_bench.py --out-dir bench_results/plots bench_results/ubq_default.json

# Optional: choose error bars from repeated samples (default: sem).
python3 scripts/plot_bench.py --error-bars stddev --out-dir bench_results/plots bench_results/ubq_default.json

# Render plots from all JSON files under bench_results/runs/* in one command.
python3 scripts/plot_runs_folder.py --runs-dir bench_results/runs --out-dir bench_results/plots
```

Outputs are grouped by `meta.machine_label` and mode, e.g.:

- `bench_results/plots/local/throughput/1p1c_throughput.png`
- `bench_results/plots/lab/throughput/1p1c_throughput.png`
- `bench_results/plots/hebrides/csv/throughput/1p1c_throughput.csv`

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
