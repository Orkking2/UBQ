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
- **Clonable handles** — every `UBQ<T>` clone shares the same underlying ring;
  reference counting frees the ring when the last clone is dropped.
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
use std::thread;

let q: UBQ<u64> = UBQ::new();

// Spawn 4 producers and 4 consumers.
let m = 100_000;
let handles: Vec<_> = (0..4)
    .flat_map(|_| {
        let pq = q.clone();
        let cq = q.clone();
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

UBQ organises elements into fixed-size *blocks* linked in a circular ring.
Two atomic head pointers — **phead** and **chead** — track which block is
currently accepting pushes and which is being drained by consumers.

Within each block, a pair of packed counters distinguishes *claimed* from
*committed* slots. A consumer spin-waits briefly on a *stability predicate*
before claiming a slot, ensuring it reads only fully-committed writes. When a
block is exhausted the last claimer atomically advances the head pointer to the
next block, recycling the old one once it has been fully consumed.

The correctness of the protocol rests on six invariants (`[C1]`–`[C6]`)
documented inline in the source.

## Benchmarks

This repo includes a benchmark harness that compares UBQ against other unbounded
queues in SPSC, MPSC, SPMC, and MPMC scenarios. Results are emitted as JSON; an
optional helper script generates throughput bar plots.

Run the default benchmark suite (release mode):

```bash
cargo bench --bench ubq_bench -- --out bench_results/ubq_default.json
```

Limit to specific queues or scenarios:

```bash
cargo bench --bench ubq_bench -- --queues=ubq,crossbeam --scenarios=spsc,mpmc
```

Generate plots (PNG) and CSVs:

```bash
python3 scripts/plot_bench.py bench_results/ubq_default.json
```

### Block size variants

UBQ's block size is controlled at compile time via feature flags. To layer
multiple block sizes on the same plot:

```bash
# Base comparison (all queues, default block size).
cargo bench --bench ubq_bench -- --out bench_results/ubq_default.json

# UBQ-only runs with alternative block sizes.
cargo bench --bench ubq_bench --features bench_small  -- --only-ubq --out bench_results/ubq_block8.json
cargo bench --bench ubq_bench --features bench_large  -- --only-ubq --out bench_results/ubq_block128.json

# Combined plot with all three sizes overlaid.
python3 scripts/plot_bench.py \
  bench_results/ubq_default.json \
  bench_results/ubq_block8.json \
  bench_results/ubq_block128.json
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
