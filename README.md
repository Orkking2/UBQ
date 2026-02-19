# UBQ

[![Crates.io](https://img.shields.io/crates/v/ubq.svg)](https://crates.io/crates/ubq)
[![Docs.rs](https://docs.rs/ubq/badge.svg)](https://docs.rs/ubq)

UBQ is a lock-free queue built from linked blocks, intended for concurrent producers and consumers.

## Usage

Add UBQ to your Cargo manifest:

```toml
ubq = "0.1"
```

A minimal example that allocates a small ring of blocks and moves a value through it:

```rust
use ubq::UBQ;

fn main() {
    let mut queue = UBQ::new();
    queue.push("hello");
    assert_eq!(queue.pop().unwrap(), "hello");
}
```

See the full API docs on [docs.rs](https://docs.rs/ubq) and the crate page on [crates.io](https://crates.io/crates/ubq).

## Benchmarks

This repo includes a custom benchmark harness that compares UBQ against other unbounded queues in SPSC, MPSC, SPMC, and MPMC scenarios. Results are emitted as JSON, and a helper script can generate four throughput bar plots (one per scenario).

Run the default benchmark suite (release mode) and write results to a file:

```bash
cargo bench --bench ubq_bench -- --out bench_results/ubq_default.json
```

Limit to a subset of queues or scenarios:

```bash
cargo bench --bench ubq_bench -- --queues=ubq,crossbeam --scenarios=spsc,mpmc
```

Generate plots (PNG) and CSVs:

```bash
python3 scripts/plot_bench.py bench_results/ubq_default.json
```

### Block size variants

UBQ’s block size can be varied via feature flags (compile-time). To layer multiple UBQ block sizes in the same plots, run the base comparison once, then add UBQ-only runs for other sizes and pass all JSON files to the plotting script:

```bash
# Base comparison (all queues, default block size).
cargo bench --bench ubq_bench -- --out bench_results/ubq_default.json

# UBQ-only runs with alternative block sizes.
cargo bench --bench ubq_bench --features bench_small -- --only-ubq --out bench_results/ubq_block8.json
cargo bench --bench ubq_bench --features bench_large -- --only-ubq --out bench_results/ubq_block128.json

# Combined plot with layered UBQ sizes.
python3 scripts/plot_bench.py \\
  bench_results/ubq_default.json \\
  bench_results/ubq_block8.json \\
  bench_results/ubq_block128.json
```

The benchmark JSON includes per-run producer/consumer counts, the measured throughput (ops/sec), and component timings (push completion, pop completion, fill/drain durations).

### Debug logging

Enable `ubq_debug` to collect thread-local logs from UBQ internals:

```bash
cargo test --features ubq_debug -- --nocapture
```

```rust
use ubq::{debug, ubq_log};

debug::set_thread_label("producer-0");
ubq_log!("start producer with batch={}", 1024);
// ... run UBQ operations ...
let entries = debug::take();
for entry in entries {
    println!(
        "[{}] tid={} {:?} {} {}",
        entry.ts_ns, entry.thread_id, entry.thread_label, entry.tag, entry.message
    );
}
```

Write the current thread’s log buffer to a directory:

```rust
debug::flush_to_dir("ubq_logs").expect("write logs");
```

For the `mpmc_integrity_smoke` test, you can set `UBQ_DEBUG_DIR=ubq_logs` to emit one log file per thread.

You can also limit log volume:

- `UBQ_DEBUG_TAGS=pop.goto_next,push.new_block` to only log tagged events with those prefixes.
- `UBQ_DEBUG_SAMPLE=1000` to log every 1000th event per thread.
- `UBQ_DEBUG_MAX=50000` to cap entries per thread.
- Reset logging uses tags `reset.attempt`, `reset.success`, and `reset.skip`.

## Loom model checking

UBQ includes opt-in loom tests for deterministic interleaving exploration of
high-contention block-boundary scenarios:

```bash
LOOM_MAX_PREEMPTIONS=3 cargo test --features loom --test loom_ubq
```

By default, the scenario runner caps model exploration at 200 permutations for
practical runtime; override with `LOOM_MAX_PERMUTATIONS`.
