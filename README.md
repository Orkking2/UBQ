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

### Experimental `dynblock`

`ubq::dynblock::UBQ` combines UBQ's packed-head batch reservations with small,
aligned virtual blocks and per-vblock atomic publication/completion counters.
Each vblock points to its larger allocation; the last completed vblock retires
that allocation for epoch reclamation. Geometry defaults to 32 slots per
vblock and 32 vblocks per allocation and is configurable through
`ubq::dynblock::Config`.

The batch API accepts owned buffers, as v8 does. Benchmark it with
`ubq-dynblock` or a geometry selector such as `ubq-dynblock-s32-v16`.
See [the design and checks](src/dynblock/DESIGN.md) for the reservation,
publication, and reclamation rules and the experiment's progress limitations.

### Experimental `bitmask`

`ubq::bitmask::UBQ` keeps v8's reservations and block chain, but every
VBlock holds exactly 64 slots and publishes through two 64-bit atomic masks:
a producer sets the bits of the slots it filled with one `fetch_or`, a
consumer reads each slot as soon as its bit is set and completes with one
`fetch_or`, and no producer index is read to tell an initialized slot from
a hole. Geometry is `ubq::bitmask::Config` with 64 slots per VBlock and 2048
VBlocks per block by default (`ubq-bitmask-s64-v<VBlocks>` in the harness).

`bitmask` also uses an attach-all freelist: the producer index counts the
blocks attached after the producer's block, so a reservation that fits in
that runway never touches the freelist; the producer crossing a block
boundary attaches everything the freelist holds; and the freelist is either
empty or a chain taken whole, so no block is freed while the queue lives.
See [src/bitmask/DESIGN.md](src/bitmask/DESIGN.md);
`lab/studies/bitmask-vs-ubq.json` compares it with v8 and the original UBQ.

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

Queue performance work runs through the lab, `./ab`, documented in
[lab/README.md](lab/README.md). A study compares queue modules or git revisions
in two stages: paired, exact-once-checked throughput runs
first, then perf and Arm Performix captures of each candidate's biggest wins
and losses, on BSC's Grace and MareNostrum 5 nodes or on this machine.

```sh
./ab new my-change --src src/v8/        # copy v8 to src/my-change and register it
# Edit src/my-change/, leaving src/v8/ as the control.
./ab launch my-change --local --quick   # smoke test on this machine
./ab launch my-change                   # Grace and MN5; cancels the previous study there
./ab status --watch                     # then fetches and reports automatically
```

`./ab queues` lists registered implementations. Use `--src src/` to fork the
original UBQ, or another variant's directory to fork that implementation.
`./ab launch v8-vs-ubq --local --quick` compares the existing v8 and original
UBQ through this same workflow. Without `--src`, `new` creates a revision
comparison (control `HEAD`, candidate `WORKTREE`).

Every lab measurement runs `bench_hardware`, which reads one JSON workload
and prints one JSON result: persistent pinned workers, fixed-size
empty-to-empty rounds, optional exact-once verification, and memory
lifecycle checkpoints. It drives UBQ, UBQ v8 (`ubq-v8-s<slots>[-v<vblocks>]`),
LUBQ, SegQueue and its BatchQueue, concurrent-queue, MS-Queue,
Mutex+VecDeque, moodycamel::CQ (`bench_moodycamel`), RBBQ (`bench_rbbq`) and
LSCQ (`bench_lfqueue`).

Focused microbenchmarks stay separate: `bench_atomic_updates`,
`bench_head_reload`, `io_uring_queue_bench`
([notes](docs/io_uring_queue_benchmarks.md)), the Disruptor JNI comparison
([notes](docs/disruptor_benchmarks.md)) and the `push_batch` Criterion
bench. The harness and all benchmark binaries sit behind the `bench_tools`
feature, which every benchmark-specific feature enables.

## License

MIT
