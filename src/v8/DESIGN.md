# UBQ v8 implementation notes

`ubq::v8::UBQ` is an experimental queue under development. The package remains
7.0.0 and `ubq::UBQ` remains the existing default queue. v8's runtime tests may
fail, time out, or crash; correctness and lock-free progress are not established.

## Source layout

- `mod.rs`: module exports and the public buffer API example.
- `queue.rs`: configuration, packed heads, reservation, publication waits,
  payload copies, and consumer completion.
- `block.rs`: runtime allocation geometry, VBlock views, owned blocks, and
  intrusive block lists.
- `freelist.rs`: queue-local cached allocations and the busy-token protocol.
- `tests.rs`: geometry validation, allocation metadata, and public teardown checks.
- `tests/freelist.rs`: private freelist tests, included beneath `freelist`.
- `tests/block.rs`: block-list ownership-transfer regression tests.
- `tests/drop.rs`: settled-state teardown fixtures, included beneath `queue`.
- `tests/v8_viability.rs` at the repository root: the integration target for
  the current buffer API. Its shared workloads and
  reference queue live in the repository's `tests/support/` directory.

The obsolete spin-lock scaffold, commented implementations, private reservation
helpers, and duplicate old-API integration tests have been removed. They do not
describe the current algorithm. The commented-out geometry benchmark example
was also removed; benchmarking belongs after functional viability.

## Geometry and ownership

`Config` uses runtime slot counts: `slots_per_vblock` and `vblocks_per_block`.
Defaults are 4096 slots per VBlock and 32 VBlocks per allocation, not a claim
about the best configuration or a page-sized layout. Each allocation contains
its block header followed by VBlock headers and aligned payload regions.
VBlocks are views into the same allocation. Copies stop at VBlock boundaries.

`OwnedBlock` owns a detached allocation. `BlockList` owns an intrusive chain.
The freelist can be empty, contain an available chain, or contain the busy
address token. Taking blocks first acquires exclusive ownership of the chain;
a return observing busy frees its detached block. Queue geometry is fixed for
its lifetime, so a cached block is reused with the same layout.

`push_batch(Vec<T>)` transfers the values and returns the emptied input buffer.
`pop_batch(Box<[MaybeUninit<T>]>)` returns a Vec backed by the output buffer.
`push` and `pop` allocate their temporary one-element buffers. Borrowed-slice
and append-to-Vec convenience operations from the old scaffold are not present.

## Reservation and publication

Each head has a separate block pointer and packed index. The index uses one
low flag bit; the remaining bits identify a reservation position. A sentinel
VBlock separates physical blocks. Both heads use the sentinel-inclusive
`(vblocks_per_block + 1) * slots_per_vblock` stride when decoding tokens. The
index/pointer transition and eventual index wrap still need protocol review;
separate atomics alone do not provide a coherent snapshot.

The producer reserves, copies each segment, and adds to its VBlock's `produced`
counter. A consumer checks that counter before copying its claimed segment:

- `produced == slots_per_vblock` permits reading the fully published VBlock.
- Otherwise, it reads the producer index after the counter, requires the index
  to refer to this same VBlock, and requires the publication total to equal
  the producer's reserved offset and cover the requested segment.
- If neither condition holds, it snoozes and repeats.

A publication total by itself is not an initialized prefix: a later producer
can publish while an earlier producer is still copying. The comparison with
reservation state is therefore part of the intended protocol, not optional.

Consumers copy before incrementing `consumed`; the full-VBlock transition
increments `consumed_vblocks`. The final full-block completion returns that
block to the freelist. The lifetime argument for stale head readers and address
reuse still needs review. At teardown, exclusive access excludes in-flight
operations. `UBQ::drop` detaches the consumer's remaining chain, clears the
producer alias, and destroys each block. The block destructor drops each
VBlock's unread `[consumed, produced)` suffix; the freelist's destructor frees
cached allocations separately. This relies on completed queue operations
leaving a valid active chain, disjoint from the freelist.

## Running checks

```sh
cargo test --no-run
cargo test
cargo test --lib v8
cargo test --test v8_viability
cargo test --doc v8
cargo check --no-default-features --lib
```

The integration tests isolate queue workloads in child processes with a
20-second deadline, so known hangs and crashes are reported as failures. Tests
remain enabled while the algorithm is incomplete. The doctest uses the current
buffer signature and may fail its assertions until the queue is corrected.

For repeatable debug/release acceptance runs and JSON diagnostics:

```sh
python3 scripts/test_v8_viability.py
python3 scripts/test_v8_viability.py --reference
```

See [the correctness milestone](../../docs/v8-viability.md) for coverage,
reference-only validation, configurable deadlines, and the remaining algorithm
decisions. Passing these workloads is not a proof of memory safety or progress.
