# bitmask

`ubq::bitmask::UBQ` keeps v8's packed-index reservations and its block chain,
and changes two things: how a VBlock publishes its slots, and how blocks
reach the chain. It is a separate experiment; `ubq::UBQ` and `ubq::v8::UBQ`
retain their implementations.

## Source layout

- `mod.rs`: module exports and the public buffer API example.
- `queue.rs`: configuration, the two heads, reservation, bitmask publication,
  consumer completion, and the boundary transitions.
- `block.rs`: allocation geometry, VBlock views with their masks, owned blocks,
  intrusive block lists, and `mask(offset, count)`.
- `freelist.rs`: the two-state block cache.
- `tests.rs`, `tests/`: geometry, teardown, list ownership, freelist, index
  encoding, unwinding; `tests/bitmask.rs` and `tests/bitmask_allocation.rs`
  at the repository root are the integration workloads.

## Storage

A VBlock holds exactly `SLOTS` = 64 slots and two cache-padded `AtomicU64`
masks, `produced` and `consumed`; bit `i` describes slot `i`. `Config` keeps
`slots_per_vblock` so the geometry selectors shared with v8 apply
(`ubq-bitmask-s64-v<VBlocks>`), but any value other than 64 is rejected with
`ConfigError::VBlockSize`. The default is 2048 VBlocks per block, 131072
slots, v8's default block capacity. With 8-byte elements a VBlock is 768
bytes (256 of headers, 512 of payload), so a block is 1.5 MiB against v8's
1 MiB for the same slots: the price of a mask per 64 slots.

```text
one allocation
┌──────────────────┬──────────┬──────────┬────────────┬──────────┬──────────┬────────────┬─────┐
│ next,            │ produced │ consumed │ 64 T slots │ produced │ consumed │ 64 T slots │ ... │
│ consumed_vblocks │ (line)   │ (line)   │            │ (line)   │ (line)   │            │     │
└──────────────────┴──────────┴──────────┴────────────┴──────────┴──────────┴────────────┴─────┘
```

## Reservation

Reservation is v8's. Each head is a block pointer and a packed index in one
cache-padded line. Positions count slots on the sentinel-inclusive stride
`(vblocks + 1) * 64`: a reservation that reaches the block end lands on the
sentinel VBlock and gives its owner the block transition, during which every
other caller of that head snoozes.

The consumer index is `position << 1 | has_next`, as in v8. The consumer
reads the producer's position only to bound its reservation (a stale value
under-reserves, nothing more); it never uses that position to decide whether
a slot is initialized. v8 reads it as a relaxed load behind an `AcqRel`
fence (an `mfence` on x86); here it is a plain `Acquire` load, which is what
the later `next` and `produced` loads need.

The producer index is `position << 8 | attached`, where `attached` is the
number of blocks linked after the producer's block, saturated at 255 (the
exact total lives in `ProducerHead::attached`, which only the sentinel owner
touches). See "Attach-all freelist" below.

## Publication and completion

A producer copies its reservation one VBlock segment at a time and publishes
each segment with `produced.fetch_or(mask(offset, count), Release)`. Every
producer's `fetch_or` joins one release sequence on the mask, so a consumer's
`Acquire` load of `produced` synchronizes with every producer whose bits it
sees.

A consumer copies its segment in order as bits appear: it loads `produced`,
counts the trailing ones from its offset, copies that many slots, and
snoozes only when the next slot's bit is still clear. A slot's bit is set by
exactly the producer that reserved it, after the value is written, so the
set bits are always initialized slots. There is no "is the total a prefix?"
question and no producer-index load in the copy loop; that is the point of
the experiment. A stalled producer still delays the consumer waiting on its
bits.

After copying, the consumer sets its bits with `consumed.fetch_or(mask,
AcqRel)`. The consumer whose `fetch_or` completes the mask (`previous | mine
== u64::MAX`) increments the block's `consumed_vblocks`; the one that
completes the block recycles it. The `AcqRel` chain on `consumed` and on
`consumed_vblocks` orders every consumer's copy before the recycle, and the
freelist's `Release`/`Acquire` pair orders the recycle before a producer
writes the block again.

`reset` clears every mask of a recycled block: 2048 x 2 stores for the
default geometry, once per 131072 items.

## Attach-all freelist

The freelist has two states: null, or a chain. A taker swaps the chain out
whole (`take_all`); a return relinks its chain's tail to the head with one
CAS (`give`). Nothing waits on a busy taker and no block is dropped because
the cache was busy; a block is freed only by `flush_freelist` or teardown.
`tests/bitmask_allocation.rs` checks that a concurrent workload frees
nothing while the queue lives.

A producer computes its runway from the index it read: the slots left in its
block plus `attached` whole blocks. A reservation needs one slot more than
it fills (the head must land in a real block). If the runway covers that,
the reservation is one CAS with no freelist access at all. Otherwise the
producer takes the whole freelist and allocates any shortfall before the
CAS, so the sentinel is held only for linking, never for allocation.

The producer that wins the sentinel takes the freelist again (so every
cached block is attached, every time), links its `spare` chain at
`ProducerHead::tail`, walks the blocks its reservation crossed, and stores
the new block pointer, the exact `attached` total, and the index with the
saturated count. A producer that obtained blocks but lost the boundary to
another producer returns them to the freelist at the end of its call.

`take_all` walks the chain to count it, so a boundary costs one dependent
load per cached block: negligible at the default geometry (one boundary per
131072 items), a real cost for tiny blocks with deep queues.

## Teardown

`Drop` walks the chain from the consumer's block. Each block drops the
values whose `produced` bit is set and `consumed` bit is clear; the freelist
frees its cached blocks separately. As in v8 this relies on completed
operations leaving a valid active chain disjoint from the freelist.

## Running checks

```sh
cargo test --lib bitmask
cargo test --test bitmask --test bitmask_allocation
cargo test --release --test bitmask --test bitmask_allocation
cargo test --doc bitmask
cargo check --no-default-features --lib
./ab launch bitmask-vs-ubq --local --quick
```

`lab/studies/bitmask-vs-ubq.json` compares the original UBQ, v8 and bitmask
at both backoff policies. Passing these tests is evidence, not a proof of
memory safety or lock-free progress.
