# dynblock

`ubq::dynblock::UBQ` combines UBQ's packed-head batch reservations with v8's
per-vblock publication and completion counters. It is a separate experiment;
the default `ubq::UBQ` and `ubq::v8::UBQ` retain their implementations.

## Storage

`Config` fixes `slots_per_vblock` and `vblocks_per_block` for the queue's
lifetime. The initial defaults are 32 and 32. These are starting points for
experiments, not tuned recommendations.

```text
one allocation
┌───────────────────┬──────────────────────┬──────────────────────┬─────┐
│ main block header │ vblock header + slots│ vblock header + slots│ ... │
│ size, consumed,   │ owner, next,         │ owner, next,         │     │
│ allocation layout │ produced, consumed   │ produced, consumed   │     │
└───────────────────┴──────────────────────┴──────────────────────┴─────┘
                    ↑ each vblock begins on its geometry-defined alignment
```

`block.rs` computes one power-of-two vblock stride/alignment large enough for
the header, aligned payload, and all packed indices including the end marker.
This also handles zero-sized and over-aligned element types. `head.rs` recovers
the vblock address by masking the low alignment bits, which hold the index.
The pointer and index always travel together in one atomic word. The main
header's `size` is its vblock count; `consumed` counts completed vblocks.

Vblocks never allocate independently and never change geometry. All but the
last vblock of a main block are linked at allocation time. When growth is
needed, an unpublished `BlockChain` owns spare whole allocations until the
producer attaches them. Losing producers free their unused spares. Payloads
are contiguous within each vblock and carry no per-slot flags or atomics.

## Reservation and publication

`queue.rs` separates reservation from copying and completion:

1. CAS the packed head forward within its current vblock.
2. If the reservation reaches the end marker, its owner advances through the
   successor chain, then publishes the final packed head. Other callers using
   that head wait until this transition finishes. Producers prepare spare
   allocations before winning the CAS, including a successor for exact fills.
3. Copy the reserved range one vblock segment at a time. Producer segments add
   to `produced`; consumer segments add to `consumed`, using AcqRel RMWs.

A whole producer batch reserves a contiguous FIFO range even when it crosses
several main blocks. Consumer batches stop at the producer's reserved end.
As in UBQ, the reservation heads can advance before copying finishes.

The produced count is a total, **not an initialized prefix** while producers
are in flight. Before reading a segment, a consumer acquires that count:

- A full-vblock count permits reading any reserved slot in that vblock.
- Otherwise it acquires the producer head *after* the count. The head must
  identify this same vblock, its reserved index must equal the produced total,
  and that total must cover the requested segment.

This prevents a later producer's publication from exposing a hole left by an
earlier producer. The AcqRel RMW chain aggregates all producer writes for the
consumer's Acquire load. A stalled producer can delay publication, and a
stalled boundary owner can delay reservations. Lock-free progress is not a
contract of this experiment.

## Reclamation

The consumer that completes a full vblock increments its owner's `consumed`
counter once. When that total reaches `size`, every payload in the allocation
has been moved out and both reservation heads have left it. The last consumer
retires the whole allocation; there is no per-vblock cache or reuse.

Completion alone does not protect a thread holding an old head snapshot.
All head operations therefore pin a Crossbeam epoch guard. A retired
allocation is deallocated only after the grace period, so a stale packed-head
CAS cannot succeed against a recycled address. Guards cover reservation and
copying. Deferred callbacks contain only the main pointer and deallocate raw
storage; they do not borrow the queue or drop payloads.

With `std`, pinning uses Crossbeam's shared thread-local collector. First use
can initialize thread-local state; subsequent scalar calls need no temporary
buffers. With `no_std + alloc`, a queue-local collector registers a temporary
handle per call. This preserves the protocol but adds allocation overhead.
Long-lived pins, including unrelated users of the shared collector, can delay
physical reclamation. There is no promise of immediate memory return at drain.

Exclusive queue teardown drops unread values in the active suffix and
releases each remaining vblock. Once all operations are complete, produced
and consumed totals describe prefixes again. Previously retired allocations
are disjoint from this suffix and remain owned by their deferred callbacks.

## API and experiments

`UBQ<T, B>` takes the same `BackoffPolicy` parameter as `ubq::UBQ`, with
`backoff::Crossbeam` as the default and `backoff::Yield` as the alternative;
every wait in the reservation, publication and boundary paths goes through
it. `bench_hardware` selects the policy with the workload's `backoff`, and a
lab study compares both by giving a queue one arm per policy.

The owned-buffer batch API matches the existing v8 benchmark adapter:

- `push(T)` and `pop()` use scalar stack storage.
- `push_batch(Vec<T>)` returns the original empty Vec for reuse.
- `pop_batch(Box<[MaybeUninit<T>]>)` returns a Vec backed by the output buffer.
- `queue::empty_vec_to_box_uninit` converts an empty output Vec back to a buffer.

Registration in `src/queue_variants.tsv` makes `ubq-dynblock` and geometry
selectors such as `ubq-dynblock-s32-v16` available to `bench_hardware` and `ab`.
The adapter name `v8` denotes the buffer API, not the reservation implementation.
`lab/studies/dynblock-vs-ubq.json` provides an A/B starting point against the
original queue, each at both backoff policies. Its comparison includes
differences in allocation geometry, publication, buffer handling, and epoch
protection; it does not isolate the counter layout alone.

```sh
cargo test --lib dynblock
cargo test --test dynblock --test dynblock_allocation
cargo test --release --test dynblock --test dynblock_allocation
cargo test --doc dynblock
cargo check --no-default-features --lib
./ab queues
```

Private tests exercise publication holes, out-of-order completion, stale
snapshots, geometry and pointer recovery. Integration workloads check FIFO,
contiguous batch reservations, repeated concurrent drains, exact-once delivery
and destruction. An allocator test checks batching, scalar buffer allocation,
and actual deallocation after completion and a grace period. Passing these
tests is evidence for the implementation, not a proof of its memory safety.
