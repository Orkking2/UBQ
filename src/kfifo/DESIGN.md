# Linked-shard channel design

The `kfifo` prototype is a linked collection of single-producer,
multi-consumer queues. Each sender owns one SPMC shard; receivers scan the
shards in round-robin order. The implemented design follows the proof-safe
path and the SPMC specialization described in [`plan.md`].

## Ownership and routing

`channel()` creates one sender, one receiver, and one permanent shard. Every
handle holds an `Arc<Core>`, so the core and all shard allocations outlive
every operation that can reach them.

- `Sender::send` requires mutable access. One active sender therefore owns the
  non-atomic producer state of its shard.
- Cloning a sender reuses an inactive, empty shard when possible; otherwise it
  appends a new shard under the registry's short structural lock.
- Cloning a receiver creates an independent scan cursor and does not allocate a
  shard.
- Published shard links are immutable. Shard addresses remain stable in the
  core-owned arena until final teardown, so receiver traversal needs no
  per-hop reference-count traffic or reclamation protocol.

Receiver scans capture the current tail ID as a finite boundary. They visit
from their cursor to that boundary, wrap once to the head, and rotate the
cursor even after an empty scan. Batch receives favor one shard long enough to
amortize routing and reservation bookkeeping.

## SPMC block layout

Each SPMC shard uses the same generic page-block substrate as regular UBQ, with
an SPMC control header and trailing plain `MaybeUninit<T>` payload:

```text
next pointer
produced counter   (cache padded)
consumed counter   (cache padded)
plain payload slots...
```

There is no per-slot state. The sole producer owns a private tail descriptor,
writes payload normally, links a successor before publishing a full block, and
then release-publishes both the block's produced prefix and the public producer
head.

Regular UBQ specializes the common block with `Slot<T>` payload cells and its
MPMC completion header. kFIFO specializes it with plain `T` cells and the two
SPMC counters above. Allocation, page geometry, intrusive linking, and pointer
alignment are therefore shared while the publication protocols remain
independent.

Consumers reserve a unique range with a CAS on the packed consumer head. A
true `HAS_NEXT` hint is installed only after an acquired producer-head
observation proves the current block full, so that common path needs no
producer-head load. A false hint is deliberately untrusted: the consumer loads
the producer head before its CAS, preserves the false hint in the proposed
head, and validates with a second producer-head load after a successful CAS.

That post-CAS check is also the recycled-address ABA defense. If a stale CAS
temporarily reserves beyond the current producer frontier, fresh consumers see
the false hint, load the producer head, and fail quickly. The owner repairs its
exact CAS edge, retaining any prefix published in the meantime. Stale
descendants perform the same check and unwind in reverse order; if publication
catches up, the corresponding range simply becomes a normal valid claim. A
claim reaching a block boundary is repaired before following the successor,
and boundary completion uses its mandatory producer-head load both to bound
the next-block range and to compute its hint.

After reservation, a consumer acquire-loads each covered block's `produced`
counter once before reading its plain slots. Consumers add completed quantities
to the block's `consumed` counter; the consumer that completes the full block
owns its recycling. Payload publication and validation use release/acquire
ordering, while consumer-head ownership and repair use acquire-release CAS.

Ordinary attached-block claims retain lock-free CAS progress. The exceptional
ABA repair chain is finite but owner-dependent: a stalled stale descendant can
pause repair until it resumes. Fully consumed blocks are reset in O(1) and are
immediately eligible for a high-water cache. A short lock protects this
intrusive cache only when a block is acquired or recycled, avoiding an
ABA-prone plain pointer stack. The current partially filled tail stays attached
and warm when the queue becomes empty.

## Native batches

`send_batch` writes a contiguous producer prefix and publishes once. A guard
publishes the actual written prefix if the input iterator panics, and the
iterator's reported exact length is never trusted for safety.

`try_recv_batch_into` claims ranges natively. Within each block it moves the
contiguous claimed payload directly into the caller's spare vector capacity
and accounts completion once for the range. The convenience
`try_recv_batch` allocates an owning vector; callers on the hot path should
reuse storage with `try_recv_batch_into`.

## Closure and teardown

Dropping a sender release-retires its producer lease, then decrements the core's
sender count. An empty receiver that observes sender count zero performs a
second complete scan before reporting `Disconnected`; this orders the final
sender's publications and prevents a last value from being mistaken for a
drained channel.

No shard is unlinked or destroyed while the core is live. Final core teardown
drops unread published values, frees attached blocks, and frees all cached
blocks. Inactive empty shards and their warmed SPMC block caches can be reused
by later sender clones.

## Remaining scaling work

The specialized SPMC storage removes the old per-slot atomic bottleneck. The
remaining many-producer/many-consumer cost is primarily outer shard discovery
and scanning. Changes such as active-shard indexing or receiver-local routing
hints should be measured separately from this inner queue.

[`plan.md`]: ../../plan.md
