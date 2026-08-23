# Moodycamel's linked-SPMC design versus LUBQ

> The immutable-table prototype discussed in earlier revisions has been
> replaced by the stable linked-shard arena implemented in [`lubq.rs`](lubq.rs).

## Scope and conclusion

This note compares the experimental Rust [`LUBQ`](lubq.rs) with the exact
Moodycamel source vendored in this repository: `concurrentqueue.h` v1.0.5,
commit `9afb99746f0f5fc94ac8aef737053ae0481ba8d1` ([source identification][mc-rev]).
The local C++ shim always uses explicit `ProducerToken` and `ConsumerToken`
operations ([shim token calls][shim-calls]), so the **explicit producer** path
is the relevant one for UBQ's benchmarks.

The short version is:

> Moodycamel makes reclamation cheap by mostly declining to do it while the
> queue is alive. Its producer catalog is a prepend-only linked list. Producer
> nodes, raw consumer pointers into those nodes, old block-index generations,
> and explicit-producer block rings all remain valid until the entire queue is
> destroyed. A producer token merely activates or deactivates a persistent
> node.

LUBQ now adopts the same catalog-lifetime invariant in Rust: `Arc<Core>` owns a
stable boxed shard arena, published nodes are never unlinked, receiver cursors
use non-owning pointers, and inactive empty producer shards are reused. The
whole arena is reclaimed only after the final sender or receiver releases the
root.

The two shapes are:

```text
Moodycamel

ConcurrentQueue (must outlive all operations)
  |
  +-- atomic producerListTail
        |
        v
      ProducerBase ----> ProducerBase ----> ProducerBase ----> null
       persistent         persistent         persistent
          |                   |
          v                   v
      circular block      circular block
          ring                ring

ConsumerToken --raw pointer--> desired/current ProducerBase


LUBQ

Arc<Core>
  |
  +-- stable arena: [Box<Shard>, Box<Shard>, ...]
  |
  +-- atomic head ----> Shard ----> Shard ----> null
                         |           |
                         v           v
                        UBQ         UBQ

Receiver --Arc<Core> + raw stable cursor--> Shard
```

## 1. The global producer catalog

### Moodycamel: persistent intrusive nodes

The global queue holds `producerListTail` and `producerCount` atomics
([global fields][mc-global-fields]). Despite its name, `producerListTail`
functions as the newest node/head of a singly linked stack:

1. Allocate a `ProducerBase` subtype.
2. Increment `producerCount`.
3. Set the new node's plain `next` pointer to the previously published node.
4. CAS the new node into `producerListTail` with release ordering.

That sequence is in [`add_producer`][mc-add-producer]. Consumers acquire-load
the published pointer before following `next`, so initialization of the node
and its immutable link is visible. Existing links are never changed.

Most importantly, nodes are **never unlinked during normal operation**. The
queue destructor walks the complete list and destroys every producer only
after the caller has guaranteed that no concurrent access remains
([destructor and quiescence rule][mc-destructor]). Consequently:

- a consumer can retain a raw `ProducerBase*` in its token;
- a traversal can follow raw `next` pointers without a hazard pointer;
- there is no producer-list ABA caused by node reuse at a different address;
- inserting one producer does not copy or invalidate the rest of the catalog.

This is a lifetime proof by containment: every list node lives at least as long
as the queue root.

The unit being linked is worth emphasizing. Moodycamel does **not** put a pair
of producer/consumer `Block*` heads directly in the global list. It links a
stable SPMC descriptor, `ProducerBase`, containing the logical tail, logical
head, optimistic-consumer accounting, a producer-owned block-ring cursor, and
the machinery for mapping logical indices to blocks. The blocks are an inner
storage detail. In that respect, Moodycamel's `ProducerBase*` is conceptually
closer to LUBQ's `Arc<Shard>` than to the originally proposed
`[AtomicPtr<Block>; 2]` entry.

Consumer discovery is correspondingly simple. A scan acquire-loads the newest
producer node, follows immutable links to `nullptr`, and wraps back to the
captured newest node. An insertion racing after that acquire load may be absent
from the current scan, but a later scan reloads the list root. Existing
`ConsumerToken` pointers require no refresh because the pointed-to nodes never
move or disappear.

### LUBQ: root-owned stable arena

LUBQ stores every `Shard` in a stable `Box` owned by `Core`. Registration takes
a structural spin lock, claims an inactive empty shard when possible, and
otherwise pushes a new box before release-publishing its pointer through the
current tail ([registration][lubq-register]). Published `next` links never
change.

Every sender and receiver holds `Arc<Core>`, so the complete arena outlives all
operations. A receiver cursor can therefore retain a raw `Shard*`; traversal
performs pointer loads without per-node Arc increments. Final `Core::drop` owns
all boxes exclusively and reclaims the complete list.

### Direct comparison

| Property | Moodycamel | Current LUBQ |
|---|---|---|
| Catalog | Intrusive prepend-only list | Root-owned append-only list plus boxed arena |
| Add producer | O(P) inactive scan, lock-free insertion on miss | O(P) inactive-empty scan under registration lock, O(1) append on miss |
| Remove producer | Never unlinks | Never unlinks |
| Reader protection | Externally guaranteed queue lifetime | `Arc<Core>` lifetime; nodes never freed early |
| Consumer pointer | Raw persistent `ProducerBase*` | Raw persistent `Shard*` |
| Runtime catalog reclamation | None | None |
| Stable-cache overhead | Raw pointer loads | Raw pointer loads |
| Traversal locality | Pointer chasing | Pointer chasing |

Both queues retain their producer topology to avoid a hot-path reclamation
protocol. LUBQ currently serializes producer registration and retains UBQ's
MPMC inner machinery; Moodycamel uses lock-free registration and a specialized
SPMC inner queue.

## 2. Producer-token destruction and reuse

Destroying a Moodycamel `ProducerToken` does not destroy its SPMC or wait for it
to drain. It clears the node's token back-pointer and release-stores
`inactive = true` ([token destructor][mc-token-drop]). Consumers ignore the
inactive flag and may continue draining the node.

When another producer token is created, `recycle_or_create_producer` walks the
list looking for an inactive node of the same explicit/implicit kind and claims
it with an acquire CAS from `true` to `false` ([producer recycle][mc-recycle]).
Only when no reusable node exists does it allocate and prepend a new node.

This has several consequences:

- Logical producer-token lifetimes and physical SPMC lifetimes are different.
- A new token may continue enqueueing at the tail of a previous token's stream,
  even while old values remain.
- The release/acquire handoff transfers the producer-private state safely.
- `producerCount` counts allocated list nodes, not currently active tokens, and
  never decreases.
- Repeated thread churn can reach a steady number of producer nodes instead of
  continually allocating and reclaiming them.

LUBQ now also treats producer ownership as a lease. Sender drop release-stores
`producer_active = false`; clone scans under the registration lock and claims
an inactive shard with an acquire CAS ([LUBQ retirement][lubq-retire]). LUBQ is
currently more conservative than Moodycamel: it reuses the shard only after
`UBQ::is_empty()` reports that all unreserved old work has drained. The UBQ
itself is not reset, so its warm final block and logical head positions remain
usable.

## 3. The explicit inner queue really is SPMC

All producers share the outer `ConcurrentQueue`, but an explicit
`ProducerToken` targets one `ExplicitProducer`. That object has:

- one producer-owned `tailBlock` and producer-private block-index variables;
- an atomic `tailIndex`, published by the sole producer;
- an atomic `headIndex`, claimed by multiple consumers;
- two auxiliary consumer counters, `dequeueOptimisticCount` and
  `dequeueOvercommit` ([producer-base fields][mc-producer-base]).

The single-producer assumption is material, not just advisory. The producer
updates `tailBlock`, `pr_blockIndexFront`, `pr_blockIndexSlotsUsed`, and the
block-index entries without producer/producer CAS. The header recommends at
most one token of each kind per thread ([token guidance][mc-token-guidance]),
and the local wrapper moves each token to one owning Rust thread rather than
sharing it ([Rust token ownership][shim-token-ownership]).

By contrast, LUBQ puts a complete MPMC UBQ in each shard
([shard definition][lubq-shard]). Sharing one `LUBQ` handle between producer
threads still works because the inner queue arbitrates producers. That is more
robust, but it gives up the central performance advantage of a true SPMC:
Moodycamel's producer does not need to CAS a producer head or publish a ready
state for each slot.

## 4. Enqueue publication

For a normal explicit enqueue, Moodycamel does this
([explicit enqueue][mc-enqueue]):

1. Relaxed-load the producer's current `tailIndex`.
2. At a block boundary, either reuse the next empty block in the producer's
   circular ring or requisition and splice in a new block.
3. If a new block was selected, publish its logical-base-to-pointer mapping in
   the block index.
4. Placement-construct `T` in the selected slot.
5. Release-store the incremented `tailIndex`.

The release store of `tailIndex` is the element-availability publication.
Because there is one producer and it constructs a contiguous prefix, consumers
cannot observe a later tail slot while an earlier producer slot is still being
filled.

The copied UBQ used by LUBQ is MPMC. A producer first CAS-reserves a range in
the shared producer head, then fills the reserved slots
([UBQ producer reservation][ubq-producer-reserve]). Different producers may
finish reservations out of order, so each slot separately release-publishes
`WRITTEN` or `SKIP`, and a consumer waits on that state
([UBQ slot state][ubq-slot]). That mechanism is necessary for MPMC, but
redundant in the normal one-LUBQ-handle/one-producer case.

This is probably the largest hot-path opportunity exposed by the comparison:
a dedicated SPMC shard can remove producer-head CAS, per-slot state, skipped
reservations, and producer-side contention from the common LUBQ configuration.

## 5. Multi-consumer claiming: optimistic count, overcommit, then head

Moodycamel's explicit dequeue is more subtle than a single `head.fetch_add(1)`
([single dequeue][mc-dequeue]):

1. Read `tail`, `dequeueOptimisticCount`, and `dequeueOvercommit` cheaply.
   The effective number of optimistic claims is
   `dequeueOptimisticCount - dequeueOvercommit`.
2. If that value appears behind `tail`, increment `dequeueOptimisticCount` to
   obtain a speculative claim number.
3. Acquire-load `tail` again. If the claim is truly below the published tail,
   it corresponds to a real element.
4. Only then `fetch_add` `headIndex` with acquire-release ordering to obtain the
   unique logical element index.
5. If the speculative claim was beyond the real tail, increment
   `dequeueOvercommit` instead, cancelling it in the effective count.

Why two claim counters plus the real head? Near empty, several consumers may
all see the same apparently available tail. The optimistic counter cheaply
serializes their attempts; the second tail check determines how many are real;
overcommit accounts for the losers without rolling any counter backward. The
actual `headIndex` advances only for proven elements and therefore remains the
contiguous FIFO position.

LUBQ delegates this problem to UBQ's packed consumer head. UBQ CAS-reserves a
range, contains a repair path if the reservation raced the producer, and
returns a borrowing `UBQIter` over the reservation
([UBQ consumer reservation][ubq-consumer-reserve]). Both schemes allocate
unique indices before moving values, but Moodycamel exploits its single
producer's monotonic published tail to separate cheap speculation from the
real head update.

For bulk dequeue, Moodycamel speculates a desired count, corrects overcommit,
and claims the actual range with one `headIndex.fetch_add(actualCount)` before
walking blocks ([bulk claim][mc-bulk-claim]). The caller supplies the output
iterator/buffer; the queue does not allocate an output collection.

LUBQ's link layer consumes one borrowed UBQ reservation per visited shard into
a caller-owned `Vec<T>` ([LUBQ batch][lubq-batch]). Within each UBQ block it
moves the reserved run first, then marks the whole run consumed with one RMW;
the normal iterator path would mark every slot separately. The
`try_recv_batch` convenience API wraps this native path in a newly allocated
`Vec::IntoIter`, while `try_recv_batch_into` reuses the caller's allocation.

## 6. Mapping logical indices to blocks

Each explicit producer owns a circular linked ring of `Block`s. Consumers do
not locate a block by walking that ring. They use an atomically published
`BlockIndexHeader` whose entries map a logical block base to a `Block*`
([block-index layout][mc-block-index]). The consumer:

1. acquire-loads the current block-index header;
2. acquire-loads its published `front` entry;
3. compares the claimed logical block base with the front entry's base;
4. computes a signed, wrap-aware block offset;
5. masks that offset into the circular index and obtains the block pointer.

At a block-index resize, the sole producer doubles the array, copies live
entries, links the new header's `prev` to the old allocation, then
release-publishes the new header ([block-index growth][mc-index-growth]). Old
headers are not freed until `ExplicitProducer` destruction
([index destruction][mc-explicit-destructor]).

This is strikingly close to the original LUBQ larray idea—copy an array and
atomically publish its replacement—but Moodycamel makes reclamation trivial by
retaining every old generation. It also does not need CAS for this particular
array because each block index has exactly one writer. LUBQ's global table has
many potential registering producers, so it needs serialization or a CAS retry
loop plus a safe reclamation mechanism.

## 7. Block reuse and why raw pointers remain valid

For the default block size of 32, an explicit block has one atomic empty flag
per element. Consumers release-store their flags after moving and destroying
values. The producer considers a block reusable only after every flag is set,
then uses an acquire fence before resetting them
([block empty protocol][mc-block-empty]). For block sizes above the configured
threshold, the flags are replaced by an atomic completed-dequeue counter.

Explicit-producer blocks form a ring. At a new block boundary, the producer
first tests `tailBlock->next`; if it is completely empty, it rotates into and
resets that block. Otherwise it allocates another block and splices it into the
ring ([block selection][mc-block-selection]). Thus:

- block allocation normally stops once the producer reaches its high-water
  number of simultaneously occupied blocks;
- consumers never free an explicit producer's blocks;
- a block pointer remains stable even while the storage cycles through many
  logical index generations;
- the block index, logical bases, and empty protocol prevent confusing the
  current generation with an older one.

The default trait explicitly notes that blocks consumed by explicit producers
are only freed when the queue is destroyed, not when a token is destroyed
([recycling trait][mc-recycle-trait]).

UBQ uses a different policy. Its last consumer resets a fully consumed block,
tries to install it in a one-block queue pool, and otherwise frees it
([UBQ reclamation][ubq-reclaim]). LUBQ's `Arc<Core>` stabilizes the outer
`Shard` descriptors, not UBQ's internal blocks. Receivers retain raw shard
pointers but obtain block reservations through UBQ on every operation; no
cursor caches a recyclable inner block address.

## 8. Choosing which producer stream to consume

The shim uses consumer tokens, so the relevant algorithm is the token-aware
path ([token dequeue][mc-token-dequeue], [rotation helper][mc-rotation]). Each
`ConsumerToken` stores:

- `initialOffset`, assigned from a global consumer ID;
- `desiredProducer`, its fair-placement target;
- `currentProducer`, which may drift while searching for work;
- `lastKnownGlobalOffset`;
- `itemsConsumedFromCurrent` ([consumer-token fields][mc-consumer-token]).

On first use, `initialOffset % producerCount` distributes consumers across the
producer list. A consumer prefers its current producer for locality. If that
stream is empty, it walks the list, wrapping from `nullptr` to the current
`producerListTail`, until it finds work or returns to its start.

After a consumer obtains the configured quota—256 items by default—from one
producer, it increments `globalExplicitConsumerOffset`. Every consumer token
that later notices a changed global offset advances its `desiredProducer` by
the delta. This creates coordinated, coarse-grained rotation: consumers retain
cache locality for a while, but no consumer can permanently own one busy
producer stream.

LUBQ uses a simpler local round robin. Each consumer gets a distinct initial
integer offset and advances to the slot after every success; even an entirely
empty scan rotates its start ([LUBQ scan][lubq-scan]). There is no global
rotation counter or quota.

The tradeoff is:

- **Moodycamel:** better locality and larger contiguous runs from one SPMC;
  globally coordinated fairness, but a shared rotation atomic and slower
  reaction measured in quota-sized chunks.
- **LUBQ:** fine-grained and decentralized rotation, but more frequent movement
  among shard metadata and less opportunity to amortize a hot shard's cache
  state.

Moodycamel's token-free dequeue is different again: it samples producer sizes
until it has seen three non-empty streams, tries the largest, then falls back to
a full scan ([untokenized heuristic][mc-no-token-dequeue]). That code is not
used by the local shim.

Neither implementation provides a meaningful global FIFO across producer
streams. Each SPMC/shard preserves its own stream order; cross-stream return
order is determined by consumer selection and races.

## 9. Queue and consumer lifetime

Moodycamel requires externally synchronized queue destruction. Its destructor
states that the queue must not be accessed concurrently, then invalidates live
producer tokens and destroys the persistent producer list
([queue destruction][mc-destructor]). `ConsumerToken` contains unregistered raw
pointers, so using it after queue destruction is simply outside the API's
lifetime contract.

The Rust benchmark wrapper supplies that missing root ownership by storing an
`Arc<MoodycamelQueue>` in every per-thread handle alongside the raw C++ tokens
([thread handle][shim-thread-handle]). The Arc keeps the C++ root—and therefore
all producer nodes—alive for the handle's operations.

LUBQ receivers likewise hold `Arc<Core>`. A separate atomic sender count marks
producer closure, while receivers can continue draining every inactive shard.
Root destruction—and therefore shard-list destruction—occurs only after all
sender and receiver handles are gone ([LUBQ lifetime][lubq-life]).

## 10. A benchmark-shim wrinkle

The Rust harness creates both a producer token and a consumer token for **every
benchmark thread**, because one thread-handle type serves every role
([handle construction][shim-handle-construction]). A consumer-only benchmark
thread therefore registers an explicit producer node that it never uses.

Because Moodycamel never removes producer nodes, those empty nodes participate
in `producerCount`, initial consumer placement, and list scans. In a benchmark
with P producer threads and C consumer threads, the local wrapper generally
creates P+C explicit producer nodes, not P. LUBQ's `consumer()` does not add a
shard.

This does not invalidate the Moodycamel benchmark, but it is important when
interpreting linked-SPMC scaling results. A role-specific harness handle could
avoid the empty streams.

The shim's comment that implicit registration "contends on a lock" is also a
little broader than the vendored implementation. The normal implicit path uses
a lock-free-ish thread-ID hash with atomic CAS; an `atomic_flag` serializes hash
resize, and threads may wait for a resize to finish
([implicit hash path][mc-implicit-hash]). A conventional mutex appears only
under the `MCDBGQ_NOLOCKFREE_IMPLICITPRODHASH` debug configuration. Explicit
tokens still avoid that hash lookup/registration path entirely, which is the
important benchmark property.

## 11. Advantages and disadvantages

### Moodycamel's approach

Advantages:

- True SPMC producer hot path: no producer-head CAS and no per-slot ready state.
- O(1) list publication after allocation, with no catalog copy.
- Raw consumer pointers are cheap because queue-wide retention makes them safe.
- Inactive producer nodes and their warmed block rings are reusable.
- Explicit blocks naturally recycle at a per-producer high-water mark.
- Caller-owned bulk output and one range claim avoid LUBQ's convenience API's
  eager result allocation; LUBQ's `try_recv_batch_into` now offers reusable
  caller-owned `Vec` storage while retaining eager shard-reservation draining.
- Consumer quotas trade a controlled amount of fairness for locality.
- No topology lock on enqueue, dequeue, registration CAS, or traversal.

Disadvantages:

- Producer nodes, inactive nodes, old block-index generations, and explicit
  block rings are retained until whole-queue destruction.
- Traversal cost follows historical producer-node count, not active count.
- Per-producer memory tends to retain historical peak occupancy.
- Raw-pointer validity depends on strict root lifetime and quiescent destruction.
- A producer token is a single-producer lease and must not be concurrently used
  as though it were an MPMC handle.
- The global rotation offset and each hot SPMC's head/counters can become shared
  cache-line contention points.
- Cross-producer FIFO and exact concurrent emptiness are intentionally absent.

### Current LUBQ approach

Advantages:

- Rust `Arc<Core>` ownership localizes the stable-arena lifetime proof.
- Raw receiver cursors traverse without per-node reference-count traffic.
- Inactive empty shards and their warm UBQs are reused across sender churn.
- Consumer creation does not create an empty producer stream.
- Whole-root teardown is automatically quiescent in the safe eager-batch API.
- Fine-grained local round robin is simple and does not write a global fairness
  counter on the dequeue path.

Disadvantages:

- Producer registration scans inactive shards and uses a structural spin lock.
- Traversal cost follows the historical/high-water shard count because nodes
  are never unlinked.
- The inner MPMC UBQ pays CAS and per-slot publication costs that a dedicated
  SPMC can avoid.
- The `pop_batch` convenience API allocates and eagerly materializes results;
  `pop_batch_into` reuses caller-owned `Vec` storage and amortizes completion
  accounting per contiguous inner-block range, but still eagerly consumes each
  borrowed inner reservation.

## 12. Implemented direction and remaining work

LUBQ now implements the stable-arena portion of the Moodycamel-shaped direction:

1. Let `Core` own every `ShardNode` until `Core::drop`.
2. Publish new nodes through an atomic append-only list.
3. Never unlink a node while the core is alive.
4. Give each node an atomic producer-lease state and reuse inactive nodes.
5. Let a consumer hold a strong `Arc<Core>` for an operation, after which raw
   node pointers are stable without per-node Arc increments.

Items 1–5 are implemented. The inner queue is now also a purpose-built SPMC:
its sole producer owns a private tail, publishes initialized block prefixes
with release stores, and consumers reserve through a packed shared head before
reading plain payload slots. Per-slot ready/skip state and producer reservation
CASes are gone.

The implementation uses stable `Box<Shard>` allocations owned by the core and
publishes `NonNull<Shard>` only after ownership is installed. Since the core
never removes or frees nodes, following immutable `next` pointers has the same
lifetime proof as Moodycamel. `Core::drop` has exclusive access and reclaims the
arena.

The remaining differences are deliberate and measurable: LUBQ retains its
registration lock and reuses only inactive *empty* shards. Shard topology
persists for the root lifetime, and scans include the high-water producer
count. The SPMC retains its high-water payload blocks in a shard-local cache;
prompt reclamation would trade away warm reuse and would not address the
remaining multi-shard scan cost.

[mc-rev]: ../../third_party/moodycamel/concurrentqueue.h#L1-L5
[mc-global-fields]: ../../third_party/moodycamel/concurrentqueue.h#L3677-L3698
[mc-add-producer]: ../../third_party/moodycamel/concurrentqueue.h#L3274-L3305
[mc-destructor]: ../../third_party/moodycamel/concurrentqueue.h#L874-L918
[mc-token-drop]: ../../third_party/moodycamel/concurrentqueue.h#L704-L720
[mc-recycle]: ../../third_party/moodycamel/concurrentqueue.h#L3255-L3272
[mc-producer-base]: ../../third_party/moodycamel/concurrentqueue.h#L1726-L1784
[mc-token-guidance]: ../../third_party/moodycamel/concurrentqueue.h#L426-L434
[mc-enqueue]: ../../third_party/moodycamel/concurrentqueue.h#L1876-L1980
[mc-dequeue]: ../../third_party/moodycamel/concurrentqueue.h#L1982-L2080
[mc-bulk-claim]: ../../third_party/moodycamel/concurrentqueue.h#L2275-L2366
[mc-block-index]: ../../third_party/moodycamel/concurrentqueue.h#L2368-L2431
[mc-index-growth]: ../../third_party/moodycamel/concurrentqueue.h#L2384-L2421
[mc-explicit-destructor]: ../../third_party/moodycamel/concurrentqueue.h#L1816-L1874
[mc-block-empty]: ../../third_party/moodycamel/concurrentqueue.h#L1587-L1691
[mc-block-selection]: ../../third_party/moodycamel/concurrentqueue.h#L1881-L1941
[mc-recycle-trait]: ../../third_party/moodycamel/concurrentqueue.h#L395-L400
[mc-token-dequeue]: ../../third_party/moodycamel/concurrentqueue.h#L1206-L1317
[mc-rotation]: ../../third_party/moodycamel/concurrentqueue.h#L1420-L1458
[mc-consumer-token]: ../../third_party/moodycamel/concurrentqueue.h#L735-L777
[mc-no-token-dequeue]: ../../third_party/moodycamel/concurrentqueue.h#L1148-L1184
[mc-implicit-hash]: ../../third_party/moodycamel/concurrentqueue.h#L3415-L3561
[shim-calls]: ../../third_party/moodycamel/shim.cpp#L38-L77
[shim-token-ownership]: ../bench_harness/baselines/moodycamel_cq.rs#L78-L112
[shim-thread-handle]: ../bench_harness/baselines/moodycamel_cq.rs#L114-L143
[shim-handle-construction]: ../bench_harness/baselines/moodycamel_cq.rs#L114-L150
[lubq-shard]: lubq.rs#L124-L155
[lubq-register]: lubq.rs#L222-L263
[lubq-retire]: lubq.rs#L333-L354
[lubq-scan]: lubq.rs#L449-L576
[lubq-batch]: lubq.rs#L383-L447
[lubq-life]: lubq.rs#L72-L96
[ubq-producer-reserve]: ../queue.rs
[ubq-slot]: ../block.rs
[ubq-consumer-reserve]: ../queue.rs
[ubq-reclaim]: ../queue.rs
