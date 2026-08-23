# LUBQ sender/receiver linked-shard plan

## Status

The proof-safe linked channel is implemented in `src/kfifo/lubq.rs`:

- `channel`, `Sender`, `Receiver`, mutable sending, and reusable producer leases;
- strong sender/receiver root ownership with an explicit live-sender count;
- O(1)-amortized tail append under a rare structural spin lock;
- lock-free forward traversal through stable non-owning node pointers;
- drain-after-final-sender semantics and an acquire-ordered final rescan;
- permanent shard headers and UBQ payloads owned by the core until root teardown;
- release/acquire producer-lease handoff and reuse of inactive empty shards;
- no drain-time unlink, payload reference counts, or concurrent UBQ reset;
- native per-shard batch claims, caller-owned reusable `Vec` output, eager
  owning convenience results, and concurrent lifecycle/drop tests.

The core stores each shard in a stable `Box` and destroys the entire arena only
after the final sender/receiver `Arc<Core>` is dropped. Receiver cursors can
therefore be raw non-owning pointers without per-hop Arc traffic or address
reuse. An empty UBQ is reused in place with its warm final block; a concurrent
shutdown/reinitialization protocol is deliberately out of scope. A lock-free
registration path, consumer quotas, and detached block-owning iterators remain
optional, benchmark-driven phases. The dedicated SPMC inner queue is now
implemented and integrated.

## Decisions

- Expose channel semantics: `channel()` returns a `Sender` and a `Receiver`.
- Senders and receivers both own the linkage strongly.
- There is no sealed state. A sender count distinguishes an open linkage from
  one whose retired shards are only being drained.
- A receiver cannot create a sender, and a sender does not create a receiver.
- `Sender::clone` is the only way to add another producer after `channel()`.
- Every sender owns a distinct SPMC shard.
- Sending requires mutable access to the sender.
- Cloning a sender claims the first inactive empty shard or appends a new one.
- Cloning a receiver is cheap and does not allocate a shard.
- The root owns both a head link and a monotonic tail reference so sender
  cloning does not scan the list.
- Producer exit release-publishes an inactive lease and never traverses or
  structurally removes the shard.
- A retired nonempty shard remains available while receivers exist, regardless
  of whether any sender remains.
- After the final sender exits, remaining receivers keep the root and nonempty
  retired shards alive long enough to drain them.
- The root is destroyed when the final sender/receiver participant disappears;
  any work not retained by a participant-owned reservation is then dropped.

## Public API shape

The initial API should look approximately like:

```rust
pub fn channel<T>() -> (Sender<T>, Receiver<T>);
pub fn channel_with<T, B: BackoffPolicy>() -> (Sender<T, B>, Receiver<T, B>);

pub struct Sender<T, B> {
    root: Arc<Root<T, B>>,
    shard: ShardPtr<T, B>,
}

pub struct Receiver<T, B> {
    root: Arc<Root<T, B>>,
    cursor: Option<ShardPtr<T, B>>,
}
```

The exact names remain open, but the role split does not.

### Sender

```rust
impl<T, B> Sender<T, B> {
    pub fn send(&mut self, value: T);
    pub fn send_batch<I>(&mut self, values: I);
}
```

Mutable sending makes the noncooperative-producer rule visible in the type
system: a shard has one producer, and callers cannot concurrently send through
one handle without adding their own synchronization.

`Sender::clone()` scans for an inactive empty producer lease and reuses its
warmed UBQ. Only when no such lease exists does it allocate and publish a new
shard. It is deliberately not a reference-count-only clone.

The initial `channel()` call allocates and publishes the first sender shard, so
there is no optional/lazy shard in `Sender`.

### Receiver

```rust
impl<T, B> Receiver<T, B> {
    pub fn try_recv(&mut self) -> Result<T, TryRecvError>;
    pub fn try_recv_batch_into(
        &mut self,
        values: &mut Vec<T>,
        size: usize,
    ) -> Result<usize, TryRecvError>;
    pub fn try_recv_batch(&mut self, size: usize) -> /* initial eager result */;
}
```

A receiver already owns the root strongly. It uses the root's sender count to
distinguish a temporarily empty open channel from a permanently producer-closed
channel which may still contain drainable work.

`Receiver` cannot literally implement Rust's `Copy` trait because `Arc<Root>`
has clone/drop bookkeeping. `Receiver::clone()` is nevertheless cheap: it
clones only the root strong reference and starts with no cursor. Cursor
inheritance and distributed initial offsets can be evaluated later.

There are no `Sender::receiver()` or `Receiver::sender()` conversions. New
roles originate only from `channel()`, `Sender::clone()`, and
`Receiver::clone()`.

## Root lifetime and producer closure

The root is an ordinary shared `Arc` allocation:

```text
Sender -----Arc----+
Sender -----Arc----+--> Root
Receiver ---Arc----+
Receiver ---Arc----+
```

Every sender and receiver is a long-lived strong root participant. The root
stores a sender count because its Arc strong count alone cannot distinguish an
open producer set from receiver-only draining:

```rust
struct Root<T, B> {
    head: AtomicLink<T, B>,
    tail: AtomicTail<T, B>,
    senders: AtomicUsize,
    arena: SpinLock<Vec<Box<Shard<T, B>>>>,
}
```

Sender count zero is terminal without a seal: receivers cannot manufacture a
sender, and `Sender::clone()` requires an existing sender. A separate receiver
count is unnecessary because every receiver already owns the root strongly;
buffered values remain arena-owned until the final root participant exits.

Consequences:

- If any sender or receiver exists, the root and linked shard structure exist.
- When the last sender drops, the channel becomes producer-closed but receivers
  continue draining retired nonempty shards.
- When an empty scan observes `senders == 0`, no future enqueue is possible.
  The receiver performs a final acquire-ordered rescan before reporting
  `Disconnected`, ensuring it cannot miss a producer's final enqueue/drop race.
- If all receivers have already dropped, sender-owned roots keep the complete
  arena and every buffered value alive. Final sender drop tears down the root
  and all queues at once.
- The final sender/receiver Arc release invokes `Root::drop` and destroys all
  remaining structural state and buffered values.
- No explicit seal/close state exists.

This now follows drain-after-producer-close channel semantics while avoiding a
separate seal state.

## Linear topology

Use a null-terminated singly linked list rooted in stable shared storage:

```text
Arc<Root>
    |
    +-- head: AtomicLink ----> Shard A --next--> Shard B --next--> Shard C --next--> null
    |
    +-- tail: AtomicTail ------------------------------------------+
```

There is no physical ring. A receiver which reaches null wraps to `root.head`
for round-robin scanning.

The root owns the first shard and every subsequently published shard in a
stable boxed arena. The singleton form is:

```text
root.head -> Shard A -> null
root.tail ------------^
```

Published links are immutable. No node is unlinked, moved, or freed before root
teardown, so receiver cursors need neither reference-count increments nor a
hazard-pointer/epoch protocol.

## Root layout

Conceptually:

```rust
struct Root<T, B> {
    head: AtomicPtr<Shard<T, B>>,
    tail: AtomicPtr<Shard<T, B>>,
    senders: AtomicUsize,
    arena: SpinLock<RegistryState<T, B>>,
}

struct RegistryState<T, B> {
    owned: Vec<Box<CachePadded<Shard<T, B>>>>,
    next_id: usize,
}
```

The registration lock protects only the arena vector, producer-lease search,
and rare tail append. Queue operations and list traversal do not acquire it.
The tail moves only when a new allocation is required; reusing an inactive
shard does not modify the topology.

## Superseded fine-grained reclamation design

The payload stakes, counted links, removal, and splicing sections below record
the rejected fine-grained reclamation alternative. The implementation instead
uses the stable arena described above: `Shard` directly owns `UBQ`, and final
`Core::drop` provides the sole physical-reclamation point.

### Historical shard layout and ownership

The control header may need to outlive its queue payload:

```rust
struct ShardHeader<T, B> {
    strong: AtomicUsize,
    state: AtomicShardState,
    next: AtomicLink<T, B>,
    queue: UnsafeCell<ManuallyDrop<SPMC<T, B>>>,
}
```

The exact representation can change, but it must distinguish:

- control-header lifetime;
- queue-payload lifetime;
- structural incoming-link ownership;
- producer ownership;
- retention for queued work after producer retirement;
- temporary shard-payload upgrades by receivers;
- ownership transferred to detached reservations, if those are added later;
- the root's current-tail header pin.

Strong stakes have explicit reasons:

1. The sender stake keeps an active shard payload upgradeable.
2. A nonempty-retention stake keeps a retired shard payload upgradeable while
   it has unreserved work and receivers still exist.
3. A receiver operation temporarily upgrades a shard before accessing its
   payload; its root ownership is already strong.
4. A future owning iterator holds a stake for its reserved range.

Long-lived stakes should be represented by guards/tokens. Code must not
manually decrement a count and then drop a guard which decrements it again.

Strong count zero is terminal for the payload. A weak edge lease must never
resurrect a queue after payload destruction begins. The header and initialized
`next` field may remain available after payload destruction until every
structural and edge-lease obligation is gone.

## Producer retirement

Dropping a sender must not change `head`, `tail`, or any predecessor link.

For an initialized sender shard:

1. Publish `retired` with release ordering after the sender's final mutation.
2. If unreserved values and receivers remain, establish or retain the nonempty
   strong stake before releasing the producer stake.
3. Release the producer stake.
4. Decrement the sender count with release ordering.
5. Drop the sender's `Arc<Root>`.

If any receiver exists, the root remains and receivers can drain the retired
headless shard even after the final sender exits. The receiver which reserves
the last available work releases the nonempty stake and makes the node
eligible for removal.

If no receivers remain, preserving queued work for future consumption is
unnecessary. A retiring sender can release its queue payload once no
reservation owns part of it. The final sender Arc release then destroys the
root and any remaining shards.

The retired/empty/retention transition needs its own state-machine proof so a
producer drop cannot observe empty while a receiver owns a reservation without
some other stake protecting the payload. It must also define the race between
last-receiver drop and producer retirement: retaining a shard unnecessarily
until root teardown is safe; dropping a shard which a receiver can still reach
is not.

## Counted atomic links

The link protocol is specialized to this list. A weak shard reference is an
edge lease, not a conventional target-local `Weak`:

```text
Weak<carrier.next -> target>
```

Conceptually:

```rust
struct LinkWord {
    target: *mut ShardHeader,
    leases: usize,
    frozen: bool,
}

struct ShardWeak {
    carrier: *const AtomicLink,
    target: *mut ShardHeader,
}
```

The root's `head` uses the same link protocol as a node's `next`.

The central invariant is:

> While any external lease derived from a link exists, that link's target
> pointer is immutable and the carrier atomic remains allocated.

Acquire a lease with a CAS that verifies the link is not frozen and increments
its lease count. Clone increments the same carrier count. Upgrade increments
the target-local strong count only if it is nonzero. Upgrade fails after queue
payload destruction begins.

Dropping a non-final lease decrements the carrier count. The final release may
freeze the edge and help unlink a retired dead target. The last-lease
transition should be one CAS from `one lease, unfrozen` to `zero leases,
frozen`; decrementing to zero and freezing separately would permit a new lease
in between.

The packed representation must define:

- how pointer, count, and flags fit on each supported target;
- lease-count overflow behavior;
- whether a wider atomic is genuinely lock-free;
- acquire/release orderings for publication, traversal, and destruction;
- a help/recovery protocol if a thread stalls after freezing an edge.

No raw pointer may be dereferenced after an unprotected atomic load.

## Receiver traversal

Traversal uses hand-over-hand leases:

1. Obtain or retain the incoming edge lease for the current shard.
2. Attempt a target-local strong upgrade before touching its queue payload.
3. If upgraded, try to reserve/pop work.
4. Acquire the current shard's successor lease before releasing the incoming
   lease when moving forward.
5. Releasing the incoming lease may help remove the old shard.
6. On null, return to `root.head`.

A scan needs a stopping rule even while nodes are inserted or removed. The
first implementation should hold its starting lease until it either finds work
or wraps back to the same target. Registration during a scan may or may not be
observed; the operation is weakly consistent rather than a strict snapshot.

Holding a cursor lease between calls avoids restarting at `head`, but a paused
receiver can then delay mutation of that incoming edge. Start with no preloaded
cursor on receiver clone and instrument lease retention before choosing a
policy.

Scalar selection remains work-conserving round robin:

- begin at the receiver's cursor when one exists, otherwise at `head`;
- skip retired/dead/empty shards;
- return the first available item;
- advance the cursor after the successful shard;
- rotate after an empty full scan.

After an empty scan, load `root.senders` with acquire ordering. If it is zero,
scan once more before returning `Disconnected`. The zero observation is
terminal and orders the final sender's retirement/final-push publication; the
second scan observes any work which arrived after its shard was visited in the
first scan. If sender count remains nonzero, return `Empty`.

Batch receive chooses locality explicitly: it asks the current shard for the
entire remaining range with one native UBQ reservation, then visits later
shards only if more values are needed. The cursor advances past every shard
which contributes work, so fairness is enforced between producer-local bursts
rather than between individual values. Scalar receive retains strict local
round robin.

## O(1) sender cloning and tail publication

`Sender::clone` performs:

1. Allocate and fully initialize a new SPMC shard.
2. Give it its producer strong stake.
3. Account for the new sender before it can become observable as a returned
   handle.
4. Acquire a lifetime-safe snapshot/pin of `root.tail`.
5. Inspect the pinned tail's `next`.
6. If `next` is null, CAS the new shard into it.
7. Publish/advance `root.tail` to the new shard.
8. If `next` was already nonnull, help advance the lagging tail and retry.
9. Return a `Sender` containing the new shard and a cloned `Arc<Root>`.

Publication failure or panic before return must roll back the new sender count
and destroy the unpublished shard. Sender count zero cannot race a legitimate
clone into existence because cloning requires access to an already existing
sender.

This is the same broad lagging-tail idea used by linked lock-free queues: the
list link is the publication linearization point, and the tail is a hint which
may briefly lag but never point past the list.

The current tail's header pin prevents it from being physically reclaimed
while an appender accesses `tail.next`. Queue-payload retention is separate;
an empty retired tail need not keep a page of queue data alive merely because
it remains the append anchor.

If a fully lock-free counted tail materially complicates the first proof, use a
small root-local append lock initially. Sender cloning is a structural, rare
operation; `send` and `recv` must not acquire that lock. The list and tail API
should allow replacing the lock with a proven counted-pointer algorithm later.

## Removal and splicing

For:

```text
carrier --incoming--> target --next--> successor
```

the target is physically removable only when:

- its sender is retired;
- it has no unreserved work;
- no owning iterator needs the queue payload;
- target-local strong ownership is zero;
- it is not the node currently pinned by `root.tail`;
- the remover owns the frozen zero-external-lease incoming link;
- no external lease still needs `target.next` as its carrier.

The intended splice is:

1. Freeze `incoming` as part of releasing its final external lease.
2. Revalidate retirement, work state, strong count, and tail identity.
3. Inspect `target.next`.
4. If `target.next` has external leases, leave a tombstone and retry later. Do
   not move counts whose weak handles are bound to the old atomic address.
5. Otherwise transfer its structural successor pointer, or null, into
   `incoming` with release ordering.
6. Clear the target's old structural successor ownership under the same
   exclusive state.
7. Destroy/deallocate the target only after no thread can access its incoming
   or outgoing atomic state.

An empty retired current tail is deliberately retained. The next sender clone
appends through it and advances `root.tail`; subsequent traversal can then
remove the old tail as an interior node.

### Required proof before physical reclamation

The implementation must prove that every `ShardWeak` can decrement the exact
carrier atomic from which it was acquired. In particular, a node cannot be
deallocated while leases originating from its `next` remain.

External lease counts cannot simply be copied to another atomic during a
splice. Existing leases are bound to the old carrier address. The first
implementation therefore requires `target.next`'s external lease count to be
zero and defers removal otherwise.

It must also prove coordination between:

- root-head replacement and receiver acquisition;
- predecessor splicing and hand-over-hand traversal;
- tail advancement and attempted reclamation of the old tail;
- final sender publication and a receiver's empty/closed decision;
- final participant/root destruction and concurrent shard-edge access;
- payload strong-zero and weak edge upgrade.

Until these proofs pass model and stress tests, retire nodes logically and
reclaim the entire list only from root teardown.

## Root teardown

Root destruction is the channel's sole physical-reclamation event. It occurs only when
no sender or receiver owns a strong root reference. Producer closure can occur
earlier, when `root.senders` reaches zero, while receivers continue draining.

`Root::drop` must:

1. exclusively take the head/tail structural state after ordinary Arc strong
   ownership reaches zero;
2. destroy every unreserved queued value exactly once;
3. release structural header ownership down the list;
4. respect any separately owned reservation if detached iterators are later
   introduced.

Receive batches remain eager so no iterator outlives its receiver operation.
`try_recv_batch_into` consumes every borrowed UBQ reservation while the strong
receiver root keeps the stable shard arena alive and appends the values to
caller-owned reusable storage; `try_recv_batch` wraps that path in a new
`Vec::IntoIter`. Its specialized UBQ-to-`Vec` path marks an entire contiguous
block range consumed with one RMW instead of using the iterator's per-element
completion update. Thus no detached reservation remains when the final root
handle is dropped.

If detached iterators are later required, they must either:

- become explicit strong-root participants, thereby delaying final root
  destruction but not changing `senders == 0` producer closure;
- or own the exact blocks/sections they reserve so root teardown can destroy
  the remaining list independently.

An iterator holding only raw pointers is not sufficient with the current UBQ
block destructor, because a reservation can share a partially consumed block
with the queue.

## Expected mutation frequency and cost

For `P` senders and many more than `P` data operations:

```text
channel setup:   one root and one shard allocation
sender cloning:  O(P) inactive-empty lease scan; O(1)-amortized append on miss
steady state:    no topology mutation
sender exit:     inactive-lease publication and sender-count release only
drain:           queue operations only; no topology or payload reclamation
producer close:  final sender count transition, followed by receiver drain
root teardown:   final sender/receiver Arc release
```

Compared with immutable tables, this avoids O(P) table copying, consumer
snapshot invalidation, per-hop Arc traffic, and drain-time removal. Registration
still takes a rare lock and scans inactive leases, while steady-state traversal
uses immutable raw links protected by root ownership.

Instrument:

- producer-lease reuse hit/miss rate;
- average and maximum shards probed per receive;
- registration-lock time and list high-water size;
- producer-drop interference with active receivers;
- scalar and batched throughput versus the immutable-table prototype;
- root teardown cost and values destroyed on disconnect.

## Implementation phases

1. **Stable arena and channel lifetime — implemented**
   - Strong sender/receiver roots, sender-count closure, stable boxed shards,
     immutable atomic links, raw receiver cursors, and whole-root teardown.

2. **Reusable producer leases — implemented**
   - Release/acquire active-bit handoff, inactive-empty reuse, and allocation
     only when every existing shard is active or still buffered.

3. **Native batches — implemented**
   - Caller-owned reusable `Vec` output and block-range completion accounting.

4. **Registration optimization, if measured**
   - Replace the registration lock or linear reusable-lease search only if
     producer-churn benchmarks justify the added state.

5. **Dedicated SPMC inner queue — implemented**
   - Removed producer-side CAS/failure machinery and per-slot state while
     retaining the stable shard and producer-lease lifetime model.

6. **Iterator work, if still required**
   - Split reservation descriptors from borrowed UBQ iterators.
   - Choose explicitly between strong-root iterators and block-owning detached
     iterators.
   - Keep iterator lifetime separate from the sender-count closure decision.

7. **Benchmark and refine**
   - Compare scalar/batched steady-state and churn-heavy scenarios with
     Moodycamel, tracking scan length and lease reuse separately.

## Tests for the stable arena

- `channel()` creates one sender shard and one strong receiver.
- Receiver clone/drop updates root ownership without adding a shard.
- Last sender drop publishes producer closure while receivers drain all
  retained buffered values.
- An empty scan racing the last sender's final enqueue/drop either returns the
  value or finds it during the mandatory final rescan.
- Last receiver drop followed by producer shutdown tears down all remaining
  queues without requiring a consumer-side drain.
- Mutable sender API preserves per-shard producer ordering.
- Simultaneously live senders own distinct shards.
- An inactive empty shard is reused before the list grows.
- An inactive buffered shard is not reused until consumers drain it.
- Empty root/singleton, multi-node traversal, reuse, and full teardown paths.
- Retired nonempty shards remain consumable after the final sender exits while
  at least one receiver exists.
- Paused receiver cursors remain valid across producer retirement and reuse.
- No allocator address reuse occurs before final root teardown.
- Values are returned at most once and dropped exactly once.
- Panic/drop tests for queue values and partially completed eager batches.
- Long-running sender churn reaches a stable shard count through lease reuse.
- Deterministic interleaving model, Miri, and sanitizers where supported.

## Acceptance criteria

- Senders and receivers keep the root alive.
- No sealed or reopenable state exists; sender count zero is the terminal
  producer-closure authority because receivers cannot create senders.
- Sender cloning reuses an inactive empty shard before allocating a new one.
- Send requires mutable sender access and each sender owns one SPMC shard.
- Producer drop performs no list traversal, payload destruction, or structural
  mutation beyond release-publishing its reusable lease.
- A retired nonempty shard remains consumable while any receiver exists,
  including after the final sender exits.
- Final participant/root destruction drops all remaining buffered values
  exactly once.
- Published shard addresses remain valid for every receiver cursor until final
  root teardown.
- The linked design shows a measured advantage or explicitly accepted tradeoff
  relative to immutable routing snapshots before replacing the prototype.

## Next phase: dedicated SPMC shard queue

### Implementation status

Implemented on 2026-08-22 in the private `src/kfifo` queue:

- page-sized blocks now contain plain payload slots plus cache-separated
  `produced` and `consumed` counters;
- the sole producer owns a non-atomic tail, writes before release publication,
  and performs no steady-state reservation CAS;
- consumers retain the packed `chead` claim, acquire-check publication once per
  covered block, and batch-move contiguous values directly into spare `Vec`
  storage;
- false-hint claims validate `phead` after their CAS and use bounded exact-CAS
  rollback if a recycled address made a stale packed-head CAS succeed; the
  rollback retains any concurrently published prefix and stale descendants
  unwind in reverse order;
- reset is constant-time and a short locked intrusive cache retains every
  surplus block at the shard's high-water allocation for immediate reuse;
- dishonest and panicking exact-size iterators publish only their written
  prefix; drop tests cover unread suffixes and non-`Copy` bulk moves;
- the Moodycamel harness now creates role-specific tokens and reuses batch
  storage, removing the two comparison artifacts identified during profiling.

The 36-test focused suite, the no-std build, and the full focused
AddressSanitizer suite pass. This includes deterministic real-address ABA,
stale-descendant unwind, partial-prefix retention, and invalid-boundary repair
tests. Miri is not installed.
ThreadSanitizer requires rebuilding nightly `std`, but the repository's vendor
snapshot does not contain nightly's `hashbrown 0.17.1`, so that check remains
pending rather than changing dependency state.

On the local 16-thread x86 host, the corrected three-repeat 1P1C sweep measured
the following handoff means (M items/s):

| Batch | LUBQ SPMC | Moodycamel | LUBQ / Moodycamel |
| ---: | ---: | ---: | ---: |
| scalar | 27.0 | 24.1 | 1.12x |
| 16 | 225.6 | 154.6 | 1.46x |
| 256 | 547.7 | 531.2 | 1.03x |
| 4096 | 841.9 | 605.9 | 1.39x |
| 65536 | 918.8 | 562.0 | 1.63x |

These are the ABA-safe bounded-rollback numbers. Removing two
reservation-announcement RMWs improves LUBQ over the preceding safe local run
by 22.7% scalar, 19.4% at batch 16, 12.1% at batch 4096, and 14.4% at batch
65536. Batch 256 is 2.1% lower. LUBQ now wins every measured mode locally;
batch 256 remains the closest comparison and a useful pressure point.

The required Grace rerun and a fresh final-code topology sweep remain pending.
The next measurements should separate consumer-head contention in 1P-many-C
from outer shard scanning in many-P/many-C before either protocol is changed.

### Objective and scope

The Grace results make the dedicated inner queue the next measured phase. Each
LUBQ shard already has exactly one producer (`Sender::send` requires `&mut
self`), but it currently embeds the general MPMC `UBQ`. That leaves the MPMC
producer-head CAS, reservation-repair path, per-slot `AtomicU8` state, skipped
slots, and full slot-reset loop on the hot path.

Implement a separate `SpmcQueue<T, B>` for `src/kfifo/lubq.rs`; do not weaken or
replace the public MPMC `UBQ`. The first integration changes only
`Shard::queue`, `Sender::{send, send_batch}`, and the two `Core` receive helpers.
The linked-shard registry, sender leases, closure detection, and receiver
routing remain unchanged so their cost can be measured independently.

The primary hypothesis is that one block-level publication counter can replace
all per-slot state. For `u64`, this should reduce each slot from the current
state-padded representation to a plain `MaybeUninit<u64>`, approximately
doubling the useful slots in a Grace 64 KiB base page and eliminating one
release store and one acquire load per item.

### Control layout and invariants

Use two logically separate counters in every SPMC block. The packed queue
`chead` remains the sole committed-reservation authority:

```text
producer-owned line                 completion/reuse line
+----------------------------+      +-------------------------+
| next | produced: AtomicUsize|      | consumed: AtomicUsize   |
+----------------------------+      +-------------------------+

trailing payload
+-------------------------------------------------------------+
| MaybeUninit<T>[capacity]                                   |
+-------------------------------------------------------------+
```

- `produced` is the length of the contiguous initialized prefix. Only the
  shard's producer writes it; consumers only load it.
- `consumed` counts completed moves/drops, not a prefix. Completion can occur
  out of order, so it is used only to decide when a sealed full block is safe
  to recycle.
- The logical committed prefix is derived from validated packed-`chead`
  reservations; it is not mirrored into another atomic. The invariant is
  `consumed <= committed <= produced <= capacity`. A false-hint CAS may move the
  raw `chead` beyond `produced` temporarily, but that tentative suffix cannot be
  read and is repaired or validated before it becomes committed. The first
  inequality is a count rather than an ordering of individual completions.
- A partial producer-tail block is open and is never recycled merely because
  `consumed == produced`. It may receive more values later.
- Before `produced` reaches `capacity`, the producer initializes and publishes
  the successor link. An acquire observation of a full block therefore also
  makes its successor reachable.
- A validated consumer claim is the ownership token which keeps that block
  alive until the reservation increments `consumed`, including when the
  reservation iterator is dropped early.
- Slots have no state byte. An unclaimed slot at or above `produced` must never
  be read or dropped; a claimed slot below `produced` is moved or dropped
  exactly once.

Keep `produced` and `consumed` on separate cache lines initially. Compact only
after profiles show that the saved header space is worth producer/consumer
cache-line sharing.

### Reservation and publication protocol

A plain fetch-add reservation followed by a `produced` check is not valid for
`try_recv`: if the check finds an unproduced slot, returning `Empty` leaves an
irrevocable hole, while waiting can block forever behind a stalled producer.
Use the following bounded protocol instead:

1. The single producer owns an ordinary, non-atomic local tail descriptor. It
   allocates/links blocks, writes a contiguous range with plain stores, then
   release-stores the new block-local `produced` prefix. Only after that does it
   release-store the queue's packed published-head snapshot. There is no
   producer CAS and no producer head visible ahead of initialized values.
2. A consumer acquire-loads `chead`. A true `HAS_NEXT` hint is a cached proof
   that the current block is full. With a false hint, the consumer also
   acquire-loads the packed producer head and uses it to bound its proposal.
3. The consumer acquire-release CASes `chead` to reserve the proposal. It
   preserves a false hint rather than manufacturing a trusted proof from the
   pre-CAS producer-head observation. This is essential because the loaded
   `chead` may be a stale snapshot whose packed address reappeared after block
   reuse.
4. After a successful false-hint CAS, the consumer acquire-loads the producer
   head again. If the whole proposal is now published, the reservation is
   valid. Otherwise it exact-CASes the proposed head back to
   `max(original, producer frontier)`, retaining only a prefix published in the
   meantime. A failed repair means a descendant extended this exact false-hint
   state; the owner rechecks publication and waits only until those finite
   descendants validate or unwind. Fresh consumers which see an overrun load
   the producer head and return empty without extending it.
5. A claim is repaired before any boundary successor is followed. Boundary
   completion always loads the producer head and uses that observation both to
   bound the successor-block claim and to publish its `HAS_NEXT` hint.
6. Before returning a reservation, the consumer acquire-loads every covered
   block's `produced` counter and verifies that the claimed end is within the
   published prefix. It can now safely read the plain slots.
7. On completion or early iterator drop, the consumer performs one
   `consumed.fetch_add(quantity, AcqRel)` per contiguous block range. The last
   completion of a sealed full block resets only its header counters and makes
   the allocation reusable; there is no per-slot reset loop.

The packed published head remains an atomic store/load because it provides the
safe current-block upper bound and block routing. It is not a producer
reservation counter. After the SPMC version is correct and measured, a second
prototype may replace it with Moodycamel-style optimistic/overcommit counters.
That alternative must retain a separate committed claim counter; it must not
turn failed optimistic attempts into holes.

Required memory-ordering edges:

| Operation | Ordering | Establishes |
| --- | --- | --- |
| producer writes payload and successor | plain / `Relaxed` while private | initialization before publication |
| producer updates block `produced` | `Release` | payload and successor visible to consumers |
| producer updates packed published head | `Release` | current safe reservation bound |
| consumer loads producer head before/after a false-hint CAS | `Acquire` | bounds an ordinary proposal and validates its current block reuse |
| consumer loads `chead` | `Acquire` | observes the current claim frontier and any published hint |
| consumer claims or repairs `chead` | `AcqRel` CAS / `Acquire` failure | unique range ownership and exact-edge rollback |
| boundary owner stores the successor `chead` | `Release` | publishes the next stable claim frontier and hint |
| consumer loads block `produced` after claim | `Acquire` | payload may be read as `T` |
| consumer adds to `consumed` | `AcqRel` initially | last completion may recycle the block |
| recycler observes full completion | acquire half of the last RMW | all moves/drops precede reset/reuse |

The usual CAS path has lock-free progress. The recycled-address exception is
safe and bounded by the finite set of stale descendants, but repair is
owner-dependent: a descendant paused after its successful CAS can delay the
chain until it resumes.

### Producer batch and panic behavior

`send_batch` must publish the number of values actually yielded, not blindly
trust `ExactSizeIterator::len()`. Use the length only to reserve enough blocks.
A short or long implementation of `ExactSizeIterator` must not expose an
uninitialized slot. A small publication guard tracks the written prefix and,
if iterator code panics, release-publishes that prefix before unwinding. This
preserves already-moved values without needing `SKIP` states.

For a normal batch, write all values first and publish once per block segment,
then update the packed head once for the final tail. Batches spanning a block
boundary must link the next block before publishing the old block as full.

### Block retention and reuse

First preserve the current proof: a block cannot be reset until every slot in
the sealed block has been claimed and every claim has completed. Remove the
slot reset loop, but otherwise keep the current one-block pool for the first
performance comparison. This isolates per-slot state and producer CAS removal
from allocator-policy changes.

In the next isolated change, retain the shard's high-water block set rather
than freeing every surplus drained block. Use a short block-cache lock or a
proved tagged structure because a plain Treiber stack can suffer ABA when the
single producer pops blocks concurrently with multiple consumers pushing
completed blocks. Pool traffic is once per page-sized block, so correctness and
simple teardown take priority over making that path lock-free.

Reuse must preserve these rules:

- a block is reusable only after it was published full, `chead` reserved its
  full range, and the last completion made `consumed == capacity`;
- reset clears `next`, `produced`, and `consumed`, but touches no
  payload slots;
- an open partial tail remains attached and warm across temporary emptiness and
  producer-lease handoff;
- queue drop visits attached and cached blocks once and drops exactly the
  published-but-unconsumed values;
- an uncommitted raw head snapshot may survive reset, but a false-hint CAS must
  validate the current producer frontier before it can dereference payload or
  follow a successor; committed claims remain represented in `consumed`.

### Implementation milestones

1. **Freeze comparable baselines**
   - Keep the Grace-2 raw results and record exact scalar, 16, 256, 4096, and
     65536-item measurements for 1P1C, 1P-many-C, enqueue-only, and
     dequeue-only cases.
   - Correct the Moodycamel harness to create only role-appropriate tokens and
     reuse producer/consumer batch buffers, then retain both old and corrected
     results. The current wrapper work biases against Moodycamel, so an SPMC
     win must survive this correction.
   - Add an exact-work profiling mode so Callgrind/perf compares the same item
     and batch counts rather than equal wall-clock intervals.

2. **Add the SPMC storage types beside MPMC UBQ**
   - Add a plain `SpmcSlot<T>` and page-sized `SpmcBlock<T>` with the two
     counters above.
   - Add layout assertions/tests showing that `u64` slots carry no atomic state
     and reporting the block capacity on 4 KiB and 64 KiB page hosts.
   - Leave `Shard` on `UBQ` until the standalone SPMC tests pass.

3. **Implement single-producer publication**
   - Replace producer reservation CAS with a private tail and release
     publication.
   - Implement scalar, batch, exact-fill, multi-block, dishonest-length, and
     unwind-guard paths.
   - Inspect optimized assembly to verify that the contiguous payload loop has
     no per-item atomic operation.

4. **Implement multi-consumer claims**
   - Port the packed `chead` claim while deleting the MPMC repair path,
     `FalseIterator`, slot wait, and `SKIP` handling.
   - Implement scalar and block-range batch completion with early-drop safety.
   - Assert the counter invariant in debug/test builds at every publication,
     claim, completion, and reset boundary.

5. **Integrate one LUBQ shard**
   - Change only `Shard::queue` and the sender/core forwarding methods.
   - Run the full existing linked-shard lifecycle, reuse, close/drain, and drop
     suite before changing routing or registration.
   - Keep a temporary feature or benchmark selector for MPMC-inner versus
     SPMC-inner A/B measurements; remove it only after the results are archived.

6. **Retain the high-water block cache**
   - Replace the one-block pool in a separate commit/change.
   - Count allocations, cache pushes/pops, and high-water blocks so repeated
     benchmark rounds prove that allocation plateaus.

7. **Profile and decide**
   - On Grace, collect throughput plus cycles, instructions, cache misses, and
     atomic-contention evidence where perf permissions allow it.
   - Attribute remaining time among payload copy, consumer `chead` contention,
     block completion, allocation, and outer shard scanning before considering
     routing changes or optimistic/overcommit reservations.

### Correctness test matrix

- 1P1C and 1P-many-C exact-once delivery for scalar and every batch size around
  `capacity - 1`, `capacity`, and `capacity + 1`.
- Producer paused before payload write, after payload write but before
  `produced`, and after `produced` but before published-head update. A
  `try_recv` must return `Empty` or a valid value, never wait on an uncommitted
  slot.
- Two consumers racing for the last published slot; only one succeeds and the
  failed claim leaves counters unchanged.
- Out-of-order completion of reservations from the same block; reuse occurs
  only after the final completion.
- Early iterator drop, empty and partial batches, exact block fill, multi-block
  batch, and producer handoff with a warm partial block.
- Lying short/long `ExactSizeIterator` and an iterator which panics after
  yielding values; no uninitialized read, leak, duplicate, or double drop.
- Non-`Copy` drop counters for normal drain, receiver disappearance, sender
  disappearance, queue teardown, and cached-block teardown.
- Long-running small-working-set reuse under Miri and sanitizers where
  supported, plus a deterministic interleaving model for publication, claim,
  completion, and reset.
- Forced exact-address ABA after real block recycle/reuse; the stale CAS rolls
  back without reading payload.
- Multiple stale descendants unwind in reverse order while a newly published
  prefix remains claimed, and an invalid block-boundary proposal repairs before
  following the successor.

### Performance gates

Correctness is mandatory; performance gates decide whether SPMC becomes the
default LUBQ shard:

- no `AtomicU8` or equivalent per-slot state and no capacity-sized reset loop;
- no producer CAS in steady-state send, and no per-item atomic in a contiguous
  batch payload loop;
- allocation count plateaus at the shard's high-water number of blocks after
  the cache milestone;
- no more than a 5% regression against current LUBQ at batches 2--256 on Grace;
- materially close the large-batch ceiling: target at least 85% of corrected
  Moodycamel best-batch 1P1C throughput, versus roughly 54% in Grace-2;
- improve the Grace best-batch mean LUBQ/Moodycamel ratio from about 0.72 to at
  least 0.85 without making producer-heavy or symmetric cases worse.

If the standalone queue removes the expected instructions and cache traffic
but linked LUBQ still misses these gates, profile outer shard selection next.
If the standalone queue itself misses, compare the bounded packed-head claim
directly with an optimistic/overcommit prototype before adding routing
complexity.
