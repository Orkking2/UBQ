//! A lock-free, unbounded multi-producer/multi-consumer queue backed by a linked
//! ring of fixed-size blocks.
//!
//! # Overview
//!
//! [`UBQ<T>`] is a **lock-free MPMC queue** with no upper bound on capacity.
//! Elements are stored in fixed-size *blocks* that form a circular ring; blocks
//! are allocated on demand and recycled once fully consumed.
//!
//! Every [`UBQ<T>`] handle is cheaply clonable — all clones share the same
//! underlying ring and may call [`UBQ::push`] and [`UBQ::pop`] concurrently from
//! any thread.  Reference counting ensures that the ring (and any unconsumed
//! elements) is freed when the last clone is dropped.
//!
//! Neither [`UBQ::push`] nor [`UBQ::pop`] ever parks the calling thread.  Both
//! operations are *lock-free*: producers and consumers make progress independently.
//! The only spin-waits occur at block boundaries, where a consumer briefly waits
//! for in-flight producers to commit their writes before claiming a slot.
//!
//! # Quick start
//!
//! ```rust
//! use ubq::UBQ;
//! use std::thread;
//!
//! let q: UBQ<u64> = UBQ::new();
//! let q2 = q.clone();
//!
//! let producer = thread::spawn(move || {
//!     for i in 0..1_000_u64 {
//!         q2.push(i);
//!     }
//! });
//!
//! producer.join().unwrap();
//!
//! for i in 0..1_000_u64 {
//!     assert_eq!(q.pop(), Some(i));
//! }
//! assert_eq!(q.pop(), None); // queue is now empty
//! ```
//!
//! # Internal design
//!
//! The queue maintains two atomic head pointers — **phead** (producer head) and
//! **chead** (consumer head) — each pointing into a circular ring of blocks.
//! Within each block, packed counters track claimed and committed producer/consumer
//! slots.  A consumer spins briefly on the *stability predicate* before claiming a
//! slot to guarantee it reads only fully-committed writes.
//!
//! Detailed correctness arguments for each invariant are annotated inline in the
//! source as `[C1]`–`[C6]`.

#![warn(missing_docs)]

use crossbeam_utils::{Backoff, CachePadded};
use std::{
    cell::UnsafeCell,
    mem::MaybeUninit,
    ptr::NonNull,
    sync::atomic::{AtomicPtr, AtomicU64, AtomicUsize, Ordering},
};

/// Atomic storage for a packed pair of counters (`high` = claimed, `low` = committed).
type A = AtomicU64;

/// Packed `F::BITS`-bit counter representation manipulated through [`A`].
/// Layout: upper `H::BITS` bits = claimed count, lower `H::BITS` bits = committed count.
type F = u64;
/// Single `H::BITS`-bit half of a packed [`F`] counter.
type H = u32;

/// A lock-free, unbounded multi-producer/multi-consumer (MPMC) queue.
///
/// See the [crate-level documentation](crate) for an overview and quick-start example.
///
/// # Cloning and ownership
///
/// `UBQ<T>` is cheaply clonable: every clone shares the same underlying block ring
/// via reference counting.  All clones may call [`push`](Self::push) and
/// [`pop`](Self::pop) concurrently without additional coordination.  The block ring
/// (including any unconsumed elements) is freed when the **last** clone is dropped.
///
/// # Implementation invariants
///
/// 1. **Block availability:** the first published block is immediately open to both
///    producers and consumers; each later block is initially open only to producers.
///
/// 2. **Stale head pointers fail fast:** a producer (resp. consumer) operating on a
///    block that is no longer `phead` (resp. `chead`) abandons that attempt, reloads
///    the head pointer, and retries.
///
/// 3. **Producer/consumer coordination per block:** `B::p` packs two counters —
///    `high(p)` = claimed slots (incremented before writing) and
///    `low(p)` = committed slots (incremented with `Release` after writing).
///    Before claiming a slot a consumer spin-waits until `high(p) == low(p)`
///    (all claims committed) or `low(p) == L` (block full).  Full blocks use an
///    unconditional `fetch_add` on `B::c`; partial blocks use a `CAS` loop.
///    `B::c` starts at `F::MAX` ("closed to consumers") and is set to `0` by the
///    consumer that advances `chead`.
pub struct UBQ<T> {
    /// Atomic pointer to phead: the block currently accepting producer pushes.
    p: NonNull<CachePadded<AtomicPtr<B<T>>>>,
    /// Atomic pointer to chead: the block currently being drained by consumers.
    c: NonNull<CachePadded<AtomicPtr<B<T>>>>,
    /// Shared reference count. Incremented by [`Clone`] and decremented by [`Drop`];
    /// when it reaches zero the last clone frees the block ring and drops any
    /// unconsumed elements.
    n: NonNull<CachePadded<AtomicUsize>>,
}

// SAFETY: All shared mutable state is accessed through `AtomicPtr` and [`A`]
// operations, or through the `UnsafeCell` slots in `B::a`, which are protected by
// the exclusive-index guarantee [C6] and the Release–Acquire pairing on `B::p` and
// `B::c`. Given `T: Sync` (resp. `T: Send`), concurrent shared access (resp.
// cross-thread transfer) of `T` values through `Q` is therefore sound.
unsafe impl<T: Sync> Sync for UBQ<T> {}
unsafe impl<T: Send> Send for UBQ<T> {}

/// Number of element slots per block.
const L: H = 32;
const _: () = assert!(
    L != H::MAX,
    "L cannot be H::MAX, as this value is used as a sentinel."
);

/// A fixed-size ring-buffer segment.
///
/// `p` and `c` each pack two [`H`] counters into a [`A`] using the upper and lower
/// `H::BITS` bits respectively; see `high()`, `low()`, and `merge()`.
struct B<T> {
    /// Pointer to the next block in the ring.
    n: CachePadded<AtomicPtr<Self>>,
    /// Producer counter. `high(p)` = slots claimed; `low(p)` = slots committed.
    p: CachePadded<A>,
    /// Consumer counter. `high(c)` = slots claimed; `low(c)` = slots committed.
    /// The sentinel value `F::MAX` means the block is not yet open to consumers.
    c: CachePadded<A>,
    /// Element storage. Slot `i` is written by the unique producer that claims index
    /// `i` and read by the unique consumer that claims index `i`. See [C6].
    a: [UnsafeCell<MaybeUninit<T>>; L as usize],
}

/// Returns the lower `H::BITS` bits of a packed counter (committed count).
fn low(r: F) -> H {
    r as H
}

/// Returns the upper `H::BITS` bits of a packed counter (claimed count).
fn high(r: F) -> H {
    (r >> H::BITS) as H
}

/// Packs two [`H`] values into a [`F`]: `h` in the upper `H::BITS` bits, `l` in the lower.
fn merge(h: H, l: H) -> F {
    (h as F) << H::BITS | l as F
}

// ─── Correctness arguments ────────────────────────────────────────────────────
//
// The labels [C1]–[C6] below are cited in SAFETY comments and inline remarks
// throughout `push` and `pop`. They capture the invariants and happens-before
// edges that make the lock-free protocol sound.
//
// [C1] STABILITY PREDICATE
//   v(p) := high(p) == low(p)  ||  low(p) == L
//
//   high(B::p) counts producers that have claimed a slot (incremented before
//   writing); low(B::p) counts producers that have committed their write
//   (incremented with Release after writing). While !v(p) there are in-flight
//   producers: slots in the range [low(p), high(p)) have been reserved but not
//   yet written. It is not safe to read or reason about any such slot. Once v(p)
//   holds, exactly one of two stable states exists:
//     · high(p) == low(p): every claimed slot has been written, so all slots in
//       [0, low(p)) contain fully initialized values.
//     · low(p) == L: the block is full; by the same argument all L slots have
//       been committed.
//   `backoff.snooze()` explicitly yields the current thread so the OS scheduler
//   has the opportunity to run in-flight producers, resolving the stall quickly.
//
// [C2] SLOT VALIDITY
//   If v(p) holds and r_ < low(p), slot r_ contains a fully initialized T.
//
//   low(B::p) is incremented with Release only after the write to the
//   corresponding slot is complete — the write and the commit are sequentially
//   ordered within each producer (see `push`). A consumer's Acquire load of B::p
//   therefore synchronizes-with each producer's Release commit, establishing
//   happens-before with the write to every slot index less than the observed
//   low(p).
//
// [C3] LAST CLAIMER OWNS THE BLOCK TRANSITION
//   Exactly one producer obtains a_ = L-1 from the atomic fetch_add on
//   high(B::p) — specifically, the one whose fetch_add reads back the old value
//   L-1. That producer is the only one to enter the block-transition branch
//   (a_ + 1 == L). At that moment phead still points to the current block p: no
//   other producer may advance phead because advancing phead is the exclusive
//   right of the last claimer, and it has not yet done so. The block transition
//   is therefore race-free with respect to phead and p.n.
//
// [C4] BLOCK-TRANSITION VISIBILITY (release sequences)
//   The block-transition stores — p.n.store(b_, Release) or n.c.store(F::MAX,
//   Release) — are sequenced-before the last claimer's own commit:
//       fetch_add(merge(0, 1), Release) on B::p  [see push, write + commit block]
//   That commit is a Release RMW on B::p. Every subsequent Release RMW on B::p
//   (i.e., every other producer's commit) extends the release sequence headed by
//   the last claimer's commit. An Acquire load of B::p that reads any value within
//   that release sequence synchronizes-with the last claimer's commit, making all
//   writes sequenced-before it (including the block-transition stores) visible to
//   the loading thread. Concretely: a consumer that Acquire-loads B::p and
//   observes low(p) == L can safely dereference p.n and will observe
//   p.n.c == F::MAX — both guaranteed by this synchronization edge.
//
// [C5] NEXT BLOCK IS READY WITHOUT SPINNING
//   When a consumer successfully claims slot r_ and then finds r_ + 1 == L:
//     (i)  r_ < low(p) — just passed the None-guard, so some element was
//          available when we checked.
//     (ii) r_ + 1 == L  ⟹  r_ = L-1  ⟹  low(p) > L-1.
//          Since low(p) ≤ L is invariant,  low(p) == L.
//   By [C4], the Acquire load of B::p (which observed low(p) == L) already
//   established visibility of the block-transition stores made by the last
//   claiming producer: c.n is non-null and c.n.c == F::MAX are both guaranteed
//   to hold on arrival. No spin is needed to wait for either condition; the former
//   spin loop waiting on n.c has therefore been removed.
//
// [C6] EXCLUSIVE SLOT ACCESS
//   Each index i in [0, L) in B::a is written by exactly one producer — the one
//   that obtains a_ = i from the atomic fetch_add on high(B::p) — and read by
//   exactly one consumer — the one that claims consumer slot r_ = i via the
//   fetch_add or CAS on high(B::c). The atomic operations on B::p and B::c assign
//   disjoint, non-overlapping indices to each thread, so no two threads ever
//   access the same slot concurrently.
//
// ─────────────────────────────────────────────────────────────────────────────

impl<T> UBQ<T> {
    /// Creates a new, empty queue.
    ///
    /// No blocks are allocated until the first call to [`push`](Self::push).
	#[inline]
    pub fn new() -> Self {
        // SAFETY:
        //   · `Box::new_zeroed().assume_init()` is used for the `p` and `c`
        //     allocations. The zero-initialized type is
        //     `CachePadded<AtomicPtr<B<T>>>`, which contains only an `AtomicPtr`.
        //     The zero bit-pattern for `AtomicPtr` is a null pointer — a valid
        //     and expected initial value meaning "no block allocated yet."
        //   · `Box::into_raw` on a non-empty `Box` is always non-null, so
        //     `NonNull::new_unchecked` is sound in all three cases.
        //   · The refcount `n` is initialised to `1` via `Box::new(...)`, which
        //     is unconditionally valid.
        unsafe {
            Self {
                p: NonNull::new_unchecked(Box::into_raw(Box::new_zeroed().assume_init())),
                c: NonNull::new_unchecked(Box::into_raw(Box::new_zeroed().assume_init())),
                n: NonNull::new_unchecked(Box::into_raw(Box::new(CachePadded::new(
                    AtomicUsize::new(1),
                )))),
            }
        }
    }

    /// Pushes `e` onto the back of the queue.
    ///
    /// This operation is lock-free and never parks the calling thread.  It may
    /// spin briefly at block boundaries while in-flight producers commit their
    /// writes.
    #[doc(alias = "enqueue")]
    #[doc(alias = "send")]
    pub fn push(&self, e: T) {
        let backoff = Backoff::new();
        // Spare block carried across loop iterations. Avoids a redundant allocation
        // when we lose a CAS race during first-block initialization.
        let mut b = None;

        '_0: loop {
            // Load phead with Acquire, pairing with the Release stores that publish
            // or advance phead (the CAS below on first push, or the block-transition
            // stores on subsequent iterations).
            //
            // SAFETY: `self.p` is a `NonNull<AtomicPtr<B<T>>>` backed by a heap
            // allocation created before `Q` is shared and never freed while any clone
            // of `Q` is alive. `as_ref()` borrows it for the lifetime of `self`.
            let mut p = unsafe { self.p.as_ref() }.load(Ordering::Acquire);

            if p.is_null() {
                // No block exists yet. Allocate one and race to install it as phead.
                //
                // SAFETY: `Box::new_zeroed()` produces a valid heap allocation.
                // `assume_init()` is sound because `B<T>` is composed entirely of:
                //   · `AtomicPtr<B<T>>` — bit-pattern zero is a valid null pointer.
                //   · `A` (×2)  — bit-pattern zero is a valid counter value.
                //   · `[UnsafeCell<MaybeUninit<T>>; L]` — `MaybeUninit` requires no
                //     initialization; each slot will be written before being read
                //     ([C2], [C6]).
                let n = Box::into_raw(unsafe { Box::new_zeroed().assume_init() });

                // SAFETY: `self.p` is valid as above.
                match unsafe { self.p.as_ref() }.compare_exchange_weak(
                    p,
                    n,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => {
                        // We won the race. By invariant 1, the first block is
                        // immediately open to both producers and consumers, so we
                        // publish it as chead too.
                        //
                        // SAFETY: `self.c` is a valid `NonNull<AtomicPtr<B<T>>>` for
                        // the same reasons as `self.p`. `n` is a live allocation that
                        // we own exclusively — the CAS succeeded and no other thread
                        // has observed this pointer yet.
                        unsafe { self.c.as_ref().store(n, Ordering::Release) };

                        p = n;
                    }
                    Err(r) => {
                        // CAS failed. Recover `n` into a Box to reuse or drop it.
                        //
                        // SAFETY: `n` was produced by `Box::into_raw` moments ago.
                        // Because the CAS failed, no other thread has ever observed
                        // this pointer, so reconstituting ownership is safe.
                        b = Some(unsafe { Box::from_raw(n) });

                        // spurious failure
                        if r.is_null() {
                            continue '_0;
                        }

                        p = r;
                    }
                }
            }

            // Non-binding hint: peek at high(p) before paying for an atomic RMW. If
            // the block already appears full we skip the fetch_add entirely. This load
            // may be stale; the authoritative check is the `a_ >= L` guard below.
            //
            // SAFETY: `p` is a valid non-null pointer to a live `B<T>`. It was either
            // just allocated (null-branch above) or loaded from phead with Acquire,
            // which synchronizes-with the Release that published it. Relaxed ordering
            // is sufficient because this is a non-binding advisory check only.
            let a = unsafe { high((*p).p.load(Ordering::Relaxed)) };

            if a >= L {
                backoff.snooze();
                continue '_0;
            }

            // Atomically claim a slot by incrementing high(B::p). The old value
            // returned is our exclusive slot index. Relaxed ordering is sufficient:
            // correctness relies on the Release commit (the low increment) that
            // follows, not on the claim itself.
            //
            // SAFETY: `p` is valid as established above.
            let a_ = unsafe { high((*p).p.fetch_add(merge(1, 0), Ordering::Relaxed)) };

            // The hint may have been stale, or another producer claimed the last slot
            // between the hint and our fetch_add. In either case we over-claimed.
            // By [C3], only the producer with a_ == L-1 may run the block transition;
            // all others that over-claim simply backoff and retry.
            if a_ >= L {
                backoff.snooze();
                continue '_0;
            }

            // [C3]: We are the last claimer (a_ == L-1). Run the block transition
            // BEFORE writing to slot a_, so that new producers can begin filling the
            // next block without waiting for our write to commit. The Release stores
            // below establish the happens-before edge described in [C4].
            if a_ + 1 == L {
                // Load p.n to determine whether a suitable successor block already
                // exists. Acquire pairs with the Release that stored p.n in a prior
                // block transition (or the initial null pointer).
                //
                // SAFETY: `p` is valid as above.
                let n = unsafe { (*p).n.load(Ordering::Acquire) };

                if n.is_null()
                    || unsafe {
                        // Check whether the existing successor block `n` has been fully
                        // consumed (low(n.c) >= L means all L consumer commits have
                        // occurred) and may therefore be recycled. Acquire pairs with the
                        // Release consumer commits (fetch_add on B::c) in `pop`.
                        //
                        // SAFETY: `n` is non-null — the `||` short-circuits when n is
                        // null, so we only reach this sub-expression when n != null. As a
                        // block in the ring loaded from p.n with Acquire, `n` is a valid
                        // live allocation for the lifetime of `Q`.
                        low((*n).c.load(Ordering::Acquire)) < L
                    }
                {
                    // `n` is either absent or not yet fully consumed. Allocate a new
                    // block (or reuse the spare). Zero-initialization is valid for the
                    // same reasons as the first-block allocation above.
                    //
                    // SAFETY: same as the first-block allocation above.
                    let mut b = b
                        .take()
                        .unwrap_or_else(|| unsafe { Box::new_zeroed().assume_init() });

                    // Link the new block into the ring:
                    //   · If no successor exists (n is null), b_.n = p, forming a
                    //     two-block cycle.
                    //   · If a successor exists but is not yet fully consumed,
                    //     b_.n = n, inserting b_ before n.
                    // b_.c = F::MAX marks it as open to producers (via phead) but
                    // not yet to consumers (sentinel; invariant 1, [C5]).
                    *b.n.get_mut() = if n.is_null() { p } else { n };
                    *b.c.get_mut() = F::MAX;

                    let b_ = Box::into_raw(b);

                    // Advance phead to b_, then link p.n to b_. Both stores are
                    // Release for [C4]: a consumer Acquire-loading B::p and observing
                    // low == L synchronizes-with these stores, guaranteeing that
                    // p.n == b_ and b_.c == F::MAX are visible.
                    //
                    // SAFETY: `b_` is a freshly allocated, exclusively owned block.
                    // `self.p` and `p` are valid as established above. By [C3] we are
                    // the sole thread modifying phead and p.n at this moment.
                    unsafe {
                        self.p.as_ref().store(b_, Ordering::Release);
                        (*p).n.store(b_, Ordering::Release);
                    }
                } else {
                    // `n` is fully consumed (low(n.c) >= L). Recycle it in place.
                    //
                    // The order of the three stores below is significant:
                    //   1. n.p = 0 (Release): reset the producer counter so that new
                    //      producers observing phead == n start claiming from slot 0.
                    //      This must precede the phead advance so producers see a
                    //      clean counter.
                    //   2. phead = n (Release): new producers may now begin pushing
                    //      to n.
                    //   3. n.c = F::MAX (Release): signal to the consumer (the one
                    //      that will claim slot L-1 of the current block p and then
                    //      advance chead) that n is ready for the consumer reset. This
                    //      is stored AFTER the phead advance so that n.p is already
                    //      zeroed before any consumer opens n for reading.
                    //
                    // All three stores are Release for [C4]: a consumer Acquire-loading
                    // B::p and observing low == L synchronizes-with these stores,
                    // making n.p == 0 and n.c == F::MAX visible on arrival.
                    //
                    // SAFETY: `n` is a valid live block confirmed fully consumed.
                    // `self.p` is valid as above. By [C3] we are the sole modifier of
                    // n.p, phead, and n.c at this moment.
                    unsafe {
                        (*n).p.store(0, Ordering::Release);
                        self.p.as_ref().store(n, Ordering::Release);

                        // `n` ready for consumer reset
                        (*n).c.store(F::MAX, Ordering::Release);
                    }
                }
            }

            // Write the element to our exclusively owned slot, then commit by
            // incrementing low(B::p) with Release.
            //
            // SAFETY:
            //   · `p` is valid as established above.
            //   · `a_` is in [0, L): the `a_ >= L` guard above enforces this.
            //   · By [C6], slot a_ is exclusively owned by this producer — the
            //     fetch_add assigned us a unique index that no other thread will
            //     write to or read from before our commit is visible.
            //   · Writing through `UnsafeCell::get()` is sound given exclusive
            //     access. `MaybeUninit::new(e)` fully initializes the slot.
            //   · The Release ordering on the commit (fetch_add on B::p) ensures
            //     the write to a[a_] happens-before any consumer that Acquire-loads
            //     B::p and observes our commit, satisfying [C2] and [C4].
            unsafe {
                (*p).a
                    .get_unchecked(a_ as usize)
                    .get()
                    .write(MaybeUninit::new(e));
                (*p).p.fetch_add(merge(0, 1), Ordering::Release);
            }

            return;
        }
    }

    /// Removes and returns the front element, or [`None`] if no committed element
    /// is currently available.
    ///
    /// Returns [`None`] when the queue is empty **or** when the current block has
    /// no further committed slots at this instant (in-flight producers may still
    /// be writing). After all producers have finished, `None` reliably indicates
    /// an empty queue.
    ///
    /// This operation is lock-free and never parks the calling thread.
    #[doc(alias = "dequeue")]
    #[doc(alias = "recv")]
    pub fn pop(&self) -> Option<T> {
        let backoff = Backoff::new();

        '_0: loop {
            // Load chead with Acquire, pairing with the Release stores that publish
            // or advance chead (the initial store in `push` for the first block, or
            // the chead-advance store in `pop` for subsequent blocks).
            //
            // SAFETY: `self.c` is a `NonNull<AtomicPtr<B<T>>>` backed by a heap
            // allocation that lives for the duration of `Q`, for the same reasons as
            // `self.p` in `push`.
            let c = unsafe { self.c.as_ref().load(Ordering::Acquire).as_ref()? };

            // Load B::c (the consumer counter) for the current block. Acquire pairs
            // with the Release store that last modified B::c: either the chead-advance
            // store that zeroed it (opening this block for consumers), or a consumer
            // commit below.
            let mut r = c.c.load(Ordering::Acquire);

            '_1: loop {
                // r_ = high(B::c) = number of consumer slots already claimed in this block.
                let mut r_ = high(r);

                if r_ >= L {
                    // All L consumer slots have already been claimed by other threads.
                    // The consumer that claimed r_ = L-1 will have advanced (or is about
                    // to advance) chead to the next block. Backoff and reload chead.
                    backoff.snooze();
                    continue '_0;
                }

                // Load B::p to evaluate the stability predicate [C1]. Acquire ordering
                // establishes happens-before with all Release commits made by producers
                // in `push`, satisfying [C2] once v(p) holds.
                let mut p = c.p.load(Ordering::Acquire);

                // [C1] STABILITY PREDICATE: v(p) := high(p) == low(p) || low(p) == L.
                // Until v(p) holds there are in-flight producers: slots in the range
                // [low(p), high(p)) are reserved but not yet written. We must not claim
                // any slot until the block reaches a stable state. `backoff.snooze()`
                // yields the thread to give those producers time to commit.
                let v = |p: F| high(p) == low(p) || low(p) == L;

                if !v(p) {
                    // Spin until stable. Each Acquire reload re-establishes the
                    // happens-before relationship required for [C2].
                    '_2: loop {
                        p = c.p.load(Ordering::Acquire);

                        if v(p) {
                            break;
                        }

                        backoff.snooze();
                    }
                }

                // v(p) now holds. By [C2], every slot in [0, low(p)) contains a fully
                // initialized value. If r_ >= low(p), all committed slots have already
                // been claimed by other consumers — there is nothing left for us in this
                // block. Return None to signal that this block is drained.
                if r_ >= low(p) {
                    return None;
                }

                // Claim a consumer slot. Two paths depending on whether the block is
                // full or partial:
                if low(p) == L {
                    // Full block: unconditional fetch_add. We verified r_ < low(p) = L
                    // above, so at least one slot was available when we checked. Relaxed
                    // ordering is sufficient because the necessary happens-before was
                    // already established by the Acquire load of B::p above ([C2]).
                    let r = high(c.c.fetch_add(merge(1, 0), Ordering::Relaxed));

                    if r >= L {
                        // Another consumer raced ahead and filled all L consumer slots
                        // between our r_ < low(p) check and the fetch_add. Backoff and
                        // reload chead; the chead advance from the winner will have
                        // occurred (or is imminent).
                        backoff.snooze();
                        continue '_0;
                    }

                    r_ = r;
                } else {
                    // Partial block: CAS to prevent over-claiming past low(p). We
                    // increment high(B::c) only if it matches the value we loaded in `r`.
                    // On failure, reload `r` with the fresh value and retry the inner
                    // loop. Relaxed ordering is sufficient (same reasoning as the
                    // full-block path).
                    match c.c.compare_exchange_weak(
                        r,
                        r + merge(1, 0),
                        Ordering::Relaxed,
                        Ordering::Relaxed,
                    ) {
                        Ok(_) => {}
                        Err(rc) => {
                            backoff.spin();

                            r = rc;
                            continue '_1;
                        }
                    }
                }

                // We have exclusively claimed slot r_. Check whether we are the last
                // consumer claimer for this block (r_ == L-1), which makes us
                // responsible for advancing chead.
                //
                // [C5]: r_ < low(p) (passed the None-guard) and r_ + 1 == L together
                // imply low(p) == L (since low(p) ≤ L is invariant). By [C4], the
                // Acquire load of B::p that observed low(p) == L already synchronizes-
                // with the last-claiming producer's Release commit, making the
                // block-transition stores visible: c.n is non-null and
                // c.n.c == F::MAX are both guaranteed on arrival. No spin is needed.
                if r_ + 1 == L {
                    // Load c.n. Acquire pairs with the Release stores in `push` that
                    // wrote p.n (both the allocation and recycle paths). By [C5], the
                    // returned pointer is guaranteed non-null.
                    let n = c.n.load(Ordering::Acquire);

                    // Open the next block for consumers by resetting its consumer
                    // counter to 0, then advance chead.
                    //
                    // SAFETY:
                    //   · `n` is non-null by [C5].
                    //   · `n` is a valid live block in the ring (loaded from c.n with
                    //     Acquire; all ring blocks are live for the lifetime of `Q`).
                    //   · We are the exclusive opener: we are the unique claimer of
                    //     consumer slot r_ = L-1 (uniqueness enforced by the atomic
                    //     fetch_add / CAS above), and n.c == F::MAX (by [C5]) confirms
                    //     no other consumer has already opened this block.
                    //   · Relaxed on the n.c store is sufficient because the Release
                    //     store of chead immediately after establishes visibility: any
                    //     consumer that Acquire-loads chead and reaches block n will
                    //     observe n.c == 0.
                    unsafe {
                        (*n).c.store(0, Ordering::Relaxed);
                        self.c.as_ref().store(n, Ordering::Release);
                    }
                }

                // Read the element from our exclusively owned slot, then commit by
                // incrementing low(B::c) with Release. The Release on the commit makes
                // our read of this slot visible to producers checking low(B::c) for
                // the recycle condition in `push`.
                //
                // SAFETY:
                //   · `r_` is in [0, L): enforced by the r_ >= L guard above and by
                //     the fetch_add / CAS which bound the claimed index to < L.
                //   · By [C6], slot r_ is exclusively owned by this consumer — the
                //     fetch_add / CAS assigned us a unique index that no other thread
                //     may read from concurrently.
                //   · By [C2], slot r_ contains a fully initialized T: the Acquire
                //     load of B::p established happens-before with the producer's
                //     Release commit for that slot, which was itself sequenced-after
                //     the write to a[r_]. `assume_init()` is therefore sound.
                let e = unsafe { (*c).a.get_unchecked(r_ as usize).get().read().assume_init() };
                c.c.fetch_add(merge(0, 1), Ordering::Release);

                return Some(e);
            }
        }
    }
}

impl<T> Clone for UBQ<T> {
    fn clone(&self) -> Self {
        // Increment the shared reference count before constructing the new handle.
        // Relaxed ordering is sufficient: the increment transfers no data —
        // producers and consumers synchronize memory through B::p and B::c in
        // `push`/`pop`. We only need to atomically prevent the count from
        // underflowing while this clone is alive.
        //
        // SAFETY: `self.n` is a `NonNull<CachePadded<AtomicUsize>>` backed by
        // the heap allocation created in `new()`. Because `self` is a live clone
        // the reference count is ≥ 1, so the allocation has not been freed.
        // `as_ref()` borrows it for the lifetime of `self`, which is valid.
        unsafe { self.n.as_ref().fetch_add(1, Ordering::Relaxed) };

        // `NonNull<_>` is `Copy`, so each field clone is a bitwise copy of the
        // raw pointer. All clones share the same phead, chead, and refcount
        // allocations; no data is deep-copied.
        Self {
            p: self.p.clone(),
            c: self.c.clone(),
            n: self.n.clone(),
        }
    }
}

impl<T> Drop for UBQ<T> {
    fn drop(&mut self) {
        // Atomically decrement the reference count. Relaxed ordering is
        // sufficient: the caller is responsible for ensuring all push/pop
        // operations are complete before the last clone is destroyed (typically
        // by joining producer/consumer threads). Under that contract, ownership
        // of any remaining elements is transferred via thread-join
        // synchronization rather than through this atomic.
        //
        // SAFETY: `self.n` is a valid `NonNull<CachePadded<AtomicUsize>>` for
        // the same reasons as in `Clone::clone`.
        let n = unsafe { self.n.as_ref().fetch_sub(1, Ordering::Relaxed) };

        if n == 1 {
            // We were the last clone (count dropped from 1 to 0). No other
            // thread holds a `UBQ` handle, so we have exclusive access to all
            // three shared allocations and to every block in the ring.
            //
            // Capture chead before freeing self.c's allocation. `get_mut()`
            // bypasses atomic operations; sound here because exclusive access
            // is guaranteed (no other clone exists, no concurrent push/pop).
            //
            // SAFETY: `self.c` is a valid `NonNull<CachePadded<AtomicPtr<B<T>>>>`.
            // `as_mut()` requires exclusive access, which holds because `n == 1`
            // before the fetch_sub guarantees we are the sole owner.
            let mut b = unsafe { *self.c.as_mut().get_mut() };

            // Reclaim the three meta-allocations (refcount, phead, chead boxes).
            // Each pointer was produced by `Box::into_raw` in `new()` and has
            // not been freed — the previous reference count of 1 ensures this
            // is the first and only reclamation.
            //
            // SAFETY: All three raw pointers are valid, non-null, and
            // exclusively owned at this point.
            unsafe {
                drop(Box::from_raw(self.n.as_ptr()));
                drop(Box::from_raw(self.p.as_ptr()));
                drop(Box::from_raw(self.c.as_ptr()));
            }

            // If no block was ever allocated (queue was created but never
            // pushed to), chead is null and there is nothing to traverse.
            if b.is_null() {
                return;
            }

            // Traverse the circular block ring starting from chead and drop
            // every unconsumed element. We stop when we return to the starting
            // block (b == b_) or hit a null next pointer (ring partially
            // formed on early teardown).
            let b_ = b;

            loop {
                // Read the next pointer and both counters directly, bypassing
                // atomic operations — valid because we have exclusive access.
                //
                // SAFETY: `b` is non-null (checked above; each subsequent
                // value is a valid ring block's `n` pointer) and points to a
                // live `B<T>` allocation.
                let n = unsafe { *(*b).n.get_mut() };

                let p = unsafe { *(*b).p.get_mut() };
                let c = unsafe { *(*b).c.get_mut() };

                debug_assert!(
                    high(p) == low(p) || low(p) == L,
                    "all producers should be finished before dropping UBQ (p = {}:{})",
                    high(p),
                    low(p)
                );
                debug_assert!(
                    high(c) == low(c) || low(c) == L,
                    "all consumers should be finished before dropping UBQ (c = {}:{})",
                    high(c),
                    low(c),
                );

                // Drop unconsumed elements in this block. The live range of
                // initialized, unconsumed slots is:
                //   · [0, low(p))        when c == F::MAX — block was never
                //     opened to consumers; every produced value is unconsumed.
                //   · [low(c), low(p))   otherwise — low(c) committed consumer
                //     reads have already moved [0, low(c)) out; the remaining
                //     range [low(c), low(p)) holds initialized unconsumed Ts.
                //
                // By [C2], every slot in [0, low(p)) contains a fully
                // initialized T. The debug_asserts above confirm no in-flight
                // producers remain, so low(p) accurately reflects all slots
                // written in this block.
                for i in if c == F::MAX { 0 } else { low(c) }..low(p) {
                    // SAFETY:
                    //   · `b` is a valid `*mut B<T>` as established above.
                    //   · `i` is in [0, low(p)) ⊆ [0, L); the offset is in
                    //     bounds of the `a` array.
                    //   · Slot `i` holds an initialized T (by [C2] and the
                    //     range argument above).
                    //   · Exclusive access is guaranteed (last-clone
                    //     exclusivity); no concurrent reads or writes occur.
                    //   · `cast::<T>()` is valid: `UnsafeCell<MaybeUninit<T>>`
                    //     is `repr(transparent)` over `MaybeUninit<T>`, which
                    //     is `repr(transparent)` over `T`; layout matches.
                    unsafe {
                        (*b).a
                            .as_mut_ptr_range()
                            .start
                            .add(i as usize)
                            .cast::<T>()
                            .drop_in_place()
                    }
                }

                b = n;

                if b.is_null() || b == b_ {
                    break;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fmt::{Debug, Write},
        sync::{
            Arc,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        },
        thread::{self, JoinHandle},
        usize,
    };

    use super::*;

    impl<T> Debug for UBQ<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            let p = unsafe { *self.p.as_ref().as_ptr() };

            if p.is_null() {
                return writeln!(f, "UBQ {{}}");
            }

            let mut s = String::new();
            let mut c = p;

            let fmt = |u: F| -> String {
                if u == F::MAX {
                    format!("full:full")
                } else {
                    format!("{:04}:{:04}", high(u), low(u))
                }
            };

            loop {
                let p_ = unsafe { *(*c).p.as_ptr() };
                let c_ = unsafe { *(*c).c.as_ptr() };

                write!(s, "\t{c:p}: p={}, c={}", fmt(p_), fmt(c_))?;

                c = unsafe { *(*c).n.as_ptr() };
                if c == p {
                    break;
                }

                write!(s, "\n")?;
            }

            write!(f, "UBQ {{\n{s}\t}}")
        }
    }

    struct DropProbe {
        dropped: Arc<AtomicUsize>,
    }

    impl DropProbe {
        fn new(dropped: Arc<AtomicUsize>) -> Self {
            Self { dropped }
        }
    }

    impl Drop for DropProbe {
        fn drop(&mut self) {
            self.dropped.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn drop_releases_all_enqueued_values() {
        let token = Arc::new(());
        let n = (L as usize * 3) + 7;

        for _ in 0..16 {
            let q = UBQ::new();

            for _ in 0..n {
                q.push(token.clone());
            }

            assert_eq!(Arc::strong_count(&token), n + 1);

            println!("q: {q:?}");

            drop(q);

            assert_eq!(Arc::strong_count(&token), 1);
        }
    }

    #[test]
    fn drop_of_final_clone_drops_items_left_in_queue() {
        let dropped = Arc::new(AtomicUsize::new(0));

        let q = UBQ::new();
        let q1 = q.clone();
        let q2 = q.clone();

        let total = (L as usize * 2) + 5;
        let popped = (L as usize / 2) + 1;

        for _ in 0..(L as usize + 1) {
            q.push(DropProbe::new(dropped.clone()));
        }
        for _ in 0..(total - (L as usize + 1)) {
            q1.push(DropProbe::new(dropped.clone()));
        }

        let mut held = Vec::with_capacity(popped);
        for _ in 0..popped {
            held.push(
                q2.pop()
                    .expect("queue should contain enough elements for this test"),
            );
        }

        drop(q);
        drop(q1);

        assert_eq!(dropped.load(Ordering::SeqCst), 0);

        let remaining = total - popped;
        drop(q2);

        assert_eq!(dropped.load(Ordering::SeqCst), remaining);

        drop(held);

        assert_eq!(dropped.load(Ordering::SeqCst), total);
    }

    #[test]
    fn fill_drain_ordered() {
        let q = UBQ::new();

        let m = 1_000_000;
        for i in 0..m {
            q.push(i);
        }

        for i in 0..m {
            assert_eq!(q.pop(), Some(i));
        }
    }

    #[test]
    fn mpmc_4p4c() {
        let q = UBQ::new();

        let flag = Arc::new(AtomicBool::new(true));

        let pf = |q: UBQ<usize>, m: usize| -> JoinHandle<()> {
            thread::spawn(move || {
                for i in 0..m {
                    q.push(i);
                }
            })
        };

        let cf = |q: UBQ<usize>, m: usize| -> JoinHandle<()> {
            let flag = flag.clone();

            thread::spawn(move || {
                for _ in 0..m {
                    loop {
                        if flag.load(Ordering::Acquire) {
                            if q.pop().is_some() {
                                break;
                            }
                        } else {
                            assert!(q.pop().is_some());
                            break;
                        }
                    }
                }
            })
        };

        let m = 1_000_001;
        let v: Vec<_> = (0..4)
            .map(|_| (pf(q.clone(), m), cf(q.clone(), m)))
            .collect();

        let v: Vec<_> = v
            .into_iter()
            .map(|(p, c)| {
                p.join().unwrap();
                c
            })
            .collect();

        flag.store(false, Ordering::Release);

        for c in v {
            c.join().unwrap()
        }
    }
}
