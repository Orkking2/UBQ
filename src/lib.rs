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
    num::NonZeroUsize,
    ptr::NonNull,
    sync::atomic::{AtomicPtr, AtomicU32, AtomicUsize, Ordering},
};

/// Atomic storage for a packed pair of counters (`high` = claimed, `low` = committed).
type A = AtomicU32;

/// Packed `F::BITS`-bit counter representation manipulated through [`A`].
/// Layout: upper `H::BITS` bits = claimed count, lower `H::BITS` bits = committed count.
type F = u32;
/// Single `H::BITS`-bit half of a packed [`F`] counter.
type H = u16;

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
    /// Shared inner state allocation.
    i: NonNull<I<T>>,
}

// SAFETY: All shared mutable state is accessed through `AtomicPtr` and [`A`]
// operations, or through the `UnsafeCell` slots in `B::a`, which are protected by
// the exclusive-index guarantee [C6] and the Release–Acquire pairing on `B::p` and
// `B::c`. Given `T: Sync` (resp. `T: Send`), concurrent shared access (resp.
// cross-thread transfer) of `T` values through `UBQ` is therefore sound.
unsafe impl<T: Sync> Sync for UBQ<T> {}
unsafe impl<T: Send> Send for UBQ<T> {}

/// Number of element slots per block.
pub const L: H = 32;
const _: () = assert!(
    L != H::MAX,
    "L cannot be H::MAX, as this value is used as a sentinel."
);

/// Returns the lower `H::BITS` bits of a packed counter (committed count).
#[inline(always)]
const fn low(r: F) -> H {
    r as H
}

/// Returns the upper `H::BITS` bits of a packed counter (claimed count).
#[inline(always)]
const fn high(r: F) -> H {
    (r >> H::BITS) as H
}

/// Packs two [`H`] values into a [`F`]: `h` in the upper `H::BITS` bits, `l` in the lower.
#[inline(always)]
const fn merge(h: H, l: H) -> F {
    (h as F) << H::BITS | l as F
}

/// # [C1] STABILITY PREDICATE:
/// `stab(u) := high(u) == low(u) || low(u) == L`
/// Until stab(u) holds there are in-flight producers (resp. consumers):
/// slots in the range [low(u), high(u)) are allocated (resp. reserved)
/// but not yet written (resp. read). We must not claim any slot until the
/// block reaches a stable state. `backoff.snooze()` yields the thread to
/// give those producers (resp. consumers) time to commit (resp. consume).
#[inline(always)]
const fn stab(u: F) -> bool {
    high(u) == low(u) || low(u) == L
}

/// Shared UBQ state pointed to by every [`UBQ<T>`] handle.
struct I<T> {
    /// Atomic pointer to phead: the block currently accepting producer pushes.
    p: CachePadded<AtomicPtr<B<T>>>,
    /// Atomic pointer to chead: the block currently being drained by consumers.
    c: CachePadded<AtomicPtr<B<T>>>,
    /// Shared reference count stored as `copies - 1`.
    ///
    /// * `0` means exactly one handle exists.
    /// * `k` means `k + 1` handles exist.
    n: CachePadded<AtomicUsize>,
}

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

impl<T> Drop for B<T> {
    fn drop(&mut self) {
        let (p, c) = unsafe { (*self.p.as_ptr(), *self.c.as_ptr()) };

        debug_assert!(
            high(p) == low(p) || low(p) == L,
            "all producers should be finished before dropping B (p = {}:{})",
            high(p),
            low(p)
        );
        debug_assert!(
            high(c) == low(c) || low(c) == L,
            "all consumers should be finished before dropping B (c = {}:{})",
            high(c),
            low(c),
        );

        // Drop unconsumed elements in this block. The live range of
        // initialized, unconsumed slots is:
        //	 · [0, 0) 			  when p == F::MAX: block is marked
        //	   for destruction during queue operation; it is guaranteed
        //     to be empty.
        //
        //   · [0, low(p))        when c == F::MAX: block was never
        //     opened to consumers; every produced value is unconsumed.
        //
        //   · [low(c), low(p))   else: low(c) committed consumer
        //     reads have already moved [0, low(c)) out; the remaining
        //     range [low(c), low(p)) holds initialized unconsumed Ts.
        let z = |u: F| if u == F::MAX { 0 } else { low(u) };

        for i in z(c)..z(p) {
            unsafe {
                self.a
                    .get_unchecked_mut(i as usize)
                    .get_mut()
                    .as_mut_ptr()
                    .drop_in_place();
            }
        }
    }
}

impl<T> UBQ<T> {
    /// Creates a new, empty queue.
    ///
    /// No blocks are allocated until the first call to [`push`](Self::push).
    #[inline]
    pub fn new() -> Self {
        // SAFETY:
        //   · `I<T>` is valid when zero-initialized: `p` and `c` are
        //     `AtomicPtr<B<T>>` (null is a valid initial value) and `n` is an
        //     `AtomicUsize` (0, representing one live copy, is valid).
        //   · `Box::into_raw` on a non-empty `Box` is always non-null.
        unsafe {
            Self {
                i: NonNull::new_unchecked(Box::into_raw(Box::new_zeroed().assume_init())),
            }
        }
    }

    /// Returns a best-effort snapshot of how many [`UBQ`] handles currently exist.
    ///
    /// This value may change immediately due to concurrent clone/drop activity.
    #[inline]
    pub fn copies(&self) -> usize {
        // SAFETY: `self.i` points to the shared inner allocation, which remains
        // live while this handle exists.
        unsafe { self.i.as_ref().copies() }
    }

    /// Pushes `e` onto the back of the queue.
    ///
    /// This operation is lock-free and never parks the calling thread.  It may
    /// spin briefly at block boundaries while in-flight producers commit their
    /// writes.
    #[doc(alias = "enqueue")]
    #[doc(alias = "send")]
    #[inline]
    pub fn push(&self, e: T) {
        // SAFETY: `self.i` points to the shared inner allocation, which remains
        // live while this handle exists.
        unsafe { self.i.as_ref().push(e) }
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
    #[inline]
    pub fn pop(&self) -> Option<T> {
        // SAFETY: `self.i` points to the shared inner allocation, which remains
        // live while this handle exists.
        unsafe { self.i.as_ref().pop() }
    }

    /// Free (as opposed to recycle) all the empty blocks in this queue.
    /// This function returns Some(n) where n is the number of blocks freed
    /// or None (0) if no blocks are freed.
    ///
    /// # SAFETY:
    ///
    /// The caller must ensure that there are no in-flight push's or pop's
    /// during the execution of this function, as it is theoretically possible,
    /// although rare, for a producer or consumer to have a stale pointer to
    /// a block that is being freed (causing a Seg. Fault). With slow producers
    /// and consumers, it's probably safe to call this method, but at your own risk!
    pub unsafe fn shrink(&self) -> Option<NonZeroUsize> {
        // SAFETY: `self.i` points to the shared inner allocation, which remains
        // live while this handle exists.
        unsafe { self.i.as_ref().shrink() }
    }

    /// Returns `true` if this UBQ contains no values.
    pub fn is_empty(&self) -> bool {
        // SAFETY: `self.i` points to the shared inner allocation, which remains
        // live while this handle exists.
        unsafe { self.i.as_ref().is_empty() }
    }
}

impl<T> Clone for UBQ<T> {
    fn clone(&self) -> Self {
        // `n` stores copies - 1. `fetch_add(1)` therefore increments the number
        // of live handles by one.
        unsafe { self.i.as_ref().n.fetch_add(1, Ordering::Relaxed) };

        // `NonNull<_>` is `Copy`; clones share the same `I<T>` allocation.
        Self { i: self.i }
    }
}

impl<T> Drop for UBQ<T> {
    fn drop(&mut self) {
        // `n` stores copies - 1. `fetch_sub(1)` returns the previous n.
        //
        // Last-owner case:
        //   previous n == 0  <=>  previous copies == 1.
        // We intentionally allow the atomic to wrap to `usize::MAX` here; the
        // inner allocation is reclaimed immediately after and cannot be observed.
        let n = unsafe { self.i.as_ref().n.fetch_sub(1, Ordering::Relaxed) };

        if n == 0 {
            // Reclaim the shared inner state and drop any queued elements.
            //
            // SAFETY:
            //   · `n == 0` implies this was the final handle.
            //   · `self.i` came from `Box::into_raw` in `new()` and has not yet
            //     been reclaimed.
            unsafe { drop(Box::from_raw(self.i.as_ptr())) }
        }
    }
}

impl<T> I<T> {
    #[inline]
    fn copies(&self) -> usize {
        self.n.load(Ordering::Relaxed).wrapping_add(1)
    }

    fn is_empty(&self) -> bool {
        let p = self.p.load(Ordering::Relaxed);

        if p.is_null() {
            return true;
        }

        let c = self.c.load(Ordering::Relaxed);

        if p != c {
            return false;
        }

        let b = unsafe { &*p };

        let p = b.p.load(Ordering::Relaxed);
        let c = b.c.load(Ordering::Relaxed);

        low(p) == low(c)
    }
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
impl<T> I<T> {
    /// All blocks between phead and chead MUST be empty, let's remove them.
    unsafe fn shrink(&self) -> Option<NonZeroUsize> {
        // If no blocks have been allocated yet, nothing to do.
        let p_ = self.p.load(Ordering::Acquire);

        if p_.is_null() {
            return None;
        }

        let f_ = unsafe { (*p_).n.load(Ordering::Relaxed) };

        // ── Single-block case: n is null, block is not part of a ring ────────
        if f_.is_null() {
            let b = unsafe { &*p_ };
            let (p, c) = (b.p.load(Ordering::Relaxed), b.c.load(Ordering::Relaxed));

            // Abort if any in-flight operations are detected.
            if !stab(p) || !stab(c) {
                return None;
            }

            // A block is empty when everything pushed to it has been consumed,
            // or when nothing was ever pushed (pre-allocated, c still sentinel).
            if low(p) != if c == F::MAX { 0 } else { low(c) } {
                return None;
            }

            // Mark for destruction so any stray accessor can detect invalidity,
            // then clear both head pointers and free the block.
            // If our CAS fails, a producer has changed p, so we should exit.
            if b.p
                .compare_exchange(p, F::MAX, Ordering::Release, Ordering::Relaxed)
                .is_err()
            {
                return None;
            }
            // Block has been successfully marked for destruction.
            self.p.store(std::ptr::null_mut(), Ordering::Release);
            self.c.store(std::ptr::null_mut(), Ordering::Release);
            unsafe { drop(Box::from_raw(p_)) };

            return NonZeroUsize::new(1);
        }

        // ── Multi-block ring ─────────────────────────────────────────────────
        // Snapshot chead before any modifications.
        let c_ = self.c.load(Ordering::Relaxed);
        let mut f = 0usize;

        // Key invariant: push advances phead into a block only when that block
        // has been fully consumed (low(n.c) >= L).  Therefore every block in
        // the segment  phead.n → … → chead  is guaranteed to be empty — no
        // per-block emptiness checks are needed inside the free loop.
        //
        // We exploit this to determine the final queue state up-front and
        // update q.p / q.c / phead.n *before* freeing anything, so that any
        // stray concurrent accessor (despite the safety contract) always sees
        // a coherent queue after the head-pointer stores complete.
        //
        // Three cases, resolved before the loop:
        //
        //   1. phead == chead, phead empty  →  q.p = q.c = null;
        //      free every block in the ring (recycled segment + phead).
        //
        //   2. phead == chead, phead not empty  →  phead.n = null;
        //      free only the recycled segment (first_n … back to phead).
        //
        //   3. phead != chead  →  phead.n = chead (shortcut over the segment);
        //      free only the recycled segment (first_n … up to chead).

        if p_ == c_ {
            // phead is the only block that may still carry live elements.
            let p = unsafe { (*p_).p.load(Ordering::Relaxed) };
            let c = unsafe { (*p_).c.load(Ordering::Relaxed) };

            if !stab(p) || !stab(c) {
                return None;
            }

            let e = low(p) == if c == F::MAX { 0 } else { low(c) };

            // A block is empty when everything pushed to it has been consumed,
            // or when nothing was ever pushed (pre-allocated, c still sentinel).
            if !e {
                // Case 1: mark phead and null both heads before freeing.
                // Mark for destruction so any stray accessor can detect invalidity,
                // then clear both head pointers and free the block.
                // If our CAS fails, a producer has changed p, so we should exit.
                if unsafe {
                    (*p_)
                        .p
                        .compare_exchange(p, F::MAX, Ordering::Release, Ordering::Relaxed)
                        .is_err()
                } {
                    return None;
                }
                self.p.store(std::ptr::null_mut(), Ordering::Release);
                self.c.store(std::ptr::null_mut(), Ordering::Release);
            } else {
                // Case 2: detach the recycled segment from phead.
                unsafe { (*p_).n.store(std::ptr::null_mut(), Ordering::Release) };
            }

            // Free the recycled segment: first_n → … → (block whose .n == phead).
            // Termination: we stop as soon as after == phead, then (in case 1
            // only) free phead itself.
            let mut cur = f_;
            loop {
                let after = unsafe { (*cur).n.load(Ordering::Relaxed) };
                unsafe { (*cur).p.store(F::MAX, Ordering::Release) };
                unsafe { drop(Box::from_raw(cur)) };
                f += 1;
                if after == p_ {
                    break;
                }
                cur = after;
            }

            if e {
                unsafe { drop(Box::from_raw(p_)) };
                f += 1;
            }
        } else {
            // Case 3: shortcut phead directly to chead, then free the segment.
            unsafe { (*p_).n.store(c_, Ordering::Release) };

            let mut cur = f_;
            loop {
                if cur == c_ {
                    break;
                }
                let after = unsafe { (*cur).n.load(Ordering::Relaxed) };
                unsafe { (*cur).p.store(F::MAX, Ordering::Release) };
                unsafe { drop(Box::from_raw(cur)) };
                f += 1;
                cur = after;
            }
        }

        NonZeroUsize::new(f)
    }

    fn push(&self, e: T) {
        let backoff = Backoff::new();
        // Spare block carried across loop iterations. Avoids a redundant allocation
        // when we lose a CAS race during first-block initialization.
        let mut b = None;

        '_0: loop {
            // Load phead with Acquire, pairing with the Release stores that publish
            // or advance phead (the CAS below on first push, or the block-transition
            // stores on subsequent iterations).
            let mut p = self.p.load(Ordering::Acquire);

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

                match self
                    .p
                    .compare_exchange_weak(p, n, Ordering::AcqRel, Ordering::Acquire)
                {
                    Ok(_) => {
                        // We won the race. By invariant 1, the first block is
                        // immediately open to both producers and consumers, so we
                        // publish it as chead too.
                        self.c.store(n, Ordering::Release);

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
                        // live allocation for the lifetime of `I`.
                        low((*n).c.load(Ordering::Acquire)) < L
                    }
                    || unsafe {
                        // SAFETY: See above.
                        let p = (*n).p.load(Ordering::Relaxed);

                        if p == F::MAX {
                            // This block is being freed by Self::shrink
                            false
                        } else {
                            // We must ensure that we are not overriding
                            // Self::shrink's setting p to the sentinel F::MAX
                            //
                            // Read more in the `else` block below.
                            (*n).p
                                .compare_exchange(p, 0, Ordering::Release, Ordering::Relaxed)
                                .is_ok()
                        }
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
                    // `p` is valid as established above. By [C3] we are the sole
                    // thread modifying phead and p.n at this moment.
                    unsafe {
                        self.p.store(b_, Ordering::Release);
                        (*p).n.store(b_, Ordering::Release);
                    }
                } else {
                    // `n` is fully consumed (low(n.c) >= L). Recycle it in place.
                    //
                    // The order of the stores below is significant:
                    //   1. n.p = 0 (Release): reset the producer counter so that new
                    //      producers observing phead == n start claiming from slot 0.
                    //      This must precede the phead advance so producers see a
                    //      clean counter. This is now handled in the conditional,
                    //		instead of a raw `store`, we must CAS to not step on the
                    // 		toes of Self::shrink, it still happens previously in time
                    // 		to (2) and (3).
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
                    // By [C3] we are the sole modifier of n.p, phead, and n.c at
                    // this moment.
                    unsafe {
                        self.p.store(n, Ordering::Release);

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

    fn pop(&self) -> Option<T> {
        let backoff = Backoff::new();

        '_0: loop {
            // Load chead with Acquire, pairing with the Release stores that publish
            // or advance chead (the initial store in `push` for the first block, or
            // the chead-advance store in `pop` for subsequent blocks).
            //
            // SAFETY: `chead` points at a live block while `I<T>` is alive.
            let c = unsafe { self.c.load(Ordering::Acquire).as_ref()? };

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

                // Somehow we've ended up in a block that is marked to be freed, retry.
                if p == F::MAX {
                    continue '_0;
                }

                if !stab(p) {
                    // Spin until stable. Each Acquire reload re-establishes the
                    // happens-before relationship required for [C2].
                    '_2: loop {
                        p = c.p.load(Ordering::Acquire);

                        if stab(p) {
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
                        self.c.store(n, Ordering::Release);
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

impl<T> Drop for I<T> {
    fn drop(&mut self) {
        // Capture chead. `get_mut()` bypasses atomics and is valid under
        // last-owner exclusivity.
        let mut b = *self.c.get_mut();

        // If no block was ever allocated, there is nothing to reclaim.
        if b.is_null() {
            return;
        }

        // Traverse the circular block ring starting from chead and drop every
        // unconsumed element. Stop when we return to the start (b == b_) or hit
        // a null next pointer (partially formed ring on teardown).
        let b_ = b;

        loop {
            // Read metadata directly, bypassing atomics under exclusive access.
            let n = unsafe { *(*b).n.get_mut() };

            // Free this block
            drop(unsafe { Box::from_raw(b) });

            b = n;

            if b.is_null() || b == b_ {
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fmt::{Debug, Write},
        ptr,
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
            let p = unsafe { self.i.as_ref().p.load(Ordering::Acquire) };

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

    #[test]
    fn is_empty_returns_correctly() {
        assert!(UBQ::<()>::new().is_empty());

        for m in 1_000..1_005 {
            let q = UBQ::new();

            for i in 0..m {
                q.push(i);
            }

            for _ in 0..m {
                q.pop().unwrap();
            }

            assert!(q.is_empty())
        }
    }

    #[test]
    fn shrink_removes_all_in_empty_queue() {
        let q = UBQ::new();

        let m = 1_000;
        for i in 0..m {
            q.push(i);
        }

        for _ in 0..m {
            q.pop().unwrap();
        }

        // No in-flight push's or pop's
        unsafe { q.shrink() };

        assert!(unsafe { *q.i.as_ref().p.as_ptr() } == ptr::null_mut())
    }
}
