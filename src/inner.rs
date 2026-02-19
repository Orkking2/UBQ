use crate::{
    block::B,
    packed::{F, L, high, low, merge, stab},
};
use crossbeam_utils::{Backoff, CachePadded};
use std::{
    mem::MaybeUninit,
    num::NonZeroUsize,
    sync::atomic::{AtomicPtr, AtomicUsize, Ordering},
};

/// Shared UBQ state pointed to by every [`UBQ<T>`] handle.
pub struct I<T> {
    /// Atomic pointer to phead: the block currently accepting producer pushes.
    pub p: CachePadded<AtomicPtr<B<T>>>,
    /// Atomic pointer to chead: the block currently being drained by consumers.
    pub c: CachePadded<AtomicPtr<B<T>>>,
    /// Shared reference count stored as `copies - 1`.
    ///
    /// * `0` means exactly one handle exists.
    /// * `k` means `k + 1` handles exist.
    pub n: CachePadded<AtomicUsize>,
}

impl<T> I<T> {
    #[inline]
    pub fn copies(&self) -> usize {
        self.n.load(Ordering::Relaxed).wrapping_add(1)
    }

    pub fn is_empty(&self) -> bool {
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
    pub unsafe fn shrink(&self) -> Option<NonZeroUsize> {
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
            if e {
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

    pub fn push(&self, e: T) {
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

    pub fn pop(&self) -> Option<T> {
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
