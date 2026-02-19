use crate::inner::I;
use std::{num::NonZeroUsize, ptr::NonNull, sync::atomic::Ordering};

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
    pub(crate) i: NonNull<I<T>>,
}

// SAFETY: All shared mutable state is accessed through `AtomicPtr` and [`A`]
// operations, or through the `UnsafeCell` slots in `B::a`, which are protected by
// the exclusive-index guarantee [C6] and the Release–Acquire pairing on `B::p` and
// `B::c`. Given `T: Sync` (resp. `T: Send`), concurrent shared access (resp.
// cross-thread transfer) of `T` values through `UBQ` is therefore sound.
unsafe impl<T: Sync> Sync for UBQ<T> {}
unsafe impl<T: Send> Send for UBQ<T> {}

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
        self.get_iref().copies()
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
        self.get_iref().push(e);
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
        self.get_iref().pop()
    }

    /// Free (as opposed to recycle) all the empty blocks in this queue.
    /// This function returns Some(n) where n is the number of blocks freed
    /// or None (0) if no blocks are freed.
    ///
    /// # SAFETY:
    ///
    /// The caller must ensure that there are no in-flight push's or pop's
    /// during the execution of this function. UBQ takes *every* precaution
    /// without sacrificing performance to enable this cleanup, but it is the
    /// caller's responsibilty to call it when no other operations are in
    /// progress, as it is technically UB for shrink to be called during other
    /// operations.
    pub unsafe fn shrink(&self) -> Option<NonZeroUsize> {
        self.get_iref().shrink()
    }

    /// Returns `true` if this UBQ contains no values.
    pub fn is_empty(&self) -> bool {
        self.get_iref().is_empty()
    }

    /// Utility function to get the underlying `self.i` as a reference.
    /// This function is safe, see the safety comment within for details.
    fn get_iref(&self) -> &I<T> {
        // SAFETY: `self.i` points to the shared inner allocation, which remains
        // live while this handle exists.
        unsafe { self.i.as_ref() }
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
