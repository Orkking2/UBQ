use crate::{
    align::A4096,
    backoff::{BackoffPolicy, Crossbeam},
    block::{Block, DEFAULT_BLOCK_LENGTH, WRITE},
    variant::{Balanced, PrepareMode, Variant},
};
use crossbeam_utils::CachePadded;
use std::{
    array, fmt,
    marker::PhantomData,
    mem::{ManuallyDrop, MaybeUninit},
    ops::DerefMut,
    ptr::null_mut,
    sync::{
        Arc,
        atomic::{AtomicPtr, AtomicUsize, Ordering, fence},
    },
};

/// Default number of pooled blocks retained by [`crate::UBQ`].
pub const DEFAULT_POOL_SIZE: usize = 1;

/// A lock-free, unbounded multi-producer/multi-consumer (MPMC) queue.
///
/// `ConfiguredUBQ` is the fully-configurable queue type. The crate-level
/// [`crate::UBQ`] alias preserves the default configuration.
///
/// ```rust
/// use ubq::{ConfiguredUBQ, align, backoff, variant};
///
/// let q = ConfiguredUBQ::<u64, variant::Balanced, backoff::Crossbeam, 2, 127, align::A256>::new();
/// q.push(42);
/// assert_eq!(q.pop(), Some(42));
/// ```
///
/// ```compile_fail
/// use ubq::{ConfiguredUBQ, align, backoff, variant};
///
/// let _ = ConfiguredUBQ::<u64, variant::Balanced, backoff::Crossbeam, 1, 1024, align::A512>::new();
/// ```
///
/// ```compile_fail
/// use ubq::{ConfiguredUBQ, backoff, variant};
///
/// #[repr(align(64))]
/// struct BadAlign([u8; 8]);
///
/// let _ = ConfiguredUBQ::<u64, variant::Balanced, backoff::Crossbeam, 1, 31, BadAlign>::new();
/// ```
pub struct ConfiguredUBQ<
    T,
    V = Balanced,
    B = Crossbeam,
    const POOL: usize = DEFAULT_POOL_SIZE,
    const BLOCK: usize = DEFAULT_BLOCK_LENGTH,
    A = A4096,
> {
    /// Atomic pointer to phead: the block currently accepting producer pushes.
    phead: CachePadded<AtomicUsize>,
    /// Atomic pointer to chead: the block currently being drained by consumers.
    chead: CachePadded<AtomicUsize>,
    /// Recycled blocks used to avoid repeated allocations.
    pool: [CachePadded<AtomicPtr<Block<T, BLOCK, A>>>; POOL],

    _variant: PhantomData<V>,
    _backoff: PhantomData<B>,
}

struct Head<T, const BLOCK: usize, A> {
    block: *mut Block<T, BLOCK, A>,
    index: usize,
}

#[inline]
fn drop_spare_block<T, const BLOCK: usize, A>(block: *mut Block<T, BLOCK, A>) {
    let _ = unsafe { Box::from_raw(block.cast::<ManuallyDrop<Block<T, BLOCK, A>>>()) };
}

impl<T, const BLOCK: usize, A> Copy for Head<T, BLOCK, A> {}

impl<T, const BLOCK: usize, A> Clone for Head<T, BLOCK, A> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T, const BLOCK: usize, A> Head<T, BLOCK, A> {
    #[inline]
    fn mask() -> usize {
        Block::<T, BLOCK, A>::block_mask()
    }

    fn new(u: usize) -> Self {
        let mask = Self::mask();
        Self {
            block: (u & !mask) as *mut Block<T, BLOCK, A>,
            index: u & mask,
        }
    }

    fn is_zero(&self) -> bool {
        self.index == 0 && self.block.is_null()
    }

    fn pack(self) -> usize {
        self.block.addr() | self.index
    }
}

// SAFETY: Slot ownership is assigned with atomic counters, and producer/consumer
// commits are synchronized with Release/Acquire ordering before cross-thread reads.
unsafe impl<T: Sync, V, B, A: Sync, const POOL: usize, const BLOCK: usize> Sync
    for ConfiguredUBQ<T, V, B, POOL, BLOCK, A>
{
}
unsafe impl<T: Send, V, B, A: Send, const POOL: usize, const BLOCK: usize> Send
    for ConfiguredUBQ<T, V, B, POOL, BLOCK, A>
{
}

impl<T, V, B, const POOL: usize, const BLOCK: usize, A> fmt::Debug
    for ConfiguredUBQ<T, V, B, POOL, BLOCK, A>
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.pad("ConfiguredUBQ { .. }")
    }
}

impl<T, V, B, const POOL: usize, const BLOCK: usize, A> ConfiguredUBQ<T, V, B, POOL, BLOCK, A>
where
    V: Variant,
    B: BackoffPolicy,
{
    const LAYOUT_CHECKS: () = Block::<T, BLOCK, A>::LAYOUT_CHECKS;

    /// Number of retained pooled blocks.
    pub const POOL_SIZE: usize = POOL;
    /// Number of slots in each block for this queue type.
    pub const BLOCK_LENGTH: usize = BLOCK;

    #[inline]
    fn pool_has_vacancy(&self) -> bool {
        self.pool
            .iter()
            .any(|b| b.load(Ordering::Relaxed).is_null())
    }

    #[inline]
    fn pool_is_empty(&self) -> bool {
        self.pool
            .iter()
            .all(|b| b.load(Ordering::Relaxed).is_null())
    }

    #[inline]
    fn should_prepare_next_block(&self, next_index: usize) -> bool {
        let () = Self::LAYOUT_CHECKS;

        match V::PREPARE_MODE {
            PrepareMode::BoundaryOnly => next_index == BLOCK,
            PrepareMode::BoundaryIfPoolHasVacancy => next_index == BLOCK && self.pool_has_vacancy(),
            PrepareMode::BoundaryIfPoolEmpty => next_index == BLOCK && self.pool_is_empty(),
            PrepareMode::BoundaryOrPoolHasVacancy => next_index == BLOCK || self.pool_has_vacancy(),
        }
    }

    #[inline]
    fn try_store_pooled_block(&self, block: *mut Block<T, BLOCK, A>) -> bool {
        self.pool.iter().any(|slot| {
            slot.compare_exchange(null_mut(), block, Ordering::Release, Ordering::Relaxed)
                .is_ok()
        })
    }

    #[inline]
    fn take_pooled_block(&self) -> Option<*mut Block<T, BLOCK, A>> {
        if !(V::RECYCLE_PRODUCER_SPARE || V::RECYCLE_CONSUMED) {
            return None;
        }

        self.pool.iter().find_map(|slot| {
            let pooled = slot.swap(null_mut(), Ordering::AcqRel);
            (!pooled.is_null()).then_some(pooled)
        })
    }

    #[inline]
    fn release_producer_spare_block(&self, block: Box<Block<T, BLOCK, A>>) {
        let new = Box::into_raw(block);

        if V::RECYCLE_PRODUCER_SPARE && self.try_store_pooled_block(new) {
            return;
        }

        drop_spare_block(new);
    }

    #[inline]
    fn release_consumed_block(&self, block: *mut Block<T, BLOCK, A>) {
        if V::RECYCLE_CONSUMED && self.try_store_pooled_block(block) {
            return;
        }

        drop_spare_block(block);
    }

    /// Creates a new, empty queue.
    ///
    /// No blocks are allocated until the first call to [`push`](Self::push).
    #[inline]
    pub fn new() -> Self {
        let () = Self::LAYOUT_CHECKS;

        Self {
            phead: CachePadded::new(AtomicUsize::new(0)),
            chead: CachePadded::new(AtomicUsize::new(0)),
            pool: array::from_fn(|_| CachePadded::new(AtomicPtr::new(null_mut()))),
            
            _variant: PhantomData,
            _backoff: PhantomData,
        }
    }

    /// Creates a new queue, like [`new`](Self::new), but using [`Arc::new_zeroed`].
    pub fn new_arc() -> Arc<Self> {
        let () = Self::LAYOUT_CHECKS;
        unsafe { Arc::new_zeroed().assume_init() }
    }

    /// Returns `true` if this UBQ contains no values.
    pub fn is_empty(&self) -> bool {
        let () = Self::LAYOUT_CHECKS;

        let chead = self.chead.load(Ordering::Acquire);
        if chead == 0 {
            return true;
        }

        let phead = self.phead.load(Ordering::Acquire);
        let mask = Head::<T, BLOCK, A>::mask();

        if (chead & !mask) != (phead & !mask) {
            return false;
        }

        ((chead & mask) >> 1) >= (phead & mask)
    }

    /// Pushes `e` onto the back of the queue.
    #[doc(alias = "enqueue")]
    #[doc(alias = "send")]
    pub fn push(&self, e: T) {
        let () = Self::LAYOUT_CHECKS;

        let backoff = B::new();
        let mut phead = Head::new(0);
        let mut next_block = None;

        // This is the only time the ptr part of phead is invalid.
        if self.phead.load(Ordering::Acquire) == 0 {
            let ptr = Box::into_raw(Block::<T, BLOCK, A>::new_zeroed());

            match self.phead.compare_exchange(
                0,
                ptr.addr() + 1,
                Ordering::Release,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    self.chead.store(ptr.addr(), Ordering::Release);
                    phead = Head {
                        index: 0,
                        block: ptr,
                    };
                }
                Err(_) => next_block = Some(unsafe { Box::from_raw(ptr) }),
            }
        }

        if phead.is_zero() {
            phead = Head::new(self.phead.load(Ordering::Acquire));

            loop {
                if phead.index >= BLOCK {
                    backoff.snooze();

                    phead = Head::new(self.phead.load(Ordering::Acquire));
                    continue;
                }

                let new_phead = Head {
                    block: phead.block,
                    index: phead.index + 1,
                };

                if next_block.is_none() && self.should_prepare_next_block(new_phead.index) {
                    next_block = Some(Block::<T, BLOCK, A>::new_zeroed());
                }

                match self.phead.compare_exchange_weak(
                    phead.pack(),
                    new_phead.pack(),
                    Ordering::SeqCst,
                    Ordering::Acquire,
                ) {
                    Ok(_) => break,
                    Err(head) => phead = Head::new(head),
                };

                backoff.spin();
            }
        }

        if phead.index + 1 == BLOCK {
            let new = if let Some(block) = next_block.take() {
                Box::into_raw(block)
            } else if let Some(pooled) = self.take_pooled_block() {
                pooled
            } else {
                Box::into_raw(Block::<T, BLOCK, A>::new_zeroed())
            };

            unsafe { (*phead.block).next.store(new, Ordering::Release) };
            self.phead.store(new.addr(), Ordering::Release);
        }

        let slot = unsafe { (*phead.block).slots.get_unchecked(phead.index) };
        unsafe { slot.value.get().write(MaybeUninit::new(e)) };

        slot.state.store(WRITE, Ordering::Release);

        if let Some(block) = next_block {
            self.release_producer_spare_block(block);
        }
    }

    /// Removes and returns the front element, or [`None`] if the queue is empty.
    #[doc(alias = "dequeue")]
    #[doc(alias = "recv")]
    pub fn pop(&self) -> Option<T> {
        let () = Self::LAYOUT_CHECKS;

        let backoff = B::new();

        // Cheap hint if queue is empty.
        if self.chead.load(Ordering::Relaxed) == 0 {
            return None;
        }

        let mut chead = Head::new(self.chead.load(Ordering::Acquire));

        loop {
            if chead.index >> 1 == BLOCK {
                backoff.snooze();
                chead = Head::new(self.chead.load(Ordering::Acquire));
                continue;
            }

            let mut new_index = chead.index + 2;

            if chead.index & 1 == 0 {
                fence(Ordering::SeqCst);
                let phead = Head::<T, BLOCK, A>::new(self.phead.load(Ordering::Relaxed));

                if phead.block.addr() == chead.block.addr() {
                    if chead.index >> 1 >= phead.index {
                        return None;
                    }
                } else {
                    new_index |= 1;
                }
            }

            let new_chead = Head {
                block: chead.block,
                index: new_index,
            };

            match self.chead.compare_exchange_weak(
                chead.pack(),
                new_chead.pack(),
                Ordering::SeqCst,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(head) => chead = Head::new(head),
            }

            backoff.spin();
        }

        chead.index >>= 1;

        if chead.index + 1 == BLOCK {
            let next = loop {
                let p = unsafe { (*chead.block).next.load(Ordering::Acquire) };

                if !p.is_null() {
                    break p;
                }

                backoff.snooze();
            };

            let has_next = unsafe { !(*next).next.load(Ordering::Relaxed).is_null() };

            self.chead.store(
                next.addr() + if has_next { 1 } else { 0 },
                Ordering::Release,
            );
        }

        let block = unsafe { &mut (*chead.block) };
        let slot = unsafe { block.slots.get_unchecked(chead.index) };

        while slot.state.load(Ordering::Acquire) & WRITE == 0 {
            backoff.snooze();
        }

        let e = unsafe { slot.value.get().read().assume_init() };

        if block.consumed.fetch_add(1, Ordering::Relaxed) + 1 == BLOCK {
            block.reset();
            self.release_consumed_block(chead.block);
        }

        Some(e)
    }
}

impl<T, V, B, const POOL: usize, const BLOCK: usize, A> Drop
    for ConfiguredUBQ<T, V, B, POOL, BLOCK, A>
{
    fn drop(&mut self) {
        let mut p = Head::<T, BLOCK, A>::new(*self.chead.get_mut()).block;

        while !p.is_null() {
            let mut b = unsafe { Box::from_raw(p) };
            p = *b.next.get_mut();
        }

        self.pool
            .iter_mut()
            .map(CachePadded::deref_mut)
            .map(AtomicPtr::get_mut)
            .filter(|p| !p.is_null())
            .for_each(|p| drop_spare_block(*p));
    }
}
