#[cfg(feature = "ubq_backoff_cq")]
use crate::backoff::Backoff;
use crate::{
    BLOCK_LENGTH,
    block::{BLOCK_MASK, Block, WRITE},
};
#[cfg(not(feature = "ubq_backoff_cq"))]
use crossbeam_utils::Backoff;
use crossbeam_utils::CachePadded;
use std::{
    array,
    mem::{ManuallyDrop, MaybeUninit},
    ops::DerefMut,
    ptr::null_mut,
    sync::{
        Arc,
        atomic::{AtomicPtr, AtomicUsize, Ordering, fence},
    },
};

#[cfg(any(
    all(feature = "ubq_v3", feature = "ubq_v4"),
    all(feature = "ubq_v3", feature = "ubq_v5"),
    all(feature = "ubq_v3", feature = "ubq_v6"),
    all(feature = "ubq_v3", feature = "ubq_v7"),
    all(feature = "ubq_v4", feature = "ubq_v5"),
    all(feature = "ubq_v4", feature = "ubq_v6"),
    all(feature = "ubq_v4", feature = "ubq_v7"),
    all(feature = "ubq_v5", feature = "ubq_v6"),
    all(feature = "ubq_v5", feature = "ubq_v7"),
    all(feature = "ubq_v6", feature = "ubq_v7"),
))]
compile_error!("Select at most one UBQ version feature: ubq_v3, ubq_v4, ubq_v5, ubq_v6, ubq_v7");

#[cfg(any(
    all(feature = "ubq_pool_1", feature = "ubq_pool_2"),
    all(feature = "ubq_pool_1", feature = "ubq_pool_4"),
    all(feature = "ubq_pool_1", feature = "ubq_pool_8"),
    all(feature = "ubq_pool_1", feature = "ubq_pool_16"),
    all(feature = "ubq_pool_1", feature = "ubq_pool_32"),
    all(feature = "ubq_pool_1", feature = "ubq_pool_64"),
    all(feature = "ubq_pool_2", feature = "ubq_pool_4"),
    all(feature = "ubq_pool_2", feature = "ubq_pool_8"),
    all(feature = "ubq_pool_2", feature = "ubq_pool_16"),
    all(feature = "ubq_pool_2", feature = "ubq_pool_32"),
    all(feature = "ubq_pool_2", feature = "ubq_pool_64"),
    all(feature = "ubq_pool_4", feature = "ubq_pool_8"),
    all(feature = "ubq_pool_4", feature = "ubq_pool_16"),
    all(feature = "ubq_pool_4", feature = "ubq_pool_32"),
    all(feature = "ubq_pool_4", feature = "ubq_pool_64"),
    all(feature = "ubq_pool_8", feature = "ubq_pool_16"),
    all(feature = "ubq_pool_8", feature = "ubq_pool_32"),
    all(feature = "ubq_pool_8", feature = "ubq_pool_64"),
    all(feature = "ubq_pool_16", feature = "ubq_pool_32"),
    all(feature = "ubq_pool_16", feature = "ubq_pool_64"),
    all(feature = "ubq_pool_32", feature = "ubq_pool_64"),
))]
compile_error!(
    "Select at most one UBQ pool feature: ubq_pool_1, ubq_pool_2, ubq_pool_4, ubq_pool_8, ubq_pool_16, ubq_pool_32, ubq_pool_64"
);

#[cfg(all(
    feature = "ubq_v6",
    any(
        feature = "ubq_pool_1",
        feature = "ubq_pool_2",
        feature = "ubq_pool_4",
        feature = "ubq_pool_8",
        feature = "ubq_pool_16",
        feature = "ubq_pool_32",
        feature = "ubq_pool_64"
    )
))]
compile_error!("ubq_v6 is no-pool and cannot be combined with ubq_pool_* features");

const POOL_SIZE: usize = if cfg!(feature = "ubq_v6") {
    0
} else if cfg!(feature = "ubq_pool_64") {
    64
} else if cfg!(feature = "ubq_pool_32") {
    32
} else if cfg!(feature = "ubq_pool_16") {
    16
} else if cfg!(feature = "ubq_pool_8") {
    8
} else if cfg!(feature = "ubq_pool_4") {
    4
} else if cfg!(feature = "ubq_pool_2") {
    2
} else {
    1
};

/// A lock-free, unbounded multi-producer/multi-consumer (MPMC) queue.
///
/// See the [crate-level documentation](crate) for an overview and quick-start
/// example. `UBQ<T>` itself is not clonable; share it with
/// [`Arc<UBQ<T>>`](std::sync::Arc).
pub struct UBQ<T> {
    /// Atomic pointer to phead: the block currently accepting producer pushes.
    phead: CachePadded<AtomicUsize>,
    /// Atomic pointer to chead: the block currently being drained by consumers.
    chead: CachePadded<AtomicUsize>,
    /// When popping, we can reuse a block instead of freeing them always.
    pool: [CachePadded<AtomicPtr<Block<T>>>; POOL_SIZE],
}

const MASK: usize = BLOCK_MASK;

struct Head<T> {
    block: *mut Block<T>,
    index: usize,
}

impl<T> Copy for Head<T> {}
impl<T> Clone for Head<T> {
    fn clone(&self) -> Self {
        Self {
            block: self.block.clone(),
            index: self.index.clone(),
        }
    }
}

impl<T> Head<T> {
    fn new(u: usize) -> Self {
        Self {
            block: (u & !MASK) as *mut Block<T>,
            index: u & MASK,
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
unsafe impl<T: Sync> Sync for UBQ<T> {}
unsafe impl<T: Send> Send for UBQ<T> {}

impl<T> UBQ<T> {
    #[inline]
    fn should_prepare_next_block(&self, next_index: usize) -> bool {
        #[cfg(feature = "ubq_v3")]
        {
            return next_index == BLOCK_LENGTH
                || self
                    .pool
                    .iter()
                    .any(|b| b.load(Ordering::Relaxed).is_null());
        }
        #[cfg(feature = "ubq_v5")]
        {
            return next_index == BLOCK_LENGTH
                && self
                    .pool
                    .iter()
                    .all(|b| b.load(Ordering::Relaxed).is_null());
        }
        #[cfg(feature = "ubq_v6")]
        {
            return next_index == BLOCK_LENGTH;
        }
        #[cfg(feature = "ubq_v7")]
        {
            return next_index == BLOCK_LENGTH
                && self
                    .pool
                    .iter()
                    .all(|b| b.load(Ordering::Relaxed).is_null());
        }
        #[cfg(any(
            feature = "ubq_v4",
            all(
                not(feature = "ubq_v3"),
                not(feature = "ubq_v5"),
                not(feature = "ubq_v6"),
                not(feature = "ubq_v7")
            )
        ))]
        {
            return next_index == BLOCK_LENGTH
                && self
                    .pool
                    .iter()
                    .any(|b| b.load(Ordering::Relaxed).is_null());
        }
    }

    #[inline]
    fn take_pooled_block(&self) -> Option<*mut Block<T>> {
        #[cfg(feature = "ubq_v6")]
        {
            None
        }
        #[cfg(not(feature = "ubq_v6"))]
        {
            self.pool.iter().find_map(|slot| {
                let pooled = slot.swap(null_mut(), Ordering::AcqRel);
                (!pooled.is_null()).then_some(pooled)
            })
        }
    }

    #[inline]
    fn release_producer_spare_block(&self, block: Box<Block<T>>) {
        #[cfg(any(feature = "ubq_v6", feature = "ubq_v7"))]
        {
            let ptr = Box::into_raw(block);
            let _ = unsafe { Box::from_raw(ptr.cast::<ManuallyDrop<Block<T>>>()) };
            return;
        }
        #[cfg(not(any(feature = "ubq_v6", feature = "ubq_v7")))]
        {
            let new = Box::into_raw(block);

            if self
                .pool
                .iter()
                .filter_map(|slot| {
                    slot.compare_exchange(null_mut(), new, Ordering::Release, Ordering::Relaxed)
                        .ok()
                })
                .next()
                .is_none()
            {
                let _ = unsafe { Box::from_raw(new.cast::<ManuallyDrop<Block<T>>>()) };
            }
        }
    }

    #[inline]
    fn release_consumed_block(&self, block: *mut Block<T>) {
        #[cfg(feature = "ubq_v6")]
        {
            let _ = unsafe { Box::from_raw(block.cast::<ManuallyDrop<Block<T>>>()) };
            return;
        }
        #[cfg(not(feature = "ubq_v6"))]
        {
            if self
                .pool
                .iter()
                .filter_map(|slot| {
                    slot.compare_exchange(null_mut(), block, Ordering::Release, Ordering::Relaxed)
                        .ok()
                })
                .next()
                .is_none()
            {
                let _ = unsafe { Box::from_raw(block.cast::<ManuallyDrop<Block<T>>>()) };
            }
        }
    }

    /// Creates a new, empty queue.
    ///
    /// No blocks are allocated until the first call to [`push`](Self::push).
    #[inline]
    pub fn new() -> Self {
        Self {
            phead: CachePadded::new(AtomicUsize::new(0)),
            chead: CachePadded::new(AtomicUsize::new(0)),
            pool: array::from_fn(|_| CachePadded::new(AtomicPtr::new(null_mut()))),
        }
    }

    /// Creates a new queue, like [`new`](Self::new), but using [`Arc::new_zeroed`].
    pub fn new_arc() -> Arc<Self> {
        unsafe { Arc::new_zeroed().assume_init() }
    }

    /// Returns `true` if this UBQ contains no values.
    pub fn is_empty(&self) -> bool {
        let chead = self.chead.load(Ordering::Acquire);
        if chead == 0 {
            return true;
        }

        let phead = self.phead.load(Ordering::Acquire);

        if (chead & !MASK) != (phead & !MASK) {
            return false;
        }

        ((chead & MASK) >> 1) >= (phead & MASK)
    }

    /// Pushes `e` onto the back of the queue.
    #[doc(alias = "enqueue")]
    #[doc(alias = "send")]
    pub fn push(&self, e: T) {
        let backoff = Backoff::new();
        let mut phead = Head::new(0);
        let mut next_block = None;

        // This is the only time the ptr part of phead is invalid.
        if self.phead.load(Ordering::Acquire) == 0 {
            let ptr = Box::into_raw(Block::new_zeroed());

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
                if phead.index == BLOCK_LENGTH {
                    backoff.snooze();

                    phead = Head::new(self.phead.load(Ordering::Acquire));
                    continue;
                }

                let new_phead = Head {
                    block: phead.block,
                    index: phead.index + 1,
                };

                if next_block.is_none() && self.should_prepare_next_block(new_phead.index) {
                    next_block = Some(Block::new_zeroed());
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

        if phead.index + 1 == BLOCK_LENGTH {
            let new = if let Some(block) = next_block.take() {
                Box::into_raw(block)
            } else if let Some(pooled) = self.take_pooled_block() {
                pooled
            } else {
                Box::into_raw(Block::new_zeroed())
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
        let backoff = Backoff::new();

        // Cheap hint if queue is empty.
        if self.chead.load(Ordering::Relaxed) == 0 {
            return None;
        }

        let mut chead = Head::new(self.chead.load(Ordering::Acquire));

        loop {
            if chead.index >> 1 == BLOCK_LENGTH {
                backoff.snooze();
                chead = Head::new(self.chead.load(Ordering::Acquire));
                continue;
            }

            let mut new_index = chead.index + 2;

            if chead.index & 1 == 0 {
                fence(Ordering::SeqCst);
                let phead = Head::<T>::new(self.phead.load(Ordering::Relaxed));

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

        if chead.index + 1 == BLOCK_LENGTH {
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

        if block.consumed.fetch_add(1, Ordering::Relaxed) + 1 == BLOCK_LENGTH {
            block.reset();
            self.release_consumed_block(chead.block);
        }

        Some(e)
    }
}

impl<T> Drop for UBQ<T> {
    fn drop(&mut self) {
        let mut p = Head::<T>::new(*self.chead.get_mut()).block;

        while !p.is_null() {
            let mut b = unsafe { Box::from_raw(p) };
            p = *b.next.get_mut();
        }

        self.pool
            .iter_mut()
            .map(CachePadded::deref_mut)
            .map(AtomicPtr::get_mut)
            .filter(|p| !p.is_null())
            .for_each(|p| drop(unsafe { Box::from_raw(p.cast::<ManuallyDrop<Block<T>>>()) }));
    }
}
