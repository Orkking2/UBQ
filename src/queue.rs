use crate::{
    backoff::{BackoffPolicy, Crossbeam},
    block::{Block, BlockChain, DEFAULT_BLOCK_SIZE},
    head::Head,
};
use alloc::sync::Arc;
use core::{
    fmt,
    marker::PhantomData,
    ptr::null_mut,
    sync::atomic::{
        AtomicPtr, AtomicUsize,
        Ordering::{AcqRel, Acquire, Relaxed, Release},
    },
};
use crossbeam_utils::CachePadded;

/// A lock-free, unbounded multi-producer/multi-consumer (MPMC) queue.
pub struct UBQ<T, const BLOCK_SIZE: usize = DEFAULT_BLOCK_SIZE, B = Crossbeam> {
    /// Atomic pointer to phead: the block currently accepting producer pushes.
    phead: CachePadded<AtomicUsize>,
    /// Atomic pointer to chead: the block currently being drained by consumers.
    chead: CachePadded<AtomicUsize>,
    /// Recycled blocks used to avoid repeated allocations.
    pool: CachePadded<AtomicPtr<Block<T, BLOCK_SIZE>>>,

    _marker: PhantomData<B>,
}

// SAFETY: Slot ownership is assigned with atomic counters, and producer/consumer
// commits are synchronized with Release/Acquire ordering before cross-thread reads.
unsafe impl<T: Send, const BLOCK_SIZE: usize, B> Sync for UBQ<T, BLOCK_SIZE, B> {}
unsafe impl<T: Send, const BLOCK_SIZE: usize, B> Send for UBQ<T, BLOCK_SIZE, B> {}

impl<T, const BLOCK_SIZE: usize, B> fmt::Debug for UBQ<T, BLOCK_SIZE, B> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.pad("UBQ { .. }")
    }
}

impl<T, const BLOCK_SIZE: usize, B: BackoffPolicy> UBQ<T, BLOCK_SIZE, B> {
    /// Number of slots in each block for this queue type.
    pub const BLOCK_LENGTH: usize = BLOCK_SIZE;

    fn acquire_phead(&self) -> Head<T, BLOCK_SIZE> {
        Head::from_usize(self.phead.load(Acquire))
    }

    fn acquire_chead(&self) -> Head<T, BLOCK_SIZE> {
        Head::from_usize(self.chead.load(Acquire))
    }

    /// Creates a new, empty queue.
    ///
    /// No blocks are allocated until the first call to [`push`](Self::push).
    #[inline]
    pub fn new() -> Self {
        Self {
            phead: CachePadded::new(AtomicUsize::new(0)),
            chead: CachePadded::new(AtomicUsize::new(0)),
            pool: CachePadded::new(AtomicPtr::new(null_mut())),

            _marker: PhantomData,
        }
    }

    /// Creates a new queue, like [`new`](Self::new), but using [`Arc::new_zeroed`].
    pub fn new_arc() -> Arc<Self> {
        unsafe { Arc::new_zeroed().assume_init() }
    }

    /// Returns `true` if this UBQ contains no values.
    pub fn is_empty(&self) -> bool {
        let phead = self.acquire_phead();

        if phead.is_zero() {
            return true;
        }

        let backoff = B::new();
        let chead = loop {
            let chead = self.acquire_chead();

            if !chead.is_zero() {
                break chead;
            }

            backoff.snooze();
        };

        phead.block == chead.block && phead.index == chead.index
    }

    /// Pushes `e` onto the back of the queue.
    #[doc(alias = "enqueue")]
    #[doc(alias = "send")]
    pub fn push(&self, e: T) {
        self.push_batch(Some(e));
    }

    /// Pushes an exact number of items onto the back of the queue.
    #[doc(alias = "enqueue_batch")]
    #[doc(alias = "send_batch")]
    pub fn push_batch<I>(&self, items: I)
    where
        I: IntoIterator<Item = T>,
        I::IntoIter: ExactSizeIterator,
    {
        let mut items = items.into_iter();
        let len = items.len();

        if len == 0 {
            return;
        }

        let mut blocks = BlockChain::new();

        let backoff = B::new();
        let mut phead = self.acquire_phead();

        if phead.is_zero() {
            // Give us exactly one block to initialize the queue.
            blocks.grow_to_fit_with(1, &self.pool);

            let (root, size) = blocks.take();

            let new_phead = Head::from_ptr(root);

            match self.phead.compare_exchange(
                phead.to_usize(),
                new_phead.to_usize(),
                Release,
                Relaxed,
            ) {
                Ok(_) => {
                    self.chead.store(Head::from_ptr(root).to_usize(), Release);
                    phead = new_phead;
                }
                Err(_) => blocks.give_back(root, size),
            }
        }

        let mut new_phead;

        loop {
            if phead.index >= BLOCK_SIZE {
                backoff.snooze();
                phead = self.acquire_phead();
                continue;
            }

            // Need (len + 1) for the exact-fill case, where we need to install
            // the successor block.
            blocks.grow_to_fit_with(
                len.strict_add(1).saturating_sub(
                    BLOCK_SIZE - phead.index + if phead.has_next { BLOCK_SIZE } else { 0 },
                ),
                &self.pool,
            );

            new_phead = Head {
                block: phead.block,
                index: phead.index.saturating_add(len).min(BLOCK_SIZE),
                ..Head::ZERO
            };

            match self.phead.compare_exchange_weak(
                phead.to_usize(),
                new_phead.to_usize(),
                AcqRel,
                Acquire,
            ) {
                Ok(_) => break,
                Err(real) => {
                    phead = Head::from_usize(real);
                    backoff.spin();
                }
            }
        }

        if new_phead.index == BLOCK_SIZE {
            let (mut root, size) = blocks.take();

            let mut remaining = len;

            let mut block = phead.block;
            let mut index = phead.index;

            loop {
                let next_atm = unsafe { &*block }.next();

                let mut next = next_atm.load(Relaxed);
                if next.is_null() {
                    next_atm.store(root, Release);
                    next = root;
                    root = null_mut();
                }

                let available = BLOCK_SIZE - index;

                if remaining < available {
                    self.phead.store(
                        Head {
                            block,
                            index: index + remaining,
                            has_next: !next.is_null(),
                        }
                        .to_usize(),
                        Release,
                    );

                    break;
                } else {
                    // remaining >= available
                    remaining -= available;
                    block = next;
                    index = 0;
                }
            }

            if !root.is_null() {
                blocks.give_back(root, size);
            }
        }

        // Let us iterate over [Slot<T>; len]
        /* Our iterator is:
         * phead.block.slots[phead.index]
         * where phead.block = phead.block->next and phead.index = 0
         * when (++phead.index) == BLOCK_LENGTH
         */
        for _ in 0..len {
            let slot = unsafe { (*phead.block).get_unchecked(phead.index) };

            phead.index += 1;

            if phead.index == BLOCK_SIZE {
                phead = Head {
                    // TODO: Do we really have to do any waiting? Are we not guaranteed that !next.is_null()?
                    block: unsafe { &*phead.block }.wait_next(&backoff),
                    ..Head::ZERO
                }
            }

            items
                .next()
                .map(|item| {
                    slot.write(item);
                })
                .unwrap_or_else(|| slot.skip());
        }
    }

    /// Removes and returns the front element, or [`None`] if the queue is empty.
    #[doc(alias = "dequeue")]
    #[doc(alias = "recv")]
    pub fn pop(&self) -> Option<T> {
        self.pop_batch(1).next()
    }

    /// Reserves and returns up to `size` items from the front of the queue.
    #[doc(alias = "dequeue_batch")]
    #[doc(alias = "receive_batch")]
    pub fn pop_batch(&self, size: usize) -> UBQIter<'_, T, BLOCK_SIZE, B> {
        if size == 0 || self.chead.load(Acquire) == Head::<T, BLOCK_SIZE>::ZERO.to_usize() {
            return UBQIter::empty(self);
        }

        let backoff = B::new();

        let mut chead = self.acquire_chead();
        let mut len = size;

        let mut new_chead;

        loop {
            if chead.index == BLOCK_SIZE {
                backoff.snooze();
                chead = self.acquire_chead();
                continue;
            }

            new_chead = chead;

            if !chead.has_next {
                let phead = self.acquire_phead();

                if phead.block == chead.block {
                    len = phead.index.saturating_sub(chead.index).min(len);

                    if len == 0 {
                        return UBQIter::empty(self);
                    }

                    new_chead.has_next = false;
                } else {
                    new_chead.has_next = true;
                }
            }

            new_chead = Head {
                index: BLOCK_SIZE.min(chead.index + len),
                ..new_chead
            };

            match self.chead.compare_exchange_weak(
                chead.to_usize(),
                new_chead.to_usize(),
                AcqRel,
                Acquire,
            ) {
                Ok(_) => {
                    // chead.has_next => phead.block != chead.block
                    if !chead.has_next {
                        let phead = self.acquire_phead();

                        if phead.block == chead.block && phead.index < new_chead.index {
                            if new_chead.index == BLOCK_SIZE {
                                let x = self.chead.compare_exchange(
                                    new_chead.to_usize(),
                                    Head {
                                        has_next: false,
                                        ..phead
                                    }
                                    .to_usize(),
                                    Release,
                                    Relaxed,
                                );

                                debug_assert!(x.is_ok(), "could not revert spinlocked chead");

                                len = phead.index.saturating_sub(chead.index).min(len);

                                if len == 0 {
                                    return UBQIter::empty(self);
                                }
                            } else {
                                self.push_batch(FalseIterator {
                                    len: new_chead.index - phead.index,
                                    _marker: PhantomData,
                                });
                            }
                        }
                    }

                    break;
                }
                Err(real) => {
                    chead = Head::from_usize(real);
                    backoff.spin();
                }
            }
        }

        if new_chead.index == BLOCK_SIZE {
            let mut phead = self.acquire_phead();

            let mut remaining = len;

            let mut block = chead.block;
            let mut index = chead.index;

            loop {
                let mut next = unsafe { &*block }.next().load(Acquire);

                let available = BLOCK_SIZE - index;

                if phead.block == block {
                    while phead.block == block && phead.index == BLOCK_SIZE {
                        backoff.snooze();
                        phead = self.acquire_phead();
                    }

                    if phead.block == block {
                        self.chead.store(
                            Head {
                                block,
                                index: phead.index.min(index + remaining),
                                has_next: false,
                            }
                            .to_usize(),
                            Release,
                        );

                        remaining -= phead.index.saturating_sub(index).min(remaining);
                        len -= remaining;

                        break;
                    }

                    next = unsafe { &*block }.next().load(Acquire);
                }

                if remaining < available {
                    self.chead.store(
                        Head {
                            block,
                            index: index + remaining,
                            has_next: true,
                        }
                        .to_usize(),
                        Release,
                    );

                    break;
                } else {
                    remaining -= available;
                    block = next;
                    index = 0;
                }
            }
        }

        return UBQIter {
            queue: self,
            block: chead.block,
            index: chead.index,
            len,
        };
    }
}

impl<T, const BLOCK_SIZE: usize, B: BackoffPolicy> Default for UBQ<T, BLOCK_SIZE, B> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T, const BLOCK_SIZE: usize, B> Drop for UBQ<T, BLOCK_SIZE, B> {
    fn drop(&mut self) {
        let mut block = Head::from_usize(*self.chead.get_mut()).block;

        while !block.is_null() {
            let next = unsafe { *(*block).next_mut().get_mut() };
            Block::<T, BLOCK_SIZE>::free(block);
            block = next;
        }

        let pool = *self.pool.get_mut();

        if !pool.is_null() {
            Block::free(pool)
        }
    }
}

pub struct UBQIter<'a, T, const BLOCK_SIZE: usize, B: BackoffPolicy> {
    queue: &'a UBQ<T, BLOCK_SIZE, B>,
    block: *mut Block<T, BLOCK_SIZE>,
    index: usize,
    /// Length, in slots, until we are exhausted
    len: usize,
}

impl<'a, T, const BLOCK_SIZE: usize, B: BackoffPolicy> Drop for UBQIter<'a, T, BLOCK_SIZE, B> {
    fn drop(&mut self) {
        // TODO: This could be made more specialized
        for _ in self {}
    }
}

impl<'a, T, const BLOCK_SIZE: usize, B: BackoffPolicy> UBQIter<'a, T, BLOCK_SIZE, B> {
    fn empty(queue: &'a UBQ<T, BLOCK_SIZE, B>) -> Self {
        Self {
            queue,
            block: null_mut(),
            index: 0,
            len: 0,
        }
    }
}

impl<'a, T, const BLOCK_SIZE: usize, B: BackoffPolicy> Iterator for UBQIter<'a, T, BLOCK_SIZE, B> {
    type Item = T;

    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, Some(self.len))
    }

    fn next(&mut self) -> Option<Self::Item> {
        let backoff = B::new();
        while self.len != 0 {
            let block = self.block;
            let bref = unsafe { &*block };

            let maybe = unsafe { bref.get_unchecked(self.index) }.read(&backoff);

            self.index += 1;
            self.len -= 1;

            if self.index == BLOCK_SIZE {
                self.block = bref.next().load(Acquire);
                self.index = 0;
            }

            if bref.consume(1) {
                Block::reset(block);

                if self
                    .queue
                    .pool
                    .compare_exchange(null_mut(), block, Release, Relaxed)
                    .is_err()
                {
                    Block::free(block)
                }
            }

            if let val @ Some(_) = maybe {
                return val;
            }
        }

        None
    }
}

struct FalseIterator<T> {
    len: usize,
    _marker: PhantomData<T>,
}

impl<T> ExactSizeIterator for FalseIterator<T> {}
impl<T> Iterator for FalseIterator<T> {
    type Item = T;

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.len, Some(self.len))
    }

    fn next(&mut self) -> Option<Self::Item> {
        self.len = self.len.saturating_sub(1);
        None
    }
}

// /// Guaranteed exclusive access to the slot represented by [left..right]; right is past
// /// the terminal slot, giving left == right => EMPTY. Otherwise, as pops happen,
// /// left will traverse the singly linked list of blocks in search of right, returning
// /// blocks to the queue's pool as they are consumed.
// pub struct TransBlockIter<'a, T, B: BackoffPolicy, const POOL: usize, const BLOCK_SIZE: usize, A> {
//     queue: &'a UBQ<T, B, POOL, BLOCK_SIZE, A>,
//     max_size: usize,
//     right: usize,
//     left: usize,
// }

// impl<'a, T, B: BackoffPolicy, const POOL: usize, const BLOCK_SIZE: usize, A>
//     TransBlockIter<'a, T, B, POOL, BLOCK_SIZE, A>
// {
//     fn empty(queue: &'a UBQ<T, B, POOL, BLOCK_SIZE, A>) -> Self {
//         let zero = Head::<T, BLOCK_SIZE, A>::zero().pack();

//         Self {
//             queue,
//             max_size: 0,
//             right: zero,
//             left: zero,
//         }
//     }

//     fn new(
//         queue: &'a UBQ<T, B, POOL, BLOCK_SIZE, A>,
//         left: Head<T, BLOCK_SIZE, A>,
//         right: Head<T, BLOCK_SIZE, A>,
//         max_size: usize,
//     ) -> Self {
//         Self {
//             queue,
//             max_size,
//             right: right.pack(),
//             left: left.pack(),
//         }
//     }
// }

// impl<'a, T, B: BackoffPolicy, const POOL: usize, const BLOCK_SIZE: usize, A> Drop
//     for TransBlockIter<'a, T, B, POOL, BLOCK_SIZE, A>
// {
//     fn drop(&mut self) {
//         while self.max_size != 0 {
//             let _ = self.next();
//         }
//     }
// }

// impl<'a, T, B: BackoffPolicy, const POOL: usize, const BLOCK_SIZE: usize, A> Iterator
//     for TransBlockIter<'a, T, B, POOL, BLOCK_SIZE, A>
// {
//     type Item = T;

//     fn next(&mut self) -> Option<Self::Item> {
//         let backoff = B::new();

//         while self.left != self.right {
//             let mut left = Head::<T, BLOCK_SIZE, A>::new(self.left);
//             let block = left.block;
//             let slot = unsafe { (*block).slots.get_unchecked(left.index) };

//             left.index += 1;
//             self.max_size -= 1;

//             // Cache the successor before this slot can make us the last
//             // consumer and allow the current block to be reset and recycled.
//             if left.index == BLOCK_SIZE {
//                 left = Head {
//                     block: loop {
//                         let next = unsafe { (*block).next.load(Acquire) };

//                         if !next.is_null() {
//                             break next;
//                         }

//                         backoff.snooze();
//                     },
//                     index: 0,
//                 };
//             }

//             self.left = left.pack();

//             let slot = slot.busy_wait_state(&backoff);
//             let out = (slot.state.load(Acquire) != SKIP)
//                 .then(|| unsafe { slot.value.get().read().assume_init() });

//             if unsafe { (*block).consumed.fetch_add(1, AcqRel) } + 1 == BLOCK_SIZE {
//                 self.queue.give_to_pool(Block::reset(block));
//             }

//             if out.is_some() {
//                 return out;
//             }
//         }

//         None
//     }

//     fn size_hint(&self) -> (usize, Option<usize>) {
//         (0, Some(self.max_size))
//     }
// }
