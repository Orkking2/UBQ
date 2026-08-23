use crate::{
    backoff::{BackoffPolicy, Crossbeam},
    block::{BlockChain, MpmcBlock as Block},
    head::{Head, HeadCodec},
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
pub struct UBQ<T, B = Crossbeam> {
    /// Atomic pointer to phead: the block currently accepting producer pushes.
    phead: CachePadded<AtomicUsize>,
    /// Atomic pointer to chead: the block currently being drained by consumers.
    chead: CachePadded<AtomicUsize>,
    /// Recycled blocks used to avoid repeated allocations.
    pool: CachePadded<AtomicPtr<Block<T>>>,

    /// Type- and page-specific packed-head geometry.
    head_codec: HeadCodec,

    _marker: PhantomData<B>,
}

// SAFETY: Slot ownership is assigned with atomic counters, and producer/consumer
// commits are synchronized with Release/Acquire ordering before cross-thread reads.
unsafe impl<T: Send, B> Sync for UBQ<T, B> {}
unsafe impl<T: Send, B> Send for UBQ<T, B> {}

impl<T, B> fmt::Debug for UBQ<T, B> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.pad("UBQ { .. }")
    }
}

impl<T, B: BackoffPolicy> UBQ<T, B> {
    /// Number of slots that fit in each one-page block.
    #[inline]
    pub fn block_length(&self) -> usize {
        self.head_codec.block_length()
    }

    fn acquire_phead(&self) -> Head<T> {
        Head::from_usize(self.phead.load(Acquire), self.head_codec)
    }

    fn acquire_chead(&self) -> Head<T> {
        Head::from_usize(self.chead.load(Acquire), self.head_codec)
    }

    /// Creates a new, empty queue.
    ///
    /// No blocks are allocated until the first call to [`push`](Self::push).
    ///
    /// # Panics
    ///
    /// Panics if one `Slot<T>` does not fit in or requires greater alignment
    /// than a system base page.
    #[inline]
    pub fn new() -> Self {
        Self {
            phead: CachePadded::new(AtomicUsize::new(0)),
            chead: CachePadded::new(AtomicUsize::new(0)),
            pool: CachePadded::new(AtomicPtr::new(null_mut())),
            head_codec: HeadCodec::new::<T>(),

            _marker: PhantomData,
        }
    }

    /// Creates a new queue in an [`Arc`].
    pub fn new_arc() -> Arc<Self> {
        Arc::new(Self::new())
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
        let len = items.len().min(usize::MAX - 1);

        if len == 0 {
            return;
        }

        let block_length = self.block_length();
        let mut blocks = BlockChain::new();

        let backoff = B::new();
        let mut phead = self.acquire_phead();

        if phead.is_zero() {
            // Give us exactly one block to initialize the queue.
            blocks.grow_to_fit_with(1, &self.pool);

            let (root, size) = blocks.take();

            let new_phead = Head::from_ptr(root);

            match self.phead.compare_exchange(
                phead.to_usize(self.head_codec),
                new_phead.to_usize(self.head_codec),
                Release,
                Relaxed,
            ) {
                Ok(_) => {
                    self.chead
                        .store(Head::from_ptr(root).to_usize(self.head_codec), Release);
                    phead = new_phead;
                }
                Err(_) => blocks.give_back(root, size),
            }
        }

        let mut new_phead;

        loop {
            if phead.index >= block_length {
                backoff.snooze();
                phead = self.acquire_phead();
                continue;
            }

            // Need (len + 1) for the exact-fill case, where we need to install
            // the successor block.
            blocks.grow_to_fit_with(
                // len < usize::MAX
                (len + 1).saturating_sub(
                    block_length - phead.index + if phead.has_next { block_length } else { 0 },
                ),
                &self.pool,
            );

            new_phead = Head {
                block: phead.block,
                index: phead.index.saturating_add(len).min(block_length),
                ..Head::ZERO
            };

            match self.phead.compare_exchange_weak(
                phead.to_usize(self.head_codec),
                new_phead.to_usize(self.head_codec),
                AcqRel,
                Acquire,
            ) {
                Ok(_) => break,
                Err(real) => {
                    phead = Head::from_usize(real, self.head_codec);
                    backoff.spin();
                }
            }
        }

        if new_phead.index == block_length {
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

                let available = block_length - index;

                if remaining < available {
                    self.phead.store(
                        Head {
                            block,
                            index: index + remaining,
                            has_next: !next.is_null(),
                        }
                        .to_usize(self.head_codec),
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
         * when (++phead.index) == self.block_length()
         */
        for _ in 0..len {
            let slot = unsafe { (*phead.block).get_unchecked(phead.index) };

            phead.index += 1;

            if phead.index == block_length {
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
    pub fn pop_batch(&self, size: usize) -> UBQIter<'_, T, B> {
        if size == 0 || self.chead.load(Acquire) == Head::<T>::ZERO.to_usize(self.head_codec) {
            return UBQIter::empty(self);
        }

        let block_length = self.block_length();
        let backoff = B::new();

        let mut chead = self.acquire_chead();
        let mut len = size;

        let mut new_chead;

        loop {
            if chead.index == block_length {
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
                index: block_length.min(chead.index + len),
                ..new_chead
            };

            match self.chead.compare_exchange_weak(
                chead.to_usize(self.head_codec),
                new_chead.to_usize(self.head_codec),
                AcqRel,
                Acquire,
            ) {
                Ok(_) => {
                    // chead.has_next => phead.block != chead.block
                    if !chead.has_next {
                        let phead = self.acquire_phead();

                        if phead.block == chead.block && phead.index < new_chead.index {
                            if new_chead.index == block_length {
                                let x = self.chead.compare_exchange(
                                    new_chead.to_usize(self.head_codec),
                                    Head {
                                        has_next: false,
                                        ..phead
                                    }
                                    .to_usize(self.head_codec),
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
                    chead = Head::from_usize(real, self.head_codec);
                    backoff.spin();
                }
            }
        }

        if new_chead.index == block_length {
            let mut phead = self.acquire_phead();

            let mut remaining = len;

            let mut block = chead.block;
            let mut index = chead.index;

            loop {
                let mut next = unsafe { &*block }.next().load(Acquire);

                let available = block_length - index;

                if phead.block == block {
                    while phead.block == block && phead.index == block_length {
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
                            .to_usize(self.head_codec),
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
                        .to_usize(self.head_codec),
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

        UBQIter {
            queue: self,
            block: chead.block,
            index: chead.index,
            len,
        }
    }
}

impl<T, B: BackoffPolicy> Default for UBQ<T, B> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T, B> Drop for UBQ<T, B> {
    fn drop(&mut self) {
        let mut block = Head::from_usize(*self.chead.get_mut(), self.head_codec).block;

        while !block.is_null() {
            let next = unsafe { *(*block).next_mut().get_mut() };
            Block::<T>::free(block);
            block = next;
        }

        let pool = *self.pool.get_mut();

        if !pool.is_null() {
            Block::free(pool)
        }
    }
}

pub struct UBQIter<'a, T, B: BackoffPolicy> {
    queue: &'a UBQ<T, B>,
    block: *mut Block<T>,
    index: usize,
    /// Length, in slots, until we are exhausted
    len: usize,
}

impl<'a, T, B: BackoffPolicy> Drop for UBQIter<'a, T, B> {
    fn drop(&mut self) {
        // TODO: This could be made more specialized
        for _ in self {}
    }
}

impl<'a, T, B: BackoffPolicy> UBQIter<'a, T, B> {
    fn empty(queue: &'a UBQ<T, B>) -> Self {
        Self {
            queue,
            block: null_mut(),
            index: 0,
            len: 0,
        }
    }
}

impl<'a, T, B: BackoffPolicy> Iterator for UBQIter<'a, T, B> {
    type Item = T;

    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, Some(self.len))
    }

    fn next(&mut self) -> Option<Self::Item> {
        let block_length = self.queue.block_length();
        let backoff = B::new();
        while self.len != 0 {
            let block = self.block;
            let bref = unsafe { &*block };

            let maybe = unsafe { bref.get_unchecked(self.index) }.read(&backoff);

            self.index += 1;
            self.len -= 1;

            if self.index == block_length {
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
