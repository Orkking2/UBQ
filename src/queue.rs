use crate::{
    align::A1024,
    backoff::{BackoffPolicy, Crossbeam},
    block::{Block, DEFAULT_BLOCK_SIZE, SKIP, WRITE},
};
use alloc::{boxed::Box, sync::Arc};
use core::{
    array, fmt, iter,
    marker::PhantomData,
    mem::{ManuallyDrop, MaybeUninit},
    ops::DerefMut,
    ptr::{null_mut, with_exposed_provenance_mut},
    sync::atomic::{
        AtomicPtr, AtomicUsize,
        Ordering::{AcqRel, Acquire, Relaxed, Release, SeqCst},
    },
};
use crossbeam_utils::CachePadded;

/// Default number of pooled blocks retained by [`crate::UBQ`].
pub const DEFAULT_POOL_SIZE: usize = 8;

/// A lock-free, unbounded multi-producer/multi-consumer (MPMC) queue.
///
/// `ConfiguredUBQ` is the fully-configurable queue type. The crate-level
/// [`crate::UBQ`] alias preserves the default configuration.
///
/// ```rust
/// use ubq::{ConfiguredUBQ, align, backoff};
///
/// let q = ConfiguredUBQ::<u64, backoff::Crossbeam, 2, 127, align::A256>::new();
/// q.push(42);
/// assert_eq!(q.pop(), Some(42));
/// ```
///
/// ```compile_fail
/// use ubq::{ConfiguredUBQ, align, backoff};
///
/// let _ = ConfiguredUBQ::<u64, backoff::Crossbeam, 1, 1024, align::A512>::new();
/// ```
///
/// ```compile_fail
/// use ubq::{ConfiguredUBQ, backoff};
///
/// #[repr(align(64))]
/// struct BadAlign([u8; 8]);
///
/// let _ = ConfiguredUBQ::<u64, backoff::Crossbeam, 1, 31, BadAlign>::new();
/// ```
pub struct ConfiguredUBQ<
    T,
    B = Crossbeam,
    const POOL: usize = DEFAULT_POOL_SIZE,
    const BLOCK_SIZE: usize = DEFAULT_BLOCK_SIZE,
    A = A1024,
> {
    /// Atomic pointer to phead: the block currently accepting producer pushes.
    phead: CachePadded<AtomicUsize>,
    /// Atomic pointer to chead: the block currently being drained by consumers.
    chead: CachePadded<AtomicUsize>,
    /// Recycled blocks used to avoid repeated allocations.
    pool: [CachePadded<AtomicPtr<Block<T, BLOCK_SIZE, A>>>; POOL],

    _backoff: PhantomData<B>,
}

struct Head<T, const BLOCK_SIZE: usize, A> {
    index: usize,
    block: *mut Block<T, BLOCK_SIZE, A>,
}

impl<T, const BLOCK_SIZE: usize, A> PartialEq for Head<T, BLOCK_SIZE, A> {
    fn eq(&self, other: &Self) -> bool {
        self.index == other.index && self.block == other.block
    }
}

#[inline]
fn drop_spare_block<T, const BLOCK_SIZE: usize, A>(block: *mut Block<T, BLOCK_SIZE, A>) {
    let _ = unsafe { Box::from_raw(block.cast::<ManuallyDrop<Block<T, BLOCK_SIZE, A>>>()) };
}

#[inline]
fn ref_to_mut_ptr<T>(r: &T) -> *mut T {
    r as *const T as *mut T
}

impl<T, const BLOCK: usize, A> Copy for Head<T, BLOCK, A> {}

impl<T, const BLOCK: usize, A> Clone for Head<T, BLOCK, A> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T, const BLOCK_SIZE: usize, A> Head<T, BLOCK_SIZE, A> {
    #[inline]
    const fn mask() -> usize {
        Block::<T, BLOCK_SIZE, A>::block_mask()
    }

    const fn new(u: usize) -> Self {
        let mask = Self::mask();

        Self {
            block: with_exposed_provenance_mut(u & !mask),
            index: u & mask,
        }
    }

    const fn zero() -> Self {
        Self {
            index: 0,
            block: null_mut(),
        }
    }

    const fn is_zero(&self) -> bool {
        self.index == 0 && self.block.is_null()
    }

    fn pack(self) -> usize {
        self.block.expose_provenance() | self.index
    }
}

// SAFETY: Slot ownership is assigned with atomic counters, and producer/consumer
// commits are synchronized with Release/Acquire ordering before cross-thread reads.
unsafe impl<T: Send, B, A: Send, const POOL: usize, const BLOCK_SIZE: usize> Sync
    for ConfiguredUBQ<T, B, POOL, BLOCK_SIZE, A>
{
}
unsafe impl<T: Send, B, A: Send, const POOL: usize, const BLOCK_SIZE: usize> Send
    for ConfiguredUBQ<T, B, POOL, BLOCK_SIZE, A>
{
}

impl<T, B, const POOL: usize, const BLOCK_SIZE: usize, A> fmt::Debug
    for ConfiguredUBQ<T, B, POOL, BLOCK_SIZE, A>
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.pad("ConfiguredUBQ { .. }")
    }
}

impl<T, B: BackoffPolicy, const POOL: usize, const BLOCK_SIZE: usize, A>
    ConfiguredUBQ<T, B, POOL, BLOCK_SIZE, A>
{
    const LAYOUT_CHECKS: () = Block::<T, BLOCK_SIZE, A>::LAYOUT_CHECKS;

    /// Number of retained pooled blocks.
    pub const POOL_SIZE: usize = POOL;
    /// Number of slots in each block for this queue type.
    pub const BLOCK_LENGTH: usize = BLOCK_SIZE;

    #[inline]
    fn give_to_pool(&self, block: *mut Block<T, BLOCK_SIZE, A>) {
        if !self.pool.iter().any(|slot| {
            slot.compare_exchange(null_mut(), block, Release, Relaxed)
                .is_ok()
        }) {
            drop_spare_block(block);
        }
    }

    #[inline]
    fn take_from_pool(&self) -> Option<Box<Block<T, BLOCK_SIZE, A>>> {
        self.pool.iter().find_map(|slot| {
            let ptr = slot.swap(null_mut(), AcqRel);

            (!ptr.is_null()).then(|| unsafe { Box::from_raw(ptr) })
        })
    }

    fn acquire_phead(&self) -> Head<T, BLOCK_SIZE, A> {
        Head::new(self.phead.load(Acquire))
    }

    fn acquire_chead(&self) -> Head<T, BLOCK_SIZE, A> {
        Head::new(self.chead.load(Acquire))
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

        let chead = self.chead.load(Acquire);
        if chead == 0 {
            return true;
        }

        let phead = self.phead.load(Acquire);
        let mask = Head::<T, BLOCK_SIZE, A>::mask();

        if (chead & !mask) != (phead & !mask) {
            return false;
        }

        ((chead & mask) >> 1) >= (phead & mask)
    }

    /// Pushes an exact number of items onto the back of the queue.
    ///
    /// This push is "atomic": every item will be placed in order without gaps.
    #[doc(alias = "enqueue_batch")]
    #[doc(alias = "send_batch")]
    pub fn push_batch<I>(&self, items: I)
    where
        I: IntoIterator<Item = T>,
        I::IntoIter: ExactSizeIterator,
    {
        // Key insight:
        // We either reserve the entirety of this block (giving us exclusive
        // access to construct subsequent blocks) or we do not fill the entire block.

        let () = Self::LAYOUT_CHECKS;

        let mut items = items.into_iter();
        let len = items.len();

        if len == 0 {
            return;
        } else if len == 1 {
            return self.push(
                items
                    .next()
                    .expect("ExactSizeIterator gave len == 1, but no items were supplied"),
            );
        }

        // There exists a minimum number of blocks needed to fit all items.
        // Note: A completely full block must link a new empty one.
        let min_blocks = 1 + len / BLOCK_SIZE;

        // Preallocate all the blocks we will (potentially) need.
        let blocks = iter::from_fn(|| self.take_from_pool())
            .chain(iter::repeat_with(Block::new_zeroed))
            .take(min_blocks)
            .collect::<Box<_>>();

        let backoff = B::new();
        let mut phead = Head::new(0);
        let mut next_block = None;

        if self.phead.load(Acquire) == 0 {
            let ptr = Box::into_raw(Block::new_zeroed());

            match self.phead.compare_exchange(
                0,
                ptr.expose_provenance() + BLOCK_SIZE.min(len),
                Release,
                Relaxed,
            ) {
                Ok(_) => {
                    self.chead.store(ptr.expose_provenance(), Release);
                    phead = Head {
                        index: 0,
                        block: ptr,
                    }
                }
                Err(_) => next_block = Some(unsafe { Box::from_raw(ptr) }),
            }
        }

        if phead.is_zero() {
            phead = self.acquire_phead();

            loop {
                if phead.index >= BLOCK_SIZE {
                    backoff.snooze();
                    phead = self.acquire_phead();
                    continue;
                }

                if next_block.is_none()
                    && phead.index + 1 == BLOCK_SIZE
                    && self.pool.iter().all(|b| b.load(Relaxed).is_null())
                {
                    next_block = Some(Block::new_zeroed());
                }

                match self.phead.compare_exchange_weak(
                    phead.pack(),
                    Head {
                        index: BLOCK_SIZE.min(phead.index + len),
                        block: phead.block,
                    }
                    .pack(),
                    SeqCst,
                    Acquire,
                ) {
                    Ok(_) => break,
                    Err(real) => phead = Head::new(real),
                }
            }
        }

        // We are responsible for the subsequent block linkage.
        if BLOCK_SIZE.min(phead.index + len) == BLOCK_SIZE {
            // Let us construct here the full linkage of blocks.
            let new_blocks = 1 + (len - (BLOCK_SIZE - phead.index)) / BLOCK_SIZE;

            debug_assert!(new_blocks <= blocks.len());

            for i in 0..new_blocks {
                if i != new_blocks - 1 {
                    unsafe {
                        blocks
                            .get_unchecked(i)
                            .next
                            .as_ptr()
                            .write(ref_to_mut_ptr(blocks.get_unchecked(i + 1)))
                    }
                }
            }

            let first_block = ref_to_mut_ptr(unsafe { &**blocks.get_unchecked(0) });
            let last_block = ref_to_mut_ptr(unsafe { &**blocks.get_unchecked(new_blocks - 1) });

            blocks
                .into_iter()
                .map(Box::into_raw)
                .enumerate()
                .filter_map(|(i, block)| (i >= new_blocks).then_some(block))
                .for_each(|block| self.give_to_pool(block));

            unsafe {
                (*phead.block).next.store(first_block, Release);
            }
            self.phead.store(
                Head {
                    index: (len - (BLOCK_SIZE - phead.index)) % BLOCK_SIZE,
                    block: last_block,
                }
                .pack(),
                Release,
            )
        } else {
            blocks
                .into_iter()
                .map(Box::into_raw)
                .for_each(|block| self.give_to_pool(block));
        }

        // At this point we are guaranteed to have `len` slots available, whether that be in the current block
        // or if that overflows into the subsequent blocks we have just allocated. These are all ours.
        for _ in 0..len {
            let slot = unsafe { (*phead.block).slots.get_unchecked(phead.index) };

            phead.index += 1;

            if phead.index == BLOCK_SIZE {
                phead = Head {
                    index: 0,
                    block: unsafe { (*phead.block).next.as_ptr().read() },
                }
            }

            let state = if let Some(item) = items.next() {
                unsafe { slot.value.get().write(MaybeUninit::new(item)) };

                WRITE
            } else {
                SKIP
            };

            slot.state.store(state, Release);
        }
    }

    /// Pushes `e` onto the back of the queue.
    #[doc(alias = "enqueue")]
    #[doc(alias = "send")]
    pub fn push(&self, e: T) {
        self.push_inner(Some(e));
    }

    /// Trigger all the mechanics of a push, but without pushing anything.
    fn faux_push(&self) {
        self.push_inner(None);
    }

    fn push_inner(&self, e_opt: Option<T>) {
        let () = Self::LAYOUT_CHECKS;

        let backoff = B::new();
        let mut phead = Head::new(0);
        let mut next_block = None;

        // This is the only time the ptr part of phead is invalid.
        if self.phead.load(Acquire) == 0 {
            let ptr = Box::into_raw(Block::new_zeroed());

            match self
                .phead
                .compare_exchange(0, ptr.expose_provenance() + 1, Release, Relaxed)
            {
                Ok(_) => {
                    self.chead.store(ptr.expose_provenance(), Release);
                    phead = Head {
                        index: 0,
                        block: ptr,
                    };
                }
                Err(_) => next_block = Some(unsafe { Box::from_raw(ptr) }),
            }
        }

        if phead.is_zero() {
            phead = self.acquire_phead();

            loop {
                if phead.index >= BLOCK_SIZE {
                    backoff.snooze();
                    phead = self.acquire_phead();
                    continue;
                }

                if next_block.is_none()
                    && phead.index + 1 == BLOCK_SIZE
                    && self.pool.iter().all(|b| b.load(Relaxed).is_null())
                {
                    next_block = Some(Block::new_zeroed());
                }

                phead = Head::new(self.phead.fetch_add(1, Acquire));

                if phead.index < BLOCK_SIZE {
                    break;
                };
            }
        }

        if phead.index + 1 == BLOCK_SIZE {
            let new = next_block
                .take()
                .map(Box::into_raw)
                .or_else(|| self.take_from_pool().map(Box::into_raw))
                .unwrap_or_else(|| Box::into_raw(Block::new_zeroed()));

            unsafe { (*phead.block).next.store(new, Release) };
            self.phead.store(new.expose_provenance(), Release);
        }

        let slot = unsafe { (*phead.block).slots.get_unchecked(phead.index) };

        let state = if let Some(e) = e_opt {
            unsafe { slot.value.get().write(MaybeUninit::new(e)) };

            WRITE
        } else {
            SKIP
        };

        slot.state.store(state, Release);

        if let Some(block) = next_block {
            self.give_to_pool(Box::into_raw(block))
        }
    }

    /// Reserves and returns up to `request_size` items from the front of the queue.
    ///
    /// The returned iterator owns one consecutive range, so concurrent consumers
    /// cannot take items from within that range. Reservation is eager but slot
    /// reads are lazy: this call advances the consumer head before the iterator
    /// yields its first item.
    ///
    /// The iterator may yield fewer than `request_size` items when fewer queue
    /// positions are available or when an inaccurate batched producer published
    /// skips. Dropping the iterator consumes and drops the unvisited part of its
    /// reservation so that blocks can still be recycled.
    #[doc(alias = "dequeue_batch")]
    #[doc(alias = "receive_batch")]
    pub fn pop_batch(&self, request_size: usize) -> TransBlockIter<'_, T, B, POOL, BLOCK_SIZE, A> {
        let () = Self::LAYOUT_CHECKS;

        if request_size == 0 || self.chead.load(Relaxed) == 0 {
            return TransBlockIter::empty(self);
        }

        let backoff = B::new();
        let mut chead = self.acquire_chead();

        // First claim the part of the request that lies in the current block.
        // A marker at BLOCK_SIZE gives this consumer exclusive ownership of the
        // boundary transition, making it safe to inspect successor pointers.
        let marker = loop {
            let start_index = chead.index >> 1;

            if start_index == BLOCK_SIZE {
                backoff.snooze();
                chead = self.acquire_chead();
                continue;
            }

            let in_block = request_size.min(BLOCK_SIZE - start_index);
            let mut new_index = ((start_index + in_block) << 1) | (chead.index & 1);

            if chead.index & 1 == 0 {
                let phead = Head::new(self.phead.load(Relaxed));

                if phead.block == chead.block {
                    if start_index >= phead.index {
                        return TransBlockIter::empty(self);
                    }

                    new_index = new_index.min(phead.index << 1);
                } else {
                    // Remember that the producer was observed beyond this
                    // block. Future consumers can then avoid reloading phead.
                    new_index |= 1;
                }
            }

            let new_chead = Head {
                block: chead.block,
                index: new_index,
            };

            match self
                .chead
                .compare_exchange_weak(chead.pack(), new_chead.pack(), SeqCst, Acquire)
            {
                Ok(_) => break new_chead,
                Err(real) => {
                    chead = Head::new(real);
                    backoff.spin();
                }
            }
        };

        let start = Head {
            block: chead.block,
            index: chead.index >> 1,
        };
        let marker_index = marker.index >> 1;

        // Validate the reservation against a producer-head load ordered after
        // the consumer CAS. In the unlikely event that the earlier relaxed
        // observation was stale, publish SKIPs for any positions the consumer
        // has already claimed but no producer has reserved.
        loop {
            let phead = Head::new(self.phead.load(SeqCst));

            if phead.block != start.block || phead.index >= marker_index {
                break;
            }

            self.faux_push();
        }

        if marker_index < BLOCK_SIZE {
            let end = Head {
                block: start.block,
                index: marker_index,
            };

            return TransBlockIter::new(self, start, end, marker_index - start.index);
        }

        // We own the boundary marker. Wait until the producer head is
        // normalized, then walk at most the requested number of slots and cap
        // the reservation at that producer frontier.
        let phead = loop {
            let phead = self.acquire_phead();

            if phead.index < BLOCK_SIZE && phead.block != start.block {
                break phead;
            }

            if phead.index < BLOCK_SIZE {
                // A stale "has next" observation allowed the consumer to reach
                // an unreserved suffix. Turn one position into a SKIP and retry.
                self.faux_push();
            } else {
                backoff.snooze();
            }
        };

        let first_block_slots = BLOCK_SIZE - start.index;
        let mut remaining = request_size - first_block_slots;
        let mut reserved = first_block_slots;
        let mut cursor = Head {
            block: loop {
                let next = unsafe { (*start.block).next.load(Acquire) };

                if !next.is_null() {
                    break next;
                }

                backoff.snooze();
            },
            index: 0,
        };

        let (end, has_next) = loop {
            if cursor.block == phead.block {
                let take = remaining.min(phead.index);
                cursor.index = take;
                reserved += take;

                break (cursor, false);
            }

            if remaining < BLOCK_SIZE {
                cursor.index = remaining;
                reserved += remaining;

                break (cursor, true);
            }

            remaining -= BLOCK_SIZE;
            reserved += BLOCK_SIZE;
            cursor.block = loop {
                let next = unsafe { (*cursor.block).next.load(Acquire) };

                if !next.is_null() {
                    break next;
                }

                backoff.snooze();
            };
        };

        self.chead.store(
            Head {
                block: end.block,
                index: (end.index << 1) | usize::from(has_next),
            }
            .pack(),
            Release,
        );

        TransBlockIter::new(self, start, end, reserved)
    }

    /// Removes and returns the front element, or [`None`] if the queue is empty.
    #[doc(alias = "dequeue")]
    #[doc(alias = "recv")]
    pub fn pop(&self) -> Option<T> {
        let () = Self::LAYOUT_CHECKS;

        let backoff = B::new();

        if self.chead.load(Relaxed) == 0 {
            return None;
        }

        let mut chead = self.acquire_chead();

        loop {
            if chead.index >> 1 == BLOCK_SIZE {
                backoff.snooze();
                chead = self.acquire_chead();
                continue;
            }

            let mut new_index = chead.index + 2;

            if chead.index & 1 == 0 {
                let phead = Head::new(self.phead.load(Relaxed));

                if phead.block == chead.block {
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

            match self
                .chead
                .compare_exchange_weak(chead.pack(), new_chead.pack(), SeqCst, Acquire)
            {
                Ok(_) => {
                    // This load *must* be ordered subsequent (in time) to the CAS of chead
                    let phead = Head::new(self.phead.load(SeqCst));

                    if phead.block == chead.block && phead.index <= chead.index >> 1 {
                        self.faux_push();
                    }

                    break;
                }
                Err(real) => chead = Head::new(real),
            }

            backoff.spin();
        }

        chead.index >>= 1;

        if chead.index + 1 == BLOCK_SIZE {
            let next = loop {
                let p = unsafe { (*chead.block).next.load(Acquire) };

                if !p.is_null() {
                    break p;
                }

                backoff.snooze();
            };

            let has_next = unsafe { !(*next).next.load(Relaxed).is_null() };

            self.chead.store(
                next.expose_provenance() + if has_next { 1 } else { 0 },
                Release,
            );
        }

        let slot =
            unsafe { (*chead.block).slots.get_unchecked(chead.index) }.busy_wait_state(&backoff);

        let out = (slot.state.load(Acquire) != SKIP)
            .then(|| unsafe { slot.value.get().read().assume_init() });

        if unsafe { (*chead.block).consumed.fetch_add(1, AcqRel) } + 1 == BLOCK_SIZE {
            self.give_to_pool(Block::reset(chead.block));
        }

        out.or_else(|| self.pop())
    }
}

impl<T, B: BackoffPolicy, const POOL: usize, const BLOCK_SIZE: usize, A> Default
    for ConfiguredUBQ<T, B, POOL, BLOCK_SIZE, A>
{
    fn default() -> Self {
        Self::new()
    }
}

impl<T, B, const POOL: usize, const BLOCK: usize, A> Drop for ConfiguredUBQ<T, B, POOL, BLOCK, A> {
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

/// Guaranteed exclusive access to the slot represented by [left..right]; right is past
/// the terminal slot, giving left == right => EMPTY. Otherwise, as pops happen,
/// left will traverse the singly linked list of blocks in search of right, returning
/// blocks to the queue's pool as they are consumed.
pub struct TransBlockIter<'a, T, B: BackoffPolicy, const POOL: usize, const BLOCK_SIZE: usize, A> {
    queue: &'a ConfiguredUBQ<T, B, POOL, BLOCK_SIZE, A>,
    max_size: usize,
    right: usize,
    left: usize,
}

impl<'a, T, B: BackoffPolicy, const POOL: usize, const BLOCK_SIZE: usize, A>
    TransBlockIter<'a, T, B, POOL, BLOCK_SIZE, A>
{
    fn empty(queue: &'a ConfiguredUBQ<T, B, POOL, BLOCK_SIZE, A>) -> Self {
        let zero = Head::<T, BLOCK_SIZE, A>::zero().pack();

        Self {
            queue,
            max_size: 0,
            right: zero,
            left: zero,
        }
    }

    fn new(
        queue: &'a ConfiguredUBQ<T, B, POOL, BLOCK_SIZE, A>,
        left: Head<T, BLOCK_SIZE, A>,
        right: Head<T, BLOCK_SIZE, A>,
        max_size: usize,
    ) -> Self {
        Self {
            queue,
            max_size,
            right: right.pack(),
            left: left.pack(),
        }
    }
}

impl<'a, T, B: BackoffPolicy, const POOL: usize, const BLOCK_SIZE: usize, A> Drop
    for TransBlockIter<'a, T, B, POOL, BLOCK_SIZE, A>
{
    fn drop(&mut self) {
        while self.max_size != 0 {
            let _ = self.next();
        }
    }
}

impl<'a, T, B: BackoffPolicy, const POOL: usize, const BLOCK_SIZE: usize, A> Iterator
    for TransBlockIter<'a, T, B, POOL, BLOCK_SIZE, A>
{
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        let backoff = B::new();

        while self.left != self.right {
            let mut left = Head::<T, BLOCK_SIZE, A>::new(self.left);
            let block = left.block;
            let slot = unsafe { (*block).slots.get_unchecked(left.index) };

            left.index += 1;
            self.max_size -= 1;

            // Cache the successor before this slot can make us the last
            // consumer and allow the current block to be reset and recycled.
            if left.index == BLOCK_SIZE {
                left = Head {
                    block: loop {
                        let next = unsafe { (*block).next.load(Acquire) };

                        if !next.is_null() {
                            break next;
                        }

                        backoff.snooze();
                    },
                    index: 0,
                };
            }

            self.left = left.pack();

            let slot = slot.busy_wait_state(&backoff);
            let out = (slot.state.load(Acquire) != SKIP)
                .then(|| unsafe { slot.value.get().read().assume_init() });

            if unsafe { (*block).consumed.fetch_add(1, AcqRel) } + 1 == BLOCK_SIZE {
                self.queue.give_to_pool(Block::reset(block));
            }

            if out.is_some() {
                return out;
            }
        }

        None
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, Some(self.max_size))
    }
}
