use crate::{
    align::A4096,
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
        fence,
    },
};
use crossbeam_utils::CachePadded;

/// Default number of pooled blocks retained by [`crate::UBQ`].
pub const DEFAULT_POOL_SIZE: usize = 1;

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
    A = A4096,
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
    fn mask() -> usize {
        Block::<T, BLOCK_SIZE, A>::block_mask()
    }

    fn new(u: usize) -> Self {
        let mask = Self::mask();

        Self {
            block: with_exposed_provenance_mut(u & !mask),
            index: u & mask,
        }
    }

    fn is_zero(&self) -> bool {
        self.index == 0 && self.block.is_null()
    }

    fn pack(self) -> usize {
        self.block.expose_provenance() | self.index
    }
}

// SAFETY: Slot ownership is assigned with atomic counters, and producer/consumer
// commits are synchronized with Release/Acquire ordering before cross-thread reads.
unsafe impl<T: Sync, B, A: Sync, const POOL: usize, const BLOCK_SIZE: usize> Sync
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
    fn release_block(&self, block: *mut Block<T, BLOCK_SIZE, A>) {
        if !self.pool.iter().any(|slot| {
            slot.compare_exchange(null_mut(), block, Release, Relaxed)
                .is_ok()
        }) {
            drop_spare_block(block);
        }
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
        // Worst case we have to allocate and free one block erroneously.
        let blocks = iter::repeat_with(Block::new_zeroed)
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
                            .write(ref_to_mut_ptr(&blocks.get_unchecked(i + 1)))
                    }
                }
            }

            let first_block = ref_to_mut_ptr(unsafe { &**blocks.get_unchecked(0) });
            let last_block = ref_to_mut_ptr(unsafe { &**blocks.get_unchecked(new_blocks - 1) });

            // The linked blocks are now queue-owned. Consuming the boxed slice
            // drops any unused preallocated blocks.
            for block in blocks.into_iter().take(new_blocks) {
                let _ = Box::into_raw(block);
            }

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
        }

        // At this point we are guaranteed to have `len` slots available, whether that be in the current block
        // or if that overflows into the subsequent blocks we have just allocated. These are all ours.
        for i in 0..len {
            let item = items.next().expect(&format!(
                "ExactSizeIterator gave len == {len}, but only produced {i} items"
            ));

            let slot = unsafe { (*phead.block).slots.get_unchecked(phead.index) };

            phead.index += 1;

            if phead.index == BLOCK_SIZE {
                phead = Head {
                    index: 0,
                    block: unsafe { (*phead.block).next.as_ptr().read() }, // next is guaranteed to not change until the block is freed
                }
            }

            unsafe { slot.value.get().write(MaybeUninit::new(item)) };
            slot.state.store(WRITE, Release);
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
                    next_block = Some(Block::<T, BLOCK_SIZE, A>::new_zeroed());
                }

                phead = Head::new(self.phead.fetch_add(1, SeqCst));

                if phead.index < BLOCK_SIZE {
                    break;
                };
            }
        }

        if phead.index + 1 == BLOCK_SIZE {
            // We are, at this point, guaranteed to be the only consuming accessor of pool.
            // That is, no other producers are interfacing with the pool until we have stored the new phead.

            let new = next_block
                .take()
                .map(Box::into_raw)
                .or_else(|| {
                    self.pool
                        .iter()
                        .find(|slot| !slot.load(Relaxed).is_null())
                        .map(|slot| slot.swap(null_mut(), AcqRel))
                })
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
            self.release_block(Box::into_raw(block))
        }
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
                fence(SeqCst);
                let phead = Head::<T, BLOCK_SIZE, A>::new(self.phead.load(Relaxed));

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
                    let phead = Head::<T, BLOCK_SIZE, A>::new(self.phead.load(SeqCst));

                    if phead.block == chead.block && phead.index <= chead.index {
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

        let slot = unsafe { (*chead.block).slots.get_unchecked(chead.index) };

        while slot.state.load(Acquire) & WRITE == 0 {
            backoff.snooze();
        }

        let out = (slot.state.load(Acquire) != SKIP)
            .then(|| unsafe { slot.value.get().read().assume_init() });

        if unsafe { (*chead.block).consumed.fetch_add(1, Relaxed) } + 1 == BLOCK_SIZE {
            unsafe { Block::reset(chead.block) };
            self.release_block(chead.block);
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
