use super::head::{Head, HeadCodec};
use crate::{
    backoff::{BackoffPolicy, Crossbeam},
    block::SpmcBlock as Block,
};
use alloc::{sync::Arc, vec::Vec};
use core::{
    cell::UnsafeCell,
    fmt,
    hint::spin_loop,
    marker::PhantomData,
    ptr::null_mut,
    sync::atomic::{
        AtomicBool, AtomicUsize,
        Ordering::{AcqRel, Acquire, Relaxed, Release},
    },
};
use crossbeam_utils::CachePadded;

/// An unbounded single-producer/multi-consumer queue.
///
/// One producer owns a private tail descriptor. Values are written before the
/// producer release-publishes a block prefix and the packed public tail.
/// Consumers reserve unique ranges through the packed consumer head and then
/// read plain payload slots; there is no per-slot atomic state. Steady-state
/// claims have lock-free CAS progress. A recycled-address ABA uses a finite,
/// owner-dependent rollback chain. Allocating or recycling a block uses a
/// short, once-per-block cache lock.
#[allow(clippy::upper_case_acronyms)]
pub struct SPMC<T, B = Crossbeam> {
    /// Last producer position made visible to consumers.
    phead: CachePadded<AtomicUsize>,
    /// Next unclaimed consumer position. A block-length index is a short-lived
    /// boundary sentinel owned by the consumer which is advancing the pointer.
    chead: CachePadded<AtomicUsize>,
    /// Tail state touched only by the active shard producer.
    producer: UnsafeCell<Producer<T>>,
    /// All fully consumed surplus blocks are retained for this shard.
    pool: CachePadded<BlockCache<T>>,
    /// Type-specific codec for page-aligned block pointers and indices.
    head_codec: HeadCodec,
    _backoff: PhantomData<B>,
}

struct Producer<T> {
    tail: Head<T>,
}

// SAFETY: LUBQ gives each SPMC shard to exactly one active mutable Sender.
// Consumers touch only atomics and uniquely claimed slots. Producer lease
// handoff is release/acquire ordered before another sender uses producer state.
unsafe impl<T: Send, B> Sync for SPMC<T, B> {}
unsafe impl<T: Send, B> Send for SPMC<T, B> {}

impl<T, B> fmt::Debug for SPMC<T, B> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.pad("SPMC { .. }")
    }
}

/// An intrusive cache protected only during once-per-block push/pop traffic.
/// A lock avoids ABA while retaining the queue's high-water block allocation.
struct BlockCache<T> {
    locked: AtomicBool,
    head: UnsafeCell<*mut Block<T>>,
}

impl<T> BlockCache<T> {
    const fn new() -> Self {
        Self {
            locked: AtomicBool::new(false),
            head: UnsafeCell::new(null_mut()),
        }
    }

    fn lock(&self) {
        while self
            .locked
            .compare_exchange_weak(false, true, Acquire, Relaxed)
            .is_err()
        {
            while self.locked.load(Relaxed) {
                spin_loop();
            }
        }
    }

    fn unlock(&self) {
        self.locked.store(false, Release);
    }

    fn take(&self) -> *mut Block<T> {
        self.lock();
        // SAFETY: the cache lock gives exclusive access to the intrusive list.
        let block = unsafe { *self.head.get() };
        if !block.is_null() {
            // SAFETY: cached blocks are detached and owned by this list.
            let next = unsafe { (*block).next().load(Relaxed) };
            // SAFETY: protected by the cache lock.
            unsafe { *self.head.get() = next };
            // SAFETY: the removed block is now exclusively owned by the caller.
            unsafe { Block::set_next_exclusive(block, null_mut()) };
        }
        self.unlock();
        block
    }

    unsafe fn give(&self, block: *mut Block<T>) {
        self.lock();
        // SAFETY: block is detached and exclusively owned after full completion;
        // the cache lock protects both the head and intrusive next links.
        unsafe {
            Block::set_next_exclusive(block, *self.head.get());
            *self.head.get() = block;
        }
        self.unlock();
    }

    fn take_all(&mut self) -> *mut Block<T> {
        *self.head.get_mut()
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.lock();
        // SAFETY: the cache lock stabilizes the intrusive list.
        let mut current = unsafe { *self.head.get() };
        let mut len = 0;
        while !current.is_null() {
            len += 1;
            // SAFETY: current is a cached block protected by the lock.
            current = unsafe { (*current).next().load(Relaxed) };
        }
        self.unlock();
        len
    }
}

impl<T, B: BackoffPolicy> SPMC<T, B> {
    /// Number of payload slots in each page-sized block.
    #[inline]
    pub fn block_length(&self) -> usize {
        self.head_codec.block_length()
    }

    #[inline]
    fn acquire_phead(&self) -> Head<T> {
        Head::from_usize(self.phead.load(Acquire), self.head_codec)
    }

    #[inline]
    fn acquire_chead(&self) -> Head<T> {
        Head::from_usize(self.chead.load(Acquire), self.head_codec)
    }

    /// Creates an empty SPMC queue without allocating a block.
    #[inline]
    pub fn new() -> Self {
        Self {
            phead: CachePadded::new(AtomicUsize::new(0)),
            chead: CachePadded::new(AtomicUsize::new(0)),
            producer: UnsafeCell::new(Producer { tail: Head::ZERO }),
            pool: CachePadded::new(BlockCache::new()),
            head_codec: HeadCodec::new::<T>(),
            _backoff: PhantomData,
        }
    }

    /// Creates an empty queue in an Arc.
    pub fn new_arc() -> Arc<Self> {
        Arc::new(Self::new())
    }

    /// Returns a momentary empty observation.
    pub fn is_empty(&self) -> bool {
        let published = self.phead.load(Acquire);
        published == 0 || published == self.chead.load(Acquire)
    }

    /// Pushes one value. LUBQ calls this only through the shard's active sender.
    #[inline]
    pub fn push(&self, value: T) {
        // SAFETY: the active mutable Sender is the sole producer for this shard.
        let producer = unsafe { &mut *self.producer.get() };
        self.write_one(producer, value);
        self.publish_tail(producer);
    }

    /// Pushes every value yielded by an exact-size iterator.
    ///
    /// The iterator length is not trusted for correctness. A panicking or
    /// dishonest iterator publishes exactly the prefix actually written.
    pub fn push_batch<I>(&self, values: I)
    where
        I: IntoIterator<Item = T>,
        I::IntoIter: ExactSizeIterator,
    {
        let values = values.into_iter();
        let _expected = values.len();
        // SAFETY: the active mutable Sender is the sole producer for this shard.
        let producer = self.producer.get();
        let mut guard = PublishGuard {
            queue: self,
            producer,
            dirty: false,
        };

        for value in values {
            // SAFETY: producer points at this shard's exclusively leased state.
            self.write_one(unsafe { &mut *producer }, value);
            guard.dirty = true;
        }

        guard.publish();
    }

    fn take_block(&self) -> *mut Block<T> {
        let cached = self.pool.take();
        if cached.is_null() {
            Block::new()
        } else {
            cached
        }
    }

    #[inline(always)]
    fn write_one(&self, producer: &mut Producer<T>, value: T) {
        if producer.tail.is_zero() {
            let root = self.take_block();
            producer.tail = Head::from_ptr(root);
            // The consumer pointer becomes valid before any public producer
            // position can refer to this block.
            self.chead
                .store(producer.tail.to_usize(self.head_codec), Release);
        }

        let block_length = self.block_length();
        let block = producer.tail.block;
        let index = producer.tail.index;
        debug_assert!(index < block_length);

        // SAFETY: the sole producer owns this slot and has not published it.
        unsafe { Block::write(block, index, value) };
        producer.tail.index = index + 1;

        if producer.tail.index == block_length {
            let next = self.take_block();
            // Link before the release publication of a full old block.
            unsafe { &*block }.next().store(next, Relaxed);
            unsafe { &*block }.publish(block_length);
            producer.tail = Head::from_ptr(next);
        }
    }

    fn publish_tail(&self, producer: &Producer<T>) {
        debug_assert!(!producer.tail.is_zero());
        if producer.tail.index != 0 {
            // SAFETY: a non-zero producer tail always points at a live block.
            unsafe { &*producer.tail.block }.publish(producer.tail.index);
        }
        self.phead
            .store(producer.tail.to_usize(self.head_codec), Release);
    }

    /// Removes and returns one value, or None if no value was published.
    #[inline]
    pub fn pop(&self) -> Option<T> {
        self.pop_batch(1).next()
    }

    /// Reserves up to `size` published values for one consumer.
    pub fn pop_batch(&self, size: usize) -> SPMCIter<'_, T, B> {
        if size == 0 {
            return SPMCIter::empty(self);
        }

        let block_length = self.block_length();
        let backoff = B::new();
        let mut chead = self.acquire_chead();

        loop {
            if chead.is_zero() {
                return SPMCIter::empty(self);
            }

            if chead.index == block_length {
                // The consumer which reserved the final slot is advancing the
                // block pointer before it can complete and recycle this block.
                backoff.snooze();
                chead = self.acquire_chead();
                continue;
            }

            // A true hint was installed only after an acquired producer-head
            // observation proved this block full. A false hint must recheck
            // phead, both for an ordinary current-tail claim and because this
            // packed address may be a stale snapshot from an older reuse.
            let available = if chead.has_next {
                block_length - chead.index
            } else {
                let phead = self.acquire_phead();
                if phead.is_zero() {
                    return SPMCIter::empty(self);
                }
                if phead.block == chead.block {
                    phead.index.saturating_sub(chead.index)
                } else {
                    block_length - chead.index
                }
            };
            let first = available.min(size);
            if first == 0 {
                return SPMCIter::empty(self);
            }

            let claimed = Head {
                block: chead.block,
                index: chead.index + first,
                // Never manufacture a trusted hint from a pre-CAS phead load.
                // If this CAS succeeds after ABA, fresh consumers must continue
                // loading phead until the claim validates its current reuse.
                has_next: chead.has_next,
            };

            match self.chead.compare_exchange_weak(
                chead.to_usize(self.head_codec),
                claimed.to_usize(self.head_codec),
                AcqRel,
                Acquire,
            ) {
                Ok(_) => {
                    let first = if chead.has_next {
                        first
                    } else {
                        self.resolve_speculative_claim(chead, claimed, &backoff)
                    };
                    if first == 0 {
                        return SPMCIter::empty(self);
                    }

                    let mut len = first;
                    if chead.index + first == block_length {
                        len = self.finish_boundary_claim(chead.block, len, size, &backoff);
                    }

                    self.validate_claim(chead.block, chead.index, len);
                    return SPMCIter {
                        queue: self,
                        block: chead.block,
                        index: chead.index,
                        len,
                    };
                }
                Err(real) => {
                    chead = Head::from_usize(real, self.head_codec);
                    backoff.spin();
                }
            }
        }
    }

    /// Validates a claim made from an untrusted false HAS_NEXT snapshot.
    ///
    /// A successful stale CAS can temporarily move chead beyond the current
    /// producer frontier. Fresh consumers observe the preserved false hint,
    /// load phead, and return empty. Stale descendants can only form a finite
    /// exact-CAS chain; each edge either becomes published or rolls back in
    /// reverse order. A partial batch keeps its newly published prefix claimed.
    fn resolve_speculative_claim(&self, original: Head<T>, claimed: Head<T>, backoff: &B) -> usize {
        debug_assert!(!original.has_next);
        debug_assert!(!claimed.has_next);
        debug_assert_eq!(original.block, claimed.block);
        debug_assert!(original.index < claimed.index);

        let full_len = claimed.index - original.index;
        loop {
            let phead = self.acquire_phead();
            debug_assert!(!phead.is_zero());

            if phead.block != original.block || phead.index >= claimed.index {
                if phead.block != original.block {
                    self.try_publish_has_next(claimed);
                }
                return full_len;
            }

            // Retain any prefix which became public before this check. If this
            // is a descendant of another speculative claim, phead can still be
            // behind original.index and the whole descendant edge rolls back.
            let repaired_index = phead.index.max(original.index);
            let repaired = Head {
                block: original.block,
                index: repaired_index,
                has_next: false,
            };

            match self.chead.compare_exchange(
                claimed.to_usize(self.head_codec),
                repaired.to_usize(self.head_codec),
                AcqRel,
                Acquire,
            ) {
                Ok(_) => return repaired_index - original.index,
                Err(real) => {
                    let real = Head::from_usize(real, self.head_codec);
                    // A validated descendant may publish the cached full-block
                    // proof while this owner is waiting to undo its edge.
                    if real.block != claimed.block || (real.has_next && real.index >= claimed.index)
                    {
                        return full_len;
                    }
                    // Otherwise an older stale consumer successfully extended
                    // this false-hint state. It must resolve its exact edge
                    // before chead can return to claimed.
                    backoff.spin();
                }
            }
        }
    }

    /// Best-effort publication of a full-block proof after post-CAS validation.
    fn try_publish_has_next(&self, claimed: Head<T>) {
        debug_assert!(!claimed.has_next);
        let hinted = Head {
            has_next: true,
            ..claimed
        };
        let claimed = claimed.to_usize(self.head_codec);
        let hinted = hinted.to_usize(self.head_codec);
        if claimed != hinted {
            let _ = self
                .chead
                .compare_exchange(claimed, hinted, AcqRel, Acquire);
        }
    }

    /// Advances the consumer pointer away from a claimed-full block while its
    /// block-length sentinel excludes other claimers. May extend this caller's
    /// reservation across further fully published blocks.
    fn finish_boundary_claim(
        &self,
        mut block: *mut Block<T>,
        mut len: usize,
        requested: usize,
        backoff: &B,
    ) -> usize {
        let block_length = self.block_length();

        loop {
            let next = loop {
                // SAFETY: the first successful claim pins block until this
                // reservation completes; full publication precedes its link.
                let next = unsafe { &*block }.next().load(Acquire);
                if !next.is_null() {
                    break next;
                }
                backoff.snooze();
            };
            block = next;

            // A boundary transition always needs the current producer frontier:
            // it bounds any extension and supplies the next block's hint.
            let phead = self.acquire_phead();
            let available = if phead.block == block {
                phead.index
            } else {
                block_length
            };
            let quantity = available.min(requested - len);
            let index = quantity;
            len += quantity;

            if index < block_length {
                self.chead.store(
                    Head {
                        block,
                        index,
                        has_next: phead.block != block,
                    }
                    .to_usize(self.head_codec),
                    Release,
                );
                return len;
            }
        }
    }

    /// Performs one acquired publication check per covered block, after the
    /// reservation has made every pointer in the range stable.
    fn validate_claim(&self, mut block: *mut Block<T>, mut index: usize, mut len: usize) {
        let block_length = self.block_length();
        while len != 0 {
            let quantity = len.min(block_length - index);
            // SAFETY: the successful chead claim protects this block.
            let published = unsafe { &*block }.acquire_produced();
            assert!(
                index + quantity <= published,
                "SPMC consumer reserved an unpublished slot"
            );
            len -= quantity;
            if len != 0 {
                // SAFETY: a cross-block claim covers a published-full block.
                block = unsafe { &*block }.next().load(Acquire);
                debug_assert!(!block.is_null());
                index = 0;
            }
        }
    }

    unsafe fn recycle(&self, block: *mut Block<T>) {
        // SAFETY: the last completion owns a sealed, fully consumed block.
        unsafe { Block::reset(block) };
        // SAFETY: reset detached the exclusively owned block.
        unsafe { self.pool.give(block) };
    }
}

struct PublishGuard<'a, T, B: BackoffPolicy> {
    queue: &'a SPMC<T, B>,
    producer: *mut Producer<T>,
    dirty: bool,
}

impl<T, B: BackoffPolicy> PublishGuard<'_, T, B> {
    fn publish(&mut self) {
        if self.dirty {
            // SAFETY: this guard exists only on the sole producer's stack.
            self.queue.publish_tail(unsafe { &*self.producer });
            self.dirty = false;
        }
    }
}

impl<T, B: BackoffPolicy> Drop for PublishGuard<'_, T, B> {
    fn drop(&mut self) {
        self.publish();
    }
}

impl<T, B: BackoffPolicy> Default for SPMC<T, B> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T, B> Drop for SPMC<T, B> {
    fn drop(&mut self) {
        let chead: Head<T> = Head::from_usize(*self.chead.get_mut(), self.head_codec);
        let mut block = chead.block;
        let mut first_unconsumed = chead.index;

        while !block.is_null() {
            // SAFETY: drop has exclusive queue ownership.
            let next = unsafe { (*block).next().load(Relaxed) };
            // SAFETY: without outstanding iterators, chead is exactly the
            // consumed prefix in its first attached block.
            unsafe { Block::free(block, first_unconsumed) };
            block = next;
            first_unconsumed = 0;
        }

        let mut cached = self.pool.take_all();
        while !cached.is_null() {
            // SAFETY: drop exclusively owns the cache list.
            let next = unsafe { (*cached).next().load(Relaxed) };
            // SAFETY: cached blocks were reset and contain no live values.
            unsafe { Block::free(cached, 0) };
            cached = next;
        }
    }
}

/// Owning view of a uniquely claimed SPMC range.
pub struct SPMCIter<'a, T, B: BackoffPolicy> {
    queue: &'a SPMC<T, B>,
    block: *mut Block<T>,
    index: usize,
    len: usize,
}

impl<T, B: BackoffPolicy> Drop for SPMCIter<'_, T, B> {
    fn drop(&mut self) {
        for _ in self {}
    }
}

impl<'a, T, B: BackoffPolicy> SPMCIter<'a, T, B> {
    fn empty(queue: &'a SPMC<T, B>) -> Self {
        Self {
            queue,
            block: null_mut(),
            index: 0,
            len: 0,
        }
    }

    /// Moves this reservation into a caller-owned Vec, completing once per
    /// contiguous block range rather than once per item.
    pub fn append_to_vec(mut self, values: &mut Vec<T>) -> usize {
        let previous_len = values.len();
        let block_length = self.queue.block_length();

        while self.len != 0 {
            let block = self.block;
            // SAFETY: this iterator's claim keeps block reachable.
            let bref = unsafe { &*block };
            let quantity = self.len.min(block_length - self.index);

            // SAFETY: validate_claim acquired publication for this unique
            // contiguous range before the iterator was returned.
            unsafe { Block::append_to_vec(block, self.index, quantity, values) };

            self.index += quantity;
            self.len -= quantity;
            if self.index == block_length {
                self.block = bref.next().load(Acquire);
                self.index = 0;
            }

            if bref.consume(quantity, block_length) {
                // SAFETY: this was the last completion of the full block.
                unsafe { self.queue.recycle(block) };
            }
        }

        values.len() - previous_len
    }
}

impl<T, B: BackoffPolicy> Iterator for SPMCIter<'_, T, B> {
    type Item = T;

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.len, Some(self.len))
    }

    fn next(&mut self) -> Option<Self::Item> {
        if self.len == 0 {
            return None;
        }

        let block_length = self.queue.block_length();
        let block = self.block;
        // SAFETY: validate_claim acquired publication for this unique slot.
        let value = unsafe { Block::read(block, self.index) };

        self.index += 1;
        self.len -= 1;
        // SAFETY: the claim keeps block alive through this next-link load.
        let bref = unsafe { &*block };
        if self.index == block_length {
            self.block = bref.next().load(Acquire);
            self.index = 0;
        }

        if bref.consume(1, block_length) {
            // SAFETY: this was the last completion of the full block.
            unsafe { self.queue.recycle(block) };
        }

        Some(value)
    }
}

impl<T, B: BackoffPolicy> ExactSizeIterator for SPMCIter<'_, T, B> {}

#[cfg(test)]
mod tests {
    use super::{Head, SPMC, SPMCIter};
    use crate::backoff::{BackoffPolicy, Crossbeam};
    use alloc::{sync::Arc, vec, vec::Vec};
    use core::{
        iter::ExactSizeIterator,
        sync::atomic::{AtomicBool, AtomicUsize, Ordering},
    };
    use std::{
        panic::{AssertUnwindSafe, catch_unwind},
        thread,
    };

    struct ShortExact {
        next: usize,
        actual: usize,
        reported: usize,
    }

    impl Iterator for ShortExact {
        type Item = usize;

        fn next(&mut self) -> Option<Self::Item> {
            if self.next == self.actual {
                None
            } else {
                let value = self.next;
                self.next += 1;
                Some(value)
            }
        }

        fn size_hint(&self) -> (usize, Option<usize>) {
            (self.reported, Some(self.reported))
        }
    }

    impl ExactSizeIterator for ShortExact {
        fn len(&self) -> usize {
            self.reported
        }
    }

    #[test]
    fn queue_crosses_a_block_when_has_next_hint_is_not_encoded() {
        let queue = SPMC::<()>::new();
        let len = queue.block_length() + 1;
        queue.push_batch(core::iter::repeat_n((), len));
        assert_eq!(queue.pop_batch(len).count(), len);
        assert!(queue.is_empty());
    }

    #[test]
    fn dishonest_exact_size_iterator_publishes_only_actual_values() {
        let queue = SPMC::<usize>::new();
        queue.push_batch(ShortExact {
            next: 0,
            actual: 7,
            reported: 70,
        });
        assert_eq!(
            queue.pop_batch(100).collect::<Vec<_>>(),
            (0..7).collect::<Vec<_>>()
        );
        assert!(queue.is_empty());
    }

    #[test]
    fn iterator_panic_publishes_the_written_prefix() {
        let queue = SPMC::<usize>::new();
        let result = catch_unwind(AssertUnwindSafe(|| {
            queue.push_batch((0..8).inspect(|&value| {
                assert!(value != 5, "iterator panic");
            }));
        }));
        assert!(result.is_err());
        assert_eq!(
            queue.pop_batch(8).collect::<Vec<_>>(),
            (0..5).collect::<Vec<_>>()
        );
        assert!(queue.is_empty());
    }

    #[test]
    fn fully_consumed_blocks_are_retained_for_reuse() {
        let queue = SPMC::<usize>::new();
        let len = queue.block_length() + 1;
        queue.push_batch(0..len);
        assert_eq!(queue.pop_batch(len).count(), len);
        assert_eq!(queue.pool.len(), 1);

        queue.push_batch(0..queue.block_length());
        assert_eq!(queue.pool.len(), 0);
        assert_eq!(
            queue.pop_batch(queue.block_length()).count(),
            queue.block_length()
        );
    }

    #[test]
    fn stale_cas_after_real_block_reuse_rolls_back_without_reading_payload() {
        let queue = SPMC::<usize>::new();
        let capacity = queue.block_length();

        queue.push_batch(0..capacity);
        let stale = queue.acquire_chead();
        assert!(!stale.has_next);
        assert_eq!(queue.pop_batch(capacity).count(), capacity);

        // Fill the second block, reusing stale.block as its successor, then
        // consume the second block so chead returns to the exact stale bits.
        queue.push_batch(capacity..2 * capacity);
        assert_eq!(queue.pop_batch(capacity).count(), capacity);
        assert_eq!(
            queue.acquire_chead().to_usize(queue.head_codec),
            stale.to_usize(queue.head_codec)
        );

        let claimed = Head {
            block: stale.block,
            index: 1,
            has_next: false,
        };
        queue
            .chead
            .compare_exchange(
                stale.to_usize(queue.head_codec),
                claimed.to_usize(queue.head_codec),
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .expect("forced stale CAS must reproduce ABA");

        let backoff = <Crossbeam as BackoffPolicy>::new();
        assert_eq!(queue.resolve_speculative_claim(stale, claimed, &backoff), 0);
        assert_eq!(
            queue.acquire_chead().to_usize(queue.head_codec),
            stale.to_usize(queue.head_codec)
        );

        queue.push(2 * capacity);
        assert_eq!(queue.pop(), Some(2 * capacity));
        assert!(queue.is_empty());
    }

    #[test]
    fn stale_descendants_unwind_in_reverse_and_keep_the_published_prefix() {
        let queue = SPMC::<usize>::new();
        assert!(queue.block_length() > 8);
        queue.push_batch(0..4);

        let original = queue.acquire_chead();
        let parent = Head {
            block: original.block,
            index: 6,
            has_next: false,
        };
        let descendant = Head {
            block: original.block,
            index: 8,
            has_next: false,
        };
        // Model two stale CAS operations which succeeded in sequence.
        queue
            .chead
            .store(descendant.to_usize(queue.head_codec), Ordering::Release);

        let backoff = <Crossbeam as BackoffPolicy>::new();
        assert_eq!(
            queue.resolve_speculative_claim(parent, descendant, &backoff),
            0
        );
        assert_eq!(
            queue.resolve_speculative_claim(original, parent, &backoff),
            4
        );
        assert_eq!(queue.acquire_chead().index, 4);

        queue.validate_claim(original.block, original.index, 4);
        let values = SPMCIter {
            queue: &queue,
            block: original.block,
            index: original.index,
            len: 4,
        }
        .collect::<Vec<_>>();
        assert_eq!(values, (0..4).collect::<Vec<_>>());
        assert!(queue.is_empty());
    }

    #[test]
    fn invalid_boundary_claim_repairs_before_following_the_successor() {
        let queue = SPMC::<usize>::new();
        let capacity = queue.block_length();
        queue.push_batch(0..capacity - 2);
        assert_eq!(queue.pop_batch(capacity - 4).count(), capacity - 4);

        let original = queue.acquire_chead();
        assert_eq!(original.index, capacity - 4);
        let claimed = Head {
            block: original.block,
            index: capacity,
            has_next: false,
        };
        queue
            .chead
            .compare_exchange(
                original.to_usize(queue.head_codec),
                claimed.to_usize(queue.head_codec),
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .unwrap();

        let backoff = <Crossbeam as BackoffPolicy>::new();
        let len = queue.resolve_speculative_claim(original, claimed, &backoff);
        assert_eq!(len, 2);
        assert_eq!(queue.acquire_chead().index, capacity - 2);

        queue.validate_claim(original.block, original.index, len);
        let values = SPMCIter {
            queue: &queue,
            block: original.block,
            index: original.index,
            len,
        }
        .collect::<Vec<_>>();
        assert_eq!(values, vec![capacity - 4, capacity - 3]);
        assert!(queue.is_empty());
    }

    #[test]
    fn out_of_order_reservation_completion_recycles_only_after_the_last_range() {
        let queue = SPMC::<usize>::new();
        let capacity = queue.block_length();
        queue.push_batch(0..capacity);

        let first = queue.pop_batch(capacity / 2);
        let second = queue.pop_batch(capacity - capacity / 2);
        drop(second);
        assert_eq!(queue.pool.len(), 0);

        drop(first);
        assert_eq!(queue.pool.len(), 1);
        assert!(queue.is_empty());
    }

    #[test]
    fn concurrent_consumers_cross_and_reuse_many_blocks_exactly_once() {
        const TOTAL: usize = 50_000;
        const CONSUMERS: usize = 4;
        const PRODUCER_BATCH: usize = 137;

        let queue = SPMC::<usize>::new_arc();
        let done = Arc::new(AtomicBool::new(false));
        let seen = Arc::new((0..TOTAL).map(|_| AtomicUsize::new(0)).collect::<Vec<_>>());
        let consumers = (0..CONSUMERS)
            .map(|_| {
                let queue = Arc::clone(&queue);
                let done = Arc::clone(&done);
                let seen = Arc::clone(&seen);
                thread::spawn(move || {
                    loop {
                        if let Some(value) = queue.pop() {
                            assert_eq!(seen[value].fetch_add(1, Ordering::Relaxed), 0);
                        } else if done.load(Ordering::Acquire) && queue.is_empty() {
                            break;
                        } else {
                            thread::yield_now();
                        }
                    }
                })
            })
            .collect::<Vec<_>>();

        for start in (0..TOTAL).step_by(PRODUCER_BATCH) {
            queue.push_batch(start..(start + PRODUCER_BATCH).min(TOTAL));
        }
        done.store(true, Ordering::Release);

        for consumer in consumers {
            consumer.join().unwrap();
        }
        assert!(seen.iter().all(|count| count.load(Ordering::Relaxed) == 1));
        assert!(queue.is_empty());
        assert!(queue.pool.len() > 1);
    }

    #[test]
    fn queue_drop_destroys_only_the_unclaimed_published_suffix() {
        struct CountDrop(Arc<AtomicUsize>);

        impl Drop for CountDrop {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::Relaxed);
            }
        }

        let dropped = Arc::new(AtomicUsize::new(0));
        let queue = SPMC::<CountDrop>::new();
        let total = queue.block_length() + 17;
        queue.push_batch((0..total).map(|_| CountDrop(Arc::clone(&dropped))));
        let claimed = queue.pop_batch(3).collect::<Vec<_>>();

        drop(queue);
        assert_eq!(dropped.load(Ordering::Relaxed), total - 3);
        drop(claimed);
        assert_eq!(dropped.load(Ordering::Relaxed), total);
    }

    #[test]
    fn bulk_vec_move_transfers_non_copy_values_without_extra_drops() {
        struct CountDrop(Arc<AtomicUsize>);

        impl Drop for CountDrop {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::Relaxed);
            }
        }

        let dropped = Arc::new(AtomicUsize::new(0));
        let queue = SPMC::<CountDrop>::new();
        let total = queue.block_length() + 17;
        queue.push_batch((0..total).map(|_| CountDrop(Arc::clone(&dropped))));

        let mut values = Vec::new();
        assert_eq!(queue.pop_batch(total).append_to_vec(&mut values), total);
        assert_eq!(values.len(), total);
        assert_eq!(dropped.load(Ordering::Relaxed), 0);
        drop(queue);
        assert_eq!(dropped.load(Ordering::Relaxed), 0);
        drop(values);
        assert_eq!(dropped.load(Ordering::Relaxed), total);
    }
}
