use crate::{
    backoff::BackoffPolicy,
    dynamic::{
        atomic_int::AtomicInt,
        block::Block,
        heads::{CHead, Excess, MAX_BLOCK_LENGTH, PHead},
        util::{new_filled_box_slice, usize_as_u16_or_MAX},
    },
};
use alloc::{boxed::Box, sync::Arc};
use core::{
    marker::PhantomData,
    ptr::{NonNull, null_mut},
};
use crossbeam_utils::CachePadded;
use portable_atomic::{AtomicPtr, Ordering};

/// A private, already-linked sequence of blocks owned by one producer.
///
/// `BlockChain` is deliberately an owning type: until the chain is linked into
/// the queue, losing or abandoning a reservation attempt simply drops the
/// chain and all of its blocks. Calling [`BlockChain::into_head`] is the single
/// ownership transition that makes the queue responsible for those blocks.
pub struct BlockChain<T> {
    /// First block to install when the queue needs a successor.
    head: NonNull<Block<T>>,
    /// Last block, used to grow the private chain without traversing it.
    tail: NonNull<Block<T>>,
    /// Total number of slots in the chain, not the number of blocks.
    cap: usize,
}

impl<T> Drop for BlockChain<T> {
    /// Reclaims a chain that was never transferred to the queue.
    ///
    /// The links are private and non-atomic while the producer owns the chain,
    /// so walking from `head` through `tail` is sufficient to free every block.
    fn drop(&mut self) {
        let Self { mut head, tail, .. } = *self;

        loop {
            let next = *unsafe { head.as_mut() }.next_mut().get_mut();
            drop(unsafe { Box::from_raw(head.as_ptr()) });

            if head == tail {
                break;
            }

            head = unsafe { NonNull::new_unchecked(next) };
        }
    }
}

impl<T> BlockChain<T> {
    /// Transfers the entire chain to the queue and returns its first block.
    ///
    /// After this call the blocks must already be reachable, or be about to
    /// become reachable, from a queue-owned link. Forgetting `self` prevents
    /// its destructor from freeing memory now owned by the queue.
    fn into_head(self) -> NonNull<Block<T>> {
        let head = self.head;
        self.forget();
        head
    }

    /// Consumes `self` and does not call [`drop`](Self::drop).
    fn forget(self) {
        core::mem::forget(self);
    }
}

pub struct DUBQ<T> {
    phead: CachePadded<AtomicInt>,
    chead: CachePadded<AtomicInt>,
    min_block_size: usize,
    pool: Box<[CachePadded<AtomicPtr<Block<T>>>]>,
}

// SAFETY: Queue positions are claimed atomically and slot publication/reads are
// synchronized with Release/Acquire orderings. Values may cross threads only
// when their type is itself safe to send.
unsafe impl<T: Send> Send for DUBQ<T> {}
// SAFETY: Shared access coordinates all mutable queue state through atomics;
// the stored values are transferred between threads rather than shared by
// reference.
unsafe impl<T: Send> Sync for DUBQ<T> {}

impl<T> Drop for DUBQ<T> {
    fn drop(&mut self) {
        // With the last Arc gone there can be no live iterator or queue
        // operation. The current consumer block is the first block still owned
        // by the linked queue; fully consumed predecessors were already pooled
        // or freed.
        let mut ptr = CHead::<T>::from_u128(self.chead.as_u128().load(Ordering::Relaxed)).ptr;
        while !ptr.is_null() {
            let mut block = unsafe { Box::from_raw(ptr) };
            ptr = *block.next_mut().get_mut();
        }

        for pooled in self.pool.iter_mut() {
            let ptr = *pooled.get_mut();
            if !ptr.is_null() {
                drop(unsafe { Box::from_raw(ptr) });
            }
        }
    }
}

/// The consumer range claimed by one call to [`DUBQ::pop_batch`].
///
/// Keeping the three results together makes it explicit that the iterator's
/// bounds and its size hint all describe the same successful reservation.
struct PopReservation<T> {
    /// First slot owned by the returned iterator.
    start: CHead<T>,
    /// First slot after the range owned by the returned iterator.
    end: CHead<T>,
    /// Number of queue positions in the range, including any `SKIP` slots.
    slots: usize,
}

impl<T> DUBQ<T> {
    /// Creates an uninitialized queue with an optional cache of recyclable
    /// blocks. The first non-empty push allocates and publishes the first
    /// producer and consumer heads.
    pub fn new(pool_size: usize, min_block_size: u16) -> Arc<Self> {
        Arc::new(Self {
            phead: CachePadded::new(AtomicInt::new(0)),
            chead: CachePadded::new(AtomicInt::new(0)),
            min_block_size: (min_block_size as usize).clamp(1, MAX_BLOCK_LENGTH),

            pool: new_filled_box_slice(|| CachePadded::new(AtomicPtr::new(null_mut())), pool_size),
        })
    }

    /// Builds a private chain from blocks currently available in the pool.
    ///
    /// Returns `None` when no first block is available. Otherwise, links as
    /// many pooled blocks as possible, stopping once `request` slots have been
    /// collected or the pool is empty. The returned capacity can therefore be
    /// smaller than `request`; [`DUBQ::ensure_ext`] grows it if necessary.
    fn take_from_pool(&self, request: usize) -> Option<BlockChain<T>> {
        if request == 0 {
            return None;
        }

        let take_block = || {
            self.pool
                .iter()
                .find_map(|p| NonNull::new(p.swap(null_mut(), Ordering::Acquire)))
        };

        let head = take_block()?;
        let mut chain = BlockChain {
            head,
            tail: head,
            cap: unsafe { head.as_ref() }.len(),
        };

        while chain.cap < request {
            let Some(block) = take_block() else { break };

            *unsafe { chain.tail.as_mut() }.next_mut().get_mut() = block.as_ptr();
            chain.tail = block;
            chain.cap += unsafe { block.as_ref() }.len();
        }

        Some(chain)
    }

    /// Resets one consumed block and attempts to place it in the pool.
    ///
    /// Resetting clears the old successor link and slot states, so this method
    /// handles one independently owned block, not an attached chain. If every
    /// pool entry is occupied, the block is returned to the caller in `Err` and
    /// will normally be dropped there.
    fn give_to_pool(&self, mut block: Box<Block<T>>) -> Result<(), Box<Block<T>>> {
        Block::reset(block.as_mut());

        let ptr = Box::into_raw(block);

        if self.pool.iter().any(|p| {
            p.compare_exchange(null_mut(), ptr, Ordering::Release, Ordering::Relaxed)
                .is_ok()
        }) {
            Ok(())
        } else {
            Err(unsafe { Box::from_raw(ptr) })
        }
    }

    /// Acquires a complete producer-head snapshot, including its pointer.
    ///
    /// A wide load is required initially and whenever a narrow CAS failure
    /// reports a different token, because the narrow value has no pointer.
    fn acquire_phead(&self) -> PHead<T> {
        PHead::from_u128(self.phead.as_u128().load(Ordering::Acquire))
    }

    /// Acquires a complete consumer-head snapshot, including its pointer.
    ///
    /// Similar to [`acquire_phead`](Self::acquire_phead).
    fn acquire_chead(&self) -> CHead<T> {
        CHead::from_u128(self.chead.as_u128().load(Ordering::Acquire))
    }

    /// Ensures that this producer privately owns enough extension capacity for
    /// a reservation attempted against `excess`.
    ///
    /// `excess` is linked successor capacity and deliberately excludes the
    /// current block. Requiring `len + 1` total successor/extension slots is
    /// conservative, but guarantees that even if the successful reservation
    /// starts at the current block's boundary, it can store every item and
    /// publish a normalized producer head with progress space remaining.
    ///
    /// A chain retained after a failed CAS is reused. It is grown only when a
    /// newer producer-head snapshot reports less useful excess than the one for
    /// which the chain was originally prepared.
    ///
    /// `ext` is guaranteed to be Some(...) after this function finishes.
    fn ensure_ext(&self, ext: &mut Option<BlockChain<T>>, excess: Excess, len: usize) {
        let target = len.checked_add(1).expect("batch is too large");
        let req = target.saturating_sub(excess.known_slots());

        if req == 0 || ext.as_ref().is_some_and(|chain| chain.cap >= req) {
            return;
        }

        let mut chain = ext
            .take()
            .or_else(|| self.take_from_pool(req))
            .unwrap_or_else(|| {
                let len = req.clamp(self.min_block_size, MAX_BLOCK_LENGTH);
                // `Box::leak` transfers the allocation into the raw block chain
                // using stable Rust; `BlockChain` remains responsible for it.
                let block = NonNull::from(Box::leak(Block::new_boxed(len, None)));

                BlockChain {
                    head: block,
                    tail: block,
                    cap: len,
                }
            });

        while chain.cap < req {
            let block_len = (req - chain.cap).clamp(self.min_block_size, MAX_BLOCK_LENGTH);
            let block = NonNull::from(Box::leak(Block::new_boxed(block_len, None)));

            *unsafe { chain.tail.as_mut() }.next_mut().get_mut() = block.as_ptr();
            chain.tail = block;
            chain.cap += block_len;
        }

        *ext = Some(chain);
    }

    /// Attempts to turn the all-zero queue into an initialized queue while
    /// reserving its first `len` slots.
    ///
    /// Contract:
    ///
    /// - `ext` contains at least `len + 1` private slots on entry.
    /// - `Ok(start)` means this producer won the zero-head CAS, transferred the
    ///   chain to the queue, initialized `chead`, normalized `phead` if the
    ///   reservation crossed a block, and may now write from `start`.
    /// - `Err(real)` means another producer initialized the queue. The returned
    ///   value is the full head observed by the failed wide CAS, and `ext` must
    ///   remain locally owned so the ordinary reservation loop can reuse it.
    ///
    /// Initialization publishes `phead` and `chead` with separate atomics. The
    /// ordinary producer path must therefore treat `phead != ZERO && chead ==
    /// ZERO` as "initialization in progress" and wait. Otherwise a second
    /// producer could return before consumers can see the queue's first block.
    fn try_init(
        &self,
        len: usize,
        len16: u16,
        ext: &mut Option<BlockChain<T>>,
    ) -> Result<PHead<T>, PHead<T>> {
        // Suggested implementation outline:
        //
        // 1. Inspect `ext` without taking ownership and construct `start` from its
        //    first block.
        // 2. Set `start.excess` to the chain capacity after the first block. This
        //    advertises all already-linked successors, saturating through
        //    [`Excess::add_capacity`].

        // 3. Construct the initial reservation marker using `len16`. The exact
        //    `len` is still retained for traversal across arbitrarily many blocks.

        // 4. Compare-exchange the full zero `phead` with that marker.

        // 5. On failure, return `PHead::from_u128(real)` without taking `ext`.
        // 6. On success, take the chain and call `into_head`, publish `chead`, and
        //    call [`DUBQ::finish_boundary`] when the marker reached the boundary.

        // Keep all ownership changes below the successful CAS. In particular,
        // do not call `ext.take()` or `into_head()` while failure is possible.

        let (head, cap) = {
            let chain = ext
                .as_ref()
                .expect("ext must contain at least len + 1 slots");

            (chain.head, chain.cap)
        };

        let mut start = PHead::from_ptr(head);
        let suc_cap = cap
            .checked_sub(usize::from(start.block_length))
            .expect("chain capacity includes its first block");

        start.excess = start.excess.add_capacity(suc_cap);

        let cstart = CHead::from_ptr(head);

        let marker = PHead {
            index: len16,
            ..start
        };

        match self.phead.as_u128().compare_exchange(
            PHead::<T>::ZERO.pack_u128(),
            marker.pack_u128(),
            Ordering::Release,
            Ordering::Acquire,
        ) {
            Ok(_) => {
                self.chead
                    .as_u128()
                    .store(cstart.pack_u128(), Ordering::Release);

                // Queue now owns `ext`.
                let _ = ext.take().map(BlockChain::forget);

                if marker.index >= marker.block_length {
                    self.finish_boundary(start, len, None);
                }

                Ok(start)
            }
            Err(real) => return Err(PHead::from_u128(real)),
        }
    }

    /// Completes a reservation whose narrow CAS installed a boundary marker.
    ///
    /// Installing that marker gives this producer exclusive responsibility for
    /// advancing the producer head: other producers observe
    /// `index >= block_length` and wait. Starting from the old normalized head,
    /// this function walks exactly `len` reserved slots, follows already-linked
    /// successors, and installs `ext` only when it encounters a null tail link.
    /// It finally replaces the marker with one full-width, normalized `phead`.
    ///
    /// The strict `remaining < available` test is essential. Equality advances
    /// into the successor and publishes index zero there; publishing an index
    /// equal to the current block length would leave a permanent boundary
    /// marker and make all other producers wait.
    fn finish_boundary(&self, start: PHead<T>, len: usize, mut ext: Option<BlockChain<T>>) {
        let mut end = start;
        let mut remaining = len;

        loop {
            let available = usize::from(end.block_length - end.index);

            if remaining < available {
                end.index += remaining as u16;
                break;
            }

            remaining -= available;

            let block = unsafe { end.ptr.as_ref_unchecked() };
            let next = NonNull::new(block.next().load(Ordering::Acquire)).unwrap_or_else(|| {
                // A null link means the private extension becomes part of the
                // queue here. Add all of its capacity before entering its first
                // block, because `with_block` subtracts that destination block
                // from the successor-only `excess` count.
                let chain = ext.take().expect("capacity planning guarantees an ext");

                end.excess = end.excess.add_capacity(chain.cap);

                let next = chain.into_head();
                block.next().store(next.as_ptr(), Ordering::Release);
                next
            });

            end = end.with_block(next);
        }

        debug_assert!(end.index < end.block_length);

        self.phead
            .as_u128()
            .store(end.pack_u128(), Ordering::Release);
    }

    /// Reconstructs the next complete producer-head snapshot after a failed
    /// narrow CAS.
    ///
    /// The returned `u64` contains the token, excess, block length, and index,
    /// but not the pointer. If its token still matches `obs.token`, the pointer
    /// has not moved and it is safe to combine the returned low fields with
    /// `obs.ptr`. If the token differs, the producer head moved to another
    /// block, so this function must perform a new wide acquire rather than
    /// retaining a stale pointer.
    fn recover_phead(&self, obs: PHead<T>, real: u64) -> PHead<T> {
        let real = PHead::from_u64(real);

        if obs.token == real.token {
            PHead {
                ptr: obs.ptr,
                ..real
            }
        } else {
            self.acquire_phead()
        }
    }

    /// Atomically reserves `len` consecutive slots and returns their starting
    /// cursor after all required block links are present.
    ///
    /// This is the topology/coordination half of `push_batch`; it never calls
    /// the user iterator or writes slot payloads. Its retry loop owns a private
    /// extension chain and moves through three producer-head states:
    ///
    /// - zero: attempt first-block initialization with a full-width CAS;
    /// - boundary: another producer owns linkage, so wait and reload;
    /// - normalized: attempt a narrow reservation CAS in the current block.
    ///
    /// A successful CAS that remains strictly inside the block is already the
    /// final producer head. A successful boundary CAS grants exclusive linkage
    /// ownership, so [`DUBQ::finish_boundary`] must normalize the head before
    /// this function returns.
    fn reserve_batch<B: BackoffPolicy>(&self, len: usize) -> PHead<T> {
        let backoff = B::new();
        let len16 = usize_as_u16_or_MAX(len);

        let mut ext = None;
        let mut obs = self.acquire_phead();

        loop {
            // Initialization-publication gate: before treating a nonzero,
            // normalized head as generally usable, also ensure `chead` is no
            // longer zero. This matters only during the first push, because a
            // successfully initialized queue never restores `chead` to zero.

            // A boundary value is a temporary lock-free ownership marker, not
            // a location at which this producer may reserve slots.
            if !obs.is_zero() && obs.index >= obs.block_length {
                backoff.snooze();
                obs = self.acquire_phead();
                continue;
            }

            self.ensure_ext(&mut ext, obs.excess, len);

            if obs.is_zero() {
                match self.try_init(len, len16, &mut ext) {
                    Ok(start) => return start,
                    Err(real) => {
                        obs = real;

                        backoff.spin();
                        continue;
                    }
                }
            }

            let marker = PHead {
                // Saturation is sufficient here: values larger than u16::MAX
                // only need to communicate "this reservation crosses the
                // boundary". `finish_boundary` uses the exact usize `len`.
                index: obs.index.saturating_add(len16),
                ..obs
            };

            match self.phead.as_u64().compare_exchange_weak(
                obs.pack_u64(),
                marker.pack_u64(),
                Ordering::Release,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    if marker.index >= marker.block_length {
                        // This producer installed the boundary marker and is
                        // consequently the only producer allowed to link and
                        // publish the eventual successor head.
                        self.finish_boundary(obs, len, ext.take());
                    }

                    return obs;
                }
                Err(real) => {
                    obs = self.recover_phead(obs, real);

                    backoff.spin();
                    continue;
                }
            }
        }
    }

    /// Writes values (or `SKIP`s) into a range already reserved by
    /// [`DUBQ::reserve_batch`].
    ///
    /// Iterator underflow is represented by `Slot::write_opt(None)`, which
    /// publishes `SKIP` and preserves progress for all reserved positions.
    fn write_reserved<I>(&self, mut cursor: PHead<T>, len: usize, items: &mut I)
    where
        I: Iterator<Item = T>,
    {
        debug_assert!(
            cursor.index < cursor.block_length,
            "write_reserved should be given cursor that points to a valid slot"
        );

        let mut left = len;

        // Note:
        // There is a possibility that, upon writing to our slot, we may be
        // preempted and a consumer may immediately consume our slot, resetting
        // the block if that slot happens to be the last one consumed in said
        // block.
        //
        // For this reason, we must not write to the last slot in a block before
        // caching it's successor. This can be achieved by storing a `&Slot<T>`
        // before incrementing `cursor.index` and updating `cursor.ptr`, and
        // writing to this slot afterwards.

        for _ in 0..len {
            let slot = unsafe {
                cursor
                    .ptr
                    .as_ref_unchecked()
                    .get_slot_unchecked(cursor.index as usize)
            };

            cursor.index += 1;
            left -= 1;

            if left != 0 && cursor.index >= cursor.block_length {
                cursor = cursor.next();
            }

            slot.write_opt(items.next());
        }
    }

    /// Pushes the items as one consecutive queue reservation.
    ///
    /// The iterator is converted once and its [`ExactSizeIterator::len`] is
    /// sampled before any slots are reserved. Other producers cannot insert an
    /// item within this batch, although a consumer may observe the batch a slot
    /// at a time as its values are published.
    ///
    /// Reservation and writing are separate phases. Consequently, a slow
    /// [`Iterator::next`] does not prevent later producers from reserving their
    /// own slots, but consumers that reach an unpublished slot may wait for it.
    ///
    /// `ExactSizeIterator` implementations are expected to report their length
    /// accurately. If the iterator ends early, the remaining reserved positions
    /// are published as skips so that consumers can continue. If it yields more
    /// than its reported length, the excess values are not pushed.
    #[doc(alias = "enqueue_batch")]
    #[doc(alias = "send_batch")]
    pub fn push_batch<I, B>(&self, items: I)
    where
        B: BackoffPolicy,
        I: IntoIterator<Item = T>,
        I::IntoIter: ExactSizeIterator,
    {
        let mut items = items.into_iter();
        let len = items.len();

        if len == 0 {
            return;
        }

        let start = self.reserve_batch::<B>(len);
        self.write_reserved(start, len, &mut items);
    }

    /// Plans the block-local consumer-head value for one reservation attempt.
    ///
    /// `start` must be a nonzero, normalized head. Its `has_next` bit is a
    /// cached proof that the producer has advanced beyond this block. Without
    /// that proof, this function acquires `phead` and either establishes it,
    /// caps the proposal at the producer frontier, or reports an empty queue.
    fn plan_pop_end(&self, start: CHead<T>, request: u16) -> Option<CHead<T>> {
        // Any value at or beyond `block_length` is only a temporary boundary
        // marker; the exact `usize` request is used later when the boundary
        // owner walks successor blocks.
        let mut end = CHead {
            index: start.index.saturating_add(request),
            ..start
        };

        if start.has_next {
            // A preceding observation already proved that at least one complete
            // successor exists, so this block can be reserved through its end
            // without reloading the producer head.
            return Some(end);
        }

        let phead = self.acquire_phead();

        if start.ptr != phead.ptr {
            // The producer is in a later block. Record that fact in the marker
            // so competing consumers can avoid another wide producer-head load.
            end.has_next = true;
        } else if start.index == phead.index {
            // Consumer and producer point to the same slot, which is the queue's
            // empty state. Nothing is installed in `chead` in this case.
            return None;
        } else {
            // Both heads are in this block, so only the already-reserved prefix
            // ending at `phead.index` is currently available to consumers.
            end.index = end.index.min(phead.index);
        }

        Some(end)
    }

    /// Reconstructs a complete consumer head after a failed narrow CAS.
    ///
    /// A narrow CAS reports every consumer-head field except the pointer. When
    /// its token still matches `observed.token`, the pointer cannot have moved
    /// and can safely be reused. A changed token means another consumer crossed
    /// a block, requiring a new full-width acquire.
    fn recover_chead(&self, observed: CHead<T>, real: u64) -> CHead<T> {
        let real = CHead::from_u64(real);

        if observed.token == real.token {
            CHead {
                ptr: observed.ptr,
                ..real
            }
        } else {
            self.acquire_chead()
        }
    }

    /// Claims the initial, block-local portion of a pop reservation.
    ///
    /// The successful narrow CAS either installs the final in-block consumer
    /// head or a boundary marker. A boundary marker gives this consumer sole
    /// responsibility for walking successor blocks and publishing a normalized
    /// full-width head before another consumer can reserve more positions.
    fn claim_pop_batch<B: BackoffPolicy>(
        &self,
        request: u16,
        backoff: &B,
    ) -> Option<(CHead<T>, CHead<T>)> {
        let mut start = self.acquire_chead();

        if start.is_zero() {
            // No producer has initialized the first block yet.
            return None;
        }

        loop {
            if start.index >= start.block_length {
                // Another consumer owns the boundary transition. Its final
                // wide store will replace this marker with a usable head.
                backoff.snooze();

                start = self.acquire_chead();
                continue;
            }

            let end = self.plan_pop_end(start, request)?;

            match self.chead.as_u64().compare_exchange_weak(
                start.pack_u64(),
                end.pack_u64(),
                Ordering::Release,
                Ordering::Acquire,
            ) {
                Ok(_) => return Some((start, end)),
                Err(real) => {
                    // Preserve the acquired pointer when the token proves that
                    // the failed CAS still describes the same consumer block.
                    start = self.recover_chead(start, real);

                    backoff.spin();
                }
            }
        }
    }

    /// Completes a pop reservation whose narrow CAS reached a block boundary.
    ///
    /// The caller owns the consumer boundary marker. This function first waits
    /// for a normalized producer frontier, then walks at most `request` slots,
    /// stopping at that frontier when fewer positions are currently available.
    /// Finally it replaces the marker with the full endpoint in one wide store.
    fn finish_pop_boundary<B: BackoffPolicy>(
        &self,
        start: CHead<T>,
        request: usize,
        backoff: &B,
    ) -> (CHead<T>, usize) {
        // The successful marker claimed every remaining slot in the first
        // block. The marker condition guarantees that this subtraction cannot
        // underflow.
        let first_block_slots = usize::from(start.block_length - start.index);
        let mut remaining = request - first_block_slots;
        let mut reserved = first_block_slots;

        // A producer uses `index >= block_length` as its own boundary marker.
        // Waiting for normalization prevents us from treating that temporary
        // value as an actual producer endpoint.
        let phead = loop {
            let phead = self.acquire_phead();

            if phead.index < phead.block_length {
                break phead;
            }

            backoff.snooze();
        };

        // Reaching the consumer boundary was legal only because a successor
        // was known to exist. Once the producer is normalized it must therefore
        // be in a later block.
        debug_assert_ne!(start.ptr, phead.ptr);

        let mut cursor = start.await_next_head(backoff);

        let end = loop {
            if cursor.ptr == phead.ptr {
                // The producer frontier caps the available prefix of its
                // current block, even when the caller requested more.
                let take = remaining.min(usize::from(phead.index));

                cursor.index = take as u16;
                cursor.has_next = false;
                reserved += take;

                break cursor;
            }

            // A block strictly before `phead` was completely reserved by
            // producers, so the consumer may claim as much of it as requested.
            let block_length = usize::from(cursor.block_length);

            if remaining < block_length {
                cursor.index = remaining as u16;
                cursor.has_next = true;
                reserved += remaining;

                break cursor;
            }

            // Equality deliberately advances to index zero in the next block.
            // Leaving index == block_length would publish another boundary
            // marker with no consumer assigned to finish it.
            remaining -= block_length;
            reserved += block_length;

            cursor = cursor.await_next_head(backoff);
        };

        self.chead
            .as_u128()
            .store(end.pack_u128(), Ordering::Release);

        (end, reserved)
    }

    /// Reserves up to `request` consecutive consumer positions.
    ///
    /// This is the coordination half of [`DUBQ::pop_batch`]; it reserves queue
    /// positions but never reads their slots. Returning `None` means the queue
    /// was empty at the producer frontier observed by this attempt.
    fn reserve_pop_batch<B: BackoffPolicy>(&self, request: usize) -> Option<PopReservation<T>> {
        if request == 0 {
            return None;
        }

        let backoff = B::new();
        let request16 = usize_as_u16_or_MAX(request);

        let (start, marker) = self.claim_pop_batch(request16, &backoff)?;

        let (end, slots) = if marker.index >= marker.block_length {
            // We installed the boundary marker, so we alone must replace it
            // with the normalized endpoint returned by this traversal.
            self.finish_pop_boundary(start, request, &backoff)
        } else {
            // This reservation stayed within one block and the successful CAS
            // already installed its final endpoint.
            (marker, usize::from(marker.index - start.index))
        };

        Some(PopReservation { start, end, slots })
    }

    /// Reserves and returns up to `request` items from the front of the queue.
    ///
    /// The returned iterator owns one consecutive range, so concurrent
    /// consumers cannot take items from within that range. Reservation is eager
    /// but slot reads are lazy: this call advances the consumer head before the
    /// iterator yields its first item, and [`DUBQIter::next`] may wait for a
    /// producer to publish an already-reserved slot.
    ///
    /// The iterator can yield fewer than `request` items when the queue contains
    /// fewer available positions or when an inaccurate batched producer
    /// published skips. Consume it to exhaustion; dropping it early abandons
    /// the unvisited portion of its reservation, which later pops cannot reclaim.
    #[doc(alias = "dequeue_batch")]
    #[doc(alias = "receive_batch")]
    pub fn pop_batch<B: BackoffPolicy>(&self, request: usize) -> DUBQIter<'_, T, B> {
        let Some(res) = self.reserve_pop_batch::<B>(request) else {
            return DUBQIter::empty(self);
        };

        DUBQIter::new(self, res.start, res.end, res.slots)
    }
}

pub struct DUBQIter<'a, T, B: BackoffPolicy> {
    queue: &'a DUBQ<T>,
    start: CHead<T>,
    end: CHead<T>,
    left: usize,

    _marker: PhantomData<B>,
}

impl<'a, T, B: BackoffPolicy> Drop for DUBQIter<'a, T, B> {
    fn drop(&mut self) {
        for _ in 0..self.left {
            let _ = self.next();
        }
    }
}

impl<'a, T, B: BackoffPolicy> Iterator for DUBQIter<'a, T, B> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        let backoff = B::new();

        // Check exhaustion before interpreting the cursor as a real block.
        // In particular, `DUBQIter::empty` uses two zero heads; attempting to
        // normalize that sentinel would dereference its null block pointer.
        if self.start.full_addr_eq(&self.end) {
            return None;
        }

        if self.start.index >= self.start.block_length {
            self.start = self.start.next();
        }

        while !self.start.full_addr_eq(&self.end) {
            let block_ptr = self.start.ptr;
            let block = unsafe { block_ptr.as_ref_unchecked() };

            let out = unsafe {
                block
                    .get_slot_unchecked(self.start.index as usize)
                    .read(&backoff)
            };

            self.start.index += 1;
            self.left -= 1;

            if self.left == 0 {
                self.start = self.end
            } else if self.start.index >= self.start.block_length {
                self.start = self.start.next();
            }

            if block.consumed().fetch_add(1, Ordering::AcqRel) == block.len() - 1 {
                let _ = self.queue.give_to_pool(unsafe { Box::from_raw(block_ptr) });
            }

            if out.is_some() {
                return out;
            }
        }

        None
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, Some(self.left))
    }
}

impl<'a, T, B: BackoffPolicy> DUBQIter<'a, T, B> {
    pub(crate) fn empty(queue: &'a DUBQ<T>) -> Self {
        Self {
            queue,
            start: CHead::ZERO,
            end: CHead::ZERO,
            left: 0,
            _marker: PhantomData,
        }
    }

    pub(crate) fn new(queue: &'a DUBQ<T>, start: CHead<T>, end: CHead<T>, left: usize) -> Self {
        Self {
            queue,
            start,
            end,
            left,
            _marker: PhantomData,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backoff::Crossbeam;
    use alloc::sync::Arc;
    use core::sync::atomic::{AtomicUsize, Ordering as CoreOrdering};

    struct DropCounter(Arc<AtomicUsize>);

    impl Drop for DropCounter {
        fn drop(&mut self) {
            self.0.fetch_add(1, CoreOrdering::Relaxed);
        }
    }

    #[test]
    fn pop_batch_stays_within_a_block_and_stops_at_the_producer_frontier() {
        let queue = DUBQ::new(0, 4);

        // Three values leave both heads in the initial four-slot block. The
        // first pop exercises the narrow-CAS-only path, while the oversized
        // second request must be capped at the current producer position.
        queue.push_batch::<_, Crossbeam>(0..3);

        assert_eq!(
            queue
                .pop_batch::<Crossbeam>(2)
                .collect::<alloc::vec::Vec<_>>(),
            [0, 1]
        );
        assert_eq!(
            queue
                .pop_batch::<Crossbeam>(usize::MAX)
                .collect::<alloc::vec::Vec<_>>(),
            [2]
        );
        assert!(queue.pop_batch::<Crossbeam>(1).next().is_none());
    }

    #[test]
    fn pop_batch_normalizes_an_endpoint_across_dynamic_blocks() {
        let queue = DUBQ::new(1, 4);

        // The first push nearly fills the initial block. The second push must
        // link an extension, giving the consumer a real boundary to traverse.
        queue.push_batch::<_, Crossbeam>(0..3);
        queue.push_batch::<_, Crossbeam>(3..11);

        assert_eq!(
            queue
                .pop_batch::<Crossbeam>(6)
                .collect::<alloc::vec::Vec<_>>(),
            (0..6).collect::<alloc::vec::Vec<_>>()
        );
        assert_eq!(
            queue
                .pop_batch::<Crossbeam>(10)
                .collect::<alloc::vec::Vec<_>>(),
            (6..11).collect::<alloc::vec::Vec<_>>()
        );
        assert!(queue.pop_batch::<Crossbeam>(1).next().is_none());
    }

    #[test]
    fn drop_releases_pending_values_across_linked_and_pooled_blocks() {
        let dropped = Arc::new(AtomicUsize::new(0));
        let queue = DUBQ::new(2, 4);

        queue.push_batch::<_, Crossbeam>((0..12).map(|_| DropCounter(Arc::clone(&dropped))));
        drop(
            queue
                .pop_batch::<Crossbeam>(5)
                .collect::<alloc::vec::Vec<_>>(),
        );
        assert_eq!(dropped.load(CoreOrdering::Relaxed), 5);

        drop(queue);
        assert_eq!(dropped.load(CoreOrdering::Relaxed), 12);
    }

    #[test]
    fn dropping_an_uninitialized_queue_is_a_noop() {
        drop(DUBQ::<DropCounter>::new(2, 4));
    }
}
