//! Public buffer API, reservation, bitmask publication and completion.

use super::{
    block::{Block, BlockList, Geometry, OwnedBlock, mask},
    freelist::FreeList,
};
use crate::backoff::{BackoffPolicy, Crossbeam};
use alloc::{boxed::Box, sync::Arc, vec::Vec};
use core::{
    fmt,
    marker::PhantomData,
    mem::MaybeUninit,
    ptr::{self, null_mut},
    sync::atomic::{
        AtomicPtr, AtomicUsize,
        Ordering::{self, AcqRel, Acquire, Relaxed, Release},
    },
};
use crossbeam_utils::CachePadded;

/// Slots per VBlock: one bit of each 64-bit publication mask.
pub const SLOTS: usize = 64;

/// Low bits of the producer index: how many blocks follow the producer's
/// block in the chain, saturated. The reservation position sits above them.
const COUNT_BITS: u32 = 8;
const COUNT_MASK: usize = (1 << COUNT_BITS) - 1;

/// buf.is_empty() ? Ok(buf) : Err(buf)
pub fn empty_vec_to_box_uninit<U>(buf: Vec<U>) -> Result<Box<[MaybeUninit<U>]>, Vec<U>> {
    if !buf.is_empty() {
        return Err(buf);
    }

    let (data, _, len) = buf.into_raw_parts();

    Ok(unsafe { Box::from_raw(ptr::slice_from_raw_parts_mut(data.cast(), len)) })
}

/// Runtime geometry. VBlocks always hold [`SLOTS`] slots; `slots_per_vblock`
/// exists so the geometry selectors shared with v8 (`-s64-v<VBlocks>`) apply.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Config {
    /// Slots per VBlock; must equal [`SLOTS`].
    pub slots_per_vblock: usize,
    /// Number of VBlocks in a single allocation.
    pub vblocks_per_block: usize,
}

impl Default for Config {
    fn default() -> Self {
        // 2048 x 64 = 131072 slots per block, v8's default block capacity.
        Self {
            slots_per_vblock: SLOTS,
            vblocks_per_block: 2048,
        }
    }
}

/// An invalid runtime allocation geometry.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConfigError {
    /// The VBlock count must be nonzero.
    ZeroCapacity,
    /// A VBlock holds exactly [`SLOTS`] slots.
    VBlockSize,
    /// The allocation or shifted reservation index cannot fit on this target.
    LayoutOverflow,
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::ZeroCapacity => "bitmask VBlock count must be nonzero",
            Self::VBlockSize => "bitmask VBlocks hold exactly 64 slots",
            Self::LayoutOverflow => "bitmask block geometry exceeds layout or index limits",
        })
    }
}
impl core::error::Error for ConfigError {}

/// An experimental unbounded MPMC queue whose VBlocks publish through bitmasks.
///
/// Reservations are v8's: a packed index per head, a sentinel VBlock while a
/// block boundary is crossed. Publication is one `fetch_or` per VBlock
/// segment on the producer side and one on the consumer side; a consumer
/// reads each slot as soon as its bit is set, so no producer index is read
/// to tell an initialized slot from a hole. A stalled producer still delays
/// the consumers waiting on its bits. Lock-free progress is not a contract.
///
/// `B` is the [`BackoffPolicy`] every wait in the queue uses, as for
/// [`crate::UBQ`]; it defaults to [`Crossbeam`].
pub struct UBQ<T, B = Crossbeam> {
    geometry: Geometry,
    producer: CachePadded<ProducerHead<T>>,
    consumer: CachePadded<ConsumerHead<T>>,
    freelist: CachePadded<FreeList<T>>,
    _backoff: PhantomData<B>,
}

// The pointer and packed reservation index are separate atomics.
struct ProducerHead<T> {
    block: AtomicPtr<Block<T>>,
    /// `position << COUNT_BITS | min(attached, COUNT_MASK)`.
    index: AtomicUsize,
    /// The last block of the chain and the exact number of blocks after
    /// `block`. Only the producer holding the boundary sentinel touches them.
    tail: AtomicPtr<Block<T>>,
    attached: AtomicUsize,
}

struct ConsumerHead<T> {
    block: AtomicPtr<Block<T>>,
    /// `position << 1 | has_next`.
    index: AtomicUsize,
}

/// (token, VBlock, offset) of a position on the sentinel-inclusive stride.
#[inline]
fn decode(position: usize, nv: usize) -> (usize, usize, usize) {
    let vindex = position / SLOTS;
    (vindex / (nv + 1), vindex % (nv + 1), position % SLOTS)
}

// The caller keeps the backing allocation or stack slot alive. These guards
// own only the not-yet-transferred input or the already-initialized output.
struct PushInput<T> {
    src: *mut T,
    len: usize,
    moved: usize,
}

impl<T> Drop for PushInput<T> {
    fn drop(&mut self) {
        unsafe {
            ptr::drop_in_place(ptr::slice_from_raw_parts_mut(
                self.src.add(self.moved),
                self.len - self.moved,
            ));
        }
    }
}

struct PopOutput<T> {
    dst: *mut T,
    len: usize,
}

impl<T> Drop for PopOutput<T> {
    fn drop(&mut self) {
        unsafe { ptr::drop_in_place(ptr::slice_from_raw_parts_mut(self.dst, self.len)) };
    }
}

// SAFETY: head CAS operations assign disjoint slot ranges. A slot is written
// once by its reserving producer before its produced bit is set (Release)
// and read once by its reserving consumer after that bit is seen (Acquire);
// the consumed bits and consumed_vblocks aggregate every read before a block
// is recycled and written again. Values are moved, never shared.
unsafe impl<T: Send, B> Send for UBQ<T, B> {}
unsafe impl<T: Send, B> Sync for UBQ<T, B> {}

impl<T, B: BackoffPolicy> Default for UBQ<T, B> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T, B> fmt::Debug for UBQ<T, B> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("bitmask::UBQ")
            .field("config", &self.geometry.config)
            .finish_non_exhaustive()
    }
}

impl<T, B: BackoffPolicy> UBQ<T, B> {
    /// Creates a queue with the default geometry, 2048 VBlocks per block.
    pub fn new() -> Self {
        Self::with_config(Config::default()).expect("invalid default geometry")
    }

    /// Creates an Arc-owned queue with default geometry.
    pub fn new_arc() -> Arc<Self> {
        Arc::new(Self::new())
    }

    /// Validates geometry and allocates the initial block. Layout errors are
    /// returned; allocator exhaustion uses Rust's normal allocation-error path.
    pub fn with_config(config: Config) -> Result<Self, ConfigError> {
        let geometry = Geometry::new::<T>(config)?;
        let block = OwnedBlock::new(geometry).into_raw();

        Ok(Self {
            geometry,
            producer: CachePadded::new(ProducerHead {
                block: AtomicPtr::new(block),
                index: AtomicUsize::new(0),
                tail: AtomicPtr::new(block),
                attached: AtomicUsize::new(0),
            }),
            consumer: CachePadded::new(ConsumerHead {
                block: AtomicPtr::new(block),
                index: AtomicUsize::new(0),
            }),
            freelist: CachePadded::new(FreeList::new()),
            _backoff: PhantomData,
        })
    }

    /// Frees currently available cached blocks and returns their count. Live
    /// queue blocks are untouched. Concurrent producers/returns can make this
    /// best effort; with immobile callers it empties the cache completely.
    pub fn flush_freelist(&self) -> usize {
        self.freelist.flush()
    }

    /// Returns the runtime geometry used by this queue.
    pub const fn config(&self) -> Config {
        self.geometry.config
    }

    /// Returns slots per allocated block (not the queue's unbounded capacity).
    pub const fn block_capacity(&self) -> usize {
        self.geometry.capacity
    }

    /// Returns bytes per allocation, including headers and alignment padding.
    pub const fn block_allocation_size(&self) -> usize {
        self.geometry.layout.size()
    }

    /// Returns the alignment of each block allocation.
    pub const fn block_allocation_align(&self) -> usize {
        self.geometry.layout.align()
    }

    /// The producer's reservation position, without the attached-block count.
    #[inline]
    fn producer_position(&self, order: Ordering) -> usize {
        self.producer.index.load(order) >> COUNT_BITS
    }

    /// Moves one value into the queue without allocating an input buffer.
    pub fn push(&self, value: T) {
        let mut value = MaybeUninit::new(value);
        let mut input = PushInput {
            src: value.as_mut_ptr(),
            len: 1,
            moved: 0,
        };
        // SAFETY: input owns the initialized stack value until transferred.
        unsafe { self.write_from(input.src, input.len, &mut input.moved) };
    }

    /// Move all the elements in `items` into the queue, returning the emptied
    /// Vec with its original allocation and capacity.
    pub fn push_batch(&self, mut items: Vec<T>) -> Vec<T> {
        let len = items.len();
        let mut input = PushInput {
            src: items.as_mut_ptr(),
            len,
            moved: 0,
        };
        // SAFETY: the guard now owns the initialized elements; the Vec retains
        // their allocation and outlives the guard, including during unwinding.
        unsafe {
            items.set_len(0);
            self.write_from(input.src, input.len, &mut input.moved);
        }
        drop(input);
        items
    }

    /// # Safety
    /// `src` contains `len` owned values and remains valid for this call. The
    /// caller must drop only the suffix after `moved` if the transfer unwinds.
    unsafe fn write_from(&self, src: *const T, len: usize, moved: &mut usize) {
        if len == 0 {
            return;
        }
        let backoff = B::new();

        let nv = self.config().vblocks_per_block;
        let cap = nv * SLOTS;
        let stride = (nv + 1) * SLOTS;

        // Blocks this call obtained for a boundary it may or may not cross.
        let mut spare = BlockList::new();

        let mut index = self.producer.index.load(Acquire);
        let mut block = self.producer.block.load(Acquire);

        loop {
            let attached = index & COUNT_MASK;
            let (token, mut vblock, mut offset) = decode(index >> COUNT_BITS, nv);

            if vblock == nv {
                backoff.snooze();

                index = self.producer.index.load(Acquire);
                block = self.producer.block.load(Acquire);

                continue;
            }

            let used = vblock * SLOTS + offset;

            // Slots reservable before the chain ends. The head must land in a
            // real block, so a reservation needs one slot more than it fills.
            // A saturated count still promises that many attached blocks.
            let runway = cap - used + attached * cap;

            if len >= runway {
                let shortfall = (len + 1 - runway).div_ceil(cap);

                if spare.len() < shortfall {
                    spare += self.freelist.take_all();

                    while spare.len() < shortfall {
                        spare.push(OwnedBlock::new(self.geometry));
                    }
                }
            }

            let end = (used + len).min(cap);
            let new_index = ((stride * token + end) << COUNT_BITS) | attached;

            match self
                .producer
                .index
                .compare_exchange_weak(index, new_index, AcqRel, Acquire)
            {
                Ok(_) => {
                    if end == cap {
                        // SAFETY: the sentinel grants this caller the block
                        // transition, and `spare` covers the reservation.
                        unsafe { self.advance_producer(block, token, used, len, &mut spare) };
                    }

                    while *moved < len {
                        let count = (SLOTS - offset).min(len - *moved);
                        let rblock = unsafe { &*block };
                        let vb = rblock.vblock(vblock);
                        let slots = rblock.slots(vblock, offset);
                        let next = if vblock + 1 == nv {
                            rblock.next.load(Acquire)
                        } else {
                            null_mut()
                        };

                        unsafe {
                            ptr::copy_nonoverlapping(src.add(*moved), slots, count);
                        }

                        // Transfer ownership before publication. Publication is
                        // our final block access: a consumer may recycle it.
                        *moved += count;
                        vb.produced.fetch_or(mask(offset, count), Release);
                        vblock += 1;
                        offset = 0;

                        if vblock == nv {
                            block = next;
                            vblock = 0;
                        }
                    }

                    break;
                }
                Err(real) => {
                    index = real;
                    block = self.producer.block.load(Acquire);
                    backoff.spin();
                }
            }
        }

        // Blocks obtained for a boundary another producer crossed first go
        // back to the cache, never to the allocator.
        self.freelist.give(spare);
    }

    /// Removes the next value, or returns None when no unclaimed value exists.
    /// The output buffer lives on the stack, including for an empty queue.
    pub fn pop(&self) -> Option<T> {
        let mut value = MaybeUninit::uninit();
        let mut output = PopOutput {
            dst: value.as_mut_ptr(),
            len: 0,
        };
        // SAFETY: the stack slot is writable; the guard owns completed copies.
        unsafe { self.read_into(output.dst, 1, &mut output.len) };
        if output.len == 0 {
            None
        } else {
            output.len = 0;
            Some(unsafe { value.assume_init() })
        }
    }

    /// Remove up to buf.len() elements. Returns a vector with buf.len() capacity,
    /// having read up to that many items.
    pub fn pop_batch(&self, mut buf: Box<[MaybeUninit<T>]>) -> Vec<T> {
        let cap = buf.len();
        let mut output = PopOutput {
            dst: buf.as_mut_ptr().cast(),
            len: 0,
        };
        // SAFETY: buf owns writable storage and outlives the initialized-prefix
        // guard. Only the completed prefix becomes initialized Vec elements.
        unsafe { self.read_into(output.dst, cap, &mut output.len) };
        let len = output.len;
        output.len = 0;
        drop(output);
        let dst = Box::into_raw(buf).cast();
        unsafe { Vec::from_raw_parts(dst, len, cap) }
    }

    /// # Safety
    /// `dst` has space for `cap` values. The caller owns exactly the prefix
    /// reported in `written`, including when a later operation unwinds.
    unsafe fn read_into(&self, dst: *mut T, cap: usize, written: &mut usize) {
        if cap == 0 {
            return;
        }
        let backoff = B::new();

        let nv = self.config().vblocks_per_block;
        let capacity = nv * SLOTS;
        let stride = (nv + 1) * SLOTS;

        let mut index = self.consumer.index.load(Acquire);
        let mut block = self.consumer.block.load(Acquire);

        loop {
            let has_next = index & 1;
            let ridx = index >> 1;

            let (token, vblock, offset) = decode(ridx, nv);

            if vblock == nv {
                backoff.snooze();

                index = self.consumer.index.load(Acquire);
                block = self.consumer.block.load(Acquire);

                continue;
            }

            let used = vblock * SLOTS + offset;
            let mut new_index = ((stride * token + (used + cap).min(capacity)) << 1) + has_next;

            if has_next == 0 {
                // The producer's reservation bounds ours; a stale (smaller)
                // value only under-reserves. Initialization is decided by the
                // produced bits, never by this position.
                let pidx = self.producer_position(Acquire);

                if ridx == pidx {
                    break;
                }

                if token == pidx / stride {
                    new_index = new_index.min(pidx << 1);
                    new_index &= !1;
                } else {
                    new_index |= 1;
                }
            }

            match self
                .consumer
                .index
                .compare_exchange_weak(index, new_index, AcqRel, Acquire)
            {
                Ok(_) => {
                    let nidx = new_index >> 1;

                    // Token and VBlock not yet updated (cleared)
                    let mut len = nidx - ridx;

                    if (nidx / SLOTS) % (nv + 1) == nv {
                        unsafe {
                            self.advance_consumer(block, token, new_index, cap, &mut len, &backoff);
                        }
                    }

                    let mut vblock = vblock;
                    let mut offset = offset;

                    while *written < len {
                        let count = (SLOTS - offset).min(len - *written);
                        let rblock = unsafe { &*block };
                        let vb = rblock.vblock(vblock);

                        // Every slot of this segment was reserved by a producer
                        // that will set its bit. Copy published slots in order
                        // as their bits appear.
                        let mut done = 0;

                        loop {
                            let produced = vb.produced.load(Acquire);
                            let ready = ((produced >> (offset + done)).trailing_ones() as usize)
                                .min(count - done);

                            if ready == 0 {
                                backoff.snooze();
                                continue;
                            }

                            unsafe {
                                ptr::copy_nonoverlapping(
                                    rblock.slots(vblock, offset + done),
                                    dst.add(*written),
                                    ready,
                                );
                            }

                            *written += ready;
                            done += ready;

                            if done == count {
                                break;
                            }
                        }

                        // Keep the successor before completion can return this block.
                        let next = if vblock + 1 == nv {
                            rblock.next.load(Acquire)
                        } else {
                            null_mut()
                        };

                        let bits = mask(offset, count);

                        if vb.consumed.fetch_or(bits, AcqRel) | bits == u64::MAX {
                            let consumed_vblocks = rblock.consumed_vblocks.fetch_add(1, AcqRel) + 1;

                            if consumed_vblocks == nv {
                                self.freelist
                                    .recycle(unsafe { OwnedBlock::from_raw(block) });
                            }
                        }

                        vblock += 1;
                        offset = 0;

                        if vblock == nv {
                            block = next;
                            vblock = 0;
                        }
                    }

                    break;
                }
                Err(real) => {
                    index = real;
                    block = self.consumer.block.load(Acquire);
                    backoff.spin();
                }
            }
        }
    }

    /// Called by the producer whose reservation reached the block end. Nobody
    /// else changes the producer head until the final index is stored, so the
    /// chain tail and the attached count are this caller's alone.
    ///
    /// # Safety
    /// The caller won the sentinel with `used` slots of `block` (token
    /// `token`) already reserved, and `spare` holds at least the blocks the
    /// reservation needs beyond the attached chain.
    unsafe fn advance_producer(
        &self,
        block: *mut Block<T>,
        token: usize,
        used: usize,
        len: usize,
        spare: &mut BlockList<T>,
    ) {
        let nv = self.config().vblocks_per_block;
        let cap = nv * SLOTS;
        let stride = (nv + 1) * SLOTS;

        // Attach everything the cache holds, every time, so later
        // reservations find their blocks already in the chain.
        *spare += self.freelist.take_all();

        let mut attached = self.producer.attached.load(Relaxed);

        if spare.len() != 0 {
            let extra = spare.len();
            let (first, last, _) = core::mem::replace(spare, BlockList::new()).into_raw_parts();
            let tail = self.producer.tail.load(Relaxed);
            // SAFETY: the chain tail's link is null and only this owner
            // writes it; the detached chain is ours until linked.
            unsafe { (*tail).next.store(first, Release) };
            self.producer.tail.store(last, Relaxed);
            attached += extra;
        }

        let depth = (used + len) / cap;
        let rem = (used + len) % cap;
        debug_assert!(1 <= depth && depth <= attached);

        let mut blk = block;
        for _ in 0..depth {
            // SAFETY: every link this walk follows was stored by this owner
            // above or published before an index this owner acquired.
            blk = unsafe { (*blk).next.load(Acquire) };
        }
        debug_assert!(!blk.is_null());

        attached -= depth;
        self.producer.attached.store(attached, Relaxed);
        self.producer.block.store(blk, Release);
        self.producer.index.store(
            ((stride * (token + depth) + rem) << COUNT_BITS) | attached.min(COUNT_MASK),
            Release,
        );
    }

    // Finish the reserved boundary transition before copying outside it.
    unsafe fn advance_consumer(
        &self,
        block: *mut Block<T>,
        token: usize,
        mut new_index: usize,
        cap: usize,
        len: &mut usize,
        backoff: &B,
    ) {
        let nv = self.config().vblocks_per_block;
        let capacity = nv * SLOTS;

        let mut pidx = self.producer_position(Acquire);
        let mut pdiv = pidx / SLOTS;

        while pdiv / (nv + 1) == token || pdiv % (nv + 1) == nv {
            backoff.snooze();
            pidx = self.producer_position(Acquire);
            pdiv = pidx / SLOTS;
        }

        let mut blk = block;

        loop {
            let nidx = new_index >> 1;
            let ndiv = nidx / SLOTS;

            let ntok = ndiv / (nv + 1);
            let ptok = pdiv / (nv + 1);

            if ntok == ptok {
                self.consumer.block.store(blk, Release);

                let left = cap - *len;

                new_index = (nidx + left.min(pidx - nidx)) << 1;

                *len += (new_index >> 1) - nidx;

                self.consumer.index.store(new_index, Release);

                break;
            } else if ndiv % (nv + 1) == nv {
                // Skip the sentinel before reserving the next block.
                blk = unsafe { &*blk }.next.load(Acquire);
                new_index = ((nidx + SLOTS) << 1) + 1;
            } else {
                let left = cap - *len;
                let count = left.min(capacity);

                *len += count;
                new_index = ((nidx + count) << 1) + 1;

                if count < capacity {
                    self.consumer.block.store(blk, Release);
                    self.consumer.index.store(new_index, Release);

                    break;
                }
            }
        }
    }
}

impl<T, B> Drop for UBQ<T, B> {
    fn drop(&mut self) {
        let block = core::mem::replace(self.consumer.block.get_mut(), null_mut());
        *self.producer.block.get_mut() = null_mut();
        *self.producer.tail.get_mut() = null_mut();

        // SAFETY: exclusive access excludes in-flight operations. With settled
        // reservations, the consumer heads the remaining active chain; earlier
        // blocks have been recycled. The producer is an alias into this chain.
        let mut blocks = unsafe { BlockList::from_raw(block) };

        while let Some(block) = blocks.pop() {
            drop(block);
        }
        // FreeList::drop handles the separately owned cached blocks.
    }
}

#[cfg(test)]
#[path = "tests/drop.rs"]
mod drop_tests;

#[cfg(test)]
#[path = "tests/transfer.rs"]
mod transfer_tests;
