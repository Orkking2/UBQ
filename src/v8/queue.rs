//! Public experimental queue API and reservation/publication protocol.

use super::{
    block::{Block, BlockList, Geometry, OwnedBlock},
    freelist::FreeList,
};
use alloc::{boxed::Box, sync::Arc, vec::Vec};
use core::{
    fmt,
    mem::MaybeUninit,
    ptr::{self, null_mut},
    sync::atomic::{
        AtomicPtr, AtomicUsize,
        Ordering::{AcqRel, Acquire, Relaxed, Release},
        fence,
    },
};
use crossbeam_utils::{Backoff, CachePadded};

/// buf.is_empty() ? Ok(buf) : Err(buf)
pub fn empty_vec_to_box_uninit<U>(buf: Vec<U>) -> Result<Box<[MaybeUninit<U>]>, Vec<U>> {
    if !buf.is_empty() {
        return Err(buf);
    }

    let (data, _, len) = buf.into_raw_parts();

    Ok(unsafe { Box::from_raw(ptr::slice_from_raw_parts_mut(data.cast(), len)) })
}

/// Read the name idiot. See [empty_vec_to_box_uninit].
pub unsafe fn empty_vec_to_box_uninit_unchecked<U>(buf: Vec<U>) -> Box<[MaybeUninit<U>]> {
    let (data, _, len) = buf.into_raw_parts();

    unsafe { Box::from_raw(ptr::slice_from_raw_parts_mut(data.cast(), len)) }
}

/// Runtime geometry, measured in element slots (headers/padding are additional).
/// Defaults are starting points for experiments, not tuned allocation sizes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Config {
    /// Number of contiguous T slots governed by one pair of publication counters.
    pub slots_per_vblock: usize,
    /// Number of virtual blocks in a single allocation.
    pub vblocks_per_block: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            slots_per_vblock: 4096,
            vblocks_per_block: 32,
        }
    }
}

/// An invalid runtime allocation geometry.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConfigError {
    /// Both dimensions must be nonzero.
    ZeroCapacity,
    /// The allocation or shifted reservation index cannot fit on this target.
    LayoutOverflow,
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::ZeroCapacity => "v8 block dimensions must be nonzero",
            Self::LayoutOverflow => "v8 block geometry exceeds layout or index limits",
        })
    }
}
impl core::error::Error for ConfigError {}

/// An experimental unbounded MPMC queue with virtual-block publication.
///
/// The reservation, publication, and reclamation protocol is still under
/// development. A stalled producer can delay consumers waiting for a stable
/// publication count. See `src/v8/DESIGN.md` before changing it.
///
/// Batch operations copy one virtual-block segment at a time. Whole-batch
/// atomicity and lock-free progress are not established contracts yet.
pub struct UBQ<T> {
    geometry: Geometry,
    producer: CachePadded<Head<T>>,
    consumer: CachePadded<Head<T>>,
    freelist: CachePadded<FreeList<T>>,
}

// The pointer and packed reservation index are separate atomics.
struct Head<T> {
    block: AtomicPtr<Block<T>>,
    index: AtomicUsize,
}

impl<T> Head<T> {
    fn new(block: *mut Block<T>) -> Self {
        Self {
            block: AtomicPtr::new(block),
            index: AtomicUsize::new(0),
        }
    }
}

// Decode the virtual-block quotient once, sharing it with the token division.
#[inline]
fn decode_index(ridx: usize, ns: usize, nv: usize) -> (usize, usize, usize) {
    let div = ridx / ns;
    (div / (nv + 1), div % (nv + 1), ridx % ns)
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

// TODO: Safety.
unsafe impl<T: Send> Send for UBQ<T> {}
unsafe impl<T: Send> Sync for UBQ<T> {}

impl<T> Default for UBQ<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> UBQ<T> {
    /// Creates a queue with experimental default geometry.
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
            producer: CachePadded::new(Head::new(block)),
            consumer: CachePadded::new(Head::new(block)),
            freelist: CachePadded::new(FreeList::new()),
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
        let backoff = Backoff::new();

        let nv = self.config().vblocks_per_block;
        let ns = self.config().slots_per_vblock;

        let mut index = self.producer.index.load(Acquire);
        let mut block = self.producer.block.load(Acquire);

        // Assume we are given a full extra block unless told otherwise.
        let min_blocks = len / (nv * ns);

        let mut blocks = self.freelist.acquire(min_blocks, self.geometry);

        loop {
            let has_next = index & 1;
            let ridx = index >> 1;

            let (token, mut vblock, mut offset) = decode_index(ridx, ns, nv);

            if vblock == nv {
                backoff.snooze();

                index = self.producer.index.load(Acquire);
                block = self.producer.block.load(Acquire);

                continue;
            }

            if block.is_null() {
                let new = self.freelist.acquire(1, self.geometry).into_raw();

                if self
                    .producer
                    .block
                    .compare_exchange(null_mut(), new, Release, Relaxed)
                    .is_ok()
                {
                    self.consumer.block.store(new, Release);
                    block = new;
                } else {
                    blocks += unsafe { BlockList::from_raw(new) };

                    index = self.producer.index.load(Acquire);
                    block = self.producer.block.load(Acquire);

                    continue;
                }
            }

            if (nv * ns) * (blocks.len() + has_next + 1) - (ns * vblock + offset) <= len {
                blocks += self.freelist.acquire(
                    len.saturating_sub(nv * ns * blocks.len()).div_ceil(nv * ns),
                    self.geometry,
                );
            }

            let new_index = ((((nv + 1) * ns) * token + (vblock * ns + offset + len).min(nv * ns))
                << 1)
                + has_next;

            match self
                .producer
                .index
                .compare_exchange_weak(index, new_index, AcqRel, Acquire)
            {
                Ok(_) => {
                    // We assume that there are enough blocks for us to push `len` elements to
                    // between the queue itself and `blocks` excess.
                    if ((new_index >> 1) / ns) % (nv + 1) == nv {
                        unsafe {
                            self.advance_producer(block, token, vblock * ns + offset, len, blocks);
                        }
                    } // Queue state repaired

                    while *moved < len {
                        let count = (ns - offset).min(len - *moved);
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
                        vb.produced.fetch_add(count, AcqRel);
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
        let backoff = Backoff::new();

        let nv = self.config().vblocks_per_block;
        let ns = self.config().slots_per_vblock;

        let mut index = self.consumer.index.load(Acquire);
        let mut block = self.consumer.block.load(Acquire);

        loop {
            let has_next = index & 1;
            let ridx = index >> 1;

            let (token, vblock, offset) = decode_index(ridx, ns, nv);

            if vblock == nv {
                backoff.snooze();

                index = self.consumer.index.load(Acquire);
                block = self.consumer.block.load(Acquire);

                continue;
            }

            let mut new_index =
                ((((nv + 1) * ns) * token + (vblock * ns + offset + cap).min(nv * ns)) << 1)
                    + has_next;

            if has_next == 0 {
                fence(AcqRel);
                let pidx = self.producer.index.load(Relaxed) >> 1;

                if ridx == pidx {
                    break;
                }

                let ptok = pidx / ((nv + 1) * ns);

                if token == ptok {
                    new_index = new_index.min(pidx << 1);
                    new_index &= !1;
                } else {
                    new_index |= 1;
                }
            }

            if block.is_null() {
                backoff.snooze();

                index = self.consumer.index.load(Acquire);
                block = self.consumer.block.load(Acquire);

                continue;
            }

            match self
                .consumer
                .index
                .compare_exchange_weak(index, new_index, AcqRel, Acquire)
            {
                Ok(_) => {
                    let nidx = new_index >> 1;
                    let ndiv = nidx / ns;

                    // Token and VBlock not yet updated (cleared)
                    let mut len = nidx - ridx;

                    if ndiv % (nv + 1) == nv {
                        unsafe {
                            self.advance_consumer(block, token, new_index, cap, &mut len, &backoff);
                        }
                    }

                    let mut token = token;
                    let mut vblock = vblock;
                    let mut offset = offset;

                    while *written < len {
                        let count = (ns - offset).min(len - *written);
                        let rblock = unsafe { &*block };
                        let vb = rblock.vblock(vblock);

                        loop {
                            let produced = vb.produced.load(Acquire);

                            if produced == ns {
                                break;
                            }

                            // Read produced first: a completed total is only a prefix
                            // when it catches up with this VBlock's reservations.
                            let pidx = self.producer.index.load(Acquire) >> 1;
                            let pdiv = pidx / ns;

                            if pdiv == (nv + 1) * token + vblock
                                && produced == pidx % ns
                                && produced >= offset + count
                            {
                                break;
                            }

                            backoff.snooze();
                        }

                        unsafe {
                            ptr::copy_nonoverlapping(
                                rblock.slots(vblock, offset),
                                dst.add(*written),
                                count,
                            );
                        }

                        // Keep the successor before completion can return this block.
                        let next = if vblock + 1 == nv {
                            rblock.next.load(Acquire)
                        } else {
                            null_mut()
                        };

                        *written += count;
                        let consumed = vb.consumed.fetch_add(count, AcqRel) + count;

                        if consumed == ns {
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
                            token += 1;
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
    // The sentinel reservation grants this caller the block transition.
    unsafe fn advance_producer(
        &self,
        block: *mut Block<T>,
        token: usize,
        mut off: usize,
        len: usize,
        blocks: BlockList<T>,
    ) {
        let nv = self.config().vblocks_per_block;
        let ns = self.config().slots_per_vblock;

        let mut curr = block;

        // Likely to be very short, allowed to be separate
        loop {
            let ref next_atm = unsafe { &*curr }.next;
            let next = next_atm.load(Relaxed);

            if next.is_null() {
                next_atm.store(blocks.into_raw(), Release);
                break;
            }

            curr = next;
        }

        let mut rem = len;
        let mut blk = block;

        let mut depth = 0;

        while rem >= nv * ns - off {
            rem -= nv * ns - off;
            blk = unsafe { &*blk }.next.load(Relaxed);
            off = 0;

            depth += 1;
        }

        let has_next = if !unsafe { &*blk }.next.load(Relaxed).is_null() {
            1
        } else {
            0
        };

        self.producer.block.store(blk, Release);
        self.producer.index.store(
            ((((nv + 1) * ns) * (token + depth) + off + rem) << 1) + has_next,
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
        backoff: &Backoff,
    ) {
        let nv = self.config().vblocks_per_block;
        let ns = self.config().slots_per_vblock;

        let mut pidx = self.producer.index.load(Acquire) >> 1;
        let mut pdiv = pidx / ns;

        while pdiv / (nv + 1) == token || pdiv % (nv + 1) == nv {
            backoff.snooze();
            pidx = self.producer.index.load(Acquire) >> 1;
            pdiv = pidx / ns;
        }

        let mut blk = block;

        loop {
            let nidx = new_index >> 1;
            let ndiv = nidx / ns;

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
                new_index = ((nidx + ns) << 1) + 1;
            } else {
                let left = cap - *len;
                let count = left.min(nv * ns);

                *len += count;
                new_index = ((nidx + count) << 1) + 1;

                if count < nv * ns {
                    self.consumer.block.store(blk, Release);
                    self.consumer.index.store(new_index, Release);

                    break;
                }
            }
        }
    }
}

impl<T> Drop for UBQ<T> {
    fn drop(&mut self) {
        let block = core::mem::replace(self.consumer.block.get_mut(), null_mut());
        *self.producer.block.get_mut() = null_mut();

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
