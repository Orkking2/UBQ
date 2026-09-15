//! Public buffer API, packed-head reservation, and vblock publication.

use super::{
    block::{self, Block, BlockChain, Geometry, VBlock},
    head::Head,
};
use crate::backoff::{BackoffPolicy, Crossbeam};
use alloc::{boxed::Box, sync::Arc, vec::Vec};
use core::{
    fmt,
    marker::PhantomData,
    mem::{ManuallyDrop, MaybeUninit},
    ptr,
    sync::atomic::{
        AtomicUsize,
        Ordering::{AcqRel, Acquire, Relaxed, Release},
    },
};
use crossbeam_epoch::Guard;
use crossbeam_utils::CachePadded;

/// Runtime geometry; fixed for the lifetime of a queue.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Config {
    /// Number of payload slots governed by one produced/consumed counter pair.
    pub slots_per_vblock: usize,
    /// Number of vblocks allocated together in one main block.
    pub vblocks_per_block: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            slots_per_vblock: 32,
            vblocks_per_block: 32,
        }
    }
}

/// An invalid allocation geometry.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConfigError {
    /// Both dimensions must be nonzero.
    ZeroCapacity,
    /// The allocation or packed reservation index cannot fit on this target.
    LayoutOverflow,
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::ZeroCapacity => "dynblock dimensions must be nonzero",
            Self::LayoutOverflow => "dynblock geometry exceeds layout or index limits",
        })
    }
}
impl core::error::Error for ConfigError {}

/// An experimental unbounded MPMC queue using UBQ's packed-head reservations.
///
/// Batches reserve one contiguous range, possibly spanning several vblocks.
/// Each copied segment publishes/completes through its vblock's counters.
/// Consumers wait for full publication or a stable partial-vblock prefix.
/// A stalled producer can therefore delay consumers and boundary reservations.
///
/// Completion of the last vblock retires its main allocation. Epoch protection
/// delays physical deallocation until old head readers have left. Under `std`
/// this uses Crossbeam's thread-local collector; `no_std` registers a temporary
/// handle per call with a queue-local collector.
///
/// `B` is the [`BackoffPolicy`] every wait in the queue uses, as for
/// [`crate::UBQ`]; it defaults to [`Crossbeam`].
pub struct UBQ<T, B = Crossbeam> {
    producer: CachePadded<AtomicUsize>,
    consumer: CachePadded<AtomicUsize>,
    geometry: Geometry,
    #[cfg(not(feature = "std"))]
    collector: crossbeam_epoch::Collector,
    _payload: PhantomData<T>,
    _backoff: PhantomData<B>,
}

// SAFETY: head CAS operations assign disjoint payload ranges. AcqRel completion
// counters aggregate producer writes before consumer Acquire reads. A slot is
// moved out exactly once; epoch guards protect stale pointers against reuse.
unsafe impl<T: Send, B> Send for UBQ<T, B> {}
unsafe impl<T: Send, B> Sync for UBQ<T, B> {}

impl<T, B: BackoffPolicy> Default for UBQ<T, B> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T, B> fmt::Debug for UBQ<T, B> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("dynblock::UBQ")
            .field("config", &self.geometry.config)
            .finish_non_exhaustive()
    }
}

impl<T, B: BackoffPolicy> UBQ<T, B> {
    /// Creates an empty queue with 32 slots per vblock and 32 vblocks per block.
    pub fn new() -> Self {
        Self::with_config(Config::default()).expect("invalid default dynblock geometry")
    }

    /// Creates a queue in an [`Arc`].
    pub fn new_arc() -> Arc<Self> {
        Arc::new(Self::new())
    }

    /// Validates geometry and allocates the initial main block.
    /// Allocator exhaustion follows Rust's normal allocation-error path.
    pub fn with_config(config: Config) -> Result<Self, ConfigError> {
        let geometry = Geometry::new::<T>(config)?;
        let mut blocks = BlockChain::<T>::empty();
        blocks.grow(1, geometry);
        let initial = Head::new(blocks.take()).encode(geometry);
        Ok(Self {
            producer: CachePadded::new(AtomicUsize::new(initial)),
            consumer: CachePadded::new(AtomicUsize::new(initial)),
            geometry,
            #[cfg(not(feature = "std"))]
            collector: crossbeam_epoch::Collector::new(),
            _payload: PhantomData,
            _backoff: PhantomData,
        })
    }

    /// Returns the queue's fixed runtime geometry.
    pub const fn config(&self) -> Config {
        self.geometry.config
    }

    /// Returns the number of slots governed by each vblock's counters.
    pub const fn block_length(&self) -> usize {
        self.geometry.config.slots_per_vblock
    }

    /// Returns payload slots per main allocation.
    pub fn block_capacity(&self) -> usize {
        self.geometry.capacity()
    }

    /// Returns bytes per main allocation, including headers and padding.
    pub const fn block_allocation_size(&self) -> usize {
        self.geometry.layout.size()
    }

    /// Returns the alignment of each main allocation.
    pub const fn block_allocation_align(&self) -> usize {
        self.geometry.layout.align()
    }

    /// Returns the power-of-two alignment used to recover vblock addresses.
    pub const fn vblock_alignment(&self) -> usize {
        self.geometry.vblock_alignment
    }

    fn pin(&self) -> Guard {
        #[cfg(feature = "std")]
        {
            crossbeam_epoch::pin()
        }
        #[cfg(not(feature = "std"))]
        {
            // Crossbeam keeps the registered local alive while its guard is
            // pinned, even after the temporary handle itself has been dropped.
            self.collector.register().pin()
        }
    }

    fn load_producer(&self) -> Head<T> {
        Head::decode(self.producer.load(Acquire), self.geometry)
    }

    fn load_consumer(&self) -> Head<T> {
        Head::decode(self.consumer.load(Acquire), self.geometry)
    }

    /// Returns whether the reservation heads currently coincide.
    /// An in-flight push may already have reserved values it has yet to publish.
    pub fn is_empty(&self) -> bool {
        let _guard = self.pin();
        let consumer = self.consumer.load(Acquire);
        consumer == self.producer.load(Acquire)
    }

    /// Pushes one value without allocating a temporary input buffer.
    pub fn push(&self, value: T) {
        let guard = self.pin();
        let start = self.reserve_push(1, &guard);
        // SAFETY: this reservation uniquely owns one uninitialized slot.
        unsafe {
            self.geometry.slots(start.block, start.index).write(value);
            (*start.block).produced.fetch_add(1, AcqRel);
        }
    }

    /// Moves a whole batch into one reservation and returns its emptied buffer.
    /// Copies and publication updates stop at each vblock boundary.
    pub fn push_batch(&self, mut values: Vec<T>) -> Vec<T> {
        if values.is_empty() {
            return values;
        }
        let guard = self.pin();
        let mut position = self.reserve_push(values.len(), &guard);
        let mut moved = 0;
        let len = values.len();
        // No user code or further allocation runs between moving the values
        // and publication. Clearing the length transfers ownership to slots.
        unsafe { values.set_len(0) };
        while moved < len {
            let count = (len - moved).min(self.block_length() - position.index);
            // SAFETY: reservation ranges are disjoint; source and destination
            // allocations cannot overlap, and all `len` source values existed.
            unsafe {
                ptr::copy_nonoverlapping(
                    values.as_ptr().add(moved),
                    self.geometry.slots(position.block, position.index),
                    count,
                );
                // Save the successor before publishing the final values can
                // let consumers complete this vblock.
                let next = (*position.block).next.load(Acquire);
                (*position.block).produced.fetch_add(count, AcqRel);
                position = Head::new(next);
            }
            moved += count;
        }
        values
    }

    /// Pops one value without allocating a temporary output buffer.
    pub fn pop(&self) -> Option<T> {
        let mut value = MaybeUninit::uninit();
        // SAFETY: output has capacity for the requested single value.
        let count = unsafe { self.pop_into(value.as_mut_ptr(), 1) };
        (count != 0).then(|| unsafe { value.assume_init() })
    }

    /// Moves up to the buffer's length into a Vec backed by that same allocation.
    pub fn pop_batch(&self, buffer: Box<[MaybeUninit<T>]>) -> Vec<T> {
        let mut buffer = Vec::from(buffer);
        // SAFETY: the buffer owns enough aligned, uninitialized output slots.
        let count = unsafe { self.pop_into(buffer.as_mut_ptr().cast(), buffer.len()) };
        let mut buffer = ManuallyDrop::new(buffer);
        let pointer = buffer.as_mut_ptr();
        let capacity = buffer.capacity();
        // SAFETY: exactly `count` slots were initialized; MaybeUninit<T> and T
        // have identical layouts, including for ZSTs and over-aligned values.
        unsafe { Vec::from_raw_parts(pointer.cast(), count, capacity) }
    }

    /// Reserves like UBQ: a successful CAS owns the range; the end marker gives
    /// its winner exclusive responsibility for advancing across linked blocks.
    fn reserve_push(&self, len: usize, _guard: &Guard) -> Head<T> {
        let needed = len.checked_add(1).expect("dynblock batch is too large");
        let length = self.block_length();
        let backoff = B::new();
        let mut spare = BlockChain::empty();
        let mut head = self.load_producer();
        loop {
            if head.index == length {
                backoff.snooze();
                head = self.load_producer();
                continue;
            }
            // SAFETY: the guard keeps this snapshot's allocation alive even if
            // another producer advances the head while we prepare spare blocks.
            let available = unsafe { self.geometry.remaining_capacity(head.block) } - head.index;
            spare.grow(needed.saturating_sub(available), self.geometry);
            let end = Head {
                index: head.index.saturating_add(len).min(length),
                ..head
            };
            match self.producer.compare_exchange_weak(
                head.encode(self.geometry),
                end.encode(self.geometry),
                AcqRel,
                Acquire,
            ) {
                Ok(_) => {
                    if end.index == length {
                        self.advance_producer(head, len, &mut spare);
                    }
                    return head;
                }
                Err(actual) => {
                    head = Head::decode(actual, self.geometry);
                    backoff.spin();
                }
            }
        }
    }

    /// Called only by the producer owning the end marker. No other producer
    /// can reserve until the final head is stored; all allocations are ready.
    fn advance_producer(&self, mut head: Head<T>, mut remaining: usize, spare: &mut BlockChain<T>) {
        loop {
            let available = self.block_length() - head.index;
            if remaining < available {
                head.index += remaining;
                self.producer.store(head.encode(self.geometry), Release);
                return;
            }
            // SAFETY: the caller's pin and reservation protect the chain.
            // Only this boundary owner writes an allocation's terminal link.
            unsafe {
                let link = &(*head.block).next;
                let mut next = link.load(Relaxed);
                if next.is_null() {
                    next = spare.take();
                    debug_assert!(!next.is_null());
                    link.store(next, Release);
                }
                remaining -= available;
                head = Head::new(next);
            }
        }
    }

    fn reserve_pop(&self, requested: usize, _guard: &Guard) -> (Head<T>, usize) {
        let length = self.block_length();
        let backoff = B::new();
        let mut head = self.load_consumer();
        loop {
            if head.index == length {
                backoff.snooze();
                head = self.load_consumer();
                continue;
            }
            let producer = self.load_producer();
            let count = if head.block == producer.block {
                requested.min(producer.index.saturating_sub(head.index))
            } else {
                requested
            };
            if count == 0 {
                return (head, 0);
            }
            let end = Head {
                index: head.index.saturating_add(count).min(length),
                ..head
            };
            match self.consumer.compare_exchange_weak(
                head.encode(self.geometry),
                end.encode(self.geometry),
                AcqRel,
                Acquire,
            ) {
                Ok(_) => {
                    let count = if end.index == length {
                        self.advance_consumer(head, count, &backoff)
                    } else {
                        count
                    };
                    return (head, count);
                }
                Err(actual) => {
                    head = Head::decode(actual, self.geometry);
                    backoff.spin();
                }
            }
        }
    }

    /// Traverse the reserved prefix while holding the consumer end marker.
    /// Stop at the producer head; exact fills move to the successor at index 0.
    fn advance_consumer(&self, mut head: Head<T>, requested: usize, backoff: &B) -> usize {
        let mut remaining = requested;
        loop {
            let producer = self.load_producer();
            if producer.block == head.block {
                if producer.index == self.block_length() {
                    backoff.snooze();
                    continue;
                }
                let count = remaining.min(producer.index - head.index);
                head.index += count;
                self.consumer.store(head.encode(self.geometry), Release);
                return requested - remaining + count;
            }
            let available = self.block_length() - head.index;
            if remaining < available {
                head.index += remaining;
                self.consumer.store(head.encode(self.geometry), Release);
                return requested;
            }
            remaining -= available;
            // SAFETY: the producer moved beyond this vblock, publishing its
            // successor. The guard and owned range protect the traversal.
            head = Head::new(unsafe { (*head.block).next.load(Acquire) });
            debug_assert!(!head.block.is_null());
        }
    }

    /// A total is not a prefix: a later producer may finish copying first.
    /// Read the publication count BEFORE the producer head. Equality means all
    /// reservations up to that head have published, including our requested end.
    fn wait_published(&self, block: *mut VBlock<T>, end: usize, backoff: &B) {
        while !self.is_published(block, end) {
            backoff.snooze();
        }
    }

    fn is_published(&self, block: *mut VBlock<T>, end: usize) -> bool {
        // SAFETY: the caller owns a consumer reservation in this vblock.
        let produced = unsafe { (*block).produced.load(Acquire) };
        if produced == self.block_length() {
            return true;
        }
        let producer = self.load_producer();
        producer.block == block && producer.index == produced && produced >= end
    }

    /// # Safety
    /// `output` has room for `requested` values and does not alias queue storage.
    unsafe fn pop_into(&self, output: *mut T, requested: usize) -> usize {
        if requested == 0 {
            return 0;
        }
        let guard = self.pin();
        let (mut position, len) = self.reserve_pop(requested, &guard);
        let backoff = B::new();
        let mut moved = 0;
        while moved < len {
            let count = (len - moved).min(self.block_length() - position.index);
            self.wait_published(position.block, position.index + count, &backoff);
            // SAFETY: this acquired prefix contains initialized values; our
            // reservation uniquely owns the source range and the output fits.
            unsafe {
                ptr::copy_nonoverlapping(
                    self.geometry.slots(position.block, position.index),
                    output.add(moved),
                    count,
                );
                let next = (*position.block).next.load(Acquire);
                self.complete(position.block, count, &guard);
                position = Head::new(next);
            }
            moved += count;
        }
        len
    }

    /// # Safety
    /// This reservation has copied out `count` slots and completes them once.
    unsafe fn complete(&self, block: *mut VBlock<T>, count: usize, guard: &Guard) {
        unsafe {
            let vblock = &*block;
            let previous = vblock.consumed.fetch_add(count, AcqRel);
            debug_assert!(previous + count <= self.block_length());
            if previous + count == self.block_length() && Block::release(vblock.owner) {
                let owner = vblock.owner;
                // All values are moved out and both heads have left this main
                // block. The callback only deallocates raw storage; it captures
                // no T or queue reference. A grace period prevents stale packed
                // heads from comparing equal to a newly reused allocation.
                guard.defer_unchecked(move || Block::free(owner));
            }
        }
    }
}

impl<T, B> Drop for UBQ<T, B> {
    fn drop(&mut self) {
        let head = Head::<T>::decode(*self.consumer.get_mut(), self.geometry);
        // SAFETY: exclusive ownership excludes in-flight reservations. Retired
        // allocations are disjoint from this remaining active suffix.
        unsafe { block::drop_remaining(head.block, self.geometry) };
    }
}

/// Reuses an empty Vec as an uninitialized output buffer, preserving capacity.
/// Returns the original Vec unchanged if it still contains values.
pub fn empty_vec_to_box_uninit<T>(buffer: Vec<T>) -> Result<Box<[MaybeUninit<T>]>, Vec<T>> {
    if !buffer.is_empty() {
        return Err(buffer);
    }
    let mut buffer = ManuallyDrop::new(buffer);
    let pointer = buffer.as_mut_ptr();
    let capacity = buffer.capacity();
    // SAFETY: the allocation holds no live values, the element layout is
    // unchanged, and the slice length matches its original Vec capacity.
    Ok(unsafe { Box::from_raw(ptr::slice_from_raw_parts_mut(pointer.cast(), capacity)) })
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
