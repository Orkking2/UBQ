use crate::{backoff::BackoffPolicy, page::page_size, slot::Slot};
use alloc::{
    alloc::{alloc, dealloc, handle_alloc_error},
    boxed::Box,
    vec::Vec,
};
use core::{
    alloc::Layout,
    cell::UnsafeCell,
    marker::PhantomData,
    mem::{MaybeUninit, align_of, size_of},
    ptr::{self, null_mut},
    sync::atomic::{
        AtomicPtr, AtomicUsize,
        Ordering::{AcqRel, Acquire, Relaxed, Release},
    },
};
use crossbeam_utils::CachePadded;

const BLOCK_LAYOUT_CACHE_BUCKETS: usize = 64;

#[derive(Clone, Copy, Debug)]
struct BlockLayout {
    allocation: Layout,
    slots_len: usize,
}

impl BlockLayout {
    #[inline(never)]
    fn new<S, H>() -> Self {
        let page_size = page_size();
        let header = Layout::new::<Block<S, H>>();
        let slot = Layout::new::<S>();
        let slots_offset = Block::<S, H>::SLOTS_OFFSET;
        // Give zero-sized values distinct, correctly aligned logical slots.
        // For every non-ZST, Rust already rounds size_of::<S>() to align_of::<S>().
        let slot_stride = Block::<S, H>::slot_stride();

        assert!(
            header.align().max(slot.align()) <= page_size,
            "Block header or slot requires alignment greater than the system page size"
        );
        assert!(
            slots_offset < page_size,
            "Block header leaves no room for slots in one system page"
        );

        let slots_len = (page_size - slots_offset) / slot_stride;
        assert!(slots_len > 0, "one slot does not fit in a system page");

        let allocation = Layout::from_size_align(page_size, page_size)
            .expect("the system page size is not a valid allocation layout");

        Self {
            allocation,
            slots_len,
        }
    }
}

struct CachedBlockLayout {
    slots_offset: usize,
    slot_stride: usize,
    layout: BlockLayout,
    next: *mut Self,
}

static BLOCK_LAYOUT_CACHE: [AtomicPtr<CachedBlockLayout>; BLOCK_LAYOUT_CACHE_BUCKETS] =
    [const { AtomicPtr::new(null_mut()) }; BLOCK_LAYOUT_CACHE_BUCKETS];

/// One page-aligned, one-page allocation with an intrusive link, a
/// protocol-specific header, and trailing protocol-specific slots.
///
/// UBQ and kFIFO use this same storage type with different header and slot
/// parameters. That keeps page geometry, pointer tagging, allocation, and
/// linking homogeneous without adding UBQ's per-slot state to kFIFO.
pub(crate) struct Block<S, H> {
    next: AtomicPtr<Self>,
    header: H,
    _slots: PhantomData<[UnsafeCell<MaybeUninit<S>>]>,
}

impl<S, H> Block<S, H> {
    const SLOTS_OFFSET: usize = match Layout::new::<Self>().extend(Layout::new::<S>()) {
        Ok((_, offset)) => offset,
        Err(_) => panic!("Block header and slot layouts overflow usize"),
    };

    #[inline]
    fn layout() -> BlockLayout {
        let slots_offset = Self::SLOTS_OFFSET;
        let slot_stride = Self::slot_stride();
        let hash = slot_stride.wrapping_mul(31) ^ slots_offset;
        let bucket = &BLOCK_LAYOUT_CACHE[(hash ^ (hash >> 6)) & (BLOCK_LAYOUT_CACHE_BUCKETS - 1)];
        let mut head = bucket.load(Acquire);
        let mut cached = head;

        while !cached.is_null() {
            // SAFETY: cache nodes are immutable and never removed after their
            // release publication through the bucket pointer.
            let entry = unsafe { &*cached };
            if entry.slot_stride == slot_stride && entry.slots_offset == slots_offset {
                return entry.layout;
            }
            cached = entry.next;
        }

        let layout = BlockLayout::new::<S, H>();
        let new = Box::into_raw(Box::new(CachedBlockLayout {
            slots_offset,
            slot_stride,
            layout,
            next: head,
        }));

        loop {
            // SAFETY: new is unpublished after creation or a failed exchange.
            unsafe { (*new).next = head };

            match bucket.compare_exchange(head, new, AcqRel, Acquire) {
                Ok(_) => return layout,
                Err(current) => {
                    cached = current;
                    while cached != head {
                        // SAFETY: current and its immutable chain were acquired
                        // from the never-removing cache bucket.
                        let entry = unsafe { &*cached };
                        if entry.slot_stride == slot_stride && entry.slots_offset == slots_offset {
                            // SAFETY: the failed exchange did not publish new.
                            drop(unsafe { Box::from_raw(new) });
                            return entry.layout;
                        }
                        cached = entry.next;
                    }
                    head = current;
                }
            }
        }
    }

    /// Maximum number of trailing slots which fit after this header.
    #[inline]
    pub(crate) fn capacity() -> usize {
        Self::layout().slots_len
    }

    #[inline]
    const fn slots_offset() -> usize {
        Self::SLOTS_OFFSET
    }

    #[inline]
    const fn slot_stride() -> usize {
        let size = size_of::<S>();
        let align = align_of::<S>();
        if size > align { size } else { align }
    }

    #[inline(always)]
    unsafe fn slot_ptr(this: *mut Self, index: usize) -> *mut MaybeUninit<S> {
        debug_assert!(index < Self::capacity());
        // SAFETY: callers keep index within capacity; SLOTS_OFFSET and stride
        // preserve S's alignment inside the page-sized allocation.
        unsafe {
            this.cast::<u8>()
                .add(Self::slots_offset() + index * Self::slot_stride())
                .cast::<MaybeUninit<S>>()
        }
    }

    fn allocate(header: H) -> *mut Self {
        let block_layout = Self::layout();
        // SAFETY: block_layout is non-zero and was constructed by Layout.
        let allocation = unsafe { alloc(block_layout.allocation) };
        if allocation.is_null() {
            handle_alloc_error(block_layout.allocation)
        }

        let block = allocation.cast::<Self>();
        // SAFETY: the page is aligned and large enough for the header. Trailing
        // slots remain uninitialized for the protocol to initialize or fill.
        unsafe {
            block.write(Self {
                next: AtomicPtr::new(null_mut()),
                header,
                _slots: PhantomData,
            });
        }
        block
    }

    unsafe fn deallocate(this: *mut Self) {
        let allocation = Self::layout().allocation;
        // SAFETY: caller has handled every initialized trailing slot and owns
        // the allocation exclusively.
        unsafe {
            ptr::drop_in_place(this);
            dealloc(this.cast(), allocation);
        }
    }

    #[inline]
    pub(crate) fn next(&self) -> &AtomicPtr<Self> {
        &self.next
    }

    #[inline]
    pub(crate) fn next_mut(&mut self) -> &mut AtomicPtr<Self> {
        &mut self.next
    }

    #[inline]
    pub(crate) unsafe fn set_next_exclusive(this: *mut Self, next: *mut Self) {
        // SAFETY: caller owns this detached/reset block or the whole queue.
        unsafe { *(*this).next.get_mut() = next };
    }

    pub(crate) fn wait_next<B>(&self, backoff: &B) -> *mut Self
    where
        B: BackoffPolicy,
    {
        loop {
            let next = self.next.load(Acquire);
            if !next.is_null() {
                return next;
            }
            backoff.snooze();
        }
    }
}

pub(crate) struct MpmcHeader {
    consumed: CachePadded<AtomicUsize>,
}

/// UBQ's page block: common page storage with stateful MPMC slots.
pub(crate) type MpmcBlock<T> = Block<Slot<T>, MpmcHeader>;

impl<T> Block<Slot<T>, MpmcHeader> {
    pub(crate) fn new() -> *mut Self {
        let block = Self::allocate(MpmcHeader {
            consumed: CachePadded::new(AtomicUsize::new(0)),
        });

        for index in 0..Self::capacity() {
            // SAFETY: the allocation owns capacity slots and each slot is
            // initialized exactly once before the block is published.
            unsafe { Self::slot_ptr(block, index).write(MaybeUninit::new(Slot::new())) };
        }
        block
    }

    /// Drops unread values and frees an exclusively owned UBQ block.
    pub(crate) fn free(this: *mut Self) {
        unsafe {
            let block = &mut *this;
            let consumed = *block.header.consumed.get_mut();
            debug_assert!(consumed <= Self::capacity());

            for index in consumed..Self::capacity() {
                Self::slot_ptr(this, index)
                    .as_mut()
                    .unwrap_unchecked()
                    .assume_init_mut()
                    .drop_inner();
            }

            Self::deallocate(this);
        }
    }

    /// Resets all initialized UBQ slots and block atomics.
    pub(crate) fn reset(this: *mut Self) {
        unsafe {
            let block = &mut *this;
            for index in 0..Self::capacity() {
                Self::slot_ptr(this, index)
                    .as_mut()
                    .unwrap_unchecked()
                    .assume_init_mut()
                    .reset();
            }
            *block.next.get_mut() = null_mut();
            *block.header.consumed.get_mut() = 0;
        }
    }

    pub(crate) unsafe fn get_unchecked(&self, index: usize) -> &Slot<T> {
        // SAFETY: caller keeps index below the page-derived block capacity.
        unsafe {
            Self::slot_ptr(self as *const Self as *mut Self, index)
                .as_ref()
                .unwrap_unchecked()
                .assume_init_ref()
        }
    }

    /// Returns true upon full completion of this UBQ block.
    #[inline]
    pub(crate) fn consume(&self, quantity: usize) -> bool {
        self.header.consumed.fetch_add(quantity, AcqRel) + quantity == Self::capacity()
    }
}

pub(crate) struct SpmcHeader {
    produced: CachePadded<AtomicUsize>,
    consumed: CachePadded<AtomicUsize>,
}

/// kFIFO's page block: common page storage with plain SPMC payload slots.
pub(crate) type SpmcBlock<T> = Block<T, SpmcHeader>;

impl<T> Block<T, SpmcHeader> {
    #[inline]
    pub(crate) fn length() -> usize {
        Self::capacity()
    }

    pub(crate) fn new() -> *mut Self {
        Self::allocate(SpmcHeader {
            produced: CachePadded::new(AtomicUsize::new(0)),
            consumed: CachePadded::new(AtomicUsize::new(0)),
        })
    }

    /// Frees an exclusively owned block, dropping its unconsumed published
    /// suffix beginning at `first_unconsumed`.
    pub(crate) unsafe fn free(this: *mut Self, first_unconsumed: usize) {
        // SAFETY: caller proves this is no longer concurrently reachable.
        let block = unsafe { &mut *this };
        let produced = *block.header.produced.get_mut();
        debug_assert!(first_unconsumed <= produced);
        debug_assert!(produced <= Self::capacity());

        for index in first_unconsumed..produced {
            // SAFETY: every slot below produced was initialized and this suffix
            // was never moved out.
            unsafe { Self::slot_ptr(this, index).read().assume_init_drop() };
        }

        // SAFETY: the header/allocation are exclusive and slots were handled.
        unsafe { Self::deallocate(this) };
    }

    /// Clears only block-level state before cache insertion. Payload bytes need
    /// no reset because full consumption moved/dropped every initialized value.
    pub(crate) unsafe fn reset(this: *mut Self) {
        // SAFETY: caller owns a sealed, fully consumed block exclusively.
        let block = unsafe { &mut *this };
        *block.next.get_mut() = null_mut();
        *block.header.produced.get_mut() = 0;
        *block.header.consumed.get_mut() = 0;
    }

    #[inline]
    pub(crate) fn publish(&self, produced: usize) {
        debug_assert!(produced <= Self::length());
        debug_assert!(self.header.produced.load(Relaxed) <= produced);
        self.header.produced.store(produced, Release);
    }

    #[inline]
    pub(crate) fn acquire_produced(&self) -> usize {
        self.header.produced.load(Acquire)
    }

    #[inline(always)]
    pub(crate) unsafe fn write(this: *mut Self, index: usize, value: T) {
        debug_assert!(index < Self::length());
        // SAFETY: the sole producer owns this unpublished slot.
        unsafe { Self::slot_ptr(this, index).write(MaybeUninit::new(value)) };
    }

    #[inline(always)]
    pub(crate) unsafe fn read(this: *mut Self, index: usize) -> T {
        debug_assert!(index < Self::length());
        // SAFETY: caller owns a unique claim below an acquired produced prefix.
        unsafe { Self::slot_ptr(this, index).read().assume_init() }
    }

    /// Moves a contiguous initialized range directly into spare Vec storage.
    #[inline]
    pub(crate) unsafe fn append_to_vec(
        this: *mut Self,
        index: usize,
        quantity: usize,
        values: &mut Vec<T>,
    ) {
        debug_assert!(index + quantity <= Self::length());
        values.reserve(quantity);
        let old_len = values.len();
        // SAFETY: the consumer uniquely owns every source slot, Vec has spare
        // capacity for quantity values, and the allocations cannot overlap.
        unsafe {
            ptr::copy_nonoverlapping(
                Self::slot_ptr(this, index).cast::<T>(),
                values.as_mut_ptr().add(old_len),
                quantity,
            );
            values.set_len(old_len + quantity);
        }
    }

    /// Returns true for the last completion of a full block.
    #[inline]
    pub(crate) fn consume(&self, quantity: usize, block_length: usize) -> bool {
        let previous = self.header.consumed.fetch_add(quantity, AcqRel);
        debug_assert!(previous + quantity <= block_length);
        previous + quantity == block_length
    }
}

pub(crate) struct BlockChain<T> {
    root: *mut MpmcBlock<T>,
    size: usize,
}

impl<T> BlockChain<T> {
    pub(crate) fn new() -> Self {
        Self {
            root: null_mut(),
            size: 0,
        }
    }

    fn give(&mut self, block: *mut MpmcBlock<T>) {
        // SAFETY: blocks in a private chain are exclusively owned.
        unsafe { MpmcBlock::set_next_exclusive(block, self.root) };
        self.root = block;
        self.size += MpmcBlock::<T>::capacity();
    }

    /// Guarantees space for at least `size` elements.
    pub(crate) fn grow_to_fit_with(&mut self, size: usize, pool: &AtomicPtr<MpmcBlock<T>>) {
        if self.size < size {
            let new = pool.swap(null_mut(), Acquire);
            if !new.is_null() {
                self.give(new);
            }
        }

        while self.size < size {
            self.give(MpmcBlock::new());
        }
    }

    pub(crate) fn take(&mut self) -> (*mut MpmcBlock<T>, usize) {
        let root = self.root;
        let size = self.size;
        self.root = null_mut();
        self.size = 0;
        (root, size)
    }

    pub(crate) fn give_back(&mut self, root: *mut MpmcBlock<T>, size: usize) {
        self.root = root;
        self.size = size;
    }
}

impl<T> Drop for BlockChain<T> {
    fn drop(&mut self) {
        let mut block = self.root;
        self.root = null_mut();

        while !block.is_null() {
            let next = unsafe { *(*block).next_mut().get_mut() };
            MpmcBlock::free(block);
            block = next;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Block, MpmcBlock, SpmcBlock, SpmcHeader};
    use crate::{page::page_size, slot::Slot};
    use core::mem::{align_of, size_of};

    #[test]
    fn both_protocol_blocks_allocate_exactly_one_aligned_page() {
        let mpmc_layout = MpmcBlock::<u64>::layout();
        let spmc_layout = SpmcBlock::<u64>::layout();
        assert_eq!(mpmc_layout.allocation.size(), page_size());
        assert_eq!(mpmc_layout.allocation.align(), page_size());
        assert_eq!(spmc_layout.allocation, mpmc_layout.allocation);

        let mpmc = MpmcBlock::<u64>::new();
        let spmc = SpmcBlock::<u64>::new();
        assert_eq!(mpmc.addr() % page_size(), 0);
        assert_eq!(spmc.addr() % page_size(), 0);
        MpmcBlock::free(mpmc);
        // SAFETY: the test block is unpublished and empty.
        unsafe { SpmcBlock::free(spmc, 0) };
    }

    #[test]
    fn trailing_slots_are_aligned_and_maximal() {
        type Raw = Block<u64, SpmcHeader>;
        let slots_offset = Raw::slots_offset();
        assert_eq!(slots_offset % align_of::<u64>(), 0);
        assert_eq!(Raw::slot_stride(), size_of::<u64>());
        assert!(slots_offset + Raw::capacity() * Raw::slot_stride() <= page_size());
        assert!(slots_offset + (Raw::capacity() + 1) * Raw::slot_stride() > page_size());
    }

    #[test]
    fn spmc_slots_have_no_per_item_state_overhead() {
        assert_eq!(SpmcBlock::<u64>::slot_stride(), size_of::<u64>());
        assert!(SpmcBlock::<u8>::length() > SpmcBlock::<u64>::length());
        assert_eq!(SpmcBlock::<()>::slot_stride(), 1);
        assert!(SpmcBlock::<()>::length() >= SpmcBlock::<u8>::length());
    }

    #[test]
    fn mpmc_and_spmc_share_the_block_but_keep_distinct_slot_protocols() {
        assert_eq!(MpmcBlock::<u64>::slot_stride(), size_of::<Slot<u64>>());
        assert_eq!(SpmcBlock::<u64>::slot_stride(), size_of::<u64>());
        assert!(SpmcBlock::<u64>::capacity() > MpmcBlock::<u64>::capacity());
    }
}
