use alloc::alloc::{alloc, dealloc, handle_alloc_error};
use core::{
    alloc::Layout,
    cell::UnsafeCell,
    marker::PhantomData,
    mem::MaybeUninit,
    ops::AddAssign,
    ptr::{self, NonNull, null_mut},
    sync::atomic::{AtomicPtr, AtomicU64, AtomicUsize, Ordering::Relaxed},
};
use crossbeam_utils::CachePadded;

use super::queue::{Config, ConfigError, SLOTS};

/// Geometry is computed once per queue; every cached block has this layout.
#[derive(Clone, Copy)]
pub(super) struct Geometry {
    pub(super) config: Config,
    pub(super) capacity: usize,
    pub(super) layout: Layout,
    vblocks_offset: usize,
    vblock_stride: usize,
    slots_offset: usize,
}

impl Geometry {
    pub(super) fn new<T>(config: Config) -> Result<Self, ConfigError> {
        if config.slots_per_vblock != SLOTS {
            return Err(ConfigError::VBlockSize);
        }
        if config.vblocks_per_block == 0 {
            return Err(ConfigError::ZeroCapacity);
        }

        let capacity = SLOTS
            .checked_mul(config.vblocks_per_block)
            .filter(|&n| n <= usize::MAX >> 1)
            .ok_or(ConfigError::LayoutOverflow)?;

        let overflow = |_| ConfigError::LayoutOverflow;

        let slots = Layout::array::<UnsafeCell<MaybeUninit<T>>>(SLOTS).map_err(overflow)?;

        let (vblock, slots_offset) = Layout::new::<VBlock>().extend(slots).map_err(overflow)?;
        let vblock = vblock.pad_to_align();

        let vblocks_size = vblock
            .size()
            .checked_mul(config.vblocks_per_block)
            .ok_or(ConfigError::LayoutOverflow)?;

        let vblocks = Layout::from_size_align(vblocks_size, vblock.align()).map_err(overflow)?;

        let (layout, vblocks_offset) = Layout::new::<Block<T>>()
            .extend(vblocks)
            .map_err(overflow)?;

        Ok(Self {
            config,
            capacity,
            layout: layout.pad_to_align(),
            vblocks_offset,
            vblock_stride: vblock.size(),
            slots_offset,
        })
    }
}

/// The bits of `count` slots from `offset`, both within one VBlock.
#[inline]
pub(super) fn mask(offset: usize, count: usize) -> u64 {
    debug_assert!(offset + count <= SLOTS);
    if count == 0 {
        0
    } else {
        (u64::MAX >> (SLOTS - count)) << offset
    }
}

// One allocation: [Block header][VBlock header | 64 T slots][VBlock header | 64 T slots]...
// VBlocks are views into trailing storage, never independent allocations.
pub(super) struct Block<T> {
    pub(super) next: AtomicPtr<Self>,
    pub(super) consumed_vblocks: AtomicUsize,
    pub(super) geometry: Geometry,

    _payload: PhantomData<UnsafeCell<T>>,
}

/// Bit `i` of `produced` says slot `i` holds a value; bit `i` of `consumed`
/// says that value has been moved out. Producers and consumers each own one
/// line, and no slot carries state of its own.
pub(super) struct VBlock {
    pub(super) produced: CachePadded<AtomicU64>,
    pub(super) consumed: CachePadded<AtomicU64>,
}

impl<T> Block<T> {
    /// The caller must keep this allocation alive for the returned reference.
    pub(super) fn vblock(&self, index: usize) -> &VBlock {
        debug_assert!(index < self.geometry.config.vblocks_per_block);

        // SAFETY: all VBlock headers are initialized on allocation; Geometry
        // includes each header's alignment and full stride.
        unsafe {
            &*(ptr::from_ref(self)
                .cast::<u8>()
                .add(self.geometry.vblocks_offset + index * self.geometry.vblock_stride)
                .cast::<VBlock>())
        }
    }

    /// Returns interior mutable payload storage. Access requires a reservation.
    pub(super) fn slots(&self, vblock: usize, offset: usize) -> *mut T {
        debug_assert!(vblock < self.geometry.config.vblocks_per_block);
        debug_assert!(offset < SLOTS);

        // SAFETY: the layout has space/alignment for these T slots, including
        // aligned, zero-byte storage for zero-sized T. No T reference is made.
        unsafe {
            ptr::from_ref(self)
                .cast::<u8>()
                .cast_mut()
                .add(
                    self.geometry.vblocks_offset
                        + vblock * self.geometry.vblock_stride
                        + self.geometry.slots_offset,
                )
                .cast::<T>()
                .add(offset)
        }
    }

    fn reset(&mut self) {
        // Exclusive ownership is essential: masks are totals, not prefixes
        // while operations are in flight. Only empty/fully drained blocks reset.
        for i in 0..self.geometry.config.vblocks_per_block {
            let vb = self.vblock(i);
            debug_assert_eq!(vb.produced.load(Relaxed), vb.consumed.load(Relaxed));

            vb.produced.store(0, Relaxed);
            vb.consumed.store(0, Relaxed);
        }

        self.consumed_vblocks.store(0, Relaxed);
        self.next.store(ptr::null_mut(), Relaxed);
    }
}

impl<T> Drop for Block<T> {
    fn drop(&mut self) {
        // Queue teardown or detached ownership guarantees all reservations have
        // settled: every produced slot not yet consumed still owns its value.
        for i in 0..self.geometry.config.vblocks_per_block {
            let vb = self.vblock(i);

            let mut live = vb.produced.load(Relaxed) & !vb.consumed.load(Relaxed);

            while live != 0 {
                let slot = live.trailing_zeros() as usize;
                // SAFETY: exactly the slots with a produced bit and no consumed
                // bit are initialized and still owned.
                unsafe { ptr::drop_in_place(self.slots(i, slot)) };
                live &= live - 1;
            }
        }
    }
}

/// The only owning handle for a detached block. Queue/list links hold raw
/// ownership instead; converting between the two must never duplicate it.
pub(super) struct OwnedBlock<T>(NonNull<Block<T>>);

impl<T> OwnedBlock<T> {
    pub(super) fn new(geometry: Geometry) -> Self {
        // SAFETY: validated nonzero layout. Allocation failure follows alloc's
        // usual abort path; no user code runs during header initialization.
        unsafe {
            let allocation = alloc(geometry.layout).cast::<Block<T>>();

            let Some(pointer) = NonNull::new(allocation) else {
                handle_alloc_error(geometry.layout)
            };

            pointer.as_ptr().write(Block {
                next: AtomicPtr::new(ptr::null_mut()),
                consumed_vblocks: AtomicUsize::new(0),
                geometry,

                _payload: PhantomData,
            });

            for i in 0..geometry.config.vblocks_per_block {
                allocation
                    .cast::<u8>()
                    .add(geometry.vblocks_offset + i * geometry.vblock_stride)
                    .cast::<VBlock>()
                    .write(VBlock {
                        produced: CachePadded::new(AtomicU64::new(0)),
                        consumed: CachePadded::new(AtomicU64::new(0)),
                    });
            }

            Self(pointer)
        }
    }

    /// # Safety
    /// `pointer` must transfer exclusive ownership of a detached allocation.
    pub(super) unsafe fn from_raw(pointer: *mut Block<T>) -> Self {
        Self(unsafe { NonNull::new_unchecked(pointer) })
    }

    pub(super) fn as_ptr(&self) -> *mut Block<T> {
        self.0.as_ptr()
    }

    pub(super) fn forget(self) {
        core::mem::forget(self);
    }

    pub(super) fn into_raw(self) -> *mut Block<T> {
        let pointer = self.as_ptr();
        self.forget();
        pointer
    }

    pub(super) fn reset(&mut self) {
        // SAFETY: this handle uniquely owns the detached block.
        unsafe { self.0.as_mut().reset() }
    }
}

impl<T> Drop for OwnedBlock<T> {
    fn drop(&mut self) {
        // Free the allocation even if a remaining T's destructor panics.
        struct Allocation(*mut u8, Layout);

        impl Drop for Allocation {
            fn drop(&mut self) {
                // SAFETY: this guard owns the original allocation and layout.
                unsafe { dealloc(self.0, self.1) }
            }
        }

        // SAFETY: unique detached ownership; Block::drop only drops live T's.
        unsafe {
            let _allocation = Allocation(self.as_ptr().cast(), self.0.as_ref().geometry.layout);
            ptr::drop_in_place(self.as_ptr());
        }
    }
}

/// An owned intrusive list whose destructor walks iteratively.
pub(super) struct BlockList<T> {
    head: *mut Block<T>,
    tail: *mut Block<T>,
    len: usize,
}

impl<T> BlockList<T> {
    pub(super) const fn new() -> Self {
        Self {
            head: ptr::null_mut(),
            tail: ptr::null_mut(),
            len: 0,
        }
    }

    pub(super) const fn len(&self) -> usize {
        self.len
    }

    /// # Safety
    /// Transfer exclusive ownership of the entire acyclic chain (or null).
    pub(super) unsafe fn from_raw(head: *mut Block<T>) -> Self {
        let mut curr = head;
        let mut tail = null_mut();
        let mut len = 0;

        while !curr.is_null() {
            tail = curr;
            curr = unsafe { &*curr }.next.load(Relaxed);
            len += 1;
        }

        Self { head, tail, len }
    }

    pub(super) fn into_raw(mut self) -> *mut Block<T> {
        self.tail = null_mut();
        self.len = 0;
        core::mem::replace(&mut self.head, ptr::null_mut())
    }

    /// Transfer the whole chain as (head, tail, len); the tail's link is null.
    pub(super) fn into_raw_parts(mut self) -> (*mut Block<T>, *mut Block<T>, usize) {
        let tail = core::mem::replace(&mut self.tail, null_mut());
        let len = core::mem::replace(&mut self.len, 0);
        (core::mem::replace(&mut self.head, null_mut()), tail, len)
    }

    pub(super) fn push(&mut self, block: OwnedBlock<T>) {
        // SAFETY: both the existing chain and detached block are uniquely owned.
        unsafe { *(*block.as_ptr()).next.get_mut() = self.head };

        if self.head.is_null() {
            self.tail = block.as_ptr();
        }

        self.len += 1;
        self.head = block.into_raw();
    }

    pub(super) fn pop(&mut self) -> Option<OwnedBlock<T>> {
        if self.head.is_null() {
            return None;
        }

        // SAFETY: transfer only the first allocation, retain the suffix.
        unsafe {
            let block = OwnedBlock::from_raw(self.head);
            self.head = core::mem::replace((*self.head).next.get_mut(), null_mut());
            if self.head.is_null() {
                self.tail = null_mut();
            }

            self.len -= 1;
            Some(block)
        }
    }
}

impl<T> AddAssign for BlockList<T> {
    fn add_assign(&mut self, rhs: Self) {
        if rhs.head.is_null() {
            return;
        }

        // SAFETY: both detached chains are exclusively owned.
        unsafe { *(*rhs.tail).next.get_mut() = self.head };
        if self.head.is_null() {
            self.tail = rhs.tail;
        }

        self.len += rhs.len;
        self.head = rhs.into_raw();
    }
}

impl<T> Drop for BlockList<T> {
    fn drop(&mut self) {
        while let Some(block) = self.pop() {
            drop(block);
        }
    }
}

#[cfg(test)]
#[path = "tests/block.rs"]
mod tests;
