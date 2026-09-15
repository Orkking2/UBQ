use super::queue::{Config, ConfigError};
use alloc::alloc::{alloc, dealloc, handle_alloc_error};
use core::{
    alloc::Layout,
    cell::UnsafeCell,
    marker::PhantomData,
    mem::MaybeUninit,
    ptr::{self, null_mut},
    sync::atomic::{
        AtomicPtr, AtomicUsize,
        Ordering::{AcqRel, Relaxed},
    },
};

/// Computed once; every vblock in the queue uses this exact layout.
#[derive(Clone, Copy, Debug)]
pub(super) struct Geometry {
    pub(super) config: Config,
    pub(super) layout: Layout,
    pub(super) vblock_alignment: usize,
    vblocks_offset: usize,
    slots_offset: usize,
}

impl Geometry {
    pub(super) fn new<T>(config: Config) -> Result<Self, ConfigError> {
        if config.slots_per_vblock == 0 || config.vblocks_per_block == 0 {
            return Err(ConfigError::ZeroCapacity);
        }

        let overflow = |_| ConfigError::LayoutOverflow;
        let slots = Layout::array::<UnsafeCell<MaybeUninit<T>>>(config.slots_per_vblock)
            .map_err(overflow)?;
        let (vblock, slots_offset) = Layout::new::<VBlock<T>>().extend(slots).map_err(overflow)?;

        // A vblock occupies one power-of-two region. Its low address bits
        // encode every slot index AND the one-past-end reservation marker.
        // The index constraint also gives ZSTs enough tag bits.
        let index_space = config
            .slots_per_vblock
            .checked_add(1)
            .ok_or(ConfigError::LayoutOverflow)?;
        let vblock_alignment = vblock
            .size()
            .max(vblock.align())
            .max(index_space)
            .checked_next_power_of_two()
            .ok_or(ConfigError::LayoutOverflow)?;
        let vblocks_size = vblock_alignment
            .checked_mul(config.vblocks_per_block)
            .ok_or(ConfigError::LayoutOverflow)?;
        let vblocks = Layout::from_size_align(vblocks_size, vblock_alignment).map_err(overflow)?;
        let (layout, vblocks_offset) = Layout::new::<Block>().extend(vblocks).map_err(overflow)?;

        Ok(Self {
            config,
            layout: layout.pad_to_align(),
            vblock_alignment,
            vblocks_offset,
            slots_offset,
        })
    }

    pub(super) fn capacity(self) -> usize {
        // Layout validation bounds this product: alignment > slots_per_vblock.
        self.config.slots_per_vblock * self.config.vblocks_per_block
    }

    /// Capacity from this vblock's first slot to its allocation's end.
    ///
    /// # Safety
    /// `vblock` must belong to a live allocation with this geometry.
    pub(super) unsafe fn remaining_capacity<T>(self, vblock: *mut VBlock<T>) -> usize {
        let owner = unsafe { (*vblock).owner };
        let offset = vblock.addr() - owner.addr() - self.vblocks_offset;
        let index = offset / self.vblock_alignment;
        (self.config.vblocks_per_block - index) * self.config.slots_per_vblock
    }

    /// # Safety
    /// The caller owns access to the requested slots in this live vblock.
    pub(super) unsafe fn slots<T>(self, vblock: *mut VBlock<T>, index: usize) -> *mut T {
        debug_assert!(index < self.config.slots_per_vblock);
        unsafe {
            vblock
                .cast::<u8>()
                .add(self.slots_offset)
                .cast::<T>()
                .add(index)
        }
    }
}

/// The allocation owner: [Block | padding | VBlock + slots | VBlock + slots ...].
/// Each vblock contributes exactly one completion to `consumed`.
pub(super) struct Block {
    pub(super) size: usize,
    pub(super) consumed: AtomicUsize,
    layout: Layout,
}

impl Block {
    /// # Safety
    /// Complete exactly one vblock; its contribution must still be outstanding.
    pub(super) unsafe fn release(owner: *mut Self) -> bool {
        let block = unsafe { &*owner };
        let previous = block.consumed.fetch_add(1, AcqRel);
        debug_assert!(previous < block.size);
        previous + 1 == block.size
    }

    /// # Safety
    /// All vblocks have been released and no thread can access this allocation.
    pub(super) unsafe fn free(owner: *mut Self) {
        let layout = unsafe { (*owner).layout };
        unsafe { dealloc(owner.cast(), layout) };
    }
}

/// A fixed-capacity view into its owner. Payload storage has no per-slot flags.
pub(super) struct VBlock<T> {
    pub(super) next: AtomicPtr<Self>,
    pub(super) owner: *mut Block,
    pub(super) produced: AtomicUsize,
    pub(super) consumed: AtomicUsize,
    _payload: PhantomData<UnsafeCell<T>>,
}

/// Exclusive ownership of an unpublished, empty chain of whole allocations.
/// Once attached, queue reservations and completion counters own its lifetime.
pub(super) struct BlockChain<T> {
    pub(super) root: *mut VBlock<T>,
    tail: *mut VBlock<T>,
    capacity: usize,
}

impl<T> BlockChain<T> {
    pub(super) fn empty() -> Self {
        Self {
            root: null_mut(),
            tail: null_mut(),
            capacity: 0,
        }
    }

    pub(super) fn grow(&mut self, needed: usize, geometry: Geometry) {
        while self.capacity < needed {
            let added_capacity = self
                .capacity
                .checked_add(geometry.capacity())
                .expect("dynblock chain capacity overflow");
            // SAFETY: validated layout; only headers are initialized. This
            // chain exclusively owns the allocation until `take` transfers it.
            unsafe {
                let base = alloc(geometry.layout);
                if base.is_null() {
                    handle_alloc_error(geometry.layout);
                }
                let owner = base.cast::<Block>();
                owner.write(Block {
                    size: geometry.config.vblocks_per_block,
                    consumed: AtomicUsize::new(0),
                    layout: geometry.layout,
                });
                for index in 0..geometry.config.vblocks_per_block {
                    let vblock = base
                        .add(geometry.vblocks_offset + index * geometry.vblock_alignment)
                        .cast::<VBlock<T>>();
                    vblock.write(VBlock {
                        next: AtomicPtr::new(null_mut()),
                        owner,
                        produced: AtomicUsize::new(0),
                        consumed: AtomicUsize::new(0),
                        _payload: PhantomData,
                    });
                    if self.tail.is_null() {
                        self.root = vblock;
                    } else {
                        (*self.tail).next.store(vblock, Relaxed);
                    }
                    self.tail = vblock;
                }
            }
            self.capacity = added_capacity;
        }
    }

    pub(super) fn take(&mut self) -> *mut VBlock<T> {
        self.tail = null_mut();
        self.capacity = 0;
        core::mem::replace(&mut self.root, null_mut())
    }
}

impl<T> Drop for BlockChain<T> {
    fn drop(&mut self) {
        let mut current = self.root;
        while !current.is_null() {
            // SAFETY: these unpublished vblocks contain no T values. Each
            // allocation is freed only after its last vblock is released.
            unsafe {
                let next = (*current).next.load(Relaxed);
                let owner = (*current).owner;
                if Block::release(owner) {
                    Block::free(owner);
                }
                current = next;
            }
        }
    }
}

/// Drop the unread suffix during exclusive queue teardown.
///
/// # Safety
/// No operations are in flight. All reservations have finished copying, so
/// the completion totals describe contiguous consumed/produced prefixes.
pub(super) unsafe fn drop_remaining<T>(mut current: *mut VBlock<T>, geometry: Geometry) {
    while !current.is_null() {
        unsafe {
            let vblock = &*current;
            let next = vblock.next.load(Relaxed);
            let consumed = vblock.consumed.load(Relaxed);
            let produced = vblock.produced.load(Relaxed);
            if consumed < produced {
                ptr::drop_in_place(ptr::slice_from_raw_parts_mut(
                    geometry.slots(current, consumed),
                    produced - consumed,
                ));
            }
            let owner = vblock.owner;
            if Block::release(owner) {
                Block::free(owner);
            }
            current = next;
        }
    }
}
