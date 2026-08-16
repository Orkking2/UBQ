use crate::{backoff::BackoffPolicy, slot::Slot};
use alloc::alloc::{alloc_zeroed, dealloc, handle_alloc_error};
use core::{
    alloc::Layout,
    mem::size_of,
    ptr::null_mut,
    sync::atomic::{
        AtomicPtr, AtomicUsize,
        Ordering::{AcqRel, Acquire},
    },
};

/// Default number of element slots per block for [`crate::UBQ`].
pub const DEFAULT_BLOCK_SIZE: usize = 511;

/// A fixed-size ring-buffer segment.
pub(crate) struct Block<T, const BLOCK_SIZE: usize = DEFAULT_BLOCK_SIZE> {
    /// Link to the successor block used by producer-head advancement/recycling.
    next: AtomicPtr<Self>,
    /// How many elements have been consumed from this block.
    consumed: AtomicUsize,
    /// Per-slot storage for values in this block.
    slots: [Slot<T>; BLOCK_SIZE],
}

impl<T, const BLOCK_SIZE: usize> Block<T, BLOCK_SIZE> {
    pub(crate) const LSHFT: u32 = usize::BITS - BLOCK_SIZE.leading_zeros() + 1;

    const LAYOUT: Layout = {
        let _ = Self::LAYOUT_CHECKS;

        match Layout::from_size_align(size_of::<Self>(), 1 << Self::LSHFT) {
            Ok(layout) => layout,
            Err(_) => panic!("did not meet layout requirements"),
        }
    };

    const LAYOUT_CHECKS: () = {
        assert!(BLOCK_SIZE > 0, "block length must be greater than zero");
        assert!(
            BLOCK_SIZE <= (usize::MAX - 1) / 2,
            "block length overflows the pointer-tag encoding"
        );
    };

    pub fn new() -> *mut Self {
        let zeroed = unsafe { alloc_zeroed(Self::LAYOUT) };

        if zeroed.is_null() {
            handle_alloc_error(Self::LAYOUT)
        }

        zeroed.cast()
    }

    /// Attempts to free slots in [consumed..BLOCK_LENGTH] using
    /// [drop_inner](Slot::drop_inner).
    pub fn free(this: *mut Self) {
        unsafe {
            let this_mut = &mut *this;

            for i in *this_mut.consumed.get_mut()..BLOCK_SIZE {
                this_mut.slots.get_unchecked_mut(i).drop_inner();
            }

            dealloc(this.cast(), Self::LAYOUT);
        }
    }

    fn reset_mut(&mut self) {
        self.slots.iter_mut().for_each(Slot::reset);

        *self.next.get_mut() = null_mut();
        *self.consumed.get_mut() = 0;
    }

    /// Reset all slots and block atomics to zero.
    pub fn reset(this: *mut Self) {
        unsafe { &mut *this }.reset_mut();
    }

    pub fn wait_next<B>(&self, backoff: &B) -> *mut Self
    where
        B: BackoffPolicy,
    {
        loop {
            let next = self.next.load(Acquire);

            if !next.is_null() {
                break next;
            }

            backoff.snooze()
        }
    }

    pub fn next(&self) -> &AtomicPtr<Self> {
        &self.next
    }

    pub fn next_mut(&mut self) -> &mut AtomicPtr<Self> {
        &mut self.next
    }

    pub unsafe fn get_unchecked(&self, index: usize) -> &Slot<T> {
        unsafe { self.slots.get_unchecked(index) }
    }

    /// Returns `true` upon full consumption.
    pub fn consume(&self, quantity: usize) -> bool {
        self.consumed.fetch_add(quantity, AcqRel) + quantity == BLOCK_SIZE
    }
}

pub struct BlockChain<T, const BLOCK_SIZE: usize> {
    root: *mut Block<T, BLOCK_SIZE>,
    size: usize,
}

impl<T, const BLOCK_SIZE: usize> BlockChain<T, BLOCK_SIZE> {
    const ZERO: Self = Self {
        root: null_mut(),
        size: 0,
    };

    pub const fn new() -> Self {
        Self::ZERO
    }

    fn give(&mut self, block: *mut Block<T, BLOCK_SIZE>) {
        unsafe { *(*block).next.get_mut() = self.root };

        self.root = block;
        self.size += BLOCK_SIZE;
    }

    /// Guarantees space for at least `size` elements.
    pub fn grow_to_fit_with(&mut self, size: usize, pool: &AtomicPtr<Block<T, BLOCK_SIZE>>) {
        if self.size < size {
            let new = pool.swap(null_mut(), Acquire);

            if !new.is_null() {
                self.give(new);
            }
        }

        while self.size < size {
            self.give(Block::new());
        }
    }

    pub fn take(&mut self) -> (*mut Block<T, BLOCK_SIZE>, usize) {
        let root = self.root;
        let size = self.size;

        self.root = null_mut();
        self.size = 0;

        (root, size)
    }

    pub fn give_back(&mut self, root: *mut Block<T, BLOCK_SIZE>, size: usize) {
        self.root = root;
        self.size = size;
    }
}

impl<T, const BLOCK_SIZE: usize> Drop for BlockChain<T, BLOCK_SIZE> {
    fn drop(&mut self) {
        let mut block = self.root;
        self.root = null_mut();

        while !block.is_null() {
            let next = unsafe { *(*block).next.get_mut() };
            Block::free(block);
            block = next;
        }
    }
}
