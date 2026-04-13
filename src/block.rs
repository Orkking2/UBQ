use crate::align::A4096;
use std::{
    cell::UnsafeCell,
    mem::{MaybeUninit, align_of, size_of},
    ptr::null_mut,
    sync::atomic::{AtomicPtr, AtomicU8, AtomicUsize, Ordering},
};

/// Default number of element slots per block for [`crate::UBQ`].
pub const DEFAULT_BLOCK_LENGTH: usize = 2047;
/// Default number of element slots per block for [`crate::UBQ`].
pub const BLOCK_LENGTH: usize = DEFAULT_BLOCK_LENGTH;

// Bits indicating the state of a slot:
// * If a value has been written into the slot, `WRITE` is set.
pub const WRITE: u8 = 1;

/// A fixed-size ring-buffer segment.
pub(crate) struct Block<T, const BLOCK: usize = DEFAULT_BLOCK_LENGTH, A = A4096> {
    /// Alignment marker used to reserve pointer-tag bits in the block address.
    pub _align: A,
    /// Link to the successor block used by producer-head advancement/recycling.
    pub next: AtomicPtr<Self>,
    /// Per-slot storage for values in this block.
    pub slots: [Slot<T>; BLOCK],
    /// How many elements have been consumed from this block.
    pub consumed: AtomicUsize,
}

pub(crate) struct Slot<T> {
    pub value: UnsafeCell<MaybeUninit<T>>,
    pub state: AtomicU8,
}

impl<T, const BLOCK: usize, A> Block<T, BLOCK, A> {
    pub(crate) const LAYOUT_CHECKS: () = {
        assert!(
            size_of::<A>() == 0,
            "alignment marker types must be zero-sized"
        );
        assert!(BLOCK > 0, "block length must be greater than zero");
        assert!(align_of::<Self>().is_power_of_two());
        assert!(
            BLOCK <= (usize::MAX - 1) / 2,
            "block length overflows the pointer-tag encoding"
        );

        let encoded_index_limit = BLOCK * 2 + 1;

        assert!(
            encoded_index_limit <= align_of::<Self>() - 1,
            "block alignment does not leave enough low bits for pointer tagging"
        );
    };

    #[inline]
    pub(crate) fn block_align() -> usize {
        let () = Self::LAYOUT_CHECKS;
        align_of::<Self>()
    }

    #[inline]
    pub(crate) fn block_mask() -> usize {
        Self::block_align() - 1
    }

    pub(crate) fn new_zeroed() -> Box<Self> {
        let () = Self::LAYOUT_CHECKS;
        unsafe { Box::new_zeroed().assume_init() }
    }

    pub(crate) unsafe fn reset(this: *mut Self) {
        let () = Self::LAYOUT_CHECKS;

        let block = unsafe { &*this };

        block.next.store(null_mut(), Ordering::Relaxed);
        block.consumed.store(0, Ordering::Relaxed);

        for slot in &block.slots {
            if slot.state.load(Ordering::Relaxed) != 0 {
                slot.state.store(0, Ordering::Relaxed);
            }
        }
    }
}

impl<T, const BLOCK: usize, A> Drop for Block<T, BLOCK, A> {
    fn drop(&mut self) {
        self.slots
            .iter_mut()
            .skip(*self.consumed.get_mut())
            .filter_map(|Slot { value, state }| (*state.get_mut() == WRITE).then_some(value))
            .for_each(|value| unsafe { value.get_mut().assume_init_drop() });
    }
}
