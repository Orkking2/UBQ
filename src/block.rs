use crossbeam_utils::Backoff;
use std::{
    cell::UnsafeCell,
    mem::MaybeUninit,
    sync::atomic::{AtomicPtr, AtomicUsize, Ordering},
};

/// Number of element slots per block.
pub const BLOCK_LENGTH: usize = 32;

/// A fixed-size ring-buffer segment.
#[repr(align(256))]
pub struct Block<T> {
    /// Link to the successor block used by producer-head advancement/recycling.
    pub next: AtomicPtr<Self>,
    /// Per-slot storage for values in this block.
    pub array: [Slot<T>; BLOCK_LENGTH],
}

// Bits indicating the state of a slot:
// * If a value has been written into the slot, `WRITE` is set.
pub const WRITE: usize = 1;
pub const READ: usize = WRITE << 1;
pub const DESTROY: usize = READ << 1;

pub struct Slot<T> {
    pub value: UnsafeCell<MaybeUninit<T>>,
    pub state: AtomicUsize,
}

impl<T> Slot<T> {
    pub fn await_write(&self) {
        let backoff = Backoff::new();

        while self.state.load(Ordering::Acquire) & WRITE == 0 {
            backoff.snooze();
        }
    }
}

impl<T> Block<T> {
    pub fn new_zeroed() -> Box<Self> {
        unsafe { Box::new_zeroed().assume_init() }
    }
}

impl<T> Drop for Block<T> {
    fn drop(&mut self) {
        self.array
            .iter_mut()
            .filter_map(|Slot { value, state }| {
                (*state.get_mut() & (WRITE | READ) == WRITE).then_some(value)
            })
            .for_each(|value| unsafe { value.get_mut().assume_init_drop() });
    }
}
