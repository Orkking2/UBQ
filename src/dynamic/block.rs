use crate::{
    backoff::BackoffPolicy,
    dynamic::{heads::MAX_BLOCK_LENGTH, slot::Slot, util::new_filled_box_slice},
};
use core::ptr::{NonNull, null_mut};
use portable_atomic::{AtomicPtr, AtomicUsize, Ordering};

pub struct Block<T> {
    consumed: AtomicUsize,
    slots: Box<[Slot<T>]>,
    next: AtomicPtr<Self>,
}

impl<T> Block<T> {
    pub fn new(size: usize, next: Option<NonNull<Self>>) -> Self {
        assert!(
            (1..=MAX_BLOCK_LENGTH).contains(&size),
            "block length must be in 1..=u16::MAX"
        );

        Self {
            consumed: AtomicUsize::new(0),
            slots: new_filled_box_slice(Slot::new, size),
            next: AtomicPtr::new(next.map(NonNull::as_ptr).unwrap_or(null_mut())),
        }
    }

    pub fn new_boxed(size: usize, next: Option<NonNull<Self>>) -> Box<Self> {
        Box::new(Self::new(size, next))
    }

    pub fn await_next<B: BackoffPolicy>(&self, backoff: &B) -> (NonNull<Self>, u16) {
        loop {
            if let Some(nonnull) = NonNull::new(self.next.load(Ordering::Acquire)) {
                break (nonnull, unsafe { nonnull.as_ref() }.slots.len() as u16);
            } else {
                backoff.snooze();
                continue;
            }
        }
    }

    /// Reset consumed to 0, next to [`null_mut()`], and [`resets`](Slot::reset) every slot.
    pub fn reset(this: &mut Self) {
        *this.consumed.get_mut() = 0;
        *this.next.get_mut() = null_mut();

        this.slots.iter_mut().for_each(Slot::reset);
    }

    pub fn consumed(&self) -> &AtomicUsize {
        &self.consumed
    }

    pub fn next(&self) -> &AtomicPtr<Self> {
        &self.next
    }

    pub fn next_mut(&mut self) -> &mut AtomicPtr<Self> {
        &mut self.next
    }

    pub fn len(&self) -> usize {
        self.slots.len()
    }

    pub unsafe fn get_slot_unchecked(&self, index: usize) -> &Slot<T> {
        unsafe { self.slots.get_unchecked(index) }
    }
}
