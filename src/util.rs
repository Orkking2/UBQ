use crossbeam_utils::Backoff;
use std::sync::atomic::{AtomicPtr, Ordering};

pub const INCR: u128 = 1u128 << usize::BITS;

/// For freeing consumed blocks.
pub const HIGHEST_BIT: usize = (1 as usize).rotate_right(1);

pub fn acquire_nonnull<T>(a: &AtomicPtr<T>) -> *mut T {
    let backoff = Backoff::new();
    let mut p = a.load(Ordering::Acquire);

    while p.is_null() {
        backoff.snooze();
        p = a.load(Ordering::Acquire);
    }

    p
}
