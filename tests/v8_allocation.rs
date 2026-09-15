//! Count only allocations made by this test thread, after queue construction.
use std::{
    alloc::{GlobalAlloc, Layout, System},
    cell::Cell,
};
use ubq::v8::{Config, UBQ};

thread_local! {
    static TRACKING: Cell<bool> = const { Cell::new(false) };
    static ALLOCATIONS: Cell<usize> = const { Cell::new(0) };
}

struct Allocator;

#[global_allocator]
static ALLOCATOR: Allocator = Allocator;

unsafe impl GlobalAlloc for Allocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if TRACKING.try_with(Cell::get).unwrap_or(false) {
            let _ = ALLOCATIONS.try_with(|n| n.set(n.get() + 1));
        }
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) };
    }
}

struct Tracking;

impl Drop for Tracking {
    fn drop(&mut self) {
        TRACKING.set(false);
    }
}

#[test]
fn scalar_transfers_and_empty_pops_do_not_allocate_temporary_buffers() {
    let q = UBQ::<u64>::with_config(Config {
        slots_per_vblock: 64,
        vblocks_per_block: 2,
    })
    .unwrap();
    ALLOCATIONS.set(0);
    TRACKING.set(true);
    let tracking = Tracking;
    for value in 0..32_u64 {
        assert_eq!(q.pop(), None);
        q.push(value);
        assert_eq!(q.pop(), Some(value));
    }
    assert_eq!(q.pop(), None);
    drop(tracking);
    assert_eq!(ALLOCATIONS.get(), 0);
}
