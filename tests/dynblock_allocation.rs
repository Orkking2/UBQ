//! Observe real allocation/deallocation through the standard epoch collector.
#![cfg(feature = "std")]

use std::{
    alloc::{GlobalAlloc, Layout, System},
    cell::Cell,
    sync::atomic::{AtomicBool, AtomicUsize, Ordering::Relaxed},
};
use ubq::dynblock::{Config, UBQ};

thread_local! {
    static TRACKED_LAYOUT: Cell<(usize, usize)> = const { Cell::new((0, 0)) };
    static MAIN_ALLOCATIONS: Cell<usize> = const { Cell::new(0) };
    static COUNT_ALL: Cell<bool> = const { Cell::new(false) };
    static ALL_ALLOCATIONS: Cell<usize> = const { Cell::new(0) };
}
static FIRST_BLOCK: AtomicUsize = AtomicUsize::new(0);
static FIRST_BLOCK_FREED: AtomicBool = AtomicBool::new(false);

struct Allocator;
#[global_allocator]
static ALLOCATOR: Allocator = Allocator;

unsafe impl GlobalAlloc for Allocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc(layout) };
        if COUNT_ALL.try_with(Cell::get).unwrap_or(false) {
            let _ = ALL_ALLOCATIONS.try_with(|n| n.set(n.get() + 1));
        }
        if TRACKED_LAYOUT.try_with(Cell::get).ok() == Some((layout.size(), layout.align())) {
            let _ = MAIN_ALLOCATIONS.try_with(|n| n.set(n.get() + 1));
            let _ = FIRST_BLOCK.compare_exchange(0, pointer.addr(), Relaxed, Relaxed);
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        if pointer.addr() == FIRST_BLOCK.load(Relaxed) {
            FIRST_BLOCK_FREED.store(true, Relaxed);
        }
        unsafe { System.dealloc(pointer, layout) };
    }
}

#[test]
fn vblocks_share_an_allocation_reclaimed_after_completion_and_a_grace_period() {
    let config = Config {
        slots_per_vblock: 31,
        vblocks_per_block: 17,
    };
    let prototype = UBQ::<usize>::with_config(config).unwrap();
    let layout = (
        prototype.block_allocation_size(),
        prototype.block_allocation_align(),
    );
    drop(prototype);
    TRACKED_LAYOUT.set(layout);
    let q = UBQ::<usize>::with_config(config).unwrap();
    assert_ne!(FIRST_BLOCK.load(Relaxed), 0);
    assert_eq!(MAIN_ALLOCATIONS.get(), 1);

    // Warm the thread-local epoch participant before counting hot-path calls.
    assert!(q.is_empty());
    COUNT_ALL.set(true);
    for value in 0..10 {
        q.push(value);
        assert_eq!(q.pop(), Some(value));
        assert_eq!(q.pop(), None);
    }
    COUNT_ALL.set(false);
    assert_eq!(
        ALL_ALLOCATIONS.get(),
        0,
        "scalar calls allocated temporary buffers"
    );

    // Hold a stale reader across completion of the entire first allocation.
    let guard = crossbeam_epoch::pin();
    for value in 10..q.block_capacity() {
        q.push(value);
    }
    assert_eq!(
        MAIN_ALLOCATIONS.get(),
        2,
        "an exact fill also installs its successor"
    );
    for value in 10..q.block_capacity() - 1 {
        assert_eq!(q.pop(), Some(value));
    }
    guard.flush();
    assert!(
        !FIRST_BLOCK_FREED.load(Relaxed),
        "one vblock still has an unread value"
    );

    assert_eq!(q.pop(), Some(q.block_capacity() - 1));
    guard.flush();
    assert!(
        !FIRST_BLOCK_FREED.load(Relaxed),
        "the pinned reader must protect the retired block"
    );
    drop(guard);

    // Collection is amortized, so provide enough unpinned transitions to
    // advance epochs and execute the flushed callback on this test thread.
    for _ in 0..10_000 {
        crossbeam_epoch::pin().flush();
        if FIRST_BLOCK_FREED.load(Relaxed) {
            break;
        }
    }
    assert!(
        FIRST_BLOCK_FREED.load(Relaxed),
        "the drained main block was never reclaimed"
    );
    assert!(q.is_empty());
    drop(q);
    TRACKED_LAYOUT.set((0, 0));
}
