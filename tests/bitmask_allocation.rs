//! Scalar transfers allocate no temporary buffers, and completed blocks go
//! back into the chain instead of to the allocator while the queue lives.
use std::{
    alloc::{GlobalAlloc, Layout, System},
    cell::Cell,
    sync::{
        Barrier,
        atomic::{AtomicBool, AtomicUsize, Ordering::Relaxed},
    },
    thread,
};
use ubq::bitmask::{Config, SLOTS, UBQ};

thread_local! {
    static TRACKING: Cell<bool> = const { Cell::new(false) };
    static ALLOCATIONS: Cell<usize> = const { Cell::new(0) };
}

// Only a block is this large; every buffer the workload below uses is smaller.
const LARGE: usize = 4096;
static WATCH_FREES: AtomicBool = AtomicBool::new(false);
static LARGE_FREES: AtomicUsize = AtomicUsize::new(0);

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
        if WATCH_FREES.load(Relaxed) && layout.size() >= LARGE {
            LARGE_FREES.fetch_add(1, Relaxed);
        }
        unsafe { System.dealloc(ptr, layout) };
    }
}

struct Tracking;

impl Drop for Tracking {
    fn drop(&mut self) {
        TRACKING.set(false);
    }
}

fn queue(vblocks: usize) -> UBQ<u64> {
    UBQ::with_config(Config {
        slots_per_vblock: SLOTS,
        vblocks_per_block: vblocks,
    })
    .unwrap()
}

#[test]
fn scalar_transfers_and_empty_pops_do_not_allocate_temporary_buffers() {
    let q = queue(2);
    assert!(q.block_allocation_size() < LARGE);
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

#[test]
fn completed_blocks_return_to_the_chain_instead_of_the_allocator() {
    const PRODUCERS: usize = 4;
    const CONSUMERS: usize = 4;
    const ROUNDS: usize = 40;
    const PER_ROUND: usize = 200;
    let q = queue(8);
    assert!(q.block_allocation_size() >= LARGE);
    WATCH_FREES.store(true, Relaxed);
    for _ in 0..4 {
        let total = PRODUCERS * ROUNDS * PER_ROUND;
        let consumed = AtomicUsize::new(0);
        let barrier = Barrier::new(PRODUCERS + CONSUMERS);
        thread::scope(|scope| {
            for _ in 0..PRODUCERS {
                let (q, barrier) = (&q, &barrier);
                scope.spawn(move || {
                    barrier.wait();
                    for round in 0..ROUNDS {
                        if round % 2 == 0 {
                            q.push_batch((0..PER_ROUND as u64).collect());
                        } else {
                            for value in 0..PER_ROUND as u64 {
                                q.push(value);
                            }
                        }
                    }
                });
            }
            for consumer in 0..CONSUMERS {
                let (q, consumed, barrier) = (&q, &consumed, &barrier);
                scope.spawn(move || {
                    barrier.wait();
                    while consumed.load(Relaxed) < total {
                        let count = if consumer % 2 == 0 {
                            q.pop_batch(Box::new_uninit_slice(97)).len()
                        } else {
                            usize::from(q.pop().is_some())
                        };
                        if count == 0 {
                            thread::yield_now();
                        } else {
                            consumed.fetch_add(count, Relaxed);
                        }
                    }
                });
            }
        });
        assert_eq!(consumed.load(Relaxed), total);
        assert_eq!(q.pop(), None);
    }
    assert_eq!(LARGE_FREES.load(Relaxed), 0, "a block was freed while the queue was live");
    drop(q);
    WATCH_FREES.store(false, Relaxed);
    assert!(LARGE_FREES.load(Relaxed) >= 1, "teardown frees the cached blocks");
}
