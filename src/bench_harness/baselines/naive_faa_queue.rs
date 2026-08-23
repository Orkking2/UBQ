//! The pathological baseline: the LCRQ/SCQ papers' own "infinite array
//! queue" strawman, deliberately reproduced *without* either paper's fix.
//!
//! Morrison & Afek's LCRQ paper (PPoPP'13, Figure 2) and Nikolaev's SCQ paper
//! (DISC'19, Figure 2/6) both open by describing the same simple FAA-based
//! queue: producers reserve a position with `fetch_add` on `tail`, consumers
//! reserve a position with `fetch_add` on `head`, and each side spins on its
//! reserved slot until the other side has caught up. Both papers then spend
//! the rest of their design closing the resulting livelock hazard — LCRQ by
//! *closing* a ring once an enqueue can't make progress and linking a fresh
//! one; SCQ by a shared *threshold* counter that lets dequeuers give up
//! cleanly once they've retried "long enough." This module is that same
//! naive design with **neither fix**, kept as a deliberately weak baseline:
//! under real contention (many threads cycling through a small, fixed-size
//! ring with no escape hatch), it can degrade sharply or genuinely fail to
//! finish within the benchmark harness's job timeout — which is the point.
//! It exercises the harness's `TimedOut`/DNF handling against a real
//! algorithm, not just an injected stall.
//!
//! Generalized from the papers' pointer-sentinel version (which swaps a
//! reserved bit pattern into a slot) to arbitrary `T` via a small per-slot
//! state byte, since a spare sentinel bit pattern isn't generically
//! available for non-pointer-sized payloads. The essential pathology is
//! unchanged: FAA-based reservation with no closing and no threshold, so a
//! slot collision is resolved only by busy-retrying until the other side
//! catches up.

use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};

use crate::bench_harness::{BenchQueue, BenchQueueOps, LogQueue, LogQueueOps, LogRecord};

/// Fixed ring capacity. Must stay comfortably above the largest
/// `scenario.consumers` this baseline is ever run against (see
/// `bounded_capacity`), and must be a power of two for cheap index masking.
/// Kept as one hardcoded constant rather than a runtime-configurable
/// `QueueKind` parameter: a capacity sweep is a legitimate future addition,
/// deliberately deferred to keep this baseline in the cheap "plain kind"
/// wiring bucket (see the benchmark expansion plan).
const CAPACITY: usize = 256;
const MASK: usize = CAPACITY - 1;

const EMPTY: u8 = 0;
const WRITING: u8 = 1;
const FULL: u8 = 2;
const READING: u8 = 3;

struct Slot<T> {
    state: AtomicU8,
    value: UnsafeCell<MaybeUninit<T>>,
}

impl<T> Slot<T> {
    fn new() -> Self {
        Self {
            state: AtomicU8::new(EMPTY),
            value: UnsafeCell::new(MaybeUninit::uninit()),
        }
    }
}

pub struct NaiveFaaQueue<T> {
    ring: Box<[Slot<T>]>,
    head: AtomicU64,
    tail: AtomicU64,
}

// SAFETY: values are moved into and out of the ring, never shared by
// reference across threads; `T: Send` is therefore sufficient, exactly as
// for `UBQ` itself.
unsafe impl<T: Send> Send for NaiveFaaQueue<T> {}
unsafe impl<T: Send> Sync for NaiveFaaQueue<T> {}

impl<T> NaiveFaaQueue<T> {
    pub fn new() -> Self {
        let ring = (0..CAPACITY).map(|_| Slot::new()).collect();
        Self {
            ring,
            head: AtomicU64::new(0),
            tail: AtomicU64::new(0),
        }
    }

    /// Reserve the next producer slot via FAA and busy-wait for it to become
    /// writable. No closing, no threshold: if the ring has wrapped all the
    /// way around into still-unconsumed data, this spins until a consumer
    /// frees the slot, with no bound on how long that can take.
    pub fn push(&self, value: T) {
        let pos = self.tail.fetch_add(1, Ordering::Relaxed);
        let slot = &self.ring[pos as usize & MASK];
        while slot
            .state
            .compare_exchange_weak(EMPTY, WRITING, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            std::hint::spin_loop();
        }
        // SAFETY: this thread exclusively owns the slot between the
        // EMPTY->WRITING transition it just won and the FULL store below;
        // no other thread can be mid-write or mid-read of the same slot in
        // that window.
        unsafe {
            (*slot.value.get()).write(value);
        }
        slot.state.store(FULL, Ordering::Release);
    }

    /// Mirrors the papers' own emptiness check (`tail <= head`) before
    /// committing to a reservation, so an empty queue returns `None`
    /// promptly rather than spinning forever waiting for a producer that
    /// may never arrive. Once a reservation is taken, though, there is
    /// still no bound on how long the wait for that specific slot can take
    /// — that unbounded wait is the deliberate pathology this baseline
    /// exists to exercise.
    pub fn try_pop(&self) -> Option<T> {
        if self.tail.load(Ordering::Relaxed) <= self.head.load(Ordering::Relaxed) {
            return None;
        }
        let pos = self.head.fetch_add(1, Ordering::Relaxed);
        let slot = &self.ring[pos as usize & MASK];
        while slot
            .state
            .compare_exchange_weak(FULL, READING, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            std::hint::spin_loop();
        }
        // SAFETY: symmetric to the write side above — exclusive ownership
        // of the slot between the FULL->READING transition and the EMPTY
        // store below.
        let value = unsafe { (*slot.value.get()).assume_init_read() };
        slot.state.store(EMPTY, Ordering::Release);
        Some(value)
    }
}

impl<T> Default for NaiveFaaQueue<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Drop for NaiveFaaQueue<T> {
    fn drop(&mut self) {
        for slot in self.ring.iter_mut() {
            if *slot.state.get_mut() == FULL {
                // SAFETY: exclusive access via `&mut self`; only FULL slots
                // hold a live, unread value.
                unsafe {
                    slot.value.get_mut().assume_init_drop();
                }
            }
        }
    }
}

impl BenchQueueOps for NaiveFaaQueue<u64> {
    fn try_send_value(&self, value: u64) -> bool {
        self.push(value);
        true
    }

    fn try_recv_value(&self) -> Option<u64> {
        self.try_pop()
    }

    fn bounded_capacity(&self) -> Option<usize> {
        Some(CAPACITY)
    }
}

impl BenchQueue for NaiveFaaQueue<u64> {
    fn new_queue() -> std::sync::Arc<Self> {
        std::sync::Arc::new(Self::new())
    }
}

impl LogQueueOps for NaiveFaaQueue<LogRecord> {
    fn send_log(&self, record: LogRecord) {
        self.push(record);
    }

    fn try_recv_log(&self) -> Option<LogRecord> {
        self.try_pop()
    }
}

impl LogQueue for NaiveFaaQueue<LogRecord> {
    fn new_log_queue() -> std::sync::Arc<Self> {
        std::sync::Arc::new(Self::new())
    }
}
