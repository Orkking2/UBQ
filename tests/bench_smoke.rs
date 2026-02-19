//! Smoke tests that verify UBQ correctly transmits every item across the same
//! SPSC / MPSC / SPMC / MPMC × throughput / fill-drain scenarios exercised by
//! the bench harness.  These run under the standard test harness via
//! `cargo test` and are independent of the bench binary.

use std::sync::{
    Arc, Barrier,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use std::thread;

use crossbeam_utils::Backoff;
use ubq::UBQ;

const SENTINEL: u64 = u64::MAX;
const ITEMS_PER_PRODUCER: u64 = 500;

fn push(q: &UBQ<u64>, value: u64) {
    q.push(value);
}

fn pop_blocking(q: &UBQ<u64>) -> u64 {
    let backoff = Backoff::new();
    loop {
        if let Some(v) = q.pop() {
            return v;
        }
        backoff.snooze();
    }
}

/// Run a concurrent throughput integrity check: producers and consumers run
/// simultaneously.  Every item must be received exactly once.
fn run_throughput_integrity(producers: usize, consumers: usize) {
    let total = ITEMS_PER_PRODUCER * producers as u64;
    let seen: Arc<Vec<AtomicBool>> = Arc::new(
        (0..total as usize).map(|_| AtomicBool::new(false)).collect(),
    );
    let consumed = Arc::new(AtomicUsize::new(0));
    let duplicates = Arc::new(AtomicUsize::new(0));

    let q: UBQ<u64> = UBQ::new();
    let total_threads = producers + consumers;
    let ready = Arc::new(Barrier::new(total_threads + 1));
    let start = Arc::new(Barrier::new(total_threads + 1));

    let mut handles = Vec::with_capacity(total_threads);

    for pid in 0..producers {
        let q = q.clone();
        let ready = ready.clone();
        let start = start.clone();
        handles.push(thread::spawn(move || {
            ready.wait();
            start.wait();
            let base = pid as u64 * ITEMS_PER_PRODUCER;
            for offset in 0..ITEMS_PER_PRODUCER {
                push(&q, base + offset);
            }
        }));
    }

    for _ in 0..consumers {
        let q = q.clone();
        let ready = ready.clone();
        let start = start.clone();
        let seen = seen.clone();
        let consumed = consumed.clone();
        let duplicates = duplicates.clone();
        handles.push(thread::spawn(move || {
            ready.wait();
            start.wait();
            loop {
                let value = pop_blocking(&q);
                if value == SENTINEL {
                    break;
                }
                let already = seen[value as usize].swap(true, Ordering::AcqRel);
                if already {
                    duplicates.fetch_add(1, Ordering::Relaxed);
                }
                consumed.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }

    ready.wait();
    start.wait();

    // Join producers first, then send sentinels, then join consumers.
    for h in handles.drain(..producers) {
        h.join().expect("producer panicked");
    }
    for _ in 0..consumers {
        push(&q, SENTINEL);
    }
    for h in handles {
        h.join().expect("consumer panicked");
    }

    assert_eq!(
        duplicates.load(Ordering::Relaxed),
        0,
        "duplicate items in throughput run ({producers}p/{consumers}c)"
    );
    assert_eq!(
        consumed.load(Ordering::Relaxed) as u64,
        total,
        "item count mismatch in throughput run ({producers}p/{consumers}c)"
    );
    let missing: Vec<usize> = seen
        .iter()
        .enumerate()
        .filter(|(_, b)| !b.load(Ordering::Acquire))
        .map(|(i, _)| i)
        .take(10)
        .collect();
    assert!(
        missing.is_empty(),
        "missing items in throughput run ({producers}p/{consumers}c): first 10 = {missing:?}"
    );
}

/// Run a fill-then-drain integrity check: producers run to completion before
/// consumers start.  Every item must be received exactly once.
fn run_fill_drain_integrity(producers: usize, consumers: usize) {
    let total = ITEMS_PER_PRODUCER * producers as u64;
    let seen: Arc<Vec<AtomicBool>> = Arc::new(
        (0..total as usize).map(|_| AtomicBool::new(false)).collect(),
    );
    let consumed = Arc::new(AtomicUsize::new(0));
    let duplicates = Arc::new(AtomicUsize::new(0));

    let q: UBQ<u64> = UBQ::new();

    // Fill phase.
    {
        let barrier = Arc::new(Barrier::new(producers + 1));
        let mut handles = Vec::with_capacity(producers);
        for pid in 0..producers {
            let q = q.clone();
            let barrier = barrier.clone();
            handles.push(thread::spawn(move || {
                barrier.wait();
                let base = pid as u64 * ITEMS_PER_PRODUCER;
                for offset in 0..ITEMS_PER_PRODUCER {
                    push(&q, base + offset);
                }
            }));
        }
        barrier.wait();
        for h in handles {
            h.join().expect("producer panicked");
        }
    }

    // Sentinels go in after all real items are enqueued.
    for _ in 0..consumers {
        push(&q, SENTINEL);
    }

    // Drain phase.
    {
        let barrier = Arc::new(Barrier::new(consumers + 1));
        let mut handles = Vec::with_capacity(consumers);
        for _ in 0..consumers {
            let q = q.clone();
            let barrier = barrier.clone();
            let seen = seen.clone();
            let consumed = consumed.clone();
            let duplicates = duplicates.clone();
            handles.push(thread::spawn(move || {
                barrier.wait();
                loop {
                    let value = pop_blocking(&q);
                    if value == SENTINEL {
                        break;
                    }
                    let already = seen[value as usize].swap(true, Ordering::AcqRel);
                    if already {
                        duplicates.fetch_add(1, Ordering::Relaxed);
                    }
                    consumed.fetch_add(1, Ordering::Relaxed);
                }
            }));
        }
        barrier.wait();
        for h in handles {
            h.join().expect("consumer panicked");
        }
    }

    assert_eq!(
        duplicates.load(Ordering::Relaxed),
        0,
        "duplicate items in fill/drain run ({producers}p/{consumers}c)"
    );
    assert_eq!(
        consumed.load(Ordering::Relaxed) as u64,
        total,
        "item count mismatch in fill/drain run ({producers}p/{consumers}c)"
    );
    let missing: Vec<usize> = seen
        .iter()
        .enumerate()
        .filter(|(_, b)| !b.load(Ordering::Acquire))
        .map(|(i, _)| i)
        .take(10)
        .collect();
    assert!(
        missing.is_empty(),
        "missing items in fill/drain run ({producers}p/{consumers}c): first 10 = {missing:?}"
    );
}

// ─── Throughput (concurrent) ─────────────────────────────────────────────────

#[test]
fn spsc_throughput() {
    run_throughput_integrity(1, 1);
}

#[test]
fn mpsc_throughput() {
    run_throughput_integrity(4, 1);
}

#[test]
fn spmc_throughput() {
    run_throughput_integrity(1, 4);
}

#[test]
fn mpmc_throughput() {
    run_throughput_integrity(4, 4);
}

// ─── Fill / drain (sequential phases) ────────────────────────────────────────

#[test]
fn spsc_fill_drain() {
    run_fill_drain_integrity(1, 1);
}

#[test]
fn mpsc_fill_drain() {
    run_fill_drain_integrity(4, 1);
}

#[test]
fn spmc_fill_drain() {
    run_fill_drain_integrity(1, 4);
}

#[test]
fn mpmc_fill_drain() {
    run_fill_drain_integrity(4, 4);
}
