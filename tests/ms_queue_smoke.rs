//! Smoke tests for the from-scratch Michael-Scott baseline queue
//! (`ubq::bench_harness::baselines::ms_queue::MsQueue`), mirroring
//! `tests/bench_smoke.rs`'s UBQ integrity checks. Unlike the trivial
//! `Mutex<VecDeque>` baseline (std-library-backed, no dedicated test), this
//! is genuinely new unsafe code (hand-rolled epoch-based reclamation) and
//! warrants the same direct correctness validation UBQ itself gets.

use std::sync::{
    Arc, Barrier,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use std::thread;

use crossbeam_utils::Backoff;
use ubq::bench_harness::baselines::ms_queue::MsQueue;

const SENTINEL: u64 = u64::MAX;
const ITEMS_PER_PRODUCER: u64 = 500;

fn pop_blocking(q: &MsQueue<u64>) -> u64 {
    let backoff = Backoff::new();
    loop {
        if let Some(v) = q.try_pop() {
            return v;
        }
        backoff.snooze();
    }
}

/// Run a concurrent throughput integrity check: producers and consumers run
/// simultaneously. Every item must be received exactly once.
fn run_throughput_integrity(producers: usize, consumers: usize) {
    let total = ITEMS_PER_PRODUCER * producers as u64;
    let seen: Arc<Vec<AtomicBool>> = Arc::new(
        (0..total as usize)
            .map(|_| AtomicBool::new(false))
            .collect(),
    );
    let consumed = Arc::new(AtomicUsize::new(0));
    let duplicates = Arc::new(AtomicUsize::new(0));

    let q: Arc<MsQueue<u64>> = Arc::new(MsQueue::new());
    let total_threads = producers + consumers;
    let ready = Arc::new(Barrier::new(total_threads + 1));
    let start = Arc::new(Barrier::new(total_threads + 1));

    let mut handles = Vec::with_capacity(total_threads);

    for pid in 0..producers {
        let q = Arc::clone(&q);
        let ready = ready.clone();
        let start = start.clone();
        handles.push(thread::spawn(move || {
            ready.wait();
            start.wait();
            let base = pid as u64 * ITEMS_PER_PRODUCER;
            for offset in 0..ITEMS_PER_PRODUCER {
                q.push(base + offset);
            }
        }));
    }

    for _ in 0..consumers {
        let q = Arc::clone(&q);
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

    for h in handles.drain(..producers) {
        h.join().expect("producer panicked");
    }
    for _ in 0..consumers {
        q.push(SENTINEL);
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
/// consumers start. Every item must be received exactly once. `MsQueue` is
/// unbounded, so unlike the naive FAA baseline this ordering is always safe.
fn run_fill_drain_integrity(producers: usize, consumers: usize) {
    let total = ITEMS_PER_PRODUCER * producers as u64;
    let seen: Arc<Vec<AtomicBool>> = Arc::new(
        (0..total as usize)
            .map(|_| AtomicBool::new(false))
            .collect(),
    );
    let consumed = Arc::new(AtomicUsize::new(0));
    let duplicates = Arc::new(AtomicUsize::new(0));

    let q: Arc<MsQueue<u64>> = Arc::new(MsQueue::new());

    {
        let barrier = Arc::new(Barrier::new(producers + 1));
        let mut handles = Vec::with_capacity(producers);
        for pid in 0..producers {
            let q = Arc::clone(&q);
            let barrier = barrier.clone();
            handles.push(thread::spawn(move || {
                barrier.wait();
                let base = pid as u64 * ITEMS_PER_PRODUCER;
                for offset in 0..ITEMS_PER_PRODUCER {
                    q.push(base + offset);
                }
            }));
        }
        barrier.wait();
        for h in handles {
            h.join().expect("producer panicked");
        }
    }

    for _ in 0..consumers {
        q.push(SENTINEL);
    }

    {
        let barrier = Arc::new(Barrier::new(consumers + 1));
        let mut handles = Vec::with_capacity(consumers);
        for _ in 0..consumers {
            let q = Arc::clone(&q);
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

macro_rules! ms_queue_throughput_test {
    ($name:ident, $producers:expr, $consumers:expr) => {
        #[test]
        fn $name() {
            run_throughput_integrity($producers, $consumers);
        }
    };
}

macro_rules! ms_queue_fill_drain_test {
    ($name:ident, $producers:expr, $consumers:expr) => {
        #[test]
        fn $name() {
            run_fill_drain_integrity($producers, $consumers);
        }
    };
}

ms_queue_throughput_test!(ms_queue_spsc_throughput, 1, 1);
ms_queue_throughput_test!(ms_queue_mpsc_throughput, 4, 1);
ms_queue_throughput_test!(ms_queue_spmc_throughput, 1, 4);
ms_queue_throughput_test!(ms_queue_mpmc_throughput, 4, 4);
ms_queue_throughput_test!(ms_queue_p8c8_throughput, 8, 8);

ms_queue_fill_drain_test!(ms_queue_spsc_fill_drain, 1, 1);
ms_queue_fill_drain_test!(ms_queue_mpsc_fill_drain, 4, 1);
ms_queue_fill_drain_test!(ms_queue_spmc_fill_drain, 1, 4);
ms_queue_fill_drain_test!(ms_queue_mpmc_fill_drain, 4, 4);
ms_queue_fill_drain_test!(ms_queue_p8c8_fill_drain, 8, 8);
