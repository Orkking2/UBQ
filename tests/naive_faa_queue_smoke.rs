//! Smoke tests for the deliberately pathological "naive FAA queue" baseline
//! (`ubq::bench_harness::baselines::naive_faa_queue::NaiveFaaQueue`) — the
//! LCRQ/SCQ papers' own strawman "infinite array queue" without either
//! paper's fix. It's slow and prone to spinning under contention by design,
//! but it must still be *correct*: no dropped, duplicated, or reordered
//! items.
//!
//! Unlike `tests/bench_smoke.rs`/`tests/ms_queue_smoke.rs`, there is
//! deliberately no fill-then-drain test here: `push` busy-waits (never
//! fails) for a free slot, and the queue has a small fixed capacity (256),
//! so a producer-completes-before-any-consumer-starts pattern would
//! deadlock once the ring fills. Every test here runs producers and
//! consumers concurrently instead, which is also the only way the real
//! benchmark harness ever drives a bounded queue.

use std::sync::{
    Arc, Barrier,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use std::thread;

use crossbeam_utils::Backoff;
use ubq::bench_harness::baselines::naive_faa_queue::NaiveFaaQueue;

const SENTINEL: u64 = u64::MAX;
const ITEMS_PER_PRODUCER: u64 = 500;

fn pop_blocking(q: &NaiveFaaQueue<u64>) -> u64 {
    let backoff = Backoff::new();
    loop {
        if let Some(v) = q.try_pop() {
            return v;
        }
        backoff.snooze();
    }
}

/// Single-threaded, uncontended: push a handful of values well under
/// capacity, then pop them back and confirm strict FIFO order. The queue is
/// slow under contention by design, but under no contention at all it must
/// still behave like an ordinary correct queue.
#[test]
fn naive_faa_queue_preserves_fifo_order_uncontended() {
    let q = NaiveFaaQueue::new();
    for value in 0..64u64 {
        q.push(value);
    }
    for expected in 0..64u64 {
        assert_eq!(pop_blocking(&q), expected);
    }
}

/// Run a concurrent throughput integrity check: producers and consumers run
/// simultaneously, so the fixed-size ring is continuously recycled rather
/// than filled once. Every item must be received exactly once.
fn run_throughput_integrity(producers: usize, consumers: usize) {
    let total = ITEMS_PER_PRODUCER * producers as u64;
    let seen: Arc<Vec<AtomicBool>> = Arc::new(
        (0..total as usize)
            .map(|_| AtomicBool::new(false))
            .collect(),
    );
    let consumed = Arc::new(AtomicUsize::new(0));
    let duplicates = Arc::new(AtomicUsize::new(0));

    let q: Arc<NaiveFaaQueue<u64>> = Arc::new(NaiveFaaQueue::new());
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

macro_rules! naive_faa_queue_throughput_test {
    ($name:ident, $producers:expr, $consumers:expr) => {
        #[test]
        fn $name() {
            run_throughput_integrity($producers, $consumers);
        }
    };
}

naive_faa_queue_throughput_test!(naive_faa_queue_spsc_throughput, 1, 1);
naive_faa_queue_throughput_test!(naive_faa_queue_mpsc_throughput, 4, 1);
naive_faa_queue_throughput_test!(naive_faa_queue_spmc_throughput, 1, 4);
naive_faa_queue_throughput_test!(naive_faa_queue_mpmc_throughput, 4, 4);
naive_faa_queue_throughput_test!(naive_faa_queue_p8c8_throughput, 8, 8);
