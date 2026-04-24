#![cfg(feature = "bench_fastfifo")]

//! Smoke tests for the local `fastfifo` benchmark backend candidate.
//!
//! `FastFifo` is bounded, so fill/drain runs must provision enough capacity
//! for the full producer phase up front. These tests validate the queue under
//! the same producer/consumer patterns our benchmark harness uses before we
//! wire it into the benchmark scheduler.

use std::sync::{
    Arc, Barrier,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use std::thread;
use std::time::{Duration, Instant};

use crossbeam_utils::Backoff;
use fastfifo::mpmc::FastFifo;

const SENTINEL: u64 = u64::MAX;
const ITEMS_PER_PRODUCER: u64 = 500;
const FASTFIFO_BLOCK_SIZE: usize = 64;
const WAIT_TIMEOUT: Duration = Duration::from_secs(10);

fn new_integrity_queue(producers: usize, consumers: usize) -> Arc<FastFifo<u64>> {
    let required_capacity =
        ITEMS_PER_PRODUCER as usize * producers + consumers + FASTFIFO_BLOCK_SIZE;
    let num_blocks = required_capacity.div_ceil(FASTFIFO_BLOCK_SIZE) + 2;
    Arc::new(FastFifo::new(num_blocks.max(2), FASTFIFO_BLOCK_SIZE))
}

fn push_blocking(q: &FastFifo<u64>, value: u64) {
    let deadline = Instant::now() + WAIT_TIMEOUT;
    let backoff = Backoff::new();
    loop {
        if q.push(value).is_ok() {
            return;
        }
        assert!(Instant::now() < deadline, "timed out pushing value {value}");
        backoff.snooze();
    }
}

fn pop_blocking(q: &FastFifo<u64>) -> u64 {
    let deadline = Instant::now() + WAIT_TIMEOUT;
    let backoff = Backoff::new();
    loop {
        if let Ok(value) = q.pop() {
            return value;
        }
        assert!(Instant::now() < deadline, "timed out popping value");
        backoff.snooze();
    }
}

fn run_bounded_fifo_order_check() {
    let q = FastFifo::new(4, 8);
    let mut pushed = 0_u64;

    loop {
        if q.push(pushed).is_ok() {
            pushed += 1;
        } else {
            break;
        }
    }

    assert!(pushed > 0, "expected bounded queue to accept some values");

    for expected in 0..pushed {
        assert_eq!(pop_blocking(&q), expected);
    }
}

fn run_throughput_integrity(producers: usize, consumers: usize) {
    let total = ITEMS_PER_PRODUCER * producers as u64;
    let seen: Arc<Vec<AtomicBool>> = Arc::new(
        (0..total as usize)
            .map(|_| AtomicBool::new(false))
            .collect(),
    );
    let consumed = Arc::new(AtomicUsize::new(0));
    let duplicates = Arc::new(AtomicUsize::new(0));

    let q = new_integrity_queue(producers, consumers);
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
                push_blocking(&q, base + offset);
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

    for handle in handles.drain(..producers) {
        handle.join().expect("producer panicked");
    }
    for _ in 0..consumers {
        push_blocking(&q, SENTINEL);
    }
    for handle in handles {
        handle.join().expect("consumer panicked");
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
        .filter(|(_, seen)| !seen.load(Ordering::Acquire))
        .map(|(idx, _)| idx)
        .take(10)
        .collect();
    assert!(
        missing.is_empty(),
        "missing items in throughput run ({producers}p/{consumers}c): first 10 = {missing:?}"
    );
}

fn run_fill_drain_integrity(producers: usize, consumers: usize) {
    let total = ITEMS_PER_PRODUCER * producers as u64;
    let seen: Arc<Vec<AtomicBool>> = Arc::new(
        (0..total as usize)
            .map(|_| AtomicBool::new(false))
            .collect(),
    );
    let consumed = Arc::new(AtomicUsize::new(0));
    let duplicates = Arc::new(AtomicUsize::new(0));

    let q = new_integrity_queue(producers, consumers);

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
                    push_blocking(&q, base + offset);
                }
            }));
        }
        barrier.wait();
        for handle in handles {
            handle.join().expect("producer panicked");
        }
    }

    for _ in 0..consumers {
        push_blocking(&q, SENTINEL);
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
        for handle in handles {
            handle.join().expect("consumer panicked");
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
        .filter(|(_, seen)| !seen.load(Ordering::Acquire))
        .map(|(idx, _)| idx)
        .take(10)
        .collect();
    assert!(
        missing.is_empty(),
        "missing items in fill/drain run ({producers}p/{consumers}c): first 10 = {missing:?}"
    );
}

macro_rules! fastfifo_throughput_test {
    ($name:ident, $producers:expr, $consumers:expr) => {
        #[test]
        fn $name() {
            run_throughput_integrity($producers, $consumers);
        }
    };
}

macro_rules! fastfifo_fill_drain_test {
    ($name:ident, $producers:expr, $consumers:expr) => {
        #[test]
        fn $name() {
            run_fill_drain_integrity($producers, $consumers);
        }
    };
}

#[test]
fn fastfifo_is_bounded_and_preserves_fifo_order() {
    run_bounded_fifo_order_check();
}

fastfifo_throughput_test!(fastfifo_spsc_throughput, 1, 1);
fastfifo_throughput_test!(fastfifo_mpsc_throughput, 4, 1);
fastfifo_throughput_test!(fastfifo_spmc_throughput, 1, 4);
fastfifo_throughput_test!(fastfifo_mpmc_throughput, 4, 4);
fastfifo_throughput_test!(fastfifo_p8c8_throughput, 8, 8);

fastfifo_fill_drain_test!(fastfifo_spsc_fill_drain, 1, 1);
fastfifo_fill_drain_test!(fastfifo_mpsc_fill_drain, 4, 1);
fastfifo_fill_drain_test!(fastfifo_spmc_fill_drain, 1, 4);
fastfifo_fill_drain_test!(fastfifo_mpmc_fill_drain, 4, 4);
fastfifo_fill_drain_test!(fastfifo_p8c8_fill_drain, 8, 8);
