#![cfg(feature = "bench_moodycamel")]

//! Smoke tests for the `moodycamel::ConcurrentQueue` FFI baseline
//! (`ubq::bench_harness::baselines::moodycamel_cq::MoodycamelQueue`),
//! mirroring `tests/rbbq_smoke.rs`'s pattern for an external/vendored queue:
//! these test the shim/wrapper boundary (does pushing a value through the
//! FFI call actually get it back out, exactly once), not the upstream
//! algorithm's own internal correctness, which is out of scope here.
//!
//! **Deliberately does not use a value-based sentinel to signal shutdown**
//! (unlike every other baseline's smoke test in this repo). moodycamel's own
//! docs are explicit that it is not linearizable across different producer
//! threads: a sentinel pushed by an orchestrator thread after joining
//! producers can be dequeued before those producers' real items, since each
//! thread gets its own internal sub-queue with no cross-thread ordering
//! guarantee. This was reproduced directly (up to 75% item loss at 8p/8c)
//! before switching to the flag-based pattern below, which is also what
//! `src/bench_harness/mod.rs`'s own measurement functions now use for
//! exactly this reason.

use std::sync::{
    Arc, Barrier,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use std::thread;

use crossbeam_utils::Backoff;
use ubq::bench_harness::baselines::moodycamel_cq::MoodycamelQueue;
use ubq::bench_harness::{BenchQueueHandleFactory, BenchQueueThreadOps};

const ITEMS_PER_PRODUCER: u64 = 500;

/// Drains through `handle` until `producers_done` is set and the queue
/// reports empty — the same pattern `bench_harness::mod`'s consumer loops
/// use, not a sentinel value (see module doc comment for why that's unsound
/// here). `handle` owns one thread's `ConsumerToken` and is never shared with
/// another thread.
fn drain_until_done(
    handle: &impl BenchQueueThreadOps,
    producers_done: &AtomicBool,
    mut on_value: impl FnMut(u64),
) {
    let backoff = Backoff::new();
    loop {
        match handle.try_recv_value() {
            Some(value) => on_value(value),
            None => {
                if producers_done.load(Ordering::Acquire) {
                    break;
                }
                backoff.snooze();
            }
        }
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

    let q = MoodycamelQueue::new_handle();
    let total_threads = producers + consumers;
    let ready = Arc::new(Barrier::new(total_threads + 1));
    let start = Arc::new(Barrier::new(total_threads + 1));
    let producers_done = Arc::new(AtomicBool::new(false));

    let mut handles = Vec::with_capacity(total_threads);

    for pid in 0..producers {
        let handle = q.producer_thread_handle();
        let ready = ready.clone();
        let start = start.clone();
        handles.push(thread::spawn(move || {
            ready.wait();
            start.wait();
            let base = pid as u64 * ITEMS_PER_PRODUCER;
            for offset in 0..ITEMS_PER_PRODUCER {
                handle.send_value(base + offset);
            }
        }));
    }

    for _ in 0..consumers {
        let handle = q.consumer_thread_handle();
        let ready = ready.clone();
        let start = start.clone();
        let seen = seen.clone();
        let consumed = consumed.clone();
        let duplicates = duplicates.clone();
        let producers_done = producers_done.clone();
        handles.push(thread::spawn(move || {
            ready.wait();
            start.wait();
            drain_until_done(&handle, &producers_done, |value| {
                let already = seen[value as usize].swap(true, Ordering::AcqRel);
                if already {
                    duplicates.fetch_add(1, Ordering::Relaxed);
                }
                consumed.fetch_add(1, Ordering::Relaxed);
            });
        }));
    }

    ready.wait();
    start.wait();

    for handle in handles.drain(..producers) {
        handle.join().expect("producer panicked");
    }
    producers_done.store(true, Ordering::Release);
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

    let q = MoodycamelQueue::new_handle();

    {
        let barrier = Arc::new(Barrier::new(producers + 1));
        let mut handles = Vec::with_capacity(producers);
        for pid in 0..producers {
            let handle = q.producer_thread_handle();
            let barrier = barrier.clone();
            handles.push(thread::spawn(move || {
                barrier.wait();
                let base = pid as u64 * ITEMS_PER_PRODUCER;
                for offset in 0..ITEMS_PER_PRODUCER {
                    handle.send_value(base + offset);
                }
            }));
        }
        barrier.wait();
        for handle in handles {
            handle.join().expect("producer panicked");
        }
    }

    // All producers are joined above, so every real item is already fully
    // enqueued (into whichever per-thread sub-queues moodycamel used) before
    // any consumer starts — the flag below is set true from the start.
    let producers_done = Arc::new(AtomicBool::new(true));

    {
        let barrier = Arc::new(Barrier::new(consumers + 1));
        let mut handles = Vec::with_capacity(consumers);
        for _ in 0..consumers {
            let handle = q.consumer_thread_handle();
            let barrier = barrier.clone();
            let seen = seen.clone();
            let consumed = consumed.clone();
            let duplicates = duplicates.clone();
            let producers_done = producers_done.clone();
            handles.push(thread::spawn(move || {
                barrier.wait();
                drain_until_done(&handle, &producers_done, |value| {
                    let already = seen[value as usize].swap(true, Ordering::AcqRel);
                    if already {
                        duplicates.fetch_add(1, Ordering::Relaxed);
                    }
                    consumed.fetch_add(1, Ordering::Relaxed);
                });
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

/// Exercises the real FFI bulk path (`mc_queue_enqueue_bulk`/
/// `mc_queue_try_dequeue_bulk` via `send_batch`/`try_recv_batch`), which the
/// scalar-only tests above never touch. This is the whole point of vendoring
/// moodycamel rather than using any other unbounded baseline.
#[test]
fn moodycamel_batch_round_trip() {
    const BATCHES: u64 = 40;
    const BATCH_SIZE: usize = 128;
    let total = BATCHES * BATCH_SIZE as u64;

    let queue = MoodycamelQueue::new_handle();
    let producer = queue.producer_thread_handle();
    let consumer = queue.consumer_thread_handle();
    for batch in 0..BATCHES {
        producer.send_batch(batch * BATCH_SIZE as u64, 0..BATCH_SIZE);
    }

    let seen: Vec<AtomicBool> = (0..total as usize)
        .map(|_| AtomicBool::new(false))
        .collect();
    let mut received = 0_usize;
    while received < total as usize {
        let request = BATCH_SIZE.min(total as usize - received);
        let got = consumer.try_recv_batch(request);
        assert!(
            got > 0,
            "try_recv_batch made no progress with items still enqueued"
        );
        received += got;
    }

    // Re-drain scalar-style to identify exactly which values came back,
    // since try_recv_batch above only reports a count. Re-run the whole
    // scenario scalar-side to check for duplicates/omissions precisely.
    let queue = MoodycamelQueue::new_handle();
    let producer = queue.producer_thread_handle();
    let consumer = queue.consumer_thread_handle();
    for batch in 0..BATCHES {
        producer.send_batch(batch * BATCH_SIZE as u64, 0..BATCH_SIZE);
    }
    let mut drained = Vec::with_capacity(total as usize);
    while (drained.len() as u64) < total {
        if let Some(v) = consumer.try_recv_value() {
            drained.push(v);
        }
    }
    for value in drained {
        let already = seen[value as usize].swap(true, Ordering::AcqRel);
        assert!(!already, "duplicate value {value} in batch round trip");
    }
    let missing: Vec<usize> = seen
        .iter()
        .enumerate()
        .filter(|(_, seen)| !seen.load(Ordering::Acquire))
        .map(|(idx, _)| idx)
        .collect();
    assert!(
        missing.is_empty(),
        "missing values in batch round trip: {missing:?}"
    );
}

macro_rules! moodycamel_throughput_test {
    ($name:ident, $producers:expr, $consumers:expr) => {
        #[test]
        fn $name() {
            run_throughput_integrity($producers, $consumers);
        }
    };
}

macro_rules! moodycamel_fill_drain_test {
    ($name:ident, $producers:expr, $consumers:expr) => {
        #[test]
        fn $name() {
            run_fill_drain_integrity($producers, $consumers);
        }
    };
}

moodycamel_throughput_test!(moodycamel_spsc_throughput, 1, 1);
moodycamel_throughput_test!(moodycamel_mpsc_throughput, 4, 1);
moodycamel_throughput_test!(moodycamel_spmc_throughput, 1, 4);
moodycamel_throughput_test!(moodycamel_mpmc_throughput, 4, 4);
moodycamel_throughput_test!(moodycamel_p8c8_throughput, 8, 8);

moodycamel_fill_drain_test!(moodycamel_spsc_fill_drain, 1, 1);
moodycamel_fill_drain_test!(moodycamel_mpsc_fill_drain, 4, 1);
moodycamel_fill_drain_test!(moodycamel_spmc_fill_drain, 1, 4);
moodycamel_fill_drain_test!(moodycamel_mpmc_fill_drain, 4, 4);
moodycamel_fill_drain_test!(moodycamel_p8c8_fill_drain, 8, 8);
