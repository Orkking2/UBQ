#![cfg(target_has_atomic = "32")]

use std::sync::{
    Arc, Barrier,
    atomic::{AtomicU32, Ordering},
};
use std::thread;
use std::time::{Duration, Instant};

use crossbeam_utils::CachePadded;

const THREADS: usize = 32;
const ITERS_PER_THREAD: usize = 1_000_000;
const REPEATS: usize = 5;

#[derive(Clone, Copy)]
struct RunResult {
    elapsed: Duration,
    total_ops: usize,
}

fn ns_per_op(run: RunResult) -> f64 {
    run.elapsed.as_secs_f64() * 1e9 / run.total_ops as f64
}

fn median_duration(mut samples: Vec<Duration>) -> Duration {
    samples.sort_unstable();
    samples[samples.len() / 2]
}

fn run_shared_atomic_fetch_or() -> RunResult {
    let bits = Arc::new(AtomicU32::new(0));
    let start_barrier = Arc::new(Barrier::new(THREADS + 1));
    let total_ops = THREADS * ITERS_PER_THREAD;

    let handles: Vec<_> = (0..THREADS)
        .map(|tid| {
            let bits = Arc::clone(&bits);
            let start_barrier = Arc::clone(&start_barrier);
            let mask = 1u32 << tid;

            thread::spawn(move || {
                start_barrier.wait();
                for _ in 0..ITERS_PER_THREAD {
                    bits.fetch_or(mask, Ordering::Release);
                }
            })
        })
        .collect();

    start_barrier.wait();
    let t0 = Instant::now();

    for h in handles {
        h.join().expect("shared worker panicked");
    }

    assert_eq!(bits.load(Ordering::SeqCst), u32::MAX);

    RunResult {
        elapsed: t0.elapsed(),
        total_ops,
    }
}

fn run_sharded_atomic_fetch_or() -> RunResult {
    // Cache-pad each atomic so this measures "separate atomics" rather than
    // false sharing on the same cache line.
    let atoms = Arc::new(
        (0..THREADS)
            .map(|_| CachePadded::new(AtomicU32::new(0)))
            .collect::<Vec<_>>(),
    );
    let start_barrier = Arc::new(Barrier::new(THREADS + 1));
    let total_ops = THREADS * ITERS_PER_THREAD;

    let handles: Vec<_> = (0..THREADS)
        .map(|tid| {
            let atoms = Arc::clone(&atoms);
            let start_barrier = Arc::clone(&start_barrier);
            let mask = 1u32 << tid;

            thread::spawn(move || {
                start_barrier.wait();
                let cell = &atoms[tid];
                for _ in 0..ITERS_PER_THREAD {
                    cell.fetch_or(mask, Ordering::Release);
                }
            })
        })
        .collect();

    start_barrier.wait();
    let t0 = Instant::now();

    for h in handles {
        h.join().expect("sharded worker panicked");
    }

    for tid in 0..THREADS {
        assert_eq!(atoms[tid].load(Ordering::SeqCst), 1u32 << tid);
    }

    RunResult {
        elapsed: t0.elapsed(),
        total_ops,
    }
}

#[test]
#[ignore = "performance benchmark; run manually with -- --ignored --nocapture"]
fn fetch_or_shared_vs_per_thread_atomics_32_bits() {
    let mut shared = Vec::with_capacity(REPEATS);
    let mut sharded = Vec::with_capacity(REPEATS);

    for _ in 0..REPEATS {
        shared.push(run_shared_atomic_fetch_or());
        sharded.push(run_sharded_atomic_fetch_or());
    }

    let shared_median = median_duration(shared.iter().map(|r| r.elapsed).collect());
    let sharded_median = median_duration(sharded.iter().map(|r| r.elapsed).collect());
    let total_ops = THREADS * ITERS_PER_THREAD;

    let shared_run = RunResult {
        elapsed: shared_median,
        total_ops,
    };
    let sharded_run = RunResult {
        elapsed: sharded_median,
        total_ops,
    };

    let ratio = shared_median.as_secs_f64() / sharded_median.as_secs_f64();

    println!(
        "fetch_or benchmark (median of {REPEATS} runs): threads={THREADS}, ops/thread={ITERS_PER_THREAD}, total_ops={total_ops}"
    );
    println!(
        "  shared single AtomicU32 : {:?} ({:.2} ns/op)",
        shared_run.elapsed,
        ns_per_op(shared_run)
    );
    println!(
        "  per-thread AtomicU32    : {:?} ({:.2} ns/op)",
        sharded_run.elapsed,
        ns_per_op(sharded_run)
    );
    println!("  shared / per-thread ratio: {:.2}x", ratio);
}
