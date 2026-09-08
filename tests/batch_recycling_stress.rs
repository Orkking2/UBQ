//! Worst-case public-API stress test for the *batch* path, mirroring
//! `recycling_aba.rs` but with `push_batch`/`pop_batch` under high consumer
//! contention and many block-recycling rounds.
//!
//! The weekend hardware campaign observed `consumed > sent` integrity
//! mismatches only on `push_batch`/`pop_batch` configurations at high
//! consumer counts (`1p64c`, `1p143c`, `72p72c`), never on scalar
//! `push`/`pop`. This test reproduces that shape locally: one producer,
//! many consumers, native batch size 256, many drain-to-empty rounds on the
//! same queue so blocks are repeatedly recycled.
//!
//! Run with:
//!
//! ```text
//! cargo test --test batch_recycling_stress -- --ignored --nocapture
//! ```
//!
//! Optional tuning:
//!
//! ```text
//! UBQ_BATCH_ABA_ROUNDS=200 UBQ_BATCH_ABA_ROUND_ITEMS=200000 \
//! UBQ_BATCH_ABA_CONSUMERS=64 UBQ_BATCH_ABA_BATCH=256 \
//! cargo test --test batch_recycling_stress -- --ignored --nocapture
//! ```

use std::{
    env,
    sync::{
        Arc, Barrier,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    thread,
};
use ubq::{UBQ, backoff};

type Q = UBQ<u64, backoff::Crossbeam>;

#[test]
#[ignore = "statistical stress test; run explicitly with --ignored --nocapture"]
fn batched_consume_never_overcounts_or_duplicates_across_recycled_rounds() {
    let rounds = env_usize("UBQ_BATCH_ABA_ROUNDS", 200);
    let round_items = env_usize("UBQ_BATCH_ABA_ROUND_ITEMS", 200_000);
    let producers = env_usize("UBQ_BATCH_ABA_PRODUCERS", 1);
    let consumers = env_usize("UBQ_BATCH_ABA_CONSUMERS", default_consumers());
    let batch = env_usize("UBQ_BATCH_ABA_BATCH", 256);

    let queue: Arc<Q> = Arc::new(Q::new());

    let mut total_sent = 0u64;
    let mut total_received = 0u64;
    let mut duplicates_total = 0usize;
    let mut missing_total = 0usize;

    for round in 0..rounds {
        let seen = Arc::new(
            (0..round_items)
                .map(|_| AtomicUsize::new(0))
                .collect::<Vec<_>>(),
        );
        let received = Arc::new(AtomicUsize::new(0));
        let done = Arc::new(AtomicBool::new(false));
        // Producers + consumers + this thread.
        let start = Arc::new(Barrier::new(consumers + producers + 1));

        let producer_threads = (0..producers)
            .map(|producer_index| {
                let queue = Arc::clone(&queue);
                let start = Arc::clone(&start);
                // Each producer owns a disjoint, contiguous value range so
                // consumers can still validate exact-once delivery.
                let base = producer_index * round_items / producers;
                let end = (producer_index + 1) * round_items / producers;
                thread::spawn(move || {
                    start.wait();
                    for first in (base..end).step_by(batch) {
                        let last = end.min(first + batch);
                        queue.push_batch((first..last).map(|value| value as u64));
                    }
                })
            })
            .collect::<Vec<_>>();

        let mut consumer_threads = Vec::with_capacity(consumers);
        for _ in 0..consumers {
            let queue = Arc::clone(&queue);
            let seen = Arc::clone(&seen);
            let received = Arc::clone(&received);
            let done = Arc::clone(&done);
            let start = Arc::clone(&start);

            consumer_threads.push(thread::spawn(move || {
                start.wait();
                let mut local_duplicates = 0usize;
                let mut after_done_empty = false;

                loop {
                    let mut count = 0usize;
                    for value in queue.pop_batch(batch) {
                        let index = value as usize;
                        if index < seen.len() {
                            if seen[index].fetch_add(1, Ordering::AcqRel) != 0 {
                                local_duplicates += 1;
                            }
                        } else {
                            // Out-of-range values are also a correctness bug;
                            // fold them into the duplicate counter so any
                            // occurrence fails the round.
                            local_duplicates += 1;
                        }
                        count += 1;
                    }

                    received.fetch_add(count, Ordering::AcqRel);

                    if count != 0 {
                        after_done_empty = false;
                        continue;
                    }

                    if done.load(Ordering::Acquire) {
                        if after_done_empty {
                            break;
                        }
                        after_done_empty = true;
                    }

                    thread::yield_now();
                }

                local_duplicates
            }));
        }

        start.wait();
        for handle in producer_threads {
            handle.join().expect("producer panicked");
        }
        done.store(true, Ordering::Release);

        let round_duplicates: usize = consumer_threads
            .into_iter()
            .map(|handle| handle.join().expect("consumer panicked"))
            .sum();

        let round_received = received.load(Ordering::Acquire);
        let round_missing = seen
            .iter()
            .filter(|slot| slot.load(Ordering::Acquire) == 0)
            .count();

        total_sent += round_items as u64;
        total_received += round_received as u64;
        duplicates_total += round_duplicates;
        missing_total += round_missing;

        if round_received != round_items || round_duplicates != 0 || round_missing != 0 {
            panic!(
                "round {round}: sent {round_items} received {round_received} \
                 duplicates {round_duplicates} missing {round_missing} \
                 (totals so far: sent {total_sent} received {total_received} \
                 duplicates {duplicates_total} missing {missing_total})"
            );
        }

        if round % (rounds / 10).max(1) == 0 {
            println!(
                "batch recycling stress progress: round={round}/{rounds} sent={total_sent} received={total_received}"
            );
        }
    }

    println!(
        "batch recycling stress: rounds={rounds} consumers={consumers} batch={batch} \
         round_items={round_items} total_sent={total_sent} total_received={total_received} \
         duplicates={duplicates_total} missing={missing_total}"
    );
}

fn default_consumers() -> usize {
    thread::available_parallelism()
        .map(|threads| threads.get() * 4)
        .unwrap_or(32)
        .clamp(16, 64)
}

fn env_usize(name: &str, default: usize) -> usize {
    env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}
