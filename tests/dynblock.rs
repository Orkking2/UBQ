//! Concurrent acceptance workloads run in child processes with a 20s deadline.
use std::{
    sync::{
        Arc, Barrier,
        atomic::{AtomicUsize, Ordering::Relaxed},
    },
    thread,
};
use ubq::{
    backoff::{BackoffPolicy, Crossbeam, Yield},
    dynblock::{Config, UBQ},
};

#[path = "support/isolated.rs"]
mod isolated;

struct Value {
    id: usize,
    drops: Arc<Vec<AtomicUsize>>,
}

impl Drop for Value {
    fn drop(&mut self) {
        self.drops[self.id].fetch_add(1, Relaxed);
    }
}

#[test]
fn concurrent_scalar_and_batch_transfers_are_exact_once() {
    if isolated::run("concurrent_scalar_and_batch_transfers_are_exact_once") {
        return;
    }
    exact_once::<Crossbeam>();
}

#[test]
fn concurrent_transfers_are_exact_once_with_yield_backoff() {
    if isolated::run("concurrent_transfers_are_exact_once_with_yield_backoff") {
        return;
    }
    exact_once::<Yield>();
}

fn exact_once<B: BackoffPolicy>() {
    const ITEMS: usize = 1001;
    for (producers, consumers) in [(1, 1), (4, 1), (1, 8), (4, 4)] {
        for (slots, blocks) in [(1, 1), (3, 2), (32, 4)] {
            let q = UBQ::<Value, B>::with_config(Config {
                slots_per_vblock: slots,
                vblocks_per_block: blocks,
            })
            .unwrap();
            // Repeated drains exercise stale heads and allocation reuse.
            for _ in 0..4 {
                let total = producers * ITEMS;
                let drops = Arc::new((0..total).map(|_| AtomicUsize::new(0)).collect::<Vec<_>>());
                let seen = (0..total).map(|_| AtomicUsize::new(0)).collect::<Vec<_>>();
                let consumed = AtomicUsize::new(0);
                let barrier = Barrier::new(producers + consumers);
                thread::scope(|scope| {
                    for producer in 0..producers {
                        let (q, drops, barrier) = (&q, &drops, &barrier);
                        scope.spawn(move || {
                            barrier.wait();
                            for start in (0..ITEMS).step_by(17) {
                                let values: Vec<_> = (start..(start + 17).min(ITEMS))
                                    .map(|i| Value {
                                        id: producer * ITEMS + i,
                                        drops: drops.clone(),
                                    })
                                    .collect();
                                if (start + producer) % 2 == 0 {
                                    assert!(q.push_batch(values).is_empty());
                                } else {
                                    for value in values {
                                        q.push(value);
                                    }
                                }
                            }
                        });
                    }
                    for consumer in 0..consumers {
                        let (q, seen, consumed, barrier) = (&q, &seen, &consumed, &barrier);
                        scope.spawn(move || {
                            barrier.wait();
                            while consumed.load(Relaxed) < total {
                                let values = if consumer % 2 == 0 {
                                    q.pop_batch(Box::new_uninit_slice(23))
                                } else {
                                    q.pop().into_iter().collect()
                                };
                                if values.is_empty() {
                                    thread::yield_now();
                                }
                                for value in values {
                                    assert!(value.id < total, "corrupt id {}", value.id);
                                    assert_eq!(
                                        seen[value.id].fetch_add(1, Relaxed),
                                        0,
                                        "duplicate {}",
                                        value.id
                                    );
                                    consumed.fetch_add(1, Relaxed);
                                }
                            }
                        });
                    }
                });
                assert_eq!(consumed.load(Relaxed), total);
                assert!(q.is_empty());
                assert!(q.pop().is_none());
                assert!(seen.iter().all(|n| n.load(Relaxed) == 1));
                assert!(drops.iter().all(|n| n.load(Relaxed) == 1));
                assert_eq!(Arc::strong_count(&drops), 1);
            }
        }
    }
}

#[test]
fn competing_producers_keep_fifo_and_whole_batches_contiguous() {
    if isolated::run("competing_producers_keep_fifo_and_whole_batches_contiguous") {
        return;
    }
    const PRODUCERS: usize = 4;
    const ITEMS: usize = 3000;
    const BATCH: usize = 10;
    for (slots, blocks) in [(1, 1), (3, 2), (32, 4)] {
        let q = UBQ::<(usize, usize)>::with_config(Config {
            slots_per_vblock: slots,
            vblocks_per_block: blocks,
        })
        .unwrap();
        let barrier = Barrier::new(PRODUCERS + 1);
        thread::scope(|scope| {
            for producer in 0..PRODUCERS {
                let (q, barrier) = (&q, &barrier);
                scope.spawn(move || {
                    barrier.wait();
                    for start in (0..ITEMS).step_by(BATCH) {
                        q.push_batch((start..start + BATCH).map(|i| (producer, i)).collect());
                        thread::yield_now();
                    }
                });
            }
            barrier.wait();
            let mut next = [0; PRODUCERS];
            let mut previous_producer = None;
            for position in 0..PRODUCERS * ITEMS {
                let (producer, index) = loop {
                    if let Some(value) = q.pop() {
                        break value;
                    }
                    thread::yield_now();
                };
                assert!(producer < PRODUCERS);
                assert_eq!(index, next[producer]);
                next[producer] += 1;
                if position % BATCH != 0 {
                    assert_eq!(Some(producer), previous_producer);
                }
                previous_producer = Some(producer);
            }
            assert_eq!(next, [ITEMS; PRODUCERS]);
        });
        assert!(q.is_empty());
    }
}
