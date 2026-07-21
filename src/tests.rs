use std::{
    fmt::Debug,
    hint::black_box,
    panic::{AssertUnwindSafe, catch_unwind},
    println,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    thread::{self},
    time::Instant,
    vec::Vec,
};

use crate::{ConfiguredUBQ, DEFAULT_BLOCK_SIZE, UBQ, align, backoff, ubq};

type TinyQueue<T> = ConfiguredUBQ<T, backoff::Crossbeam, 2, 7, align::A64>;

#[test]
fn push_batch_preserves_order_at_every_block_offset() {
    const BATCH_LEN: usize = 23;

    for prefix in 0..TinyQueue::<usize>::BLOCK_LENGTH {
        let q = TinyQueue::new();

        for value in 0..prefix {
            q.push(value);
        }
        q.push_batch(prefix..prefix + BATCH_LEN);

        for expected in 0..prefix + BATCH_LEN {
            assert_eq!(q.pop(), Some(expected), "prefix={prefix}");
        }
        assert_eq!(q.pop(), None, "prefix={prefix}");
        assert!(q.is_empty(), "prefix={prefix}");
    }
}

#[test]
fn push_batch_handles_empty_single_and_exact_block_batches() {
    let q = TinyQueue::new();

    q.push_batch([]);
    assert!(q.is_empty());

    q.push_batch([0]);
    q.push_batch(1..8);
    q.push_batch(8..15);
    q.push(15);

    for expected in 0..16 {
        assert_eq!(q.pop(), Some(expected));
    }
    assert_eq!(q.pop(), None);
}

#[test]
fn concurrent_push_batches_do_not_interleave() {
    const PRODUCERS: usize = 4;
    const BATCHES: usize = 200;
    const BATCH_LEN: usize = 11;

    let q = Arc::new(TinyQueue::new());
    let producers = (0..PRODUCERS)
        .map(|producer| {
            let q = Arc::clone(&q);
            thread::spawn(move || {
                for batch in 0..BATCHES {
                    q.push_batch((0..BATCH_LEN).map(|offset| (producer, batch, offset)));
                }
            })
        })
        .collect::<Vec<_>>();

    for producer in producers {
        producer.join().unwrap();
    }

    for _ in 0..PRODUCERS * BATCHES {
        let (producer, batch, offset) = q.pop().unwrap();
        assert_eq!(offset, 0);

        for expected_offset in 1..BATCH_LEN {
            assert_eq!(q.pop(), Some((producer, batch, expected_offset)));
        }
    }
    assert_eq!(q.pop(), None);
}

#[test]
fn push_batch_is_safe_with_concurrent_consumers() {
    const PRODUCERS: usize = 4;
    const CONSUMERS: usize = 4;
    const ITEMS_PER_PRODUCER: usize = 10_000;
    const BATCH_LEN: usize = 13;
    const TOTAL: usize = PRODUCERS * ITEMS_PER_PRODUCER;

    let q: Arc<TinyQueue<usize>> = Arc::new(TinyQueue::new());
    let consumed = Arc::new(AtomicUsize::new(0));
    let seen: Arc<Vec<AtomicUsize>> = Arc::new((0..TOTAL).map(|_| AtomicUsize::new(0)).collect());

    let consumers = (0..CONSUMERS)
        .map(|_| {
            let q = Arc::clone(&q);
            let consumed = Arc::clone(&consumed);
            let seen = Arc::clone(&seen);
            thread::spawn(move || {
                while consumed.load(Ordering::Acquire) < TOTAL {
                    if let Some(value) = q.pop() {
                        assert!(value < TOTAL);
                        let prior_count = seen[value].fetch_add(1, Ordering::Relaxed);
                        consumed.fetch_add(1, Ordering::Release);
                        assert_eq!(prior_count, 0);
                    } else {
                        thread::yield_now();
                    }
                }
            })
        })
        .collect::<Vec<_>>();

    let producers = (0..PRODUCERS)
        .map(|producer| {
            let q = Arc::clone(&q);
            thread::spawn(move || {
                let first = producer * ITEMS_PER_PRODUCER;
                let end = first + ITEMS_PER_PRODUCER;
                for batch_first in (first..end).step_by(BATCH_LEN) {
                    q.push_batch(batch_first..end.min(batch_first + BATCH_LEN));
                }
            })
        })
        .collect::<Vec<_>>();

    for producer in producers {
        producer.join().unwrap();
    }
    for consumer in consumers {
        consumer.join().unwrap();
    }

    assert_eq!(consumed.load(Ordering::Relaxed), TOTAL);
    assert!(seen.iter().all(|count| count.load(Ordering::Relaxed) == 1));
    assert_eq!(q.pop(), None);
}

struct ShortExactIterator {
    next: usize,
}

impl Iterator for ShortExactIterator {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        (self.next < 2).then(|| {
            let value = self.next;
            self.next += 1;
            value
        })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (4, Some(4))
    }
}

impl ExactSizeIterator for ShortExactIterator {
    fn len(&self) -> usize {
        4
    }
}

#[test]
fn invalid_exact_size_iterator_does_not_block_the_queue() {
    let q = TinyQueue::new();

    let result = catch_unwind(AssertUnwindSafe(|| {
        q.push_batch(ShortExactIterator { next: 0 });
    }));
    assert!(result.is_err());

    q.push(2);
    assert_eq!(q.pop(), Some(0));
    assert_eq!(q.pop(), Some(1));
    assert_eq!(q.pop(), Some(2));
    assert_eq!(q.pop(), None);
}

#[test]
fn dropping_queue_releases_batched_values() {
    let token = Arc::new(());
    let q = TinyQueue::new();
    let values = (0..25).map(|_| Arc::clone(&token)).collect::<Vec<_>>();

    q.push_batch(values);
    assert_eq!(Arc::strong_count(&token), 26);
    drop(q);
    assert_eq!(Arc::strong_count(&token), 1);
}

#[test]
fn drop_releases_all_enqueued_values() {
    let token = Arc::new(());
    let n = (DEFAULT_BLOCK_SIZE * 3) + 7;

    for _ in 0..16 {
        let q = UBQ::new();

        for _ in 0..n {
            q.push(token.clone());
        }

        assert_eq!(Arc::strong_count(&token), n + 1);

        println!("q: {q:?}");

        drop(q);

        assert_eq!(Arc::strong_count(&token), 1);
    }
}

#[test]
fn fill_drain_ordered() {
    let q = UBQ::new();

    let m = 1_000_000;
    for i in 0..m {
        q.push(i);
    }

    for i in 0..m {
        assert_eq!(q.pop(), Some(i));
    }
}

#[test]
fn refill_drain_recycled_blocks() {
    let q = UBQ::new();
    let per_round = DEFAULT_BLOCK_SIZE * 3 + 17;

    for round in 0..64 {
        for i in 0..per_round {
            q.push((round, i));
        }

        for i in 0..per_round {
            assert_eq!(q.pop(), Some((round, i)));
        }

        assert_eq!(q.pop(), None);
    }
}

#[test]
// 8x2x10_000_001
// Seg: 1.63769375s
// UBQ: 5.440279166s

// Notes:
// Look for page faults. VTune, perf
// Warm up before running tests.
// Look for better benchmarkers.
fn mpmc() {
    let q = UBQ::new_arc();
    // let q = Arc::new(SegQueue::new());

    let flag = Arc::new(AtomicBool::new(true));

    let epoch = Instant::now();

    let m = 1_000_001;
    let v: Vec<_> = (0..8)
        .map(|_| {
            (
                {
                    let q = q.clone();

                    thread::spawn(move || {
                        for i in 0..m {
                            q.push(black_box((i % u8::MAX as i32) as u8));
                        }
                    })
                },
                {
                    let flag = flag.clone();
                    let q = q.clone();

                    thread::spawn(move || {
                        for _ in 0..m {
                            loop {
                                if flag.load(Ordering::Acquire) {
                                    if black_box(q.pop()).is_some() {
                                        break;
                                    }
                                } else {
                                    assert!(black_box(q.pop()).is_some());
                                    break;
                                }
                            }
                        }
                    })
                },
            )
        })
        .collect();

    let v: Vec<_> = v
        .into_iter()
        .map(|(p, c)| {
            p.join().unwrap();
            c
        })
        .collect();

    flag.store(false, Ordering::Release);

    for c in v {
        c.join().unwrap()
    }

    println!("{:?}", epoch.elapsed());
}

#[test]
fn configured_queue_supports_non_default_pool_and_preset_block() {
    let q = ConfiguredUBQ::<u64, backoff::Crossbeam, 8, 127, align::A256>::new();

    for i in 0..10_000 {
        q.push(i);
    }

    for i in 0..10_000 {
        assert_eq!(q.pop(), Some(i));
    }

    assert_eq!(q.pop(), None);
}

#[test]
fn configured_queue_supports_arbitrary_block_with_explicit_alignment() {
    #[repr(align(1024))]
    #[derive(Clone, Copy, Debug, Default)]
    struct A1024;

    let q = ConfiguredUBQ::<u64, backoff::Crossbeam, 2, 100, A1024>::new();

    for i in 0..2_000 {
        q.push(i);
    }

    for i in 0..2_000 {
        assert_eq!(q.pop(), Some(i));
    }

    assert_eq!(q.pop(), None);
}

#[test]
fn ubq_macro_defaults_to_public_alias() {
    let q: ConfiguredUBQ<u64> = ubq!(type: u64);
    q.push(9);
    assert_eq!(q.pop(), Some(9));
}

#[test]
fn ubq_macro_applies_explicit_overrides() {
    let q: ConfiguredUBQ<u64, backoff::Yield, 2, 127, align::A256> = ubq!(
        type: u64,
        backoff: backoff::Yield,
        pool: 2,
        block: 127,
    );

    q.push(11);
    assert_eq!(q.pop(), Some(11));
}

#[test]
fn ubq_macro_supports_custom_alignment_override() {
    #[repr(align(1024))]
    #[derive(Clone, Copy, Debug, Default)]
    struct A1024;

    let q: ConfiguredUBQ<u64, backoff::Crossbeam, 4, 100, A1024> = ubq!(
        type: u64,
        pool: 4,
        block: 100,
        align: A1024,
    );

    q.push(13);
    assert_eq!(q.pop(), Some(13));
}

// Seg: 2.12s
// UBQ: 5.15s
#[test]
fn push_test() {
    let q = UBQ::new_arc();
    // let q = Arc::new(SegQueue::new());

    let epoch = Instant::now();

    let v = (0..8)
        .map(|_| {
            let q = q.clone();

            thread::spawn(move || {
                for i in 0..1_000_000 {
                    q.push(black_box(i));
                }
            })
        })
        .collect::<Vec<_>>();

    v.into_iter().for_each(|h| h.join().unwrap());

    println!("{:?}", epoch.elapsed());
}

// #[test]
// fn is_empty_returns_correctly() {
//     assert!(UBQ::<()>::new().is_empty());

//     for m in 1_000..1_005 {
//         let q = UBQ::new();

//         for i in 0..m {
//             q.push(i);
//         }

//         for _ in 0..m {
//             q.pop().unwrap();
//         }

//         assert!(q.is_empty())
//     }
// }
