use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    thread,
};
use ubq::{UBQ, backoff};

type PageQueue<T> = UBQ<T, backoff::Crossbeam>;

#[test]
fn empty_single_and_varied_batch_lengths_preserve_fifo_order() {
    const LENGTHS: &[usize] = &[0, 1, 2, 6, 7, 8, 13, 14, 15, 22, 23, 64];

    for &len in LENGTHS {
        let q = PageQueue::new();
        q.push_batch(0..len);

        for expected in 0..len {
            assert_eq!(q.pop(), Some(expected), "batch length {len}");
        }
        assert_eq!(q.pop(), None, "batch length {len}");
        assert!(q.is_empty(), "batch length {len}");
    }
}

#[test]
fn batches_work_from_every_offset_and_across_multiple_blocks() {
    let probe = PageQueue::<usize>::new();
    let block_length = probe.block_length();
    let batch_len = 23;

    for prefix in [0, 1, block_length / 2, block_length - 1] {
        let q = PageQueue::new();

        for value in 0..prefix {
            q.push(value);
        }
        q.push_batch(prefix..prefix + batch_len);

        for expected in 0..prefix + batch_len {
            assert_eq!(q.pop(), Some(expected), "starting offset {prefix}");
        }
        assert_eq!(q.pop(), None, "starting offset {prefix}");
    }
}

#[test]
fn scalar_and_batched_pushes_can_be_mixed_at_boundaries() {
    let q = PageQueue::new();

    q.push(0);
    q.push_batch(1..7);
    q.push_batch(7..14);
    q.push(14);
    q.push_batch(15..37);
    q.push(37);

    for expected in 0..38 {
        assert_eq!(q.pop(), Some(expected));
    }
    assert_eq!(q.pop(), None);
}

#[test]
fn blocks_can_be_recycled_across_many_batched_rounds() {
    const ITEMS_PER_ROUND: usize = 53;

    let q = PageQueue::new();
    for round in 0..100 {
        let base = round * ITEMS_PER_ROUND;
        let end = base + ITEMS_PER_ROUND;

        for first in (base..end).step_by(9) {
            q.push_batch(first..end.min(first + 9));
        }
        for expected in base..end {
            assert_eq!(q.pop(), Some(expected), "round {round}");
        }
        assert_eq!(q.pop(), None, "round {round}");
    }
}

#[test]
fn dropping_queue_releases_all_batched_values() {
    let token = Arc::new(());
    let q = PageQueue::new();
    let values = (0..25).map(|_| Arc::clone(&token)).collect::<Vec<_>>();

    q.push_batch(values);
    assert_eq!(Arc::strong_count(&token), 26);

    for _ in 0..9 {
        drop(q.pop().unwrap());
    }
    assert_eq!(Arc::strong_count(&token), 17);

    drop(q);
    assert_eq!(Arc::strong_count(&token), 1);
}

#[test]
fn zero_sized_and_large_values_cross_block_boundaries() {
    let zst = PageQueue::new();
    zst.push_batch([(); 25]);
    assert_eq!((0..25).filter_map(|_| zst.pop()).count(), 25);
    assert_eq!(zst.pop(), None);

    let large = PageQueue::new();
    large.push_batch((0_u8..25).map(|value| [value; 256]));
    for expected in 0_u8..25 {
        assert_eq!(large.pop(), Some([expected; 256]));
    }
    assert_eq!(large.pop(), None);
}

#[test]
fn concurrent_batches_are_never_interleaved() {
    const PRODUCERS: usize = 4;
    const BATCHES: usize = 250;
    const BATCH_LEN: usize = 11;

    let q = Arc::new(PageQueue::new());
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
fn mixed_concurrent_producers_deliver_every_item_once() {
    const PRODUCERS: usize = 4;
    const ITEMS_PER_PRODUCER: usize = 5_000;
    const TOTAL: usize = PRODUCERS * ITEMS_PER_PRODUCER;

    let q = Arc::new(PageQueue::new());
    let producers = (0..PRODUCERS)
        .map(|producer| {
            let q = Arc::clone(&q);
            thread::spawn(move || {
                let first = producer * ITEMS_PER_PRODUCER;
                let end = first + ITEMS_PER_PRODUCER;

                for chunk in (first..end).step_by(17) {
                    let chunk_end = end.min(chunk + 17);
                    if (chunk / 17).is_multiple_of(2) {
                        q.push_batch(chunk..chunk_end);
                    } else {
                        for value in chunk..chunk_end {
                            q.push(value);
                        }
                    }
                }
            })
        })
        .collect::<Vec<_>>();

    for producer in producers {
        producer.join().unwrap();
    }

    let mut seen = vec![false; TOTAL];
    for _ in 0..TOTAL {
        let value = q.pop().unwrap();
        assert!(value < TOTAL);
        assert!(!seen[value], "duplicate value {value}");
        seen[value] = true;
    }
    assert!(seen.into_iter().all(|value| value));
    assert_eq!(q.pop(), None);
}

#[test]
fn batched_mpmc_delivers_every_item_once() {
    const PRODUCERS: usize = 4;
    const CONSUMERS: usize = 4;
    const ITEMS_PER_PRODUCER: usize = 10_000;
    const TOTAL: usize = PRODUCERS * ITEMS_PER_PRODUCER;

    let q: Arc<PageQueue<usize>> = Arc::new(PageQueue::new());
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
                        let previous = seen[value].fetch_add(1, Ordering::Relaxed);
                        consumed.fetch_add(1, Ordering::Release);
                        assert_eq!(previous, 0, "duplicate value {value}");
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
                for chunk in (first..end).step_by(13) {
                    q.push_batch(chunk..end.min(chunk + 13));
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
