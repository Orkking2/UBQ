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
fn empty_zero_and_oversized_requests_stop_at_the_producer_frontier() {
    let queue = PageQueue::new();

    assert_eq!(queue.pop_batch(0).next(), None);
    assert_eq!(queue.pop_batch(3).next(), None);

    queue.push_batch(0..5);

    assert!(queue.pop_batch(0).next().is_none());
    assert_eq!(
        queue.pop_batch(usize::MAX).collect::<Vec<_>>(),
        (0..5).collect::<Vec<_>>()
    );
    assert_eq!(queue.pop_batch(1).next(), None);
}

#[test]
fn reservations_cross_multiple_fixed_blocks_and_preserve_fifo_order() {
    let queue = PageQueue::new();
    let block_length = queue.block_length();
    let total = block_length * 3 + 3;
    queue.push_batch(0..total);

    assert_eq!(
        queue.pop_batch(block_length - 1).collect::<Vec<_>>(),
        (0..block_length - 1).collect::<Vec<_>>()
    );
    assert_eq!(
        queue.pop_batch(block_length + 2).collect::<Vec<_>>(),
        (block_length - 1..block_length * 2 + 1).collect::<Vec<_>>()
    );
    assert_eq!(
        queue.pop_batch(total).collect::<Vec<_>>(),
        (block_length * 2 + 1..total).collect::<Vec<_>>()
    );
    assert!(queue.is_empty());
}

#[test]
fn every_start_offset_and_boundary_endpoint_interoperate_with_scalar_pop() {
    let probe = PageQueue::<usize>::new();
    let block_length = probe.block_length();
    for prefix in [0, 1, block_length - 1, block_length, block_length + 1] {
        for request in [1, block_length - 1, block_length, block_length + 1] {
            let queue = PageQueue::new();
            let total = prefix + request + 1;
            queue.push_batch(0..total);

            assert_eq!(
                queue.pop_batch(prefix).collect::<Vec<_>>(),
                (0..prefix).collect::<Vec<_>>()
            );
            assert_eq!(
                queue.pop_batch(request).collect::<Vec<_>>(),
                (prefix..prefix + request).collect::<Vec<_>>()
            );
            assert_eq!(queue.pop(), Some(prefix + request));
        }
    }
}

#[test]
fn skipped_positions_do_not_appear_as_items() {
    let queue = PageQueue::new();

    queue.push_batch(ShortExactSize {
        values: 10..12,
        advertised: 9,
    });
    queue.push(12);

    assert_eq!(queue.pop_batch(10).collect::<Vec<_>>(), [10, 11, 12]);
    assert!(queue.is_empty());
}

#[test]
fn dropping_an_iterator_drains_its_entire_reservation() {
    let dropped = Arc::new(AtomicUsize::new(0));
    let queue = PageQueue::new();

    queue.push_batch((0..20).map(|_| DropCounter(Arc::clone(&dropped))));

    let mut batch = queue.pop_batch(13);
    drop(batch.next());
    assert_eq!(dropped.load(Ordering::Relaxed), 1);

    drop(batch);
    assert_eq!(dropped.load(Ordering::Relaxed), 13);

    assert_eq!(queue.pop_batch(20).count(), 7);
    assert_eq!(dropped.load(Ordering::Relaxed), 20);
}

#[test]
fn concurrent_batch_consumers_deliver_every_item_once() {
    const CONSUMERS: usize = 4;
    const ITEMS: usize = 20_000;

    let queue: Arc<PageQueue<usize>> = Arc::new(PageQueue::new());
    queue.push_batch(0..ITEMS);

    let seen = Arc::new((0..ITEMS).map(|_| AtomicUsize::new(0)).collect::<Vec<_>>());
    let consumed = Arc::new(AtomicUsize::new(0));

    let consumers = (0..CONSUMERS)
        .map(|_| {
            let queue = Arc::clone(&queue);
            let seen = Arc::clone(&seen);
            let consumed = Arc::clone(&consumed);

            thread::spawn(move || {
                loop {
                    let batch = queue.pop_batch(23);
                    let mut count = 0;

                    for value in batch {
                        assert_eq!(seen[value].fetch_add(1, Ordering::Relaxed), 0);
                        count += 1;
                    }

                    if count == 0 {
                        break;
                    }

                    consumed.fetch_add(count, Ordering::Relaxed);
                }
            })
        })
        .collect::<Vec<_>>();

    for consumer in consumers {
        consumer.join().unwrap();
    }

    assert_eq!(consumed.load(Ordering::Relaxed), ITEMS);
    assert!(seen.iter().all(|count| count.load(Ordering::Relaxed) == 1));
}

#[test]
fn concurrent_batched_producers_and_consumers_deliver_every_item_once() {
    const PRODUCERS: usize = 3;
    const CONSUMERS: usize = 3;
    const ITEMS_PER_PRODUCER: usize = 4_000;
    const ITEMS: usize = PRODUCERS * ITEMS_PER_PRODUCER;

    let queue: Arc<PageQueue<usize>> = Arc::new(PageQueue::new());
    let seen = Arc::new((0..ITEMS).map(|_| AtomicUsize::new(0)).collect::<Vec<_>>());
    let consumed = Arc::new(AtomicUsize::new(0));

    let consumers = (0..CONSUMERS)
        .map(|_| {
            let queue = Arc::clone(&queue);
            let seen = Arc::clone(&seen);
            let consumed = Arc::clone(&consumed);

            thread::spawn(move || {
                while consumed.load(Ordering::Acquire) < ITEMS {
                    let mut found = false;

                    for value in queue.pop_batch(29) {
                        assert_eq!(seen[value].fetch_add(1, Ordering::Relaxed), 0);
                        consumed.fetch_add(1, Ordering::Release);
                        found = true;
                    }

                    if !found {
                        thread::yield_now();
                    }
                }
            })
        })
        .collect::<Vec<_>>();

    let producers = (0..PRODUCERS)
        .map(|producer| {
            let queue = Arc::clone(&queue);

            thread::spawn(move || {
                let start = producer * ITEMS_PER_PRODUCER;
                let end = start + ITEMS_PER_PRODUCER;

                for first in (start..end).step_by(17) {
                    queue.push_batch(first..end.min(first + 17));
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

    assert_eq!(consumed.load(Ordering::Relaxed), ITEMS);
    assert!(seen.iter().all(|count| count.load(Ordering::Relaxed) == 1));
}

struct DropCounter(Arc<AtomicUsize>);

impl Drop for DropCounter {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

struct ShortExactSize {
    values: std::ops::Range<usize>,
    advertised: usize,
}

impl Iterator for ShortExactSize {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        self.values.next()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.advertised, Some(self.advertised))
    }
}

impl ExactSizeIterator for ShortExactSize {
    fn len(&self) -> usize {
        self.advertised
    }
}
