//! The same behavioral assertions run against v8 and the reference control.
use crate::{Config, Queue};
use std::{
    cell::Cell,
    collections::VecDeque,
    sync::{
        Arc, Barrier,
        atomic::{AtomicUsize, Ordering::Relaxed},
    },
    thread,
};

mod isolated;

fn isolated(name: &str) -> bool {
    let module = module_path!().split_once("::").unwrap().1;
    isolated::run(&format!("{module}::{name}"))
}

fn queue<T>(slots: usize, blocks: usize) -> Queue<T> {
    Queue::with_config(Config {
        slots_per_vblock: slots,
        vblocks_per_block: blocks,
    })
    .unwrap()
}

fn pop<T>(q: &Queue<T>, max: usize) -> Vec<T> {
    let buffer = Box::<[T]>::new_uninit_slice(max);
    let pointer = buffer.as_ptr().cast::<T>();
    let values = q.pop_batch(buffer);
    assert!(values.len() <= max, "pop exceeded requested limit {max}");
    if size_of::<T>() != 0 {
        assert_eq!(values.capacity(), max, "output capacity changed");
        assert_eq!(values.as_ptr(), pointer, "output buffer was not reused");
    }
    values
}

fn push<T>(q: &Queue<T>, values: Vec<T>) {
    let pointer = values.as_ptr();
    let capacity = values.capacity();
    let empty = q.push_batch(values);
    assert!(empty.is_empty(), "push did not move every input value");
    assert_eq!(empty.capacity(), capacity, "input capacity changed");
    assert_eq!(empty.as_ptr(), pointer, "input buffer was not returned");
}

#[test]
fn empty_and_zero_capacity_operations() {
    if isolated("empty_and_zero_capacity_operations") {
        return;
    }
    let q = queue::<usize>(3, 2);
    for _ in 0..3 {
        assert_eq!(q.pop(), None);
        assert!(pop(&q, 7).is_empty());
        push(&q, Vec::with_capacity(9));
    }
    q.push(42);
    assert!(pop(&q, 0).is_empty());
    push(&q, vec![]);
    assert_eq!(q.pop(), Some(42));
    assert_eq!(q.pop(), None);
}

#[test]
fn scalar_fifo_across_empty_refill_cycles() {
    if isolated("scalar_fifo_across_empty_refill_cycles") {
        return;
    }
    for (slots, blocks) in [(1, 1), (2, 3), (7, 2), (32, 4)] {
        let q = queue(slots, blocks);
        for round in 0..16 {
            for i in 0..slots * blocks + 1 {
                q.push((round, i));
            }
            for i in 0..slots * blocks + 1 {
                assert_eq!(q.pop(), Some((round, i)));
            }
            assert_eq!(q.pop(), None);
        }
    }
}

#[test]
fn batches_cross_virtual_and_physical_boundaries() {
    if isolated("batches_cross_virtual_and_physical_boundaries") {
        return;
    }
    for (slots, blocks) in [(1, 1), (2, 3), (7, 2), (32, 4)] {
        let capacity = slots * blocks;
        let sizes = [
            0,
            1,
            slots - 1,
            slots,
            slots + 1,
            capacity - 1,
            capacity,
            capacity + 1,
            capacity * 3 + 1,
        ];
        for prefix in [0, 1, slots - 1, capacity - 1] {
            for count in sizes {
                let q = queue(slots, blocks);
                push(&q, (0..prefix).collect());
                assert_eq!(pop(&q, prefix + 1), (0..prefix).collect::<Vec<_>>());
                push(&q, (prefix..prefix + count).collect());
                let mut next = prefix;
                for max in sizes.into_iter().cycle().take(count + 12) {
                    let actual = pop(&q, max);
                    // With quiescent callers, up to means min(request, available).
                    let end = (next + max).min(prefix + count);
                    assert_eq!(
                        actual,
                        (next..end).collect::<Vec<_>>(),
                        "geometry={slots}x{blocks}, prefix={prefix}, count={count}, max={max}"
                    );
                    next = end;
                    if next == prefix + count {
                        break;
                    }
                }
                assert_eq!(next, prefix + count);
                assert_eq!(q.pop(), None);
            }
        }
    }
}

#[test]
fn seeded_operations_match_vecdeque() {
    if isolated("seeded_operations_match_vecdeque") {
        return;
    }
    for (slots, blocks) in [(1, 1), (3, 2), (7, 3)] {
        for seed in [1_u64, 0x5eed, 0xdead_beef] {
            let q = queue(slots, blocks);
            let mut model = VecDeque::new();
            let mut state = seed;
            let mut id = 0;
            for step in 0..2000 {
                state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
                let count = ((state >> 32) as usize) % (slots * blocks * 3 + 2);
                match (state >> 16) % 5 {
                    0 => {
                        q.push(id);
                        model.push_back(id);
                        id += 1;
                    }
                    1 => {
                        let values: Vec<_> = (id..id + count).collect();
                        model.extend(values.iter().copied());
                        push(&q, values);
                        id += count;
                    }
                    2 => assert_eq!(q.pop(), model.pop_front(), "seed={seed}, step={step}"),
                    3 => {
                        let expected: Vec<_> = model.drain(..count.min(model.len())).collect();
                        assert_eq!(pop(&q, count), expected, "seed={seed}, step={step}");
                    }
                    _ => {
                        q.flush_freelist();
                    }
                }
            }
            assert_eq!(
                pop(&q, model.len() + 1),
                model.into_iter().collect::<Vec<_>>()
            );
            assert_eq!(q.pop(), None);
        }
    }
}

struct Owned {
    id: usize,
    drops: Arc<Vec<AtomicUsize>>,
}

impl Drop for Owned {
    fn drop(&mut self) {
        self.drops[self.id].fetch_add(1, Relaxed);
    }
}

fn counters(count: usize) -> Arc<Vec<AtomicUsize>> {
    Arc::new((0..count).map(|_| AtomicUsize::new(0)).collect())
}

#[test]
fn partial_drain_and_teardown_drop_each_owned_value_once() {
    if isolated("partial_drain_and_teardown_drop_each_owned_value_once") {
        return;
    }
    for take in [0, 1, 3, 6, 7, 41] {
        let drops = counters(41);
        let q = queue(3, 2);
        push(
            &q,
            (0..41)
                .map(|id| Owned {
                    id,
                    drops: drops.clone(),
                })
                .collect(),
        );
        let values = pop(&q, take);
        assert_eq!(
            values.iter().map(|v| v.id).collect::<Vec<_>>(),
            (0..take).collect::<Vec<_>>()
        );
        q.flush_freelist();
        assert!(drops.iter().all(|n| n.load(Relaxed) == 0));
        drop(q);
        for (id, n) in drops.iter().enumerate() {
            assert_eq!(
                n.load(Relaxed),
                usize::from(id >= take),
                "id={id}, take={take}"
            );
        }
        drop(values);
        assert!(drops.iter().all(|n| n.load(Relaxed) == 1));
    }
}

#[test]
fn zero_sized_overaligned_and_send_only_values() {
    if isolated("zero_sized_overaligned_and_send_only_values") {
        return;
    }
    static DROPS: AtomicUsize = AtomicUsize::new(0);
    struct Zst;
    impl Drop for Zst {
        fn drop(&mut self) {
            DROPS.fetch_add(1, Relaxed);
        }
    }
    DROPS.store(0, Relaxed);
    let q = queue(3, 2);
    push(&q, (0..37).map(|_| Zst).collect());
    drop(pop(&q, 13));
    assert_eq!(DROPS.load(Relaxed), 13);
    drop(q);
    assert_eq!(DROPS.load(Relaxed), 37);

    #[repr(align(8192))]
    #[derive(Debug, PartialEq)]
    struct Aligned(usize);
    let q = queue(2, 3);
    push(&q, (0..17).map(Aligned).collect());
    assert_eq!(pop(&q, 20), (0..17).map(Aligned).collect::<Vec<_>>());

    let q = queue(2, 2);
    thread::scope(|s| {
        s.spawn(|| q.push(Cell::new(42))).join().unwrap();
    });
    assert_eq!(q.pop().unwrap().get(), 42);
}

#[test]
fn flushing_preserves_live_values_and_allows_refill() {
    if isolated("flushing_preserves_live_values_and_allows_refill") {
        return;
    }
    let q = queue(2, 3);
    for round in 0..20 {
        push(&q, (round * 100..round * 100 + 41).collect());
        assert_eq!(
            pop(&q, 7),
            (round * 100..round * 100 + 7).collect::<Vec<_>>()
        );
        q.flush_freelist();
        assert_eq!(q.flush_freelist(), 0);
        assert_eq!(
            pop(&q, 50),
            (round * 100 + 7..round * 100 + 41).collect::<Vec<_>>()
        );
        assert_eq!(q.pop(), None);
    }
}

#[test]
fn concurrent_transfers_deliver_and_drop_every_value_once() {
    if isolated("concurrent_transfers_deliver_and_drop_every_value_once") {
        return;
    }
    const ITEMS: usize = 257;
    for (producers, consumers) in [(1, 1), (4, 1), (1, 4), (4, 4)] {
        for (slots, blocks) in [(1, 1), (3, 2), (32, 4)] {
            let total = producers * ITEMS;
            let q = queue(slots, blocks);
            let drops = counters(total);
            let seen = counters(total);
            let consumed = AtomicUsize::new(0);
            let barrier = Barrier::new(producers + consumers + 1);
            thread::scope(|scope| {
                for producer in 0..producers {
                    let (q, drops, barrier) = (&q, &drops, &barrier);
                    scope.spawn(move || {
                        barrier.wait();
                        for start in (0..ITEMS).step_by(7) {
                            let values = (start..(start + 7).min(ITEMS))
                                .map(|i| Owned {
                                    id: producer * ITEMS + i,
                                    drops: drops.clone(),
                                })
                                .collect::<Vec<_>>();
                            if (start + producer) % 2 == 0 {
                                push(q, values);
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
                                pop(q, 11)
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
                                    "duplicate id {}",
                                    value.id
                                );
                                consumed.fetch_add(1, Relaxed);
                            }
                        }
                    });
                }
                barrier.wait();
                while consumed.load(Relaxed) < total {
                    q.flush_freelist();
                    thread::yield_now();
                }
            });
            assert_eq!(consumed.load(Relaxed), total);
            assert!(q.pop().is_none());
            drop(q);
            assert!(seen.iter().all(|n| n.load(Relaxed) == 1));
            assert!(drops.iter().all(|n| n.load(Relaxed) == 1));
        }
    }
}

#[test]
fn concurrent_producers_preserve_per_producer_fifo() {
    if isolated("concurrent_producers_preserve_per_producer_fifo") {
        return;
    }
    const PRODUCERS: usize = 4;
    const ITEMS: usize = 300;
    let q = queue(3, 2);
    let barrier = Barrier::new(PRODUCERS + 1);
    thread::scope(|scope| {
        for producer in 0..PRODUCERS {
            let (q, barrier) = (&q, &barrier);
            scope.spawn(move || {
                barrier.wait();
                for start in (0..ITEMS).step_by(10) {
                    push(q, (start..start + 10).map(|i| (producer, i)).collect());
                    thread::yield_now();
                }
            });
        }
        barrier.wait();
        let mut next = [0; PRODUCERS];
        while next.iter().sum::<usize>() < PRODUCERS * ITEMS {
            for (producer, index) in pop(&q, 7) {
                assert!(producer < PRODUCERS);
                assert_eq!(index, next[producer]);
                next[producer] += 1;
            }
            thread::yield_now();
        }
        assert_eq!(next, [ITEMS; PRODUCERS]);
    });
    assert_eq!(q.pop(), None);
}
