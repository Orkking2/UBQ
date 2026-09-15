//! Acceptance workloads for the bitmask queue. Concurrent cases run in child
//! processes with a 20-second deadline, so a hang is a failure, not a stall.
use std::{
    cell::Cell,
    collections::VecDeque,
    sync::{
        Arc, Barrier,
        atomic::{AtomicUsize, Ordering::Relaxed},
    },
    thread,
};
use ubq::{
    backoff::{BackoffPolicy, Crossbeam, Yield},
    bitmask::{Config, SLOTS, UBQ},
};

#[path = "support/isolated.rs"]
mod isolated;

fn queue<T, B: BackoffPolicy>(vblocks: usize) -> UBQ<T, B> {
    UBQ::with_config(Config {
        slots_per_vblock: SLOTS,
        vblocks_per_block: vblocks,
    })
    .unwrap()
}

fn pop<T, B: BackoffPolicy>(q: &UBQ<T, B>, max: usize) -> Vec<T> {
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

fn push<T, B: BackoffPolicy>(q: &UBQ<T, B>, values: Vec<T>) {
    let pointer = values.as_ptr();
    let capacity = values.capacity();
    let empty = q.push_batch(values);
    assert!(empty.is_empty(), "push did not move every input value");
    assert_eq!(empty.capacity(), capacity, "input capacity changed");
    assert_eq!(empty.as_ptr(), pointer, "input buffer was not returned");
}

#[test]
fn empty_and_zero_capacity_operations() {
    let q = queue::<usize, Crossbeam>(2);
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
    for vblocks in [1, 2, 3, 8] {
        let q = queue::<_, Crossbeam>(vblocks);
        let capacity = SLOTS * vblocks;
        for round in 0..16 {
            for i in 0..capacity + 1 {
                q.push((round, i));
            }
            for i in 0..capacity + 1 {
                assert_eq!(q.pop(), Some((round, i)));
            }
            assert_eq!(q.pop(), None);
        }
    }
}

#[test]
fn batches_cross_virtual_and_physical_boundaries() {
    for vblocks in [1, 2, 3] {
        let capacity = SLOTS * vblocks;
        let sizes = [
            0,
            1,
            SLOTS - 1,
            SLOTS,
            SLOTS + 1,
            capacity - 1,
            capacity,
            capacity + 1,
            capacity * 3 + 1,
        ];
        for prefix in [0, 1, SLOTS - 1, capacity - 1] {
            for count in sizes {
                let q = queue::<_, Crossbeam>(vblocks);
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
                        "vblocks={vblocks}, prefix={prefix}, count={count}, max={max}"
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
    for vblocks in [1, 2, 3] {
        let capacity = SLOTS * vblocks;
        for seed in [1_u64, 0x5eed, 0xdead_beef] {
            let q = queue::<_, Crossbeam>(vblocks);
            let mut model = VecDeque::new();
            let mut state = seed;
            let mut id = 0;
            for step in 0..2000 {
                state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
                let count = ((state >> 32) as usize) % (capacity * 3 + 2);
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
    for take in [0, 1, 64, 65, 128, 299, 300] {
        let drops = counters(300);
        let q = queue::<_, Crossbeam>(2);
        push(
            &q,
            (0..300)
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
    static DROPS: AtomicUsize = AtomicUsize::new(0);
    struct Zst;
    impl Drop for Zst {
        fn drop(&mut self) {
            DROPS.fetch_add(1, Relaxed);
        }
    }
    DROPS.store(0, Relaxed);
    let q = queue::<_, Crossbeam>(2);
    push(&q, (0..200).map(|_| Zst).collect());
    drop(pop(&q, 70));
    assert_eq!(DROPS.load(Relaxed), 70);
    drop(q);
    assert_eq!(DROPS.load(Relaxed), 200);

    #[repr(align(8192))]
    #[derive(Debug, PartialEq)]
    struct Aligned(usize);
    let q = queue::<_, Crossbeam>(2);
    push(&q, (0..130).map(Aligned).collect());
    assert_eq!(pop(&q, 140), (0..130).map(Aligned).collect::<Vec<_>>());

    let q = queue::<_, Crossbeam>(1);
    thread::scope(|s| {
        s.spawn(|| q.push(Cell::new(42))).join().unwrap();
    });
    assert_eq!(q.pop().unwrap().get(), 42);
}

#[test]
fn flushing_preserves_live_values_and_allows_refill() {
    let q = queue::<_, Crossbeam>(3);
    for round in 0..20 {
        push(&q, (round * 1000..round * 1000 + 250).collect());
        assert_eq!(
            pop(&q, 7),
            (round * 1000..round * 1000 + 7).collect::<Vec<_>>()
        );
        q.flush_freelist();
        assert_eq!(q.flush_freelist(), 0);
        assert_eq!(
            pop(&q, 300),
            (round * 1000 + 7..round * 1000 + 250).collect::<Vec<_>>()
        );
        assert_eq!(q.pop(), None);
    }
}

#[test]
fn concurrent_transfers_are_exact_once() {
    if isolated::run("concurrent_transfers_are_exact_once") {
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
        for vblocks in [1, 2, 3, 32] {
            let q = queue::<Owned, B>(vblocks);
            // Repeated drains exercise stale heads and block reuse.
            for _ in 0..4 {
                let total = producers * ITEMS;
                let drops = counters(total);
                let seen = counters(total);
                let consumed = AtomicUsize::new(0);
                let barrier = Barrier::new(producers + consumers);
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
                });
                assert_eq!(consumed.load(Relaxed), total);
                assert!(q.pop().is_none());
                assert!(seen.iter().all(|n| n.load(Relaxed) == 1));
                assert!(drops.iter().all(|n| n.load(Relaxed) == 1));
            }
        }
    }
}

#[test]
fn concurrent_producers_preserve_per_producer_fifo() {
    if isolated::run("concurrent_producers_preserve_per_producer_fifo") {
        return;
    }
    const PRODUCERS: usize = 4;
    const ITEMS: usize = 300;
    let q = queue::<_, Crossbeam>(2);
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
