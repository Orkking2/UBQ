use super::*;
use alloc::{collections::VecDeque, vec};
use core::{
    cell::Cell,
    mem::{align_of, size_of},
};

fn config(slots: usize, blocks: usize) -> Config {
    Config {
        slots_per_vblock: slots,
        vblocks_per_block: blocks,
    }
}

fn queue<T>(slots: usize, blocks: usize) -> UBQ<T> {
    UBQ::with_config(config(slots, blocks)).unwrap()
}

#[test]
fn invalid_geometry_is_rejected() {
    for cfg in [config(0, 1), config(1, 0)] {
        assert_eq!(
            UBQ::<u8>::with_config(cfg).unwrap_err(),
            ConfigError::ZeroCapacity
        );
    }
    for cfg in [
        config(usize::MAX, 1),
        config(1, usize::MAX),
        config(usize::MAX / 2, 2),
    ] {
        assert_eq!(
            UBQ::<u64>::with_config(cfg).unwrap_err(),
            ConfigError::LayoutOverflow
        );
        assert_eq!(
            UBQ::<()>::with_config(cfg).unwrap_err(),
            ConfigError::LayoutOverflow
        );
    }
}

fn check_geometry<T>() {
    for slots in [1, 2, 7, 32, 129] {
        for blocks in [1, 3, 8] {
            let q = queue::<T>(slots, blocks);
            assert_eq!(q.config(), config(slots, blocks));
            assert_eq!(q.block_capacity(), slots * blocks);
            assert!(q.vblock_alignment().is_power_of_two());
            assert!(q.vblock_alignment() > slots);
            assert!(q.block_allocation_align() >= align_of::<T>());
            let mut current = q.load_producer().block;
            let base = current.addr();
            let owner = unsafe { (*current).owner };
            for ordinal in 0..blocks {
                assert_eq!(current.addr(), base + ordinal * q.vblock_alignment());
                assert_eq!(current.addr() % q.vblock_alignment(), 0);
                assert_eq!(unsafe { (*current).owner }, owner);
                assert_eq!(
                    unsafe { q.geometry.remaining_capacity(current) },
                    (blocks - ordinal) * slots
                );
                for index in 0..=slots {
                    let decoded = Head::decode(
                        Head {
                            block: current,
                            index,
                        }
                        .encode(q.geometry),
                        q.geometry,
                    );
                    assert_eq!(decoded.block, current);
                    assert_eq!(decoded.index, index);
                    if index < slots {
                        let payload = unsafe { q.geometry.slots(current, index) };
                        assert_eq!(payload.addr() % align_of::<T>(), 0);
                        assert!(
                            payload.addr() + size_of::<T>()
                                <= current.addr() + q.vblock_alignment()
                        );
                    }
                }
                current = unsafe { (*current).next.load(Relaxed) };
            }
            assert!(current.is_null());
        }
    }
}

#[test]
fn geometry_recovers_vblocks_and_aligned_payloads() {
    #[repr(align(8192))]
    struct Aligned;
    check_geometry::<()>();
    check_geometry::<u8>();
    check_geometry::<u64>();
    check_geometry::<Aligned>();
}

#[test]
fn later_publication_does_not_expose_an_earlier_unwritten_slot() {
    for slots in [2, 4] {
        let q = queue::<usize>(slots, 2);
        let guard = q.pin();
        let earlier = q.reserve_push(1, &guard);
        q.push(22);
        // The publication total covers slot 0 numerically, but only slot 1
        // contains a value. A partial block must compare against reservations.
        assert_eq!(unsafe { (*earlier.block).produced.load(Acquire) }, 1);
        assert!(!q.is_published(earlier.block, 1));
        unsafe {
            q.geometry.slots(earlier.block, earlier.index).write(11);
            (*earlier.block).produced.fetch_add(1, AcqRel);
        }
        assert!(q.is_published(earlier.block, 2));
        assert_eq!(q.pop_batch(Box::new_uninit_slice(8)), [11, 22]);
        assert!(q.is_empty());
    }
}

#[test]
fn allocation_completion_waits_for_the_last_out_of_order_vblock() {
    let q = queue::<usize>(2, 3);
    q.push_batch((0..6).collect());
    let guard = q.pin();
    let (first, count) = q.reserve_pop(2, &guard);
    assert_eq!(count, 2);
    let owner = unsafe { (*first.block).owner };
    // Delay the earliest consumer while later consumers complete its siblings.
    assert_eq!(q.pop_batch(Box::new_uninit_slice(8)), [2, 3, 4, 5]);
    assert_eq!(unsafe { (*owner).consumed.load(Acquire) }, 2);
    for index in 0..2 {
        assert_eq!(
            unsafe { q.geometry.slots(first.block, index).read() },
            index
        );
    }
    unsafe { q.complete(first.block, 2, &guard) };
    // The final completion retires the main block, but our pinned snapshot
    // still makes its metadata safe to read until this guard leaves.
    assert_eq!(unsafe { (*owner).consumed.load(Acquire) }, 3);
    assert_eq!(unsafe { (*owner).size }, 3);
    assert!(q.is_empty());
}

#[test]
fn a_pinned_old_head_cannot_reserve_in_a_reused_allocation() {
    let q = queue::<usize>(1, 1);
    let guard = q.pin();
    let old = q.load_consumer();
    let owner = unsafe { (*old.block).owner };
    for value in 0..1000 {
        q.push(value);
        assert_eq!(q.pop(), Some(value));
    }
    assert!(
        q.consumer
            .compare_exchange(
                old.encode(q.geometry),
                Head { index: 1, ..old }.encode(q.geometry),
                AcqRel,
                Acquire,
            )
            .is_err()
    );
    assert_eq!(unsafe { (*owner).consumed.load(Acquire) }, 1);
    drop(guard);
}

#[test]
fn mixed_batches_match_vecdeque_across_refills() {
    for slots in [1, 2, 7, 32] {
        for blocks in [1, 3, 8] {
            let q = queue(slots, blocks);
            let mut model = VecDeque::new();
            let mut state = 19_u64;
            for step in 0..2000 {
                state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
                let count = ((state >> 32) % 91) as usize;
                match state % 4 {
                    0 => {
                        q.push(step);
                        model.push_back(step);
                    }
                    1 => {
                        let input: Vec<_> = (0..count).map(|offset| step + offset).collect();
                        model.extend(input.iter().copied());
                        let pointer = input.as_ptr();
                        let capacity = input.capacity();
                        let returned = q.push_batch(input);
                        assert!(returned.is_empty());
                        assert_eq!(returned.as_ptr(), pointer);
                        assert_eq!(returned.capacity(), capacity);
                    }
                    2 => {
                        assert_eq!(q.pop(), model.pop_front());
                    }
                    _ => {
                        let output = Box::new_uninit_slice(count);
                        let pointer = output.as_ptr().cast::<usize>();
                        let actual = q.pop_batch(output);
                        assert_eq!(actual.as_ptr(), pointer);
                        let expected: Vec<_> = model.drain(..count.min(model.len())).collect();
                        assert_eq!(actual, expected);
                    }
                }
                assert_eq!(q.is_empty(), model.is_empty());
            }
            assert_eq!(
                q.pop_batch(Box::new_uninit_slice(model.len() + 1)),
                model.into_iter().collect::<Vec<_>>()
            );
            assert_eq!(q.pop(), None);
        }
    }
}

#[test]
fn teardown_drops_each_remaining_value_once() {
    struct Value(usize, Arc<Vec<AtomicUsize>>);
    impl Drop for Value {
        fn drop(&mut self) {
            self.1[self.0].fetch_add(1, Relaxed);
        }
    }
    for count in [0, 1, 5, 6, 7, 42, 101] {
        for drained in [0, count / 2, count] {
            let counters = Arc::new((0..count).map(|_| AtomicUsize::new(0)).collect::<Vec<_>>());
            let q = queue(3, 2);
            q.push_batch((0..count).map(|id| Value(id, counters.clone())).collect());
            drop(q.pop_batch(Box::new_uninit_slice(drained)));
            drop(q);
            assert!(counters.iter().all(|n| n.load(Relaxed) == 1));
            assert_eq!(Arc::strong_count(&counters), 1);
        }
    }
}

#[test]
fn zero_sized_overaligned_and_send_only_values_work() {
    static DROPS: AtomicUsize = AtomicUsize::new(0);
    #[repr(align(8192))]
    struct AlignedZst;
    impl Drop for AlignedZst {
        fn drop(&mut self) {
            DROPS.fetch_add(1, Relaxed);
        }
    }
    let q = queue(3, 2);
    q.push_batch((0..41).map(|_| AlignedZst).collect());
    let mut received = q.pop_batch(Box::new_uninit_slice(17));
    assert_eq!(received.len(), 17);
    received.clear();
    let buffer = empty_vec_to_box_uninit(received).ok().unwrap();
    assert_eq!(q.pop_batch(buffer).len(), 24);
    drop(q);
    assert_eq!(DROPS.load(Relaxed), 41);

    #[repr(align(4096))]
    struct AlignedValue(usize);
    let q = queue(2, 3);
    q.push_batch((0..17).map(AlignedValue).collect());
    assert_eq!(
        q.pop_batch(Box::new_uninit_slice(30))
            .iter()
            .map(|v| v.0)
            .collect::<Vec<_>>(),
        (0..17).collect::<Vec<_>>()
    );

    let q = queue(2, 3);
    std::thread::scope(|s| {
        s.spawn(|| q.push(Cell::new(42))).join().unwrap();
    });
    assert_eq!(q.pop().unwrap().get(), 42);
    assert_eq!(empty_vec_to_box_uninit(vec![1]).unwrap_err(), [1]);
}
