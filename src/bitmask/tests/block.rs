use super::*;
use alloc::sync::Arc;

#[test]
fn adding_lists_transfers_ownership_and_preserves_both_chains() {
    struct Value(Arc<AtomicUsize>);
    impl Drop for Value {
        fn drop(&mut self) {
            self.0.fetch_add(1, Relaxed);
        }
    }
    let geometry = Geometry::new::<Value>(Config {
        slots_per_vblock: SLOTS,
        vblocks_per_block: 1,
    })
    .unwrap();

    for left in 0..3 {
        for right in 0..3 {
            let drops = Arc::new(AtomicUsize::new(0));
            let make = |count| {
                let mut list = BlockList::new();
                for _ in 0..count {
                    let block = OwnedBlock::<Value>::new(geometry);
                    // SAFETY: exclusively initialize the one owned slot.
                    unsafe {
                        (*block.as_ptr()).slots(0, 0).write(Value(drops.clone()));
                        (*block.as_ptr()).vblock(0).produced.store(mask(0, 1), Relaxed);
                    }
                    list.push(block);
                }
                list
            };
            let mut list = make(left);
            let rhs = make(right);
            let left_head = list.head;
            let right_head = rhs.head;
            let tail = if left == 0 { rhs.tail } else { list.tail };
            list += rhs;
            assert_eq!(list.len(), left + right);
            assert_eq!(list.tail, tail);
            assert_eq!(drops.load(Relaxed), 0, "concatenation destroyed values");
            assert_eq!(list.head, if right == 0 { left_head } else { right_head });
            for remaining in (0..left + right).rev() {
                drop(list.pop().unwrap());
                assert_eq!(list.len(), remaining);
            }
            assert!(list.pop().is_none());
            assert!(list.tail.is_null());
            assert_eq!(drops.load(Relaxed), left + right);
            assert_eq!(Arc::strong_count(&drops), 1);
        }
    }
}

#[test]
fn masks_cover_exactly_the_requested_slots() {
    assert_eq!(mask(0, 0), 0);
    assert_eq!(mask(0, SLOTS), u64::MAX);
    assert_eq!(mask(63, 1), 1 << 63);
    assert_eq!(mask(3, 5), 0b11111 << 3);
    assert_eq!(mask(60, 4), 0xF << 60);
    for offset in 0..SLOTS {
        for count in 0..=SLOTS - offset {
            assert_eq!(mask(offset, count).count_ones() as usize, count);
        }
    }
}
