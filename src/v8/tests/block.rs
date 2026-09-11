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
        slots_per_vblock: 1,
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
                        (*block.as_ptr()).vblock(0).produced.store(1, Relaxed);
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
