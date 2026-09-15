use super::*;
use crate::bitmask::{Config, SLOTS};

fn geometry() -> Geometry {
    Geometry::new::<usize>(Config {
        slots_per_vblock: SLOTS,
        vblocks_per_block: 2,
    })
    .unwrap()
}

#[test]
fn take_all_detaches_the_whole_chain_and_leaves_the_cache_empty() {
    let cache = FreeList::new();
    let block = OwnedBlock::<usize>::new(geometry());
    let pointer = block.as_ptr();
    cache.recycle(block);
    for _ in 0..4 {
        cache.recycle(OwnedBlock::new(geometry()));
    }
    let mut taken = cache.take_all();
    assert_eq!(taken.len(), 5);
    assert!(cache.head.load(Relaxed).is_null());
    assert_eq!(cache.flush(), 0);
    // Returns push at the head, so the first recycled block ends the chain.
    let mut last = None;
    while let Some(block) = taken.pop() {
        last = Some(block.as_ptr());
        drop(block);
    }
    assert_eq!(last, Some(pointer));
}

#[test]
fn giving_a_chain_keeps_earlier_returns_and_frees_nothing() {
    let cache = FreeList::<usize>::new();
    cache.recycle(OwnedBlock::new(geometry()));
    let mut list = BlockList::new();
    for _ in 0..3 {
        list.push(OwnedBlock::new(geometry()));
    }
    cache.give(list);
    cache.give(BlockList::new());
    assert_eq!(cache.take_all().len(), 4);
    assert_eq!(cache.flush(), 0);
}
