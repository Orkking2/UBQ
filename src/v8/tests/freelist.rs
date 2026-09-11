use super::*;
use crate::v8::Config;

fn geometry() -> Geometry {
    Geometry::new::<usize>(Config {
        slots_per_vblock: 2,
        vblocks_per_block: 2,
    })
    .unwrap()
}

#[test]
fn partial_take_restores_suffix_and_reuses_allocations() {
    let cache = FreeList::new();
    let block = OwnedBlock::<usize>::new(geometry());
    let pointer = block.as_ptr();
    cache.recycle(block);
    let mut taken = cache.take(1);
    assert_eq!(taken.pop().unwrap().as_ptr(), pointer);
    assert_eq!(cache.flush(), 0);
    for _ in 0..5 {
        cache.recycle(OwnedBlock::new(geometry()));
    }
    let mut taken = cache.take(2);
    assert!(taken.pop().is_some());
    assert!(taken.pop().is_some());
    assert!(taken.pop().is_none());
    assert_eq!(cache.flush(), 3);
    assert_eq!(cache.flush(), 0);
}

#[test]
fn busy_cache_misses_without_waiting_and_returns_free_immediately() {
    let cache = FreeList::<usize>::new();
    cache.head.store(FreeList::busy(), Relaxed);
    assert!(cache.take(1).pop().is_none());
    assert_eq!(cache.flush(), 0);
    cache.recycle(OwnedBlock::new(geometry()));
    assert_eq!(cache.head.load(Relaxed), FreeList::busy());
    let mut allocated = cache.acquire(2, geometry());
    assert!(allocated.pop().is_some());
    assert!(allocated.pop().is_some());
    assert!(allocated.pop().is_none());
    cache.head.store(ptr::null_mut(), Relaxed);
}
