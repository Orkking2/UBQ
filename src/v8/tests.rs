use super::{Config, ConfigError, UBQ};
use alloc::{sync::Arc, vec::Vec};
use core::sync::atomic::{AtomicUsize, Ordering::Relaxed};

fn config(slots: usize, blocks: usize) -> Config {
    Config {
        slots_per_vblock: slots,
        vblocks_per_block: blocks,
    }
}

#[test]
fn invalid_geometry_is_rejected_before_allocating() {
    for cfg in [config(0, 1), config(1, 0)] {
        assert!(matches!(
            UBQ::<u8>::with_config(cfg),
            Err(ConfigError::ZeroCapacity)
        ));
    }
    for cfg in [
        config(usize::MAX, 2),
        config(usize::MAX >> 1, 1),
        config(1, usize::MAX),
    ] {
        assert!(matches!(
            UBQ::<u64>::with_config(cfg),
            Err(ConfigError::LayoutOverflow)
        ));
    }
}

#[test]
fn runtime_geometry_reports_capacity_and_layout() {
    for slots in [1, 2, 7, 32] {
        for blocks in [1, 3, 8] {
            let cfg = config(slots, blocks);
            let q = UBQ::<usize>::with_config(cfg).unwrap();
            assert_eq!(q.config(), cfg);
            assert_eq!(q.block_capacity(), slots * blocks);
            assert!(q.block_allocation_size() >= slots * blocks * size_of::<usize>());
            assert!(q.block_allocation_align() >= align_of::<usize>());
        }
    }
    #[repr(align(8192))]
    struct Aligned;
    let q = UBQ::<Aligned>::with_config(config(2, 3)).unwrap();
    assert!(q.block_allocation_align() >= align_of::<Aligned>());
}

#[test]
fn drop_releases_every_queued_value_without_popping() {
    struct Value(usize, Arc<Vec<AtomicUsize>>);
    impl Drop for Value {
        fn drop(&mut self) {
            self.1[self.0].fetch_add(1, Relaxed);
        }
    }

    // Cover partial, exact-full, and multi-block batches.
    for count in [0, 1, 5, 6, 7, 12, 41, 42] {
        let drops = Arc::new((0..count).map(|_| AtomicUsize::new(0)).collect::<Vec<_>>());
        let q = UBQ::<Value>::with_config(config(3, 2)).unwrap();
        if count != 0 {
            let returned = q.push_batch((0..count).map(|id| Value(id, drops.clone())).collect());
            assert!(returned.is_empty());
        }
        assert!(drops.iter().all(|n| n.load(Relaxed) == 0));
        drop(q);
        assert!(drops.iter().all(|n| n.load(Relaxed) == 1), "count={count}");
        assert_eq!(Arc::strong_count(&drops), 1);
    }
}
