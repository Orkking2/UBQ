use super::{Config, ConfigError, SLOTS, UBQ};
use alloc::{sync::Arc, vec::Vec};
use core::sync::atomic::{AtomicUsize, Ordering::Relaxed};

fn config(vblocks: usize) -> Config {
    Config {
        slots_per_vblock: SLOTS,
        vblocks_per_block: vblocks,
    }
}

#[test]
fn invalid_geometry_is_rejected_before_allocating() {
    assert!(matches!(
        UBQ::<u8>::with_config(config(0)),
        Err(ConfigError::ZeroCapacity)
    ));
    for slots in [0, 1, 63, 65, 4096] {
        assert!(matches!(
            UBQ::<u8>::with_config(Config {
                slots_per_vblock: slots,
                vblocks_per_block: 1,
            }),
            Err(ConfigError::VBlockSize)
        ));
    }
    for vblocks in [usize::MAX, usize::MAX / SLOTS, (usize::MAX >> 1) / SLOTS + 1] {
        assert!(matches!(
            UBQ::<u64>::with_config(config(vblocks)),
            Err(ConfigError::LayoutOverflow)
        ));
    }
}

#[test]
fn runtime_geometry_reports_capacity_and_layout() {
    for vblocks in [1, 3, 8, 2048] {
        let cfg = config(vblocks);
        let q = UBQ::<usize>::with_config(cfg).unwrap();
        assert_eq!(q.config(), cfg);
        assert_eq!(q.block_capacity(), SLOTS * vblocks);
        assert!(q.block_allocation_size() >= SLOTS * vblocks * size_of::<usize>());
        assert!(q.block_allocation_align() >= align_of::<usize>());
    }
    assert_eq!(UBQ::<usize>::new().config(), Config::default());
    #[repr(align(8192))]
    struct Aligned;
    let q = UBQ::<Aligned>::with_config(config(2)).unwrap();
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

    // Cover partial, exact-full, and multi-block batches of two 64-slot VBlocks.
    for count in [0, 1, 63, 64, 65, 127, 128, 129, 300, 384, 385] {
        let drops = Arc::new((0..count).map(|_| AtomicUsize::new(0)).collect::<Vec<_>>());
        let q = UBQ::<Value>::with_config(config(2)).unwrap();
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
