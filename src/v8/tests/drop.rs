//! Settled-state fixtures exercise teardown independently of broken reservations.
use super::*;

struct Value(usize, Arc<Vec<AtomicUsize>>);

impl Drop for Value {
    fn drop(&mut self) {
        self.1[self.0].fetch_add(1, Relaxed);
    }
}

fn counters(count: usize) -> Arc<Vec<AtomicUsize>> {
    Arc::new((0..count).map(|_| AtomicUsize::new(0)).collect())
}

#[test]
fn drop_preserves_popped_values_and_drops_the_live_suffix() {
    let drops = counters(7);
    let q = UBQ::<Value>::with_config(Config {
        slots_per_vblock: 2,
        vblocks_per_block: 2,
    })
    .unwrap();
    let first = q.consumer.block.load(Relaxed);
    let extra = q.freelist.acquire(2, q.geometry).into_raw();
    let mut popped = Vec::new();

    // SAFETY: the fixture exclusively owns the queue. Initialize a full first
    // block, a partial second block, and an empty preallocated successor.
    // Then move the consumed prefix out and record its settled counters.
    unsafe {
        (*first).next.store(extra, Relaxed);
        for id in 0..7 {
            let (block, slot) = if id < 4 { (first, id) } else { (extra, id - 4) };
            (*block)
                .slots(slot / 2, slot % 2)
                .write(Value(id, drops.clone()));
            (*block).vblock(slot / 2).produced.fetch_add(1, Relaxed);
        }
        for slot in 0..3 {
            popped.push((*first).slots(slot / 2, slot % 2).read());
            (*first).vblock(slot / 2).consumed.fetch_add(1, Relaxed);
        }
        (*first).consumed_vblocks.store(1, Relaxed);
    }
    q.consumer.index.store(3 << 1, Relaxed);
    q.producer.block.store(extra, Relaxed);
    q.producer.index.store((6 + 3) << 1, Relaxed);

    drop(q);
    for (id, n) in drops.iter().enumerate() {
        assert_eq!(n.load(Relaxed), usize::from(id >= 3), "id={id}");
    }
    drop(popped);
    assert!(drops.iter().all(|n| n.load(Relaxed) == 1));
    assert_eq!(Arc::strong_count(&drops), 1);
}

#[test]
fn drop_starts_at_advanced_consumer_and_leaves_cached_blocks_to_freelist() {
    let drops = counters(3);
    let q = UBQ::<Value>::with_config(Config {
        slots_per_vblock: 2,
        vblocks_per_block: 1,
    })
    .unwrap();
    let old = q.consumer.block.load(Relaxed);
    let next = OwnedBlock::<Value>::new(q.geometry).into_raw();

    // SAFETY: exclusively prepare an advanced head and a fully consumed cached
    // predecessor. No cached allocation is part of the remaining active chain.
    unsafe {
        for id in 0..2 {
            (*old).slots(0, id).write(Value(id, drops.clone()));
        }
        (*old).vblock(0).produced.store(2, Relaxed);
        for id in 0..2 {
            drop((*old).slots(0, id).read());
        }
        (*old).vblock(0).consumed.store(2, Relaxed);
        (*old).consumed_vblocks.store(1, Relaxed);

        (*next).slots(0, 0).write(Value(2, drops.clone()));
        (*next).vblock(0).produced.store(1, Relaxed);
        q.consumer.block.store(next, Relaxed);
        q.consumer.index.store(4 << 1, Relaxed);
        q.producer.block.store(next, Relaxed);
        q.producer.index.store(5 << 1, Relaxed);
        q.freelist.recycle(OwnedBlock::from_raw(old));
    }

    drop(q);
    assert!(drops.iter().all(|n| n.load(Relaxed) == 1));
    assert_eq!(Arc::strong_count(&drops), 1);
}
