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

fn config(vblocks: usize) -> Config {
    Config {
        slots_per_vblock: SLOTS,
        vblocks_per_block: vblocks,
    }
}

#[test]
fn drop_preserves_popped_values_and_drops_the_live_suffix() {
    // A full first block (128), three values in the second, an empty successor.
    let drops = counters(131);
    let q = UBQ::<Value>::with_config(config(2)).unwrap();
    let first = q.consumer.block.load(Relaxed);
    let mut chain = BlockList::new();
    chain.push(OwnedBlock::new(q.geometry));
    chain.push(OwnedBlock::new(q.geometry));
    let extra = chain.into_raw();
    let mut popped = Vec::new();

    // SAFETY: the fixture exclusively owns the queue. Then move the consumed
    // prefix (the whole first VBlock and 6 of the second) out and record it.
    unsafe {
        (*first).next.store(extra, Relaxed);
        for id in 0..131 {
            let (block, slot) = if id < 128 { (first, id) } else { (extra, id - 128) };
            (*block)
                .slots(slot / SLOTS, slot % SLOTS)
                .write(Value(id, drops.clone()));
            (*block)
                .vblock(slot / SLOTS)
                .produced
                .fetch_or(mask(slot % SLOTS, 1), Relaxed);
        }
        for slot in 0..70 {
            popped.push((*first).slots(slot / SLOTS, slot % SLOTS).read());
            (*first)
                .vblock(slot / SLOTS)
                .consumed
                .fetch_or(mask(slot % SLOTS, 1), Relaxed);
        }
        (*first).consumed_vblocks.store(1, Relaxed);
        q.producer.tail.store((*extra).next.load(Relaxed), Relaxed);
    }
    q.consumer.index.store(70 << 1, Relaxed);
    q.producer.block.store(extra, Relaxed);
    q.producer.attached.store(1, Relaxed);
    // Stride 3 x 64: token 1, offset 3, one block attached after it.
    q.producer.index.store(((3 * SLOTS + 3) << COUNT_BITS) | 1, Relaxed);

    drop(q);
    for (id, n) in drops.iter().enumerate() {
        assert_eq!(n.load(Relaxed), usize::from(id >= 70), "id={id}");
    }
    drop(popped);
    assert!(drops.iter().all(|n| n.load(Relaxed) == 1));
    assert_eq!(Arc::strong_count(&drops), 1);
}

#[test]
fn drop_starts_at_advanced_consumer_and_leaves_cached_blocks_to_freelist() {
    let drops = counters(65);
    let q = UBQ::<Value>::with_config(config(1)).unwrap();
    let old = q.consumer.block.load(Relaxed);
    let next = OwnedBlock::<Value>::new(q.geometry).into_raw();

    // SAFETY: exclusively prepare an advanced head and a fully consumed cached
    // predecessor. No cached allocation is part of the remaining active chain.
    unsafe {
        for id in 0..SLOTS {
            (*old).slots(0, id).write(Value(id, drops.clone()));
        }
        (*old).vblock(0).produced.store(u64::MAX, Relaxed);
        for id in 0..SLOTS {
            drop((*old).slots(0, id).read());
        }
        (*old).vblock(0).consumed.store(u64::MAX, Relaxed);
        (*old).consumed_vblocks.store(1, Relaxed);

        (*next).slots(0, 0).write(Value(64, drops.clone()));
        (*next).vblock(0).produced.store(mask(0, 1), Relaxed);
        q.consumer.block.store(next, Relaxed);
        q.consumer.index.store((2 * SLOTS) << 1, Relaxed);
        q.producer.block.store(next, Relaxed);
        q.producer.tail.store(next, Relaxed);
        q.producer.index.store((2 * SLOTS + 1) << COUNT_BITS, Relaxed);
        q.freelist.recycle(OwnedBlock::from_raw(old));
    }

    drop(q);
    assert!(drops.iter().all(|n| n.load(Relaxed) == 1));
    assert_eq!(Arc::strong_count(&drops), 1);
}
