use super::*;
use std::panic::{AssertUnwindSafe, catch_unwind};

struct Value(usize, Arc<Vec<AtomicUsize>>);

impl Drop for Value {
    fn drop(&mut self) {
        self.1[self.0].fetch_add(1, Relaxed);
    }
}

#[test]
fn unwinding_drops_only_the_untransferred_input_suffix() {
    let drops = Arc::new((0..3).map(|_| AtomicUsize::new(0)).collect::<Vec<_>>());
    let mut items = (0..3)
        .map(|id| Value(id, drops.clone()))
        .collect::<Vec<_>>();
    let mut transferred = None;
    let result = catch_unwind(AssertUnwindSafe(|| {
        let mut input = PushInput {
            src: items.as_mut_ptr(),
            len: items.len(),
            moved: 0,
        };
        // SAFETY: the guard owns the elements; the Vec retains their storage.
        // Simulate one successful move before a subsequent operation unwinds.
        unsafe {
            items.set_len(0);
            transferred = Some(input.src.read());
        }
        input.moved = 1;
        panic!("after moving the first value");
    }));
    assert!(result.is_err());
    assert_eq!(drops[0].load(Relaxed), 0);
    assert_eq!(drops[1].load(Relaxed), 1);
    assert_eq!(drops[2].load(Relaxed), 1);
    drop(transferred);
    drop(items);
    assert!(drops.iter().all(|n| n.load(Relaxed) == 1));
    assert_eq!(Arc::strong_count(&drops), 1);
}

#[test]
fn unwinding_drops_only_the_initialized_output_prefix() {
    let drops = Arc::new((0..3).map(|_| AtomicUsize::new(0)).collect::<Vec<_>>());
    let mut buf = Box::<[Value]>::new_uninit_slice(3);
    let result = catch_unwind(AssertUnwindSafe(|| {
        let mut output = PopOutput::<Value> {
            dst: buf.as_mut_ptr().cast(),
            len: 0,
        };
        for id in 0..2 {
            // SAFETY: initialize the next empty slot and give it to the guard.
            unsafe { output.dst.add(id).write(Value(id, drops.clone())) };
            output.len += 1;
        }
        panic!("before initializing the last slot");
    }));
    assert!(result.is_err());
    assert_eq!(drops[0].load(Relaxed), 1);
    assert_eq!(drops[1].load(Relaxed), 1);
    assert_eq!(drops[2].load(Relaxed), 0);
    drop(buf);
    assert_eq!(Arc::strong_count(&drops), 1);
}

#[test]
fn decoding_matches_the_sentinel_stride() {
    for nv in [1, 3, 8, 2048] {
        for position in (0..(nv + 1) * SLOTS * 3).step_by(7) {
            assert_eq!(
                decode(position, nv),
                (
                    position / ((nv + 1) * SLOTS),
                    (position / SLOTS) % (nv + 1),
                    position % SLOTS
                )
            );
        }
    }
}

#[test]
fn producer_index_carries_the_attached_count_below_the_position() {
    let q = UBQ::<u64>::with_config(Config {
        slots_per_vblock: SLOTS,
        vblocks_per_block: 1,
    })
    .unwrap();
    // Filling the first block exactly attaches one fresh block and lands on it.
    q.push_batch((0..64).collect());
    assert_eq!(q.producer.index.load(Relaxed) >> COUNT_BITS, 2 * SLOTS);
    assert_eq!(q.producer.index.load(Relaxed) & COUNT_MASK, 0);
    assert_eq!(q.producer.attached.load(Relaxed), 0);
    // Draining recycles the first block; the next boundary attaches it.
    assert_eq!(q.pop_batch(Box::new_uninit_slice(64)).len(), 64);
    q.push_batch((0..65).collect());
    assert_eq!(q.producer_position(Relaxed), 4 * SLOTS + 1);
    assert_eq!(q.producer.attached.load(Relaxed), 0);
    assert_eq!(q.pop_batch(Box::new_uninit_slice(65)).len(), 65);
    assert_eq!(q.flush_freelist(), 1);
}
