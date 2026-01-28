#[path = "logger.rs"]
mod logger;

use logger::{init_trace_to_file, spawn_ubq_tracer};
use rand::{
    Rng,
    distributions::{Distribution, Uniform},
};
use ubq::{PopErr, PushErr, UBQ, UBQBounds};
use std::mem;

fn new_queue<T>(block_size: usize, min: usize, max: usize) -> UBQ<T> {
    UBQ::new(block_size, UBQBounds { min, max })
}

fn random_queue_params(rng: &mut impl Rng) -> (usize, usize, usize) {
    // Keep block sizes power-of-two to stay cache friendly and predictable.
    let block_size_pow = Uniform::from(3u32..=9u32).sample(rng);
    let block_size = 1usize << block_size_pow;

    // Keep bounds tight to avoid runaway allocations while still varying shapes.
    let min_blocks = Uniform::from(2usize..=8).sample(rng);
    let max_blocks = min_blocks + Uniform::from(0usize..=6).sample(rng);

    (block_size, min_blocks, max_blocks)
}

#[test]
fn fill_empty_ubq() {
    init_trace_to_file("fill_empty_ubq").unwrap();

    const RUNS: usize = 64;
    const MAX_TOTAL_BYTES: usize = 1 << 20; // cap at 1 MiB of i32 storage per run

    let mut rng = rand::thread_rng();

    for _ in 0..RUNS {
        let (block_size, min_blocks, max_blocks) = random_queue_params(&mut rng);
        let params = (block_size, min_blocks, max_blocks);

        let total_slots = block_size
            .checked_mul(max_blocks)
            .expect("generated params overflowed total slots");
        let total_bytes = total_slots
            .checked_mul(mem::size_of::<i32>())
            .expect("generated params overflowed total bytes");
        assert!(
            total_bytes <= MAX_TOTAL_BYTES,
            "generated params {:?} would allocate {} bytes",
            params,
            total_bytes
        );

        let mut queue = new_queue(block_size, min_blocks, max_blocks);

        for value in 0..total_slots as i32 {
            if let Err((returned, err)) = queue.push(value) {
                panic!(
                    "push of {} failed for params {:?} (returned {}, err={:?})",
                    value, params, returned, err
                );
            }
        }

        assert!(
            queue.allocated_blocks() <= max_blocks,
            "queue allocated {} blocks for params {:?}",
            queue.allocated_blocks(),
            params
        );

        for expected in 0..total_slots as i32 {
            assert_eq!(
                queue.pop().unwrap(),
                expected,
                "pop mismatch for params {:?}",
                params
            );
        }

        assert!(matches!(queue.pop(), Err(PopErr::Empty)));
    }
}

#[test]
fn push_pop_within_single_block() {
    init_trace_to_file("push_pop_within_single_block").unwrap();

    let mut q = new_queue(2, 2, 2);

    let kill_switch = spawn_ubq_tracer(q.clone());

    println!("created queue");

    for i in 0..3 {
        println!("pushing {i}");
        let res = q.push(i);
        assert!(res.is_ok(), "push {i} failed: {:?}", res);
    }

    for i in 0..3 {
        println!("popping {i}");
        assert_eq!(q.pop().unwrap(), i);
    }

    assert!(matches!(q.pop(), Err(PopErr::Empty)));

    drop(kill_switch);
}

#[test]
fn allocates_new_block_until_max_bound() {
    init_trace_to_file("allocates_new_block_until_max_bound").unwrap();

    let max = 3;
    let block_size = 3;

    let mut q = new_queue(block_size, 2, max);

    let kill_switch = spawn_ubq_tracer(q.clone());

    let iter_base = 0..(max * block_size);

    let results = iter_base.clone().map(|v| {
        println!("push {v}");
        (v, q.push(v))
    });
    for (value, res) in results {
        assert!(res.is_ok(), "push {value} failed: {:?}", res);
    }

    println!("pushing past last");
    assert_eq!(
        q.push(usize::MAX),
        Err((usize::MAX, PushErr::BlockAllocBoundsReached))
    );

    for expected in iter_base {
        println!("popping {expected}");
        assert_eq!(q.pop().unwrap(), expected);
    }

    println!("popping past last");
    assert_eq!(q.pop(), Err(PopErr::Empty));

    drop(kill_switch);
}
