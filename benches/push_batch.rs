use std::{
    env,
    hint::black_box,
    time::{Duration, Instant},
};
use ubq::UBQ;

const DEFAULT_ITEMS: usize = 1_000_000;
const DEFAULT_SAMPLES: usize = 7;
const DEFAULT_BATCH_SIZES: &[usize] = &[2, 4, 8, 16, 32, 64, 256, 2_048];

fn main() {
    let items = env_usize("UBQ_BATCH_BENCH_ITEMS", DEFAULT_ITEMS);
    let samples = env_usize("UBQ_BATCH_BENCH_SAMPLES", DEFAULT_SAMPLES);

    assert!(items > 0, "UBQ_BATCH_BENCH_ITEMS must be greater than zero");
    assert!(
        samples > 0,
        "UBQ_BATCH_BENCH_SAMPLES must be greater than zero"
    );

    println!("items={items} samples={samples}");
    println!("batch\trepeated ns/item\tbatched ns/item\tspeedup");

    for &batch_size in DEFAULT_BATCH_SIZES {
        let mut repeated = Vec::with_capacity(samples);
        let mut batched = Vec::with_capacity(samples);

        // Alternate order to reduce systematic cache/frequency bias.
        for sample in 0..samples {
            if sample % 2 == 0 {
                repeated.push(measure_repeated(items));
                batched.push(measure_batched(items, batch_size));
            } else {
                batched.push(measure_batched(items, batch_size));
                repeated.push(measure_repeated(items));
            }
        }

        let repeated = median(repeated).as_nanos() as f64 / items as f64;
        let batched = median(batched).as_nanos() as f64 / items as f64;
        println!(
            "{batch_size}\t{repeated:.2}\t\t{batched:.2}\t\t{:.2}x",
            repeated / batched
        );
    }
}

fn measure_repeated(items: usize) -> Duration {
    let queue = UBQ::new();
    let start = Instant::now();

    for item in 0..items {
        queue.push(item);
    }

    let elapsed = start.elapsed();
    verify_and_drain(&queue, items);
    elapsed
}

fn measure_batched(items: usize, batch_size: usize) -> Duration {
    let queue = UBQ::new();
    let start = Instant::now();

    for first in (0..items).step_by(batch_size) {
        queue.push_batch(first..items.min(first + batch_size));
    }

    let elapsed = start.elapsed();
    verify_and_drain(&queue, items);
    elapsed
}

fn verify_and_drain(queue: &UBQ<usize>, items: usize) {
    for expected in 0..items {
        assert_eq!(black_box(queue.pop()), Some(expected));
    }
    assert_eq!(queue.pop(), None);
}

fn median(mut samples: Vec<Duration>) -> Duration {
    samples.sort_unstable();
    samples[samples.len() / 2]
}

fn env_usize(name: &str, default: usize) -> usize {
    env::var(name)
        .ok()
        .map(|value| {
            value
                .parse()
                .unwrap_or_else(|_| panic!("{name} must be a positive integer"))
        })
        .unwrap_or(default)
}
