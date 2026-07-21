use std::{
    env,
    hint::black_box,
    sync::{Arc, Barrier},
    thread,
    time::{Duration, Instant},
};
use ubq::UBQ;

const DEFAULT_ITEMS: usize = 1_000_000;
const DEFAULT_SAMPLES: usize = 7;
const BATCH_SIZES: &[usize] = &[2, 4, 8, 16, 32, 64, 128, 256, 512, 1_024, 2_048];

#[derive(Clone, Copy)]
enum PushMode {
    Repeated,
    Batched(usize),
}

fn main() {
    // Cargo also executes harness-free benches during `cargo test --all-targets`.
    // Only benchmark runs receive the `--bench` marker.
    if cfg!(test) && !env::args().any(|argument| argument == "--bench") {
        return;
    }

    let items = env_usize("UBQ_BATCH_BENCH_ITEMS", DEFAULT_ITEMS);
    let samples = env_usize("UBQ_BATCH_BENCH_SAMPLES", DEFAULT_SAMPLES);
    let producers = env_usize(
        "UBQ_BATCH_BENCH_PRODUCERS",
        thread::available_parallelism()
            .map(|count| count.get().min(4))
            .unwrap_or(1),
    );

    assert!(items > 0, "UBQ_BATCH_BENCH_ITEMS must be greater than zero");
    assert!(
        samples > 0,
        "UBQ_BATCH_BENCH_SAMPLES must be greater than zero"
    );
    assert!(
        producers > 0,
        "UBQ_BATCH_BENCH_PRODUCERS must be greater than zero"
    );

    println!("items={items} samples={samples}");
    run_table("single producer, fill only", items, samples, 1);

    if producers > 1 {
        run_table(
            &format!("{producers} producers, contended fill"),
            items,
            samples,
            producers,
        );
    }
}

fn run_table(label: &str, items: usize, samples: usize, producers: usize) {
    println!("\n{label}");
    println!("batch\trepeated ns/item\tbatched ns/item\tspeedup");

    for &batch_size in BATCH_SIZES {
        let mut repeated = Vec::with_capacity(samples);
        let mut batched = Vec::with_capacity(samples);

        // Alternating order reduces systematic allocator and CPU-frequency bias.
        for sample in 0..samples {
            if sample.is_multiple_of(2) {
                repeated.push(measure(items, producers, PushMode::Repeated));
                batched.push(measure(items, producers, PushMode::Batched(batch_size)));
            } else {
                batched.push(measure(items, producers, PushMode::Batched(batch_size)));
                repeated.push(measure(items, producers, PushMode::Repeated));
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

fn measure(items: usize, producers: usize, mode: PushMode) -> Duration {
    if producers == 1 {
        let queue = UBQ::new();
        let start = Instant::now();
        push_range(&queue, 0, items, mode);
        let elapsed = start.elapsed();
        verify_and_drain(&queue, items);
        return elapsed;
    }

    let queue = Arc::new(UBQ::new());
    let start_barrier = Arc::new(Barrier::new(producers + 1));
    let workers = (0..producers)
        .map(|producer| {
            let queue = Arc::clone(&queue);
            let start_barrier = Arc::clone(&start_barrier);
            let first = items * producer / producers;
            let end = items * (producer + 1) / producers;

            thread::spawn(move || {
                start_barrier.wait();
                push_range(&queue, first, end, mode);
            })
        })
        .collect::<Vec<_>>();

    let start = Instant::now();
    start_barrier.wait();
    for worker in workers {
        worker.join().unwrap();
    }
    let elapsed = start.elapsed();

    verify_and_drain_unordered(&queue, items);
    elapsed
}

fn push_range(queue: &UBQ<usize>, first: usize, end: usize, mode: PushMode) {
    match mode {
        PushMode::Repeated => {
            for item in first..end {
                queue.push(item);
            }
        }
        PushMode::Batched(batch_size) => {
            for batch_first in (first..end).step_by(batch_size) {
                queue.push_batch(batch_first..end.min(batch_first + batch_size));
            }
        }
    }
}

fn verify_and_drain(queue: &UBQ<usize>, items: usize) {
    for expected in 0..items {
        assert_eq!(black_box(queue.pop()), Some(expected));
    }
    assert_eq!(queue.pop(), None);
}

fn verify_and_drain_unordered(queue: &UBQ<usize>, items: usize) {
    let mut sum = 0_u128;
    let mut xor = 0_usize;

    for _ in 0..items {
        let value = black_box(queue.pop()).expect("benchmark queue lost an item");
        sum += value as u128;
        xor ^= value;
    }

    assert_eq!(sum, (items as u128 - 1) * items as u128 / 2);
    assert_eq!(xor, xor_through(items));
    assert_eq!(queue.pop(), None);
}

fn xor_through(items: usize) -> usize {
    match items % 4 {
        0 => 0,
        1 => items - 1,
        2 => 1,
        _ => items,
    }
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
