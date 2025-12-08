use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion};
use crossbeam_channel as cb;
use std::{
    sync::atomic::{AtomicUsize, Ordering},
    sync::Arc,
    thread,
};
use tokio::runtime::Builder;
use ubq::ubq::{PopErr, UBQBounds, UBQ};

const MSG_COUNT: usize = 50_000;
const BLOCK_SIZE: usize = 256;
const MIN_BLOCKS: usize = 2;
const MAX_BLOCKS: usize = 1024;

fn push_pop_one_k(c: &mut Criterion) {
    let mut group = c.benchmark_group("ubq_push_pop");

    group.bench_function(format!("push_pop_1024_fresh_queue"), |b| {
        b.iter_batched(
            || UBQ::new(BLOCK_SIZE, UBQBounds { min: MIN_BLOCKS, max: MAX_BLOCKS }),
            |mut queue| {
                for i in 0..1024 {
                    queue.push(black_box(i)).unwrap();
                }

                for _ in 0..1024 {
                    let _ = black_box(queue.pop().unwrap());
                }
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("push_pop_256_reused_queue", |b| {
        b.iter_batched_ref(
            || UBQ::new(BLOCK_SIZE, UBQBounds { min: MIN_BLOCKS, max: MAX_BLOCKS }),
            |queue| {
                for i in 0..256 {
                    queue.push(black_box(i)).unwrap();
                }

                for _ in 0..256 {
                    let _ = black_box(queue.pop().unwrap());
                }
            },
            BatchSize::NumBatches(32),
        );
    });

    group.finish();
}

fn spsc_ubq(c: &mut Criterion) {
    let mut group = c.benchmark_group("spsc_ubq");
    group.bench_function("spsc_ubq_unboundedish_50k", |b| {
        b.iter(|| {
            let mut producer = UBQ::new(BLOCK_SIZE, UBQBounds { min: MIN_BLOCKS, max: MAX_BLOCKS });
            let mut consumer = producer.clone();

            let prod_handle = thread::spawn(move || {
                for i in 0..MSG_COUNT {
                    loop {
                        if producer.push(black_box(i as i32)).is_ok() {
                            break;
                        }
                    }
                }
            });

            let cons_handle = thread::spawn(move || {
                for _ in 0..MSG_COUNT {
                    loop {
                        match consumer.pop() {
                            Ok(v) => {
                                black_box(v);
                                break;
                            }
                            Err(PopErr::Busy) | Err(PopErr::Empty) => continue,
                        }
                    }
                }
            });

            prod_handle.join().unwrap();
            cons_handle.join().unwrap();
        })
    });
    group.finish();
}

fn spsc_crossbeam_channel(c: &mut Criterion) {
    let mut group = c.benchmark_group("spsc_crossbeam_channel");
    group.bench_function("spsc_crossbeam_unbounded_50k", |b| {
        b.iter(|| {
            let (tx, rx) = cb::unbounded::<i32>();
            let prod = thread::spawn(move || {
                for i in 0..MSG_COUNT {
                    tx.send(black_box(i as i32)).unwrap();
                }
            });
            let cons = thread::spawn(move || {
                for _ in 0..MSG_COUNT {
                    let v = rx.recv().unwrap();
                    black_box(v);
                }
            });
            prod.join().unwrap();
            cons.join().unwrap();
        })
    });

    group.bench_function("spsc_crossbeam_bounded_50k_cap_1024", |b| {
        b.iter(|| {
            let (tx, rx) = cb::bounded::<i32>(1024);
            let prod = thread::spawn(move || {
                for i in 0..MSG_COUNT {
                    tx.send(black_box(i as i32)).unwrap();
                }
            });
            let cons = thread::spawn(move || {
                for _ in 0..MSG_COUNT {
                    let v = rx.recv().unwrap();
                    black_box(v);
                }
            });
            prod.join().unwrap();
            cons.join().unwrap();
        })
    });
    group.finish();
}

fn mpmc_ubq(c: &mut Criterion) {
    let mut group = c.benchmark_group("mpmc_ubq");
    const PRODUCERS: usize = 4;
    const CONSUMERS: usize = 4;
    const PER_PRODUCER: usize = 12_500;
    let total_msgs = PRODUCERS * PER_PRODUCER;

    group.bench_function("mpmc_ubq_unboundedish_50k", |b| {
        b.iter(|| {
            let mut base = UBQ::new(BLOCK_SIZE, UBQBounds { min: MIN_BLOCKS, max: MAX_BLOCKS });
            let mut producers: Vec<UBQ<i32>> = (0..PRODUCERS - 1).map(|_| base.clone()).collect();
            producers.push(base.clone());

            let mut consumers: Vec<UBQ<i32>> = (0..CONSUMERS - 1).map(|_| base.clone()).collect();
            consumers.push(base);

            let consumed = Arc::new(AtomicUsize::new(0));

            let prod_handles: Vec<_> = producers
                .into_iter()
                .map(|mut q| {
                    thread::spawn(move || {
                        for i in 0..PER_PRODUCER {
                            loop {
                                if q.push(black_box(i as i32)).is_ok() {
                                    break;
                                }
                            }
                        }
                    })
                })
                .collect();

            let cons_handles: Vec<_> = consumers
                .into_iter()
                .map(|mut q| {
                    let consumed = consumed.clone();
                    thread::spawn(move || {
                        while consumed.load(Ordering::Relaxed) < total_msgs {
                            match q.pop() {
                                Ok(v) => {
                                    black_box(v);
                                    consumed.fetch_add(1, Ordering::Relaxed);
                                }
                                Err(PopErr::Busy) | Err(PopErr::Empty) => continue,
                            }
                        }
                    })
                })
                .collect();

            for h in prod_handles {
                h.join().unwrap();
            }
            for h in cons_handles {
                h.join().unwrap();
            }
        })
    });

    group.finish();
}

fn mpmc_crossbeam_channel(c: &mut Criterion) {
    let mut group = c.benchmark_group("mpmc_crossbeam_channel");
    const PRODUCERS: usize = 4;
    const CONSUMERS: usize = 4;
    const PER_PRODUCER: usize = 12_500;
    let total_msgs = PRODUCERS * PER_PRODUCER;

    group.bench_function("mpmc_crossbeam_unbounded_50k", |b| {
        b.iter(|| {
            let (tx, rx) = cb::unbounded::<i32>();
            let prod_handles: Vec<_> = (0..PRODUCERS)
                .map(|_| {
                    let tx = tx.clone();
                    thread::spawn(move || {
                        for i in 0..PER_PRODUCER {
                            tx.send(black_box(i as i32)).unwrap();
                        }
                    })
                })
                .collect();
            drop(tx);

            let consumed = Arc::new(AtomicUsize::new(0));
            let cons_handles: Vec<_> = (0..CONSUMERS)
                .map(|_| {
                    let rx = rx.clone();
                    let consumed = consumed.clone();
                    thread::spawn(move || {
                        while consumed.load(Ordering::Relaxed) < total_msgs {
                            if let Ok(v) = rx.recv() {
                                black_box(v);
                                consumed.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    })
                })
                .collect();

            for h in prod_handles {
                h.join().unwrap();
            }
            for h in cons_handles {
                h.join().unwrap();
            }
        })
    });

    group.bench_function("mpmc_crossbeam_bounded_50k_cap_1024", |b| {
        b.iter(|| {
            let (tx, rx) = cb::bounded::<i32>(1024);
            let prod_handles: Vec<_> = (0..PRODUCERS)
                .map(|_| {
                    let tx = tx.clone();
                    thread::spawn(move || {
                        for i in 0..PER_PRODUCER {
                            tx.send(black_box(i as i32)).unwrap();
                        }
                    })
                })
                .collect();
            drop(tx);

            let consumed = Arc::new(AtomicUsize::new(0));
            let cons_handles: Vec<_> = (0..CONSUMERS)
                .map(|_| {
                    let rx = rx.clone();
                    let consumed = consumed.clone();
                    thread::spawn(move || {
                        while consumed.load(Ordering::Relaxed) < total_msgs {
                            if let Ok(v) = rx.recv() {
                                black_box(v);
                                consumed.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    })
                })
                .collect();

            for h in prod_handles {
                h.join().unwrap();
            }
            for h in cons_handles {
                h.join().unwrap();
            }
        })
    });

    group.finish();
}

fn tokio_mpsc_push_pop(c: &mut Criterion) {
    let mut group = c.benchmark_group("tokio_mpsc_push_pop");
    let rt = Builder::new_current_thread().enable_all().build().unwrap();
    group.bench_function("tokio_mpsc_1024", |b| {
        b.iter(|| {
            rt.block_on(async {
                let capacity = 1024;
                let (tx, mut rx) = tokio::sync::mpsc::channel::<i32>(capacity);

                for i in 0..capacity {
                    tx.send(black_box(i.try_into().unwrap())).await.unwrap();
                }

                drop(tx);

                for _ in 0..capacity {
                    let v = rx.recv().await.unwrap();
                    black_box(v);
                }
            })
        });
    });
    group.finish();
}

fn async_channel_push_pop(c: &mut Criterion) {
    let mut group = c.benchmark_group("async_channel_push_pop");
    let rt = Builder::new_current_thread().enable_all().build().unwrap();
    group.bench_function("async_channel_1024", |b| {
        b.iter(|| {
            rt.block_on(async {
                let capacity = 1024;
                let (tx, rx) = async_channel::bounded::<i32>(capacity);

                for i in 0..capacity {
                    tx.send(black_box(i.try_into().unwrap())).await.unwrap();
                }

                drop(tx);
                for _ in 0..capacity {
                    let v = rx.recv().await.unwrap();
                    black_box(v);
                }
            })
        });
    });
    group.finish();
}

criterion_group!(
    benches,
    push_pop_one_k,
    spsc_ubq,
    spsc_crossbeam_channel,
    mpmc_ubq,
    mpmc_crossbeam_channel,
    tokio_mpsc_push_pop,
    async_channel_push_pop
);
criterion_main!(benches);
