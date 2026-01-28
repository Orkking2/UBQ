use criterion::{BatchSize, Criterion, criterion_group};
use crossbeam_channel as cb;
use serde_json::Value;
use std::{
    collections::HashMap,
    env, fs,
    hint::black_box,
    io::{self, Write},
    mem,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::runtime::Builder;
use ubq::{UBQ, UBQBounds};

const DEFAULT_BLOCK_SIZE: usize = 512;
const DEFAULT_FILL_BLOCKS: usize = 512;
const DEFAULT_MPMC_PRODUCERS: usize = 4;
const DEFAULT_MPMC_CONSUMERS: usize = 4;
const DEFAULT_MPSC_PRODUCERS: usize = 4;
const DEFAULT_SPMC_CONSUMERS: usize = 4;
const DEFAULT_SPIN_TIMEOUT_SECS: u64 = 10;
const DEFAULT_CHANNEL_CAPACITY: usize = 1024;

#[cfg(all(feature = "bench_small", feature = "bench_medium"))]
compile_error!("enable only one of bench_small, bench_medium, bench_large");
#[cfg(all(feature = "bench_small", feature = "bench_large"))]
compile_error!("enable only one of bench_small, bench_medium, bench_large");
#[cfg(all(feature = "bench_medium", feature = "bench_large"))]
compile_error!("enable only one of bench_small, bench_medium, bench_large");

const DEFAULT_SCALE: usize = if cfg!(feature = "bench_large") {
    4
} else if cfg!(feature = "bench_medium") {
    2
} else {
    1
};

// Runtime overrides use UBQ_BENCH_* env vars; compile-time scaling uses bench_* features.
#[derive(Clone, Debug)]
struct BenchConfig {
    block_size: usize,
    fill_blocks: usize,
    msg_count: usize,
    mpmc_producers: usize,
    mpmc_consumers: usize,
    mpsc_producers: usize,
    spmc_consumers: usize,
    mpmc_msg_per_prod: usize,
    mpmc_msg_per_cons: usize,
    mpsc_msg_per_prod: usize,
    spmc_msg_per_cons: usize,
    spin_timeout: Duration,
    channel_capacity: usize,
}

impl BenchConfig {
    fn load() -> Self {
        let scale = env_usize("UBQ_BENCH_SCALE", DEFAULT_SCALE);
        let block_size = env_usize(
            "UBQ_BENCH_BLOCK_SIZE",
            DEFAULT_BLOCK_SIZE.saturating_mul(scale),
        );
        let fill_blocks = env_usize(
            "UBQ_BENCH_FILL_BLOCKS",
            DEFAULT_FILL_BLOCKS.saturating_mul(scale),
        );
        let msg_count = env_usize(
            "UBQ_BENCH_MSG_COUNT",
            block_size.saturating_mul(fill_blocks),
        );
        let mpmc_producers = env_usize("UBQ_BENCH_MPMC_PRODUCERS", DEFAULT_MPMC_PRODUCERS);
        let mpmc_consumers = env_usize("UBQ_BENCH_MPMC_CONSUMERS", DEFAULT_MPMC_CONSUMERS);
        let mpsc_producers = env_usize("UBQ_BENCH_MPSC_PRODUCERS", DEFAULT_MPSC_PRODUCERS);
        let spmc_consumers = env_usize("UBQ_BENCH_SPMC_CONSUMERS", DEFAULT_SPMC_CONSUMERS);
        let channel_capacity = env_usize(
            "UBQ_BENCH_CHANNEL_CAPACITY",
            DEFAULT_CHANNEL_CAPACITY.saturating_mul(scale),
        );
        let spin_timeout_secs = env_u64("UBQ_BENCH_SPIN_TIMEOUT_SECS", DEFAULT_SPIN_TIMEOUT_SECS);

        let mpmc_msg_per_prod = msg_count / mpmc_producers;
        let mpmc_msg_per_cons = msg_count / mpmc_consumers;
        let mpsc_msg_per_prod = msg_count / mpsc_producers;
        let spmc_msg_per_cons = msg_count / spmc_consumers;

        let cfg = Self {
            block_size,
            fill_blocks,
            msg_count,
            mpmc_producers,
            mpmc_consumers,
            mpsc_producers,
            spmc_consumers,
            mpmc_msg_per_prod,
            mpmc_msg_per_cons,
            mpsc_msg_per_prod,
            spmc_msg_per_cons,
            spin_timeout: Duration::from_secs(spin_timeout_secs),
            channel_capacity,
        };
        cfg.validate();
        cfg
    }

    fn validate(&self) {
        assert!(self.block_size > 0, "block size must be > 0");
        assert!(self.fill_blocks > 0, "fill blocks must be > 0");
        assert!(self.msg_count > 0, "msg count must be > 0");
        assert!(self.mpmc_producers > 0, "mpmc producers must be > 0");
        assert!(self.mpmc_consumers > 0, "mpmc consumers must be > 0");
        assert!(self.mpsc_producers > 0, "mpsc producers must be > 0");
        assert!(self.spmc_consumers > 0, "spmc consumers must be > 0");
        assert!(
            self.msg_count % self.mpmc_producers == 0,
            "msg count must be divisible by mpmc producers"
        );
        assert!(
            self.msg_count % self.mpmc_consumers == 0,
            "msg count must be divisible by mpmc consumers"
        );
        assert!(
            self.msg_count % self.mpsc_producers == 0,
            "msg count must be divisible by mpsc producers"
        );
        assert!(
            self.msg_count % self.spmc_consumers == 0,
            "msg count must be divisible by spmc consumers"
        );
        assert!(self.channel_capacity > 0, "channel capacity must be > 0");
    }
}

#[derive(Clone, Copy)]
enum BenchCap {
    Unbounded,
    Bounded(usize),
}

fn bench_id(
    label: &str,
    cfg: &BenchConfig,
    prod: usize,
    cons: usize,
    cap: BenchCap,
    elem_name: &str,
    elem_size: usize,
) -> String {
    let cap_label = match cap {
        BenchCap::Unbounded => "unbounded".to_string(),
        BenchCap::Bounded(capacity) => capacity.to_string(),
    };
    format!(
        "{label}: msgs={}, prod={prod}, cons={cons}, cap={cap_label}, elem={elem_name}({elem_size}B)",
        cfg.msg_count,
    )
}

fn ubq_bench_id(label: &str, cfg: &BenchConfig, prod: usize, cons: usize) -> String {
    format!(
        "{label}: msgs={}, prod={prod}, cons={cons}, blocks={}, block_size={}, elem=usize({}B)",
        cfg.msg_count,
        cfg.fill_blocks,
        cfg.block_size,
        mem::size_of::<usize>()
    )
}

fn env_usize(key: &str, default: usize) -> usize {
    env::var(key)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(default)
}

fn env_u64(key: &str, default: u64) -> u64 {
    env::var(key)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(default)
}

fn spin_backoff(spins: &mut u32) {
    if *spins < 64 {
        std::hint::spin_loop();
    } else {
        thread::yield_now();
    }
    *spins = spins.wrapping_add(1);
}

fn panic_timeout(
    role: &str,
    ubq: &mut UBQ<usize>,
    produced: &AtomicUsize,
    consumed: &AtomicUsize,
) -> ! {
    let produced_now = produced.load(Ordering::Relaxed);
    let consumed_now = consumed.load(Ordering::Relaxed);

    #[cfg(feature = "ubq_debug")]
    {
        let state = ubq.debug_state();
        panic!(
            "{role} timed out (produced={produced_now}, consumed={consumed_now}) state={state:?}"
        );
    }

    #[cfg(not(feature = "ubq_debug"))]
    {
        let _ = ubq;
        panic!(
            "{role} timed out (produced={produced_now}, consumed={consumed_now}); re-run with `--features ubq_debug` to dump queue state"
        );
    }
}

pub fn new_ubq<T>(cfg: &BenchConfig) -> UBQ<T> {
    UBQ::new(
        cfg.block_size,
        UBQBounds {
            max: cfg.fill_blocks,
            min: 1,
        },
    )
}

pub fn bench_ubq_spsc_fill_and_empty(cfg: &BenchConfig, mut queue: UBQ<usize>) {
    for i in 0..cfg.msg_count {
        queue.push(black_box(i)).unwrap();
    }

    for _ in 0..cfg.msg_count {
        let _ = black_box(queue.pop().unwrap());
    }
}

pub fn bench_ubq_spsc_fill_and_empty_simultaneous(cfg: &BenchConfig, queue: UBQ<usize>) {
    let mut pq = queue;
    let mut cq = pq.clone();
    let msg_count = cfg.msg_count;

    let ph = thread::spawn(move || {
        for i in 0..msg_count {
            match pq.push(black_box(i)) {
                Ok(()) => {
                    // produced_handle.fetch_add(1, Ordering::Relaxed);
                }
                Err((_val, _err)) => {
                    // failed_handle.store(true, Ordering::Relaxed);
                    break;
                }
            }
        }
        // done_handle.store(true, Ordering::Release);
    });

    let ch = thread::spawn(move || {
        let mut consumed = 0usize;
        // let mut spins = 0u32;

        while consumed < msg_count {
            match cq.pop() {
                Ok(v) => {
                    black_box(v);
                    consumed += 1;
                    // spins = 0;
                }
                Err(_) => {
                    // std::hint::spin_loop();
                    // spins = spins.wrapping_add(1);
                    // if spins % 64 == 0 {
                    //     thread::yield_now();
                    // }

                    // if done_handle.load(Ordering::Acquire) {
                    // let produced_now = produced_handle.load(Ordering::Acquire);
                    //     if consumed >= produced_now {
                    break;
                    //     }
                    // }
                }
            }
        }
    });

    ph.join().unwrap();
    ch.join().unwrap();

    // assert!(
    //     !producer_failed.load(Ordering::Relaxed),
    //     "producer hit allocation bounds before emitting {MSG_COUNT} messages"
    // );
}

pub fn new_pc<T>(queue: UBQ<T>, producers: usize, consumers: usize) -> (Vec<UBQ<T>>, Vec<UBQ<T>>) {
    assert!(producers > 0, "mpmc producers must be > 0");
    assert!(consumers > 0, "mpmc consumers must be > 0");
    let p: Vec<_> = (0..producers).map(|_| queue.clone()).collect();
    let mut c: Vec<_> = (0..consumers - 1).map(|_| queue.clone()).collect();
    c.push(queue);

    (p, c)
}

pub fn new_mpsc<T>(queue: UBQ<T>, producers: usize) -> (Vec<UBQ<T>>, UBQ<T>) {
    assert!(producers > 0, "mpsc producers must be > 0");
    let p: Vec<_> = (0..producers).map(|_| queue.clone()).collect();
    (p, queue)
}

pub fn new_spmc<T>(queue: UBQ<T>, consumers: usize) -> (UBQ<T>, Vec<UBQ<T>>) {
    assert!(consumers > 0, "spmc consumers must be > 0");
    let c: Vec<_> = (0..consumers).map(|_| queue.clone()).collect();
    (queue, c)
}

fn pubq2ph(
    pubq: Vec<UBQ<usize>>,
    produced: Arc<AtomicUsize>,
    consumed: Arc<AtomicUsize>,
    msgs_per_prod: usize,
    spin_timeout: Duration,
) -> Vec<std::thread::JoinHandle<()>> {
    pubq.into_iter()
        .map(|mut ubq| {
            let produced = produced.clone();
            let consumed = consumed.clone();
            thread::spawn(move || {
                for i in 0..msgs_per_prod {
                    let mut spins = 0u32;
                    let mut spin_start: Option<Instant> = None;
                    loop {
                        match ubq.push(black_box(i)) {
                            Ok(()) => {
                                produced.fetch_add(1, Ordering::Relaxed);
                                break;
                            }
                            Err((_val, ubq::PushErr::ListHasBeenDeallocated)) => {
                                panic!("producer push failed: list deallocated");
                            }
                            Err((_val, ubq::PushErr::BlockAllocBoundsReached)) => {
                                let start = spin_start.get_or_insert_with(Instant::now);
                                if start.elapsed() > spin_timeout {
                                    panic_timeout("producer", &mut ubq, &produced, &consumed);
                                }
                                spin_backoff(&mut spins);
                            }
                        }
                    }
                }

                #[cfg(feature = "ubq_debug")]
                log::trace!("push done {:?}", ubq.debug_state());
            })
        })
        .collect()
}

fn cubq2ch(
    cubq: Vec<UBQ<usize>>,
    produced: Arc<AtomicUsize>,
    consumed: Arc<AtomicUsize>,
    msgs_per_cons: usize,
    spin_timeout: Duration,
) -> Vec<std::thread::JoinHandle<()>> {
    cubq.into_iter()
        .map(|mut ubq| {
            let produced = produced.clone();
            let consumed = consumed.clone();
            thread::spawn(move || {
                for _ in 0..msgs_per_cons {
                    let mut spins = 0u32;
                    let mut spin_start: Option<Instant> = None;
                    loop {
                        match black_box(ubq.pop()) {
                            Ok(_) => {
                                consumed.fetch_add(1, Ordering::Relaxed);
                                break;
                            }
                            Err(_) => {
                                let start = spin_start.get_or_insert_with(Instant::now);
                                if start.elapsed() > spin_timeout {
                                    panic_timeout("consumer", &mut ubq, &produced, &consumed);
                                }
                                spin_backoff(&mut spins);
                            }
                        }
                    }
                }

                #[cfg(feature = "ubq_debug")]
                log::trace!("pop done {:?}", ubq.debug_state());
            })
        })
        .collect()
}

pub fn bench_ubq_mpmc_fill_and_empty(cfg: &BenchConfig, x: (Vec<UBQ<usize>>, Vec<UBQ<usize>>)) {
    let (p, c) = x;

    let produced = Arc::new(AtomicUsize::new(0));
    let consumed = Arc::new(AtomicUsize::new(0));

    let ph = pubq2ph(
        p,
        produced.clone(),
        consumed.clone(),
        cfg.mpmc_msg_per_prod,
        cfg.spin_timeout,
    );
    let ch = cubq2ch(
        c,
        produced.clone(),
        consumed.clone(),
        cfg.mpmc_msg_per_cons,
        cfg.spin_timeout,
    );

    ph.into_iter().for_each(|h| h.join().unwrap());
    ch.into_iter().for_each(|h| h.join().unwrap());

    let produced_total = produced.load(Ordering::Relaxed);
    let consumed_total = consumed.load(Ordering::Relaxed);
    assert_eq!(
        produced_total, cfg.msg_count,
        "producer count mismatch: produced={produced_total} expected={}",
        cfg.msg_count
    );
    assert_eq!(
        consumed_total, cfg.msg_count,
        "consumer count mismatch: consumed={consumed_total} expected={}",
        cfg.msg_count
    );
}

pub fn bench_ubq_mpsc_fill_and_empty(cfg: &BenchConfig, x: (Vec<UBQ<usize>>, UBQ<usize>)) {
    debug_assert_eq!(cfg.msg_count % cfg.mpsc_producers, 0);
    let (p, c) = x;

    let produced = Arc::new(AtomicUsize::new(0));
    let consumed = Arc::new(AtomicUsize::new(0));

    let ph = pubq2ph(
        p,
        produced.clone(),
        consumed.clone(),
        cfg.mpsc_msg_per_prod,
        cfg.spin_timeout,
    );
    let ch = cubq2ch(
        vec![c],
        produced.clone(),
        consumed.clone(),
        cfg.msg_count,
        cfg.spin_timeout,
    );

    ph.into_iter().for_each(|h| h.join().unwrap());
    ch.into_iter().for_each(|h| h.join().unwrap());

    let produced_total = produced.load(Ordering::Relaxed);
    let consumed_total = consumed.load(Ordering::Relaxed);
    assert_eq!(
        produced_total, cfg.msg_count,
        "producer count mismatch: produced={produced_total} expected={}",
        cfg.msg_count
    );
    assert_eq!(
        consumed_total, cfg.msg_count,
        "consumer count mismatch: consumed={consumed_total} expected={}",
        cfg.msg_count
    );
}

pub fn bench_ubq_spmc_fill_and_empty(cfg: &BenchConfig, x: (UBQ<usize>, Vec<UBQ<usize>>)) {
    debug_assert_eq!(cfg.msg_count % cfg.spmc_consumers, 0);
    let (p, c) = x;

    let produced = Arc::new(AtomicUsize::new(0));
    let consumed = Arc::new(AtomicUsize::new(0));

    let ph = pubq2ph(
        vec![p],
        produced.clone(),
        consumed.clone(),
        cfg.msg_count,
        cfg.spin_timeout,
    );
    let ch = cubq2ch(
        c,
        produced.clone(),
        consumed.clone(),
        cfg.spmc_msg_per_cons,
        cfg.spin_timeout,
    );

    ph.into_iter().for_each(|h| h.join().unwrap());
    ch.into_iter().for_each(|h| h.join().unwrap());

    let produced_total = produced.load(Ordering::Relaxed);
    let consumed_total = consumed.load(Ordering::Relaxed);
    assert_eq!(
        produced_total, cfg.msg_count,
        "producer count mismatch: produced={produced_total} expected={}",
        cfg.msg_count
    );
    assert_eq!(
        consumed_total, cfg.msg_count,
        "consumer count mismatch: consumed={consumed_total} expected={}",
        cfg.msg_count
    );
}

fn ubq(c: &mut Criterion) {
    let cfg = BenchConfig::load();
    let create_id = format!(
        "Create: blocks={}, block_size={}, elem=usize({}B)",
        cfg.fill_blocks,
        cfg.block_size,
        mem::size_of::<usize>()
    );
    c.bench_function(&create_id, |b| b.iter(|| new_ubq::<usize>(&cfg)));

    let mut spsc_group = c.benchmark_group("ubq_spsc");

    spsc_group.bench_function(ubq_bench_id("Fill & Empty", &cfg, 1, 1), |b| {
        b.iter_batched(
            || new_ubq(&cfg),
            |queue| bench_ubq_spsc_fill_and_empty(&cfg, queue),
            BatchSize::SmallInput,
        );
    });

    spsc_group.bench_function(ubq_bench_id("Fill & Empty Simultaneous", &cfg, 1, 1), |b| {
        b.iter_batched(
            || new_ubq(&cfg),
            |queue| bench_ubq_spsc_fill_and_empty_simultaneous(&cfg, queue),
            BatchSize::SmallInput,
        );
    });

    spsc_group.finish();

    let mut mpsc_group = c.benchmark_group("ubq_mpmc");
    mpsc_group.sample_size(10);
    mpsc_group.measurement_time(Duration::from_secs(5));
    mpsc_group.bench_function(
        ubq_bench_id("Fill & Empty", &cfg, cfg.mpsc_producers, 1),
        |b| {
            b.iter_batched(
                || new_mpsc(new_ubq(&cfg), cfg.mpsc_producers),
                |queues| bench_ubq_mpsc_fill_and_empty(&cfg, queues),
                BatchSize::LargeInput,
            )
        },
    );
    mpsc_group.finish();

    let mut spmc_group = c.benchmark_group("SPMC");
    spmc_group.sample_size(10);
    spmc_group.measurement_time(Duration::from_secs(5));
    spmc_group.bench_function(
        ubq_bench_id("Fill & Empty", &cfg, 1, cfg.spmc_consumers),
        |b| {
            b.iter_batched(
                || new_spmc(new_ubq(&cfg), cfg.spmc_consumers),
                |queues| bench_ubq_spmc_fill_and_empty(&cfg, queues),
                BatchSize::LargeInput,
            )
        },
    );
    spmc_group.finish();

    let mut mpmc_group = c.benchmark_group("MPMC");
    mpmc_group.sample_size(10);
    mpmc_group.measurement_time(Duration::from_secs(5));

    mpmc_group.bench_function(
        ubq_bench_id("Fill & Empty", &cfg, cfg.mpmc_producers, cfg.mpmc_consumers),
        |b| {
            b.iter_batched(
                || new_pc(new_ubq(&cfg), cfg.mpmc_producers, cfg.mpmc_consumers),
                |queues| bench_ubq_mpmc_fill_and_empty(&cfg, queues),
                BatchSize::LargeInput,
            )
        },
    );

    mpmc_group.finish();
}

fn spsc_crossbeam_channel(c: &mut Criterion) {
    let cfg = BenchConfig::load();
    let elem_size = mem::size_of::<i32>();
    let mut group = c.benchmark_group("spsc_crossbeam_channel");
    group.bench_function(
        bench_id(
            "unbounded",
            &cfg,
            1,
            1,
            BenchCap::Unbounded,
            "i32",
            elem_size,
        ),
        |b| {
            let msg_count = cfg.msg_count;
            b.iter(|| {
                let (tx, rx) = cb::unbounded::<i32>();
                let prod = thread::spawn(move || {
                    for i in 0..msg_count {
                        tx.send(black_box(i as i32)).unwrap();
                    }
                });
                let cons = thread::spawn(move || {
                    for _ in 0..msg_count {
                        let v = rx.recv().unwrap();
                        black_box(v);
                    }
                });
                prod.join().unwrap();
                cons.join().unwrap();
            })
        },
    );

    group.bench_function(
        bench_id(
            "bounded",
            &cfg,
            1,
            1,
            BenchCap::Bounded(cfg.channel_capacity),
            "i32",
            elem_size,
        ),
        |b| {
            let msg_count = cfg.msg_count;
            let capacity = cfg.channel_capacity;
            b.iter(|| {
                let (tx, rx) = cb::bounded::<i32>(capacity);
                let prod = thread::spawn(move || {
                    for i in 0..msg_count {
                        tx.send(black_box(i as i32)).unwrap();
                    }
                });
                let cons = thread::spawn(move || {
                    for _ in 0..msg_count {
                        let v = rx.recv().unwrap();
                        black_box(v);
                    }
                });
                prod.join().unwrap();
                cons.join().unwrap();
            })
        },
    );
    group.finish();
}

fn mpsc_crossbeam_channel(c: &mut Criterion) {
    let cfg = BenchConfig::load();
    let elem_size = mem::size_of::<i32>();
    let mut group = c.benchmark_group("mpsc_crossbeam_channel");
    group.bench_function(
        bench_id(
            "unbounded",
            &cfg,
            cfg.mpsc_producers,
            1,
            BenchCap::Unbounded,
            "i32",
            elem_size,
        ),
        |b| {
            let msg_count = cfg.msg_count;
            let msg_per_prod = cfg.mpsc_msg_per_prod;
            let producers = cfg.mpsc_producers;
            b.iter(|| {
                let (tx, rx) = cb::unbounded::<i32>();
                let prod_handles: Vec<_> = (0..producers)
                    .map(|_| {
                        let tx = tx.clone();
                        thread::spawn(move || {
                            for i in 0..msg_per_prod {
                                tx.send(black_box(i as i32)).unwrap();
                            }
                        })
                    })
                    .collect();
                drop(tx);

                let cons = thread::spawn(move || {
                    for _ in 0..msg_count {
                        let v = rx.recv().unwrap();
                        black_box(v);
                    }
                });

                for h in prod_handles {
                    h.join().unwrap();
                }
                cons.join().unwrap();
            })
        },
    );

    group.bench_function(
        bench_id(
            "bounded",
            &cfg,
            cfg.mpsc_producers,
            1,
            BenchCap::Bounded(cfg.channel_capacity),
            "i32",
            elem_size,
        ),
        |b| {
            let msg_count = cfg.msg_count;
            let msg_per_prod = cfg.mpsc_msg_per_prod;
            let producers = cfg.mpsc_producers;
            let capacity = cfg.channel_capacity;
            b.iter(|| {
                let (tx, rx) = cb::bounded::<i32>(capacity);
                let prod_handles: Vec<_> = (0..producers)
                    .map(|_| {
                        let tx = tx.clone();
                        thread::spawn(move || {
                            for i in 0..msg_per_prod {
                                tx.send(black_box(i as i32)).unwrap();
                            }
                        })
                    })
                    .collect();
                drop(tx);

                let cons = thread::spawn(move || {
                    for _ in 0..msg_count {
                        let v = rx.recv().unwrap();
                        black_box(v);
                    }
                });

                for h in prod_handles {
                    h.join().unwrap();
                }
                cons.join().unwrap();
            })
        },
    );

    group.finish();
}

fn spmc_crossbeam_channel(c: &mut Criterion) {
    let cfg = BenchConfig::load();
    let elem_size = mem::size_of::<i32>();
    let mut group = c.benchmark_group("spmc_crossbeam_channel");
    group.bench_function(
        bench_id(
            "unbounded",
            &cfg,
            1,
            cfg.spmc_consumers,
            BenchCap::Unbounded,
            "i32",
            elem_size,
        ),
        |b| {
            let msg_count = cfg.msg_count;
            let msg_per_cons = cfg.spmc_msg_per_cons;
            let consumers = cfg.spmc_consumers;
            b.iter(|| {
                let (tx, rx) = cb::unbounded::<i32>();

                let prod = thread::spawn(move || {
                    for i in 0..msg_count {
                        tx.send(black_box(i as i32)).unwrap();
                    }
                });

                let cons_handles: Vec<_> = (0..consumers)
                    .map(|_| {
                        let rx = rx.clone();
                        thread::spawn(move || {
                            for _ in 0..msg_per_cons {
                                let v = rx.recv().unwrap();
                                black_box(v);
                            }
                        })
                    })
                    .collect();

                prod.join().unwrap();
                for h in cons_handles {
                    h.join().unwrap();
                }
            })
        },
    );

    group.bench_function(
        bench_id(
            "bounded",
            &cfg,
            1,
            cfg.spmc_consumers,
            BenchCap::Bounded(cfg.channel_capacity),
            "i32",
            elem_size,
        ),
        |b| {
            let msg_count = cfg.msg_count;
            let msg_per_cons = cfg.spmc_msg_per_cons;
            let consumers = cfg.spmc_consumers;
            let capacity = cfg.channel_capacity;
            b.iter(|| {
                let (tx, rx) = cb::bounded::<i32>(capacity);

                let prod = thread::spawn(move || {
                    for i in 0..msg_count {
                        tx.send(black_box(i as i32)).unwrap();
                    }
                });

                let cons_handles: Vec<_> = (0..consumers)
                    .map(|_| {
                        let rx = rx.clone();
                        thread::spawn(move || {
                            for _ in 0..msg_per_cons {
                                let v = rx.recv().unwrap();
                                black_box(v);
                            }
                        })
                    })
                    .collect();

                prod.join().unwrap();
                for h in cons_handles {
                    h.join().unwrap();
                }
            })
        },
    );

    group.finish();
}

fn mpmc_crossbeam_channel(c: &mut Criterion) {
    let cfg = BenchConfig::load();
    let elem_size = mem::size_of::<i32>();
    let mut group = c.benchmark_group("mpmc_crossbeam_channel");
    group.bench_function(
        bench_id(
            "unbounded",
            &cfg,
            cfg.mpmc_producers,
            cfg.mpmc_consumers,
            BenchCap::Unbounded,
            "i32",
            elem_size,
        ),
        |b| {
            let msg_count = cfg.msg_count;
            let msg_per_prod = cfg.mpmc_msg_per_prod;
            let producers = cfg.mpmc_producers;
            let consumers = cfg.mpmc_consumers;
            b.iter(|| {
                let (tx, rx) = cb::unbounded::<i32>();
                let prod_handles: Vec<_> = (0..producers)
                    .map(|_| {
                        let tx = tx.clone();
                        thread::spawn(move || {
                            for i in 0..msg_per_prod {
                                tx.send(black_box(i as i32)).unwrap();
                            }
                        })
                    })
                    .collect();
                drop(tx);

                let consumed = Arc::new(AtomicUsize::new(0));
                let cons_handles: Vec<_> = (0..consumers)
                    .map(|_| {
                        let rx = rx.clone();
                        let consumed = consumed.clone();
                        thread::spawn(move || {
                            while consumed.load(Ordering::Relaxed) < msg_count {
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
        },
    );

    group.bench_function(
        bench_id(
            "bounded",
            &cfg,
            cfg.mpmc_producers,
            cfg.mpmc_consumers,
            BenchCap::Bounded(cfg.channel_capacity),
            "i32",
            elem_size,
        ),
        |b| {
            let msg_count = cfg.msg_count;
            let msg_per_prod = cfg.mpmc_msg_per_prod;
            let producers = cfg.mpmc_producers;
            let consumers = cfg.mpmc_consumers;
            let capacity = cfg.channel_capacity;
            b.iter(|| {
                let (tx, rx) = cb::bounded::<i32>(capacity);
                let prod_handles: Vec<_> = (0..producers)
                    .map(|_| {
                        let tx = tx.clone();
                        thread::spawn(move || {
                            for i in 0..msg_per_prod {
                                tx.send(black_box(i as i32)).unwrap();
                            }
                        })
                    })
                    .collect();
                drop(tx);

                let consumed = Arc::new(AtomicUsize::new(0));
                let cons_handles: Vec<_> = (0..consumers)
                    .map(|_| {
                        let rx = rx.clone();
                        let consumed = consumed.clone();
                        thread::spawn(move || {
                            while consumed.load(Ordering::Relaxed) < msg_count {
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
        },
    );

    group.finish();
}

fn spsc_flume_channel(c: &mut Criterion) {
    let cfg = BenchConfig::load();
    let elem_size = mem::size_of::<i32>();
    let mut group = c.benchmark_group("spsc_flume_channel");
    group.bench_function(
        bench_id(
            "unbounded",
            &cfg,
            1,
            1,
            BenchCap::Unbounded,
            "i32",
            elem_size,
        ),
        |b| {
            let msg_count = cfg.msg_count;
            b.iter(|| {
                let (tx, rx) = flume::unbounded::<i32>();
                let prod = thread::spawn(move || {
                    for i in 0..msg_count {
                        tx.send(black_box(i as i32)).unwrap();
                    }
                });
                let cons = thread::spawn(move || {
                    for _ in 0..msg_count {
                        let v = rx.recv().unwrap();
                        black_box(v);
                    }
                });
                prod.join().unwrap();
                cons.join().unwrap();
            })
        },
    );

    group.bench_function(
        bench_id(
            "bounded",
            &cfg,
            1,
            1,
            BenchCap::Bounded(cfg.channel_capacity),
            "i32",
            elem_size,
        ),
        |b| {
            let msg_count = cfg.msg_count;
            let capacity = cfg.channel_capacity;
            b.iter(|| {
                let (tx, rx) = flume::bounded::<i32>(capacity);
                let prod = thread::spawn(move || {
                    for i in 0..msg_count {
                        tx.send(black_box(i as i32)).unwrap();
                    }
                });
                let cons = thread::spawn(move || {
                    for _ in 0..msg_count {
                        let v = rx.recv().unwrap();
                        black_box(v);
                    }
                });
                prod.join().unwrap();
                cons.join().unwrap();
            })
        },
    );
    group.finish();
}

fn mpsc_flume_channel(c: &mut Criterion) {
    let cfg = BenchConfig::load();
    let elem_size = mem::size_of::<i32>();
    let mut group = c.benchmark_group("mpsc_flume_channel");
    group.bench_function(
        bench_id(
            "unbounded",
            &cfg,
            cfg.mpsc_producers,
            1,
            BenchCap::Unbounded,
            "i32",
            elem_size,
        ),
        |b| {
            let msg_count = cfg.msg_count;
            let msg_per_prod = cfg.mpsc_msg_per_prod;
            let producers = cfg.mpsc_producers;
            b.iter(|| {
                let (tx, rx) = flume::unbounded::<i32>();
                let prod_handles: Vec<_> = (0..producers)
                    .map(|_| {
                        let tx = tx.clone();
                        thread::spawn(move || {
                            for i in 0..msg_per_prod {
                                tx.send(black_box(i as i32)).unwrap();
                            }
                        })
                    })
                    .collect();
                drop(tx);

                let cons = thread::spawn(move || {
                    for _ in 0..msg_count {
                        let v = rx.recv().unwrap();
                        black_box(v);
                    }
                });

                for h in prod_handles {
                    h.join().unwrap();
                }
                cons.join().unwrap();
            })
        },
    );

    group.bench_function(
        bench_id(
            "bounded",
            &cfg,
            cfg.mpsc_producers,
            1,
            BenchCap::Bounded(cfg.channel_capacity),
            "i32",
            elem_size,
        ),
        |b| {
            let msg_count = cfg.msg_count;
            let msg_per_prod = cfg.mpsc_msg_per_prod;
            let producers = cfg.mpsc_producers;
            let capacity = cfg.channel_capacity;
            b.iter(|| {
                let (tx, rx) = flume::bounded::<i32>(capacity);
                let prod_handles: Vec<_> = (0..producers)
                    .map(|_| {
                        let tx = tx.clone();
                        thread::spawn(move || {
                            for i in 0..msg_per_prod {
                                tx.send(black_box(i as i32)).unwrap();
                            }
                        })
                    })
                    .collect();
                drop(tx);

                let cons = thread::spawn(move || {
                    for _ in 0..msg_count {
                        let v = rx.recv().unwrap();
                        black_box(v);
                    }
                });

                for h in prod_handles {
                    h.join().unwrap();
                }
                cons.join().unwrap();
            })
        },
    );

    group.finish();
}

fn spmc_flume_channel(c: &mut Criterion) {
    let cfg = BenchConfig::load();
    let elem_size = mem::size_of::<i32>();
    let mut group = c.benchmark_group("spmc_flume_channel");
    group.bench_function(
        bench_id(
            "unbounded",
            &cfg,
            1,
            cfg.spmc_consumers,
            BenchCap::Unbounded,
            "i32",
            elem_size,
        ),
        |b| {
            let msg_count = cfg.msg_count;
            let msg_per_cons = cfg.spmc_msg_per_cons;
            let consumers = cfg.spmc_consumers;
            b.iter(|| {
                let (tx, rx) = flume::unbounded::<i32>();
                let prod = thread::spawn(move || {
                    for i in 0..msg_count {
                        tx.send(black_box(i as i32)).unwrap();
                    }
                });

                let cons_handles: Vec<_> = (0..consumers)
                    .map(|_| {
                        let rx = rx.clone();
                        thread::spawn(move || {
                            for _ in 0..msg_per_cons {
                                let v = rx.recv().unwrap();
                                black_box(v);
                            }
                        })
                    })
                    .collect();

                prod.join().unwrap();
                for h in cons_handles {
                    h.join().unwrap();
                }
            })
        },
    );

    group.bench_function(
        bench_id(
            "bounded",
            &cfg,
            1,
            cfg.spmc_consumers,
            BenchCap::Bounded(cfg.channel_capacity),
            "i32",
            elem_size,
        ),
        |b| {
            let msg_count = cfg.msg_count;
            let msg_per_cons = cfg.spmc_msg_per_cons;
            let consumers = cfg.spmc_consumers;
            let capacity = cfg.channel_capacity;
            b.iter(|| {
                let (tx, rx) = flume::bounded::<i32>(capacity);
                let prod = thread::spawn(move || {
                    for i in 0..msg_count {
                        tx.send(black_box(i as i32)).unwrap();
                    }
                });

                let cons_handles: Vec<_> = (0..consumers)
                    .map(|_| {
                        let rx = rx.clone();
                        thread::spawn(move || {
                            for _ in 0..msg_per_cons {
                                let v = rx.recv().unwrap();
                                black_box(v);
                            }
                        })
                    })
                    .collect();

                prod.join().unwrap();
                for h in cons_handles {
                    h.join().unwrap();
                }
            })
        },
    );

    group.finish();
}

fn mpmc_flume_channel(c: &mut Criterion) {
    let cfg = BenchConfig::load();
    let elem_size = mem::size_of::<i32>();
    let mut group = c.benchmark_group("mpmc_flume_channel");
    group.bench_function(
        bench_id(
            "unbounded",
            &cfg,
            cfg.mpmc_producers,
            cfg.mpmc_consumers,
            BenchCap::Unbounded,
            "i32",
            elem_size,
        ),
        |b| {
            let msg_count = cfg.msg_count;
            let msg_per_prod = cfg.mpmc_msg_per_prod;
            let producers = cfg.mpmc_producers;
            let consumers = cfg.mpmc_consumers;
            b.iter(|| {
                let (tx, rx) = flume::unbounded::<i32>();
                let prod_handles: Vec<_> = (0..producers)
                    .map(|_| {
                        let tx = tx.clone();
                        thread::spawn(move || {
                            for i in 0..msg_per_prod {
                                tx.send(black_box(i as i32)).unwrap();
                            }
                        })
                    })
                    .collect();
                drop(tx);

                let consumed = Arc::new(AtomicUsize::new(0));
                let cons_handles: Vec<_> = (0..consumers)
                    .map(|_| {
                        let rx = rx.clone();
                        let consumed = consumed.clone();
                        thread::spawn(move || {
                            while consumed.load(Ordering::Relaxed) < msg_count {
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
        },
    );

    group.bench_function(
        bench_id(
            "bounded",
            &cfg,
            cfg.mpmc_producers,
            cfg.mpmc_consumers,
            BenchCap::Bounded(cfg.channel_capacity),
            "i32",
            elem_size,
        ),
        |b| {
            let msg_count = cfg.msg_count;
            let msg_per_prod = cfg.mpmc_msg_per_prod;
            let producers = cfg.mpmc_producers;
            let consumers = cfg.mpmc_consumers;
            let capacity = cfg.channel_capacity;
            b.iter(|| {
                let (tx, rx) = flume::bounded::<i32>(capacity);
                let prod_handles: Vec<_> = (0..producers)
                    .map(|_| {
                        let tx = tx.clone();
                        thread::spawn(move || {
                            for i in 0..msg_per_prod {
                                tx.send(black_box(i as i32)).unwrap();
                            }
                        })
                    })
                    .collect();
                drop(tx);

                let consumed = Arc::new(AtomicUsize::new(0));
                let cons_handles: Vec<_> = (0..consumers)
                    .map(|_| {
                        let rx = rx.clone();
                        let consumed = consumed.clone();
                        thread::spawn(move || {
                            while consumed.load(Ordering::Relaxed) < msg_count {
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
        },
    );

    group.finish();
}

fn tokio_mpsc_push_pop(c: &mut Criterion) {
    let cfg = BenchConfig::load();
    let elem_size = mem::size_of::<i32>();
    let mut group = c.benchmark_group("tokio_mpsc_push_pop");
    let rt = Builder::new_current_thread().enable_all().build().unwrap();
    group.bench_function(
        bench_id(
            "bounded",
            &cfg,
            1,
            1,
            BenchCap::Bounded(cfg.channel_capacity),
            "i32",
            elem_size,
        ),
        |b| {
            let msg_count = cfg.msg_count;
            let capacity = cfg.channel_capacity;
            b.iter(|| {
                rt.block_on(async {
                    let (tx, mut rx) = tokio::sync::mpsc::channel::<i32>(capacity);
                    let prod = tokio::task::spawn(async move {
                        for i in 0..msg_count {
                            tx.send(black_box(i as i32)).await.unwrap();
                        }
                    });
                    let cons = tokio::task::spawn(async move {
                        for _ in 0..msg_count {
                            let v = rx.recv().await.unwrap();
                            black_box(v);
                        }
                    });
                    prod.await.unwrap();
                    cons.await.unwrap();
                })
            });
        },
    );
    group.finish();
}

fn async_channel_push_pop(c: &mut Criterion) {
    let cfg = BenchConfig::load();
    let elem_size = mem::size_of::<i32>();
    let mut group = c.benchmark_group("async_channel_push_pop");
    let rt = Builder::new_current_thread().enable_all().build().unwrap();
    group.bench_function(
        bench_id(
            "bounded",
            &cfg,
            1,
            1,
            BenchCap::Bounded(cfg.channel_capacity),
            "i32",
            elem_size,
        ),
        |b| {
            let msg_count = cfg.msg_count;
            let capacity = cfg.channel_capacity;
            b.iter(|| {
                rt.block_on(async {
                    let (tx, rx) = async_channel::bounded::<i32>(capacity);
                    let prod = tokio::task::spawn(async move {
                        for i in 0..msg_count {
                            tx.send(black_box(i as i32)).await.unwrap();
                        }
                    });
                    let cons = tokio::task::spawn(async move {
                        for _ in 0..msg_count {
                            let v = rx.recv().await.unwrap();
                            black_box(v);
                        }
                    });
                    prod.await.unwrap();
                    cons.await.unwrap();
                })
            });
        },
    );
    group.finish();
}

#[derive(Clone, Debug)]
struct BenchHeader {
    bench_id: String,
    group: String,
    bench_name: String,
    msgs: Option<u64>,
    prod: Option<u64>,
    cons: Option<u64>,
    blocks: Option<u64>,
    block_size: Option<u64>,
    cap: Option<String>,
    elem: Option<String>,
    elem_bytes: Option<u64>,
}

#[derive(Clone, Debug)]
struct TimeSample {
    lower_ns: f64,
    estimate_ns: f64,
    upper_ns: f64,
    unit: String,
}

#[derive(Clone, Debug)]
struct BenchSample {
    header: BenchHeader,
    time: TimeSample,
}

fn merge_bench_criterion_to_csv() -> io::Result<()> {
    if !(env_flag("UBQ_BENCH_CRITERION_TO_CSV") || env_flag("UBQ_BENCH_LOG_TO_CSV")) {
        return Ok(());
    }

    let _ = io::stdout().flush();

    let criterion_root = PathBuf::from(
        env::var("UBQ_BENCH_CRITERION_DIR").unwrap_or_else(|_| "target/criterion".to_string()),
    );
    let csv_path = PathBuf::from(
        env::var("UBQ_BENCH_CSV_PATH").unwrap_or_else(|_| "target/bench.csv".to_string()),
    );

    if !criterion_root.exists() {
        eprintln!(
            "[ubq_perf] criterion output not found; set UBQ_BENCH_CRITERION_DIR or disable UBQ_BENCH_CRITERION_TO_CSV (path={})",
            criterion_root.display()
        );
        return Ok(());
    }

    let samples = parse_criterion_dir(&criterion_root)?;
    if samples.is_empty() {
        eprintln!(
            "[ubq_perf] no criterion estimates found in {}",
            criterion_root.display()
        );
        return Ok(());
    }

    let run_epoch_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let run_id = env::var("UBQ_BENCH_RUN_ID")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| run_epoch_secs.to_string());

    write_bench_csv(&csv_path, run_epoch_secs, &run_id, &samples)?;
    eprintln!(
        "[ubq_perf] merged {} rows into {}",
        samples.len(),
        csv_path.display()
    );
    Ok(())
}

fn parse_criterion_dir(root: &Path) -> io::Result<Vec<BenchSample>> {
    let mut estimate_paths = Vec::new();
    collect_estimates_paths(root, &mut estimate_paths)?;
    let selected = select_estimates_paths(estimate_paths);
    let mut samples = Vec::new();

    for (bench_dir, estimates_path) in selected {
        if let Some(sample) = parse_criterion_sample(root, &bench_dir, &estimates_path)? {
            samples.push(sample);
        }
    }

    Ok(samples)
}

fn collect_estimates_paths(root: &Path, out: &mut Vec<PathBuf>) -> io::Result<()> {
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            let name = path.file_name().and_then(|s| s.to_str()).unwrap_or("");
            if name == "report" {
                continue;
            }
            collect_estimates_paths(&path, out)?;
        } else if path.file_name().and_then(|s| s.to_str()) == Some("estimates.json") {
            out.push(path);
        }
    }

    Ok(())
}

fn select_estimates_paths(paths: Vec<PathBuf>) -> Vec<(PathBuf, PathBuf)> {
    let mut selected: HashMap<PathBuf, (u8, PathBuf)> = HashMap::new();

    for path in paths {
        let run_dir = match path.parent() {
            Some(dir) => dir,
            None => continue,
        };
        let run_kind = run_dir.file_name().and_then(|s| s.to_str()).unwrap_or("");
        if run_kind != "base" && run_kind != "new" {
            continue;
        }
        let bench_dir = match run_dir.parent() {
            Some(dir) => dir.to_path_buf(),
            None => continue,
        };
        let priority = if run_kind == "new" { 1 } else { 0 };
        let entry = selected
            .entry(bench_dir)
            .or_insert((priority, path.clone()));
        if priority > entry.0 {
            *entry = (priority, path.clone());
        }
    }

    selected
        .into_iter()
        .map(|(bench_dir, (_priority, path))| (bench_dir, path))
        .collect()
}

fn parse_criterion_sample(
    root: &Path,
    bench_dir: &Path,
    estimates_path: &Path,
) -> io::Result<Option<BenchSample>> {
    let time = match parse_estimates_json(estimates_path)? {
        Some(time) => time,
        None => return Ok(None),
    };
    let run_dir = estimates_path.parent().unwrap_or(bench_dir);
    let meta = parse_benchmark_meta(run_dir);
    let group_dir = bench_dir.parent().and_then(|parent| {
        if parent == root {
            None
        } else {
            parent
                .file_name()
                .and_then(|s| s.to_str())
                .map(|s| s.to_string())
        }
    });
    let header = build_header(meta, bench_dir, group_dir);

    Ok(Some(BenchSample { header, time }))
}

fn parse_estimates_json(path: &Path) -> io::Result<Option<TimeSample>> {
    let contents = fs::read_to_string(path)?;
    let json: Value = serde_json::from_str(&contents)
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?;
    let (lower_ns, estimate_ns, upper_ns) =
        match extract_estimate(&json, "mean").or_else(|| extract_estimate(&json, "median")) {
            Some(stats) => stats,
            None => return Ok(None),
        };

    Ok(Some(TimeSample {
        lower_ns,
        estimate_ns,
        upper_ns,
        unit: "ns".to_string(),
    }))
}

fn extract_estimate(json: &Value, key: &str) -> Option<(f64, f64, f64)> {
    let stat = json.get(key)?;
    let estimate = stat.get("point_estimate")?.as_f64()?;
    let ci = stat.get("confidence_interval")?;
    let lower = ci.get("lower_bound")?.as_f64()?;
    let upper = ci.get("upper_bound")?.as_f64()?;
    Some((lower, estimate, upper))
}

#[derive(Clone, Debug)]
struct BenchmarkMeta {
    group_id: Option<String>,
    function_id: Option<String>,
    full_id: Option<String>,
}

fn parse_benchmark_meta(run_dir: &Path) -> Option<BenchmarkMeta> {
    let path = run_dir.join("benchmark.json");
    let contents = fs::read_to_string(path).ok()?;
    let json: Value = serde_json::from_str(&contents).ok()?;

    Some(BenchmarkMeta {
        group_id: json
            .get("group_id")
            .and_then(|v| v.as_str())
            .map(|v| v.to_string()),
        function_id: json
            .get("function_id")
            .and_then(|v| v.as_str())
            .map(|v| v.to_string()),
        full_id: json
            .get("full_id")
            .and_then(|v| v.as_str())
            .map(|v| v.to_string()),
    })
}

fn build_header(
    meta: Option<BenchmarkMeta>,
    bench_dir: &Path,
    group_dir: Option<String>,
) -> BenchHeader {
    let bench_dir_name = bench_dir
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("")
        .to_string();

    let (bench_id, group, bench_name, params) = if let Some(meta) = meta {
        let full_id_ref = meta.full_id.as_deref();
        let id_source = meta
            .function_id
            .as_deref()
            .or(full_id_ref)
            .unwrap_or(&bench_dir_name);
        let (bench_name, params) = split_bench_id(id_source);
        let group = if meta.function_id.is_some() {
            meta.group_id.unwrap_or_default()
        } else {
            String::new()
        };
        let bench_id = meta.full_id.clone().unwrap_or_else(|| {
            if !group.is_empty() && !id_source.contains('/') {
                format!("{group}/{id_source}")
            } else {
                id_source.to_string()
            }
        });
        (bench_id, group, bench_name, params)
    } else {
        let (bench_name, params) = split_bench_dir_name(&bench_dir_name);
        let group = group_dir.unwrap_or_default();
        let bench_id = if group.is_empty() {
            bench_dir_name.clone()
        } else {
            format!("{group}/{bench_dir_name}")
        };
        (bench_id, group, bench_name, params)
    };

    let mut header = BenchHeader {
        bench_id,
        group,
        bench_name,
        msgs: None,
        prod: None,
        cons: None,
        blocks: None,
        block_size: None,
        cap: None,
        elem: None,
        elem_bytes: None,
    };

    if let Some(params) = params {
        parse_param_string(&params, &mut header);
    }

    header
}

fn split_bench_id(id: &str) -> (String, Option<String>) {
    if let Some((name, params)) = id.split_once(':') {
        (name.trim().to_string(), Some(params.trim().to_string()))
    } else {
        (id.trim().to_string(), None)
    }
}

fn split_bench_dir_name(name: &str) -> (String, Option<String>) {
    if let Some((bench, params)) = name.split_once("_ ") {
        (bench.trim().to_string(), Some(params.trim().to_string()))
    } else {
        (name.trim().to_string(), None)
    }
}

fn parse_param_string(params: &str, header: &mut BenchHeader) {
    for part in params.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        if let Some((key, value)) = part.split_once('=') {
            let value = value.trim();
            match key.trim() {
                "msgs" => header.msgs = value.parse::<u64>().ok(),
                "prod" => header.prod = value.parse::<u64>().ok(),
                "cons" => header.cons = value.parse::<u64>().ok(),
                "blocks" => header.blocks = value.parse::<u64>().ok(),
                "block_size" => header.block_size = value.parse::<u64>().ok(),
                "cap" => header.cap = Some(value.to_string()),
                "elem" => {
                    header.elem = Some(value.to_string());
                    header.elem_bytes = parse_elem_bytes(value);
                }
                _ => {}
            }
        }
    }
}

fn parse_elem_bytes(elem: &str) -> Option<u64> {
    let start = elem.find('(')?;
    let end = elem.find('B')?;
    if end <= start + 1 {
        return None;
    }
    elem.get((start + 1)..end)?.parse::<u64>().ok()
}

fn write_bench_csv(
    path: &Path,
    run_epoch_secs: u64,
    run_id: &str,
    samples: &[BenchSample],
) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    let mut file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)?;
    let needs_header = file.metadata().map(|m| m.len() == 0).unwrap_or(true);
    if needs_header {
        file.write_all(
            b"run_epoch_secs,run_id,bench_id,group,bench_name,time_lower_ns,time_estimate_ns,time_upper_ns,time_unit,msgs,prod,cons,blocks,block_size,cap,elem,elem_bytes,throughput_msgs_per_sec,ns_per_msg\n",
        )?;
    }

    for sample in samples {
        let lower_ns = sample.time.lower_ns;
        let estimate_ns = sample.time.estimate_ns;
        let upper_ns = sample.time.upper_ns;
        let throughput = sample.header.msgs.and_then(|msgs| {
            if estimate_ns > 0.0 {
                Some(msgs as f64 / (estimate_ns / 1e9))
            } else {
                None
            }
        });
        let ns_per_msg = sample.header.msgs.and_then(|msgs| {
            if msgs > 0 {
                Some(estimate_ns / msgs as f64)
            } else {
                None
            }
        });

        let fields = [
            run_epoch_secs.to_string(),
            csv_escape(run_id),
            csv_escape(&sample.header.bench_id),
            csv_escape(&sample.header.group),
            csv_escape(&sample.header.bench_name),
            fmt_f64(lower_ns),
            fmt_f64(estimate_ns),
            fmt_f64(upper_ns),
            csv_escape(&sample.time.unit),
            fmt_opt_u64(sample.header.msgs),
            fmt_opt_u64(sample.header.prod),
            fmt_opt_u64(sample.header.cons),
            fmt_opt_u64(sample.header.blocks),
            fmt_opt_u64(sample.header.block_size),
            csv_escape(sample.header.cap.as_deref().unwrap_or("")),
            csv_escape(sample.header.elem.as_deref().unwrap_or("")),
            fmt_opt_u64(sample.header.elem_bytes),
            fmt_opt_f64(throughput),
            fmt_opt_f64(ns_per_msg),
        ];

        let line = format!("{}\n", fields.join(","));
        file.write_all(line.as_bytes())?;
    }

    Ok(())
}

fn env_flag(name: &str) -> bool {
    env::var(name)
        .ok()
        .map(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(false)
}

fn csv_escape(value: &str) -> String {
    if value.contains(',') || value.contains('"') || value.contains('\n') {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_string()
    }
}

fn fmt_f64(value: f64) -> String {
    if value.is_finite() {
        format!("{value:.6}")
    } else {
        String::new()
    }
}

fn fmt_opt_f64(value: Option<f64>) -> String {
    value.map(fmt_f64).unwrap_or_default()
}

fn fmt_opt_u64(value: Option<u64>) -> String {
    value.map(|v| v.to_string()).unwrap_or_default()
}

criterion_group!(
    name = criterion_main;
    config = Criterion::default().configure_from_args();
    targets =
        ubq,
        spsc_crossbeam_channel,
        mpsc_crossbeam_channel,
        spmc_crossbeam_channel,
        mpmc_crossbeam_channel,
        spsc_flume_channel,
        mpsc_flume_channel,
        spmc_flume_channel,
        mpmc_flume_channel,
        tokio_mpsc_push_pop,
        async_channel_push_pop
);

fn main() {
    criterion_main();
    if let Err(err) = merge_bench_criterion_to_csv() {
        eprintln!("[ubq_perf] criterion merge failed: {err}");
    }
}
