use std::fs;
use std::path::PathBuf;
use std::sync::{
    Arc, Barrier, OnceLock,
    atomic::{AtomicU64, Ordering},
};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use concurrent_queue::PopError;
use crossbeam_utils::Backoff;
use serde::Serialize;

use ubq::{BLOCK_LENGTH, UBQ};

const SENTINEL: u64 = u64::MAX;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum QueueKind {
    Ubq,
    SegQueue,
    ConcurrentQueue,
}

impl QueueKind {
    fn name(self) -> &'static str {
        match self {
            QueueKind::Ubq => "ubq",
            QueueKind::SegQueue => "segqueue",
            QueueKind::ConcurrentQueue => "concurrent-queue",
        }
    }

    fn parse(input: &str) -> Option<Self> {
        match input.trim().to_ascii_lowercase().as_str() {
            "ubq" => Some(QueueKind::Ubq),
            "segqueue" | "crossbeam" | "crossbeam-segqueue" | "crossbeam-channel" => {
                Some(QueueKind::SegQueue)
            }
            "concurrent-queue" | "concurrent" => Some(QueueKind::ConcurrentQueue),
            _ => None,
        }
    }
}

#[derive(Clone)]
enum Queue {
    Ubq(Arc<UBQ<u64>>),
    SegQueue(Arc<crossbeam_queue::SegQueue<u64>>),
    ConcurrentQueue(Arc<concurrent_queue::ConcurrentQueue<u64>>),
}

fn make_queue(kind: QueueKind) -> Queue {
    match kind {
        QueueKind::Ubq => Queue::Ubq(Arc::new(UBQ::new())),
        QueueKind::SegQueue => Queue::SegQueue(Arc::new(crossbeam_queue::SegQueue::new())),
        QueueKind::ConcurrentQueue => {
            Queue::ConcurrentQueue(Arc::new(concurrent_queue::ConcurrentQueue::unbounded()))
        }
    }
}

fn send(queue: &Queue, value: u64) {
    match queue {
        Queue::Ubq(q) => q.push(value),
        Queue::SegQueue(q) => q.push(value),
        Queue::ConcurrentQueue(q) => q.push(value).expect("send failed"),
    }
}

fn recv(queue: &Queue) -> u64 {
    let backoff = Backoff::new();
    loop {
        match queue {
            Queue::Ubq(q) => {
                if let Some(value) = q.pop() {
                    return value;
                }
            }
            Queue::SegQueue(q) => {
                if let Some(value) = q.pop() {
                    return value;
                }
            }
            Queue::ConcurrentQueue(q) => match q.pop() {
                Ok(value) => return value,
                Err(PopError::Empty) => {}
                Err(PopError::Closed) => panic!("recv failed: queue closed"),
            },
        }
        backoff.snooze();
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ScenarioKind {
    SPSC,
    MPSC,
    SPMC,
    MPMC,
}

impl ScenarioKind {
    fn name(self) -> &'static str {
        match self {
            ScenarioKind::SPSC => "spsc",
            ScenarioKind::MPSC => "mpsc",
            ScenarioKind::SPMC => "spmc",
            ScenarioKind::MPMC => "mpmc",
        }
    }

    fn parse(input: &str) -> Option<Self> {
        match input.trim().to_ascii_lowercase().as_str() {
            "spsc" => Some(ScenarioKind::SPSC),
            "mpsc" => Some(ScenarioKind::MPSC),
            "spmc" => Some(ScenarioKind::SPMC),
            "mpmc" => Some(ScenarioKind::MPMC),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Mode {
    Throughput,
    FillDrain,
}

impl Mode {
    fn name(self) -> &'static str {
        match self {
            Mode::Throughput => "throughput",
            Mode::FillDrain => "fill_drain",
        }
    }

    fn parse(input: &str) -> Option<Self> {
        match input.trim().to_ascii_lowercase().as_str() {
            "throughput" => Some(Mode::Throughput),
            "fill_drain" | "fill-drain" => Some(Mode::FillDrain),
            _ => None,
        }
    }
}

#[derive(Clone, Debug)]
struct ScenarioConfig {
    kind: ScenarioKind,
    producers: usize,
    consumers: usize,
}

#[derive(Debug)]
struct BenchConfig {
    items_per_producer: u64,
    queues: Vec<QueueKind>,
    scenarios: Vec<ScenarioConfig>,
    modes: Vec<Mode>,
    out_path: Option<PathBuf>,
}

#[derive(Serialize)]
struct ScenarioMeta {
    name: String,
    producers: usize,
    consumers: usize,
}

#[derive(Serialize)]
struct Meta {
    timestamp_unix_ms: u128,
    block_cap: usize,
    items_per_producer: u64,
    queues: Vec<String>,
    scenarios: Vec<ScenarioMeta>,
    available_parallelism: Option<usize>,
}

#[derive(Serialize)]
struct Record {
    queue: String,
    scenario: String,
    mode: String,
    producers: usize,
    consumers: usize,
    items_per_producer: u64,
    total_items: u64,
    consumed_items: u64,
    elapsed_ns: u64,
    ops_per_sec: Option<f64>,
    push_elapsed_ns: Option<u64>,
    pop_elapsed_ns: Option<u64>,
    fill_elapsed_ns: Option<u64>,
    drain_elapsed_ns: Option<u64>,
    block_cap: usize,
}

#[derive(Serialize)]
struct Output {
    meta: Meta,
    results: Vec<Record>,
}

fn main() {
    let config = parse_args();
    let output = run_benches(&config);

    let json = serde_json::to_string_pretty(&output).expect("serialize results");

    match config.out_path.as_ref() {
        Some(path) => {
            if let Some(parent) = path.parent() {
                if !parent.as_os_str().is_empty() {
                    fs::create_dir_all(parent).expect("create output directory");
                }
            }
            fs::write(path, json).expect("write output file");
        }
        None => {
            println!("{json}");
        }
    }
}

fn parse_args() -> BenchConfig {
    let mut items_per_producer: u64 = 1_000_000;
    let mut queues = vec![
        QueueKind::Ubq,
        QueueKind::SegQueue,
        QueueKind::ConcurrentQueue,
    ];
    let mut modes = vec![Mode::Throughput, Mode::FillDrain];

    let mut mpsc_producers = 4usize;
    let mut spmc_consumers = 4usize;
    let mut mpmc_producers = 4usize;
    let mut mpmc_consumers = 4usize;

    let mut scenario_filter: Option<Vec<ScenarioKind>> = None;
    let mut out_path: Option<PathBuf> = None;

    let mut args = std::env::args().skip(1).peekable();
    while let Some(arg) = args.next() {
        if arg == "--help" || arg == "-h" {
            print_usage_and_exit();
        }

        let (key, value) = if let Some((key, value)) = arg.split_once('=') {
            (key.to_string(), Some(value.to_string()))
        } else {
            (arg.clone(), None)
        };

        match key.as_str() {
            "--bench" => {
                // Cargo may append --bench when running `cargo bench`.
            }
            "--items-per-producer" => {
                let value = value
                    .or_else(|| args.next())
                    .unwrap_or_else(|| die("--items-per-producer requires a value"));
                items_per_producer = parse_u64(&value, "items_per_producer");
            }
            "--queues" => {
                let value = value
                    .or_else(|| args.next())
                    .unwrap_or_else(|| die("--queues requires a value"));
                queues = parse_list(&value, QueueKind::parse, "queues");
            }
            "--modes" => {
                let value = value
                    .or_else(|| args.next())
                    .unwrap_or_else(|| die("--modes requires a value"));
                modes = parse_list(&value, Mode::parse, "modes");
            }
            "--scenarios" => {
                let value = value
                    .or_else(|| args.next())
                    .unwrap_or_else(|| die("--scenarios requires a value"));
                scenario_filter = Some(parse_list(&value, ScenarioKind::parse, "scenarios"));
            }
            "--mpsc-producers" => {
                let value = value
                    .or_else(|| args.next())
                    .unwrap_or_else(|| die("--mpsc-producers requires a value"));
                mpsc_producers = parse_usize(&value, "mpsc_producers");
            }
            "--spmc-consumers" => {
                let value = value
                    .or_else(|| args.next())
                    .unwrap_or_else(|| die("--spmc-consumers requires a value"));
                spmc_consumers = parse_usize(&value, "spmc_consumers");
            }
            "--mpmc-producers" => {
                let value = value
                    .or_else(|| args.next())
                    .unwrap_or_else(|| die("--mpmc-producers requires a value"));
                mpmc_producers = parse_usize(&value, "mpmc_producers");
            }
            "--mpmc-consumers" => {
                let value = value
                    .or_else(|| args.next())
                    .unwrap_or_else(|| die("--mpmc-consumers requires a value"));
                mpmc_consumers = parse_usize(&value, "mpmc_consumers");
            }
            "--out" => {
                let value = value
                    .or_else(|| args.next())
                    .unwrap_or_else(|| die("--out requires a value"));
                out_path = Some(PathBuf::from(value));
            }
            "--only-ubq" => {
                queues = vec![QueueKind::Ubq];
            }
            "--throughput-only" => {
                modes = vec![Mode::Throughput];
            }
            unknown => {
                die(&format!("Unknown argument: {unknown}"));
            }
        }
    }

    let scenarios = vec![
        ScenarioConfig {
            kind: ScenarioKind::SPSC,
            producers: 1,
            consumers: 1,
        },
        ScenarioConfig {
            kind: ScenarioKind::MPSC,
            producers: mpsc_producers,
            consumers: 1,
        },
        ScenarioConfig {
            kind: ScenarioKind::SPMC,
            producers: 1,
            consumers: spmc_consumers,
        },
        ScenarioConfig {
            kind: ScenarioKind::MPMC,
            producers: mpmc_producers,
            consumers: mpmc_consumers,
        },
    ];

    let scenarios = if let Some(filter) = scenario_filter {
        scenarios
            .into_iter()
            .filter(|scenario| filter.contains(&scenario.kind))
            .collect()
    } else {
        scenarios
    };

    for scenario in &scenarios {
        if scenario.producers == 0 || scenario.consumers == 0 {
            die("producers and consumers must be > 0 for all scenarios");
        }
    }

    if items_per_producer == 0 {
        die("items_per_producer must be > 0");
    }

    BenchConfig {
        items_per_producer,
        queues,
        scenarios,
        modes,
        out_path,
    }
}

fn run_benches(config: &BenchConfig) -> Output {
    let available_parallelism = std::thread::available_parallelism().ok().map(|n| n.get());
    let timestamp_unix_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_millis();

    let meta = Meta {
        timestamp_unix_ms,
        block_cap: BLOCK_LENGTH as usize,
        items_per_producer: config.items_per_producer,
        queues: config.queues.iter().map(|q| q.name().to_string()).collect(),
        scenarios: config
            .scenarios
            .iter()
            .map(|s| ScenarioMeta {
                name: s.kind.name().to_string(),
                producers: s.producers,
                consumers: s.consumers,
            })
            .collect(),
        available_parallelism,
    };

    let mut results = Vec::new();

    for &queue in &config.queues {
        for scenario in &config.scenarios {
            let total_items = total_items(config.items_per_producer, scenario.producers);

            for &mode in &config.modes {
                let record = match mode {
                    Mode::Throughput => {
                        bench_throughput(queue, scenario, config.items_per_producer)
                    }
                    Mode::FillDrain => bench_fill_drain(queue, scenario, config.items_per_producer),
                };

                if record.total_items != total_items {
                    die("internal error: total_items mismatch");
                }

                results.push(record);
            }
        }
    }

    Output { meta, results }
}

fn bench_throughput(
    queue: QueueKind,
    scenario: &ScenarioConfig,
    items_per_producer: u64,
) -> Record {
    let total_items = total_items(items_per_producer, scenario.producers);

    let queue_handle = make_queue(queue);

    let total_threads = scenario.producers + scenario.consumers;
    let ready = Arc::new(Barrier::new(total_threads + 1));
    let start_gate = Arc::new(Barrier::new(total_threads + 1));
    let start = Arc::new(OnceLock::new());

    let producer_max = Arc::new(AtomicU64::new(0));
    let consumer_max = Arc::new(AtomicU64::new(0));
    let consumed_total = Arc::new(AtomicU64::new(0));

    let mut producer_handles = Vec::with_capacity(scenario.producers);
    for producer_id in 0..scenario.producers {
        let queue_handle = queue_handle.clone();
        let ready = ready.clone();
        let start_gate = start_gate.clone();
        let start = start.clone();
        let producer_max = producer_max.clone();
        producer_handles.push(thread::spawn(move || {
            ready.wait();
            start_gate.wait();
            let start: Instant = *start.get().expect("start set");
            let base = (producer_id as u64)
                .checked_mul(items_per_producer)
                .expect("item count overflow");
            for offset in 0..items_per_producer {
                let value = base.checked_add(offset).expect("item count overflow");
                send(&queue_handle, value);
            }
            let end_ns = start.elapsed().as_nanos() as u64;
            producer_max.fetch_max(end_ns, Ordering::Relaxed);
        }));
    }

    let mut consumer_handles = Vec::with_capacity(scenario.consumers);
    for _ in 0..scenario.consumers {
        let queue_handle = queue_handle.clone();
        let ready = ready.clone();
        let start_gate = start_gate.clone();
        let start = start.clone();
        let consumer_max = consumer_max.clone();
        let consumed_total = consumed_total.clone();
        consumer_handles.push(thread::spawn(move || {
            ready.wait();
            start_gate.wait();
            let start = *start.get().expect("start set");
            loop {
                let value = recv(&queue_handle);
                if value == SENTINEL {
                    break;
                }
                consumed_total.fetch_add(1, Ordering::Relaxed);
            }
            let end_ns = start.elapsed().as_nanos() as u64;
            consumer_max.fetch_max(end_ns, Ordering::Relaxed);
        }));
    }

    ready.wait();
    start.set(Instant::now()).ok();
    start_gate.wait();

    for handle in producer_handles {
        handle.join().expect("producer join failed");
    }

    for _ in 0..scenario.consumers {
        send(&queue_handle, SENTINEL);
    }

    for handle in consumer_handles {
        handle.join().expect("consumer join failed");
    }

    let elapsed_ns = start.get().expect("start set").elapsed().as_nanos() as u64;

    let consumed = consumed_total.load(Ordering::Relaxed);
    if consumed != total_items {
        warn_mismatch(queue, scenario, total_items, consumed);
    }

    let ops_per_sec = if elapsed_ns > 0 && consumed > 0 {
        Some(consumed as f64 / (elapsed_ns as f64 / 1_000_000_000.0))
    } else {
        None
    };

    Record {
        queue: queue.name().to_string(),
        scenario: scenario.kind.name().to_string(),
        mode: Mode::Throughput.name().to_string(),
        producers: scenario.producers,
        consumers: scenario.consumers,
        items_per_producer,
        total_items,
        consumed_items: consumed,
        elapsed_ns,
        ops_per_sec,
        push_elapsed_ns: Some(producer_max.load(Ordering::Relaxed)),
        pop_elapsed_ns: Some(consumer_max.load(Ordering::Relaxed)),
        fill_elapsed_ns: None,
        drain_elapsed_ns: None,
        block_cap: BLOCK_LENGTH as usize,
    }
}

fn bench_fill_drain(
    queue: QueueKind,
    scenario: &ScenarioConfig,
    items_per_producer: u64,
) -> Record {
    let total_items = total_items(items_per_producer, scenario.producers);

    let queue_handle = make_queue(queue);

    let fill_elapsed = run_producers_only(&queue_handle, scenario.producers, items_per_producer);

    for _ in 0..scenario.consumers {
        send(&queue_handle, SENTINEL);
    }

    let (drain_elapsed, consumed) = run_consumers_only(&queue_handle, scenario.consumers);

    if consumed != total_items {
        warn_mismatch(queue, scenario, total_items, consumed);
    }

    let elapsed_ns = (fill_elapsed + drain_elapsed).as_nanos() as u64;

    Record {
        queue: queue.name().to_string(),
        scenario: scenario.kind.name().to_string(),
        mode: Mode::FillDrain.name().to_string(),
        producers: scenario.producers,
        consumers: scenario.consumers,
        items_per_producer,
        total_items,
        consumed_items: consumed,
        elapsed_ns,
        ops_per_sec: None,
        push_elapsed_ns: None,
        pop_elapsed_ns: None,
        fill_elapsed_ns: Some(fill_elapsed.as_nanos() as u64),
        drain_elapsed_ns: Some(drain_elapsed.as_nanos() as u64),
        block_cap: BLOCK_LENGTH as usize,
    }
}

fn run_producers_only(queue_handle: &Queue, producers: usize, items_per_producer: u64) -> Duration {
    let ready = Arc::new(Barrier::new(producers + 1));
    let start_gate = Arc::new(Barrier::new(producers + 1));
    let start = Arc::new(OnceLock::new());
    let max_end = Arc::new(AtomicU64::new(0));

    let mut handles = Vec::with_capacity(producers);
    for producer_id in 0..producers {
        let queue_handle = queue_handle.clone();
        let ready = ready.clone();
        let start_gate = start_gate.clone();
        let start = start.clone();
        let max_end = max_end.clone();
        handles.push(thread::spawn(move || {
            ready.wait();
            start_gate.wait();
            let start: Instant = *start.get().expect("start set");
            let base = (producer_id as u64)
                .checked_mul(items_per_producer)
                .expect("item count overflow");
            for offset in 0..items_per_producer {
                let value = base.checked_add(offset).expect("item count overflow");
                send(&queue_handle, value);
            }
            let end_ns = start.elapsed().as_nanos() as u64;
            max_end.fetch_max(end_ns, Ordering::Relaxed);
        }));
    }

    ready.wait();
    start.set(Instant::now()).ok();
    start_gate.wait();

    for handle in handles {
        handle.join().expect("producer join failed");
    }

    Duration::from_nanos(max_end.load(Ordering::Relaxed))
}

fn run_consumers_only(queue_handle: &Queue, consumers: usize) -> (Duration, u64) {
    let ready = Arc::new(Barrier::new(consumers + 1));
    let start_gate = Arc::new(Barrier::new(consumers + 1));
    let start = Arc::new(OnceLock::new());
    let max_end = Arc::new(AtomicU64::new(0));
    let consumed_total = Arc::new(AtomicU64::new(0));

    let mut handles = Vec::with_capacity(consumers);
    for _ in 0..consumers {
        let queue_handle = queue_handle.clone();
        let ready = ready.clone();
        let start_gate = start_gate.clone();
        let start = start.clone();
        let max_end = max_end.clone();
        let consumed_total = consumed_total.clone();
        handles.push(thread::spawn(move || {
            ready.wait();
            start_gate.wait();
            let start: Instant = *start.get().expect("start set");
            loop {
                let value = recv(&queue_handle);
                if value == SENTINEL {
                    break;
                }
                consumed_total.fetch_add(1, Ordering::Relaxed);
            }
            let end_ns = start.elapsed().as_nanos() as u64;
            max_end.fetch_max(end_ns, Ordering::Relaxed);
        }));
    }

    ready.wait();
    start.set(Instant::now()).ok();
    start_gate.wait();

    for handle in handles {
        handle.join().expect("consumer join failed");
    }

    let elapsed = Duration::from_nanos(max_end.load(Ordering::Relaxed));
    let consumed = consumed_total.load(Ordering::Relaxed);
    (elapsed, consumed)
}

fn total_items(items_per_producer: u64, producers: usize) -> u64 {
    let total = items_per_producer
        .checked_mul(producers as u64)
        .unwrap_or_else(|| die("total items overflow"));
    if total >= SENTINEL {
        die("total items must be < u64::MAX");
    }
    total
}

fn parse_list<T>(value: &str, parse: impl Fn(&str) -> Option<T>, label: &str) -> Vec<T> {
    let mut out = Vec::new();
    for part in value.split(',') {
        let item = parse(part).unwrap_or_else(|| die(&format!("Invalid {label} entry: {part}")));
        out.push(item);
    }
    if out.is_empty() {
        die(&format!("{label} list cannot be empty"));
    }
    out
}

fn parse_u64(value: &str, label: &str) -> u64 {
    value
        .parse()
        .unwrap_or_else(|_| die(&format!("Invalid {label}: {value}")))
}

fn parse_usize(value: &str, label: &str) -> usize {
    value
        .parse()
        .unwrap_or_else(|_| die(&format!("Invalid {label}: {value}")))
}

fn die(message: &str) -> ! {
    eprintln!("{message}");
    std::process::exit(1);
}

fn warn_mismatch(queue: QueueKind, scenario: &ScenarioConfig, expected: u64, got: u64) {
    eprintln!(
        "warning: {queue} {scenario} consumed count mismatch: expected {expected}, got {got}",
        queue = queue.name(),
        scenario = scenario.kind.name()
    );
}

fn print_usage_and_exit() -> ! {
    let usage = r#"UBQ bench harness

Usage:
  cargo bench --bench ubq_bench -- [options]

Options:
  --items-per-producer N    Items each producer enqueues (default: 1_000_000)
  --queues LIST             Comma list: ubq,segqueue,concurrent-queue
  --modes LIST              Comma list: throughput,fill_drain
  --scenarios LIST          Comma list: spsc,mpsc,spmc,mpmc
  --mpsc-producers N        Producers for MPSC (default: 4)
  --spmc-consumers N        Consumers for SPMC (default: 4)
  --mpmc-producers N        Producers for MPMC (default: 4)
  --mpmc-consumers N        Consumers for MPMC (default: 4)
  --out PATH                Write JSON output to PATH instead of stdout
  --only-ubq                Shortcut for --queues=ubq
  --throughput-only         Shortcut for --modes=throughput
"#;
    println!("{usage}");
    std::process::exit(0);
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::panic::{AssertUnwindSafe, resume_unwind};
    use std::sync::{
        Arc, Barrier, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
        mpsc::{self, RecvTimeoutError},
    };
    use std::thread;
    use std::time::Duration;

    const DEFAULT_TEST_ITEMS_PER_PRODUCER: u64 = 200;
    const DEFAULT_TEST_TIMEOUT_SECS: u64 = 30;

    fn test_timeout() -> Duration {
        std::env::var("UBQ_TEST_TIMEOUT_SECS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .map(Duration::from_secs)
            .unwrap_or(Duration::from_secs(DEFAULT_TEST_TIMEOUT_SECS))
    }

    fn run_with_timeout(name: &'static str, f: impl FnOnce() + Send + 'static) {
        run_with_timeout_context(
            name,
            Arc::new(Mutex::new("context unavailable".to_string())),
            f,
        );
    }

    fn run_with_timeout_context(
        name: &'static str,
        timeout_context: Arc<Mutex<String>>,
        f: impl FnOnce() + Send + 'static,
    ) {
        let timeout = test_timeout();
        let (tx, rx) = mpsc::channel();

        thread::spawn(move || {
            let result = std::panic::catch_unwind(AssertUnwindSafe(f));
            let _ = tx.send(result);
        });

        match rx.recv_timeout(timeout) {
            Ok(Ok(())) => {}
            Ok(Err(payload)) => resume_unwind(payload),
            Err(RecvTimeoutError::Timeout) => {
                let context = timeout_context
                    .lock()
                    .map(|value| value.clone())
                    .unwrap_or_else(|_| "context unavailable (poisoned)".to_string());
                panic!(
                    "test `{name}` timed out after {}s (last context: {context})",
                    timeout.as_secs()
                )
            }
            Err(RecvTimeoutError::Disconnected) => {
                panic!("test `{name}` ended unexpectedly before reporting")
            }
        }
    }

    fn set_timeout_context(timeout_context: &Arc<Mutex<String>>, value: impl Into<String>) {
        if let Ok(mut slot) = timeout_context.lock() {
            *slot = value.into();
        }
    }

    fn test_items_per_producer() -> u64 {
        std::env::var("UBQ_BENCH_TEST_ITEMS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(DEFAULT_TEST_ITEMS_PER_PRODUCER)
    }

    fn test_scenarios() -> Vec<ScenarioConfig> {
        vec![
            ScenarioConfig {
                kind: ScenarioKind::SPSC,
                producers: 1,
                consumers: 1,
            },
            ScenarioConfig {
                kind: ScenarioKind::MPSC,
                producers: 3,
                consumers: 1,
            },
            ScenarioConfig {
                kind: ScenarioKind::SPMC,
                producers: 1,
                consumers: 3,
            },
            ScenarioConfig {
                kind: ScenarioKind::MPMC,
                producers: 3,
                consumers: 3,
            },
        ]
    }

    fn test_queues() -> [QueueKind; 3] {
        [
            QueueKind::Ubq,
            QueueKind::SegQueue,
            QueueKind::ConcurrentQueue,
        ]
    }

    fn scenario_label(scenario: &ScenarioConfig) -> String {
        format!(
            "{}({}p/{}c)",
            scenario.kind.name(),
            scenario.producers,
            scenario.consumers
        )
    }

    fn record_error(
        error_count: &AtomicUsize,
        first_error: &Mutex<Option<String>>,
        message: String,
    ) {
        error_count.fetch_add(1, Ordering::Relaxed);
        let mut slot = first_error.lock().expect("first_error lock");
        if slot.is_none() {
            *slot = Some(message);
        }
    }

    fn assert_no_integrity_errors(
        queue: QueueKind,
        scenario: &ScenarioConfig,
        mode: Mode,
        seen: &[AtomicBool],
        error_count: &AtomicUsize,
        first_error: &Mutex<Option<String>>,
        expected_total: u64,
        consumed_total: u64,
    ) {
        let label = scenario_label(scenario);
        assert_eq!(
            consumed_total,
            expected_total,
            "{} {} consumed mismatch in {} mode",
            queue.name(),
            label,
            mode.name()
        );

        let errors = error_count.load(Ordering::Relaxed);
        if errors > 0 {
            let first = first_error.lock().expect("first_error lock");
            let message = first.as_deref().unwrap_or("unknown consumer error");
            panic!(
                "{} {} integrity errors in {} mode: {errors} (first: {message})",
                queue.name(),
                label,
                mode.name()
            );
        }

        let mut missing_count = 0usize;
        let mut missing_samples = Vec::new();
        for (idx, seen_flag) in seen.iter().enumerate() {
            if !seen_flag.load(Ordering::Acquire) {
                missing_count += 1;
                if missing_samples.len() < 10 {
                    missing_samples.push(idx);
                }
            }
        }
        assert_eq!(
            missing_count,
            0,
            "{} {} missing values in {} mode: {} (samples: {:?})",
            queue.name(),
            label,
            mode.name(),
            missing_count,
            missing_samples
        );
    }

    fn run_throughput_integrity(
        queue: QueueKind,
        scenario: &ScenarioConfig,
        items_per_producer: u64,
    ) {
        let total = total_items(items_per_producer, scenario.producers);
        let seen = Arc::new(
            (0..usize::try_from(total).expect("total items should fit usize"))
                .map(|_| AtomicBool::new(false))
                .collect::<Vec<_>>(),
        );
        let consumed_total = Arc::new(AtomicUsize::new(0));
        let error_count = Arc::new(AtomicUsize::new(0));
        let first_error = Arc::new(Mutex::new(None::<String>));

        let queue_handle = make_queue(queue);
        let total_threads = scenario.producers + scenario.consumers;
        let ready = Arc::new(Barrier::new(total_threads + 1));
        let start_gate = Arc::new(Barrier::new(total_threads + 1));

        let mut producer_handles = Vec::with_capacity(scenario.producers);
        for producer_id in 0..scenario.producers {
            let queue_handle = queue_handle.clone();
            let ready = Arc::clone(&ready);
            let start_gate = Arc::clone(&start_gate);
            producer_handles.push(thread::spawn(move || {
                ready.wait();
                start_gate.wait();
                let base = (producer_id as u64)
                    .checked_mul(items_per_producer)
                    .expect("item count overflow");
                for offset in 0..items_per_producer {
                    let value = base.checked_add(offset).expect("item count overflow");
                    send(&queue_handle, value);
                }
            }));
        }

        let mut consumer_handles = Vec::with_capacity(scenario.consumers);
        for _ in 0..scenario.consumers {
            let queue_handle = queue_handle.clone();
            let ready = Arc::clone(&ready);
            let start_gate = Arc::clone(&start_gate);
            let seen = Arc::clone(&seen);
            let consumed_total = Arc::clone(&consumed_total);
            let error_count = Arc::clone(&error_count);
            let first_error = Arc::clone(&first_error);
            consumer_handles.push(thread::spawn(move || {
                ready.wait();
                start_gate.wait();
                loop {
                    let value = recv(&queue_handle);
                    if value == SENTINEL {
                        break;
                    }
                    let idx = value as usize;
                    if idx >= seen.len() {
                        record_error(
                            &error_count,
                            &first_error,
                            format!("out-of-range value {value}"),
                        );
                        continue;
                    }
                    let already_seen = seen[idx].swap(true, Ordering::AcqRel);
                    if already_seen {
                        record_error(
                            &error_count,
                            &first_error,
                            format!("duplicate value {value}"),
                        );
                    }
                    consumed_total.fetch_add(1, Ordering::Relaxed);
                }
            }));
        }

        ready.wait();
        start_gate.wait();

        for handle in producer_handles {
            handle.join().expect("producer join failed");
        }

        for _ in 0..scenario.consumers {
            send(&queue_handle, SENTINEL);
        }

        for handle in consumer_handles {
            handle.join().expect("consumer join failed");
        }

        assert_no_integrity_errors(
            queue,
            scenario,
            Mode::Throughput,
            seen.as_slice(),
            &error_count,
            &first_error,
            total,
            consumed_total.load(Ordering::Relaxed) as u64,
        );
    }

    fn run_fill_drain_integrity(
        queue: QueueKind,
        scenario: &ScenarioConfig,
        items_per_producer: u64,
    ) {
        let total = total_items(items_per_producer, scenario.producers);
        let seen = Arc::new(
            (0..usize::try_from(total).expect("total items should fit usize"))
                .map(|_| AtomicBool::new(false))
                .collect::<Vec<_>>(),
        );
        let consumed_total = Arc::new(AtomicUsize::new(0));
        let error_count = Arc::new(AtomicUsize::new(0));
        let first_error = Arc::new(Mutex::new(None::<String>));

        let queue_handle = make_queue(queue);
        run_producers_only(&queue_handle, scenario.producers, items_per_producer);

        for _ in 0..scenario.consumers {
            send(&queue_handle, SENTINEL);
        }

        let ready = Arc::new(Barrier::new(scenario.consumers + 1));
        let start_gate = Arc::new(Barrier::new(scenario.consumers + 1));

        let mut consumer_handles = Vec::with_capacity(scenario.consumers);
        for _ in 0..scenario.consumers {
            let queue_handle = queue_handle.clone();
            let ready = Arc::clone(&ready);
            let start_gate = Arc::clone(&start_gate);
            let seen = Arc::clone(&seen);
            let consumed_total = Arc::clone(&consumed_total);
            let error_count = Arc::clone(&error_count);
            let first_error = Arc::clone(&first_error);
            consumer_handles.push(thread::spawn(move || {
                ready.wait();
                start_gate.wait();
                loop {
                    let value = recv(&queue_handle);
                    if value == SENTINEL {
                        break;
                    }
                    let idx = value as usize;
                    if idx >= seen.len() {
                        record_error(
                            &error_count,
                            &first_error,
                            format!("out-of-range value {value}"),
                        );
                        continue;
                    }
                    let already_seen = seen[idx].swap(true, Ordering::AcqRel);
                    if already_seen {
                        record_error(
                            &error_count,
                            &first_error,
                            format!("duplicate value {value}"),
                        );
                    }
                    consumed_total.fetch_add(1, Ordering::Relaxed);
                }
            }));
        }

        ready.wait();
        start_gate.wait();

        for handle in consumer_handles {
            handle.join().expect("consumer join failed");
        }

        assert_no_integrity_errors(
            queue,
            scenario,
            Mode::FillDrain,
            seen.as_slice(),
            &error_count,
            &first_error,
            total,
            consumed_total.load(Ordering::Relaxed) as u64,
        );
    }

    #[test]
    fn bench_throughput_records_match_expectations() {
        let timeout_context = Arc::new(Mutex::new("starting benchmark matrix".to_string()));
        run_with_timeout_context(
            "bench_throughput_records_match_expectations",
            Arc::clone(&timeout_context),
            move || {
                let items_per_producer = test_items_per_producer();
                for queue in test_queues() {
                    for scenario in test_scenarios() {
                        set_timeout_context(
                            &timeout_context,
                            format!(
                                "queue={} scenario={} mode={}",
                                queue.name(),
                                scenario_label(&scenario),
                                Mode::Throughput.name()
                            ),
                        );
                        let expected_total = total_items(items_per_producer, scenario.producers);
                        let record = bench_throughput(queue, &scenario, items_per_producer);

                        assert_eq!(record.queue, queue.name());
                        assert_eq!(record.scenario, scenario.kind.name());
                        assert_eq!(record.mode, Mode::Throughput.name());
                        assert_eq!(record.producers, scenario.producers);
                        assert_eq!(record.consumers, scenario.consumers);
                        assert_eq!(record.total_items, expected_total);
                        assert_eq!(record.consumed_items, expected_total);
                        assert!(record.push_elapsed_ns.is_some());
                        assert!(record.pop_elapsed_ns.is_some());
                        assert!(record.fill_elapsed_ns.is_none());
                        assert!(record.drain_elapsed_ns.is_none());
                    }
                }
                set_timeout_context(&timeout_context, "completed benchmark matrix");
            },
        );
    }

    #[test]
    fn bench_fill_drain_records_match_expectations() {
        let timeout_context = Arc::new(Mutex::new("starting benchmark matrix".to_string()));
        run_with_timeout_context(
            "bench_fill_drain_records_match_expectations",
            Arc::clone(&timeout_context),
            move || {
                let items_per_producer = test_items_per_producer();
                for queue in test_queues() {
                    for scenario in test_scenarios() {
                        set_timeout_context(
                            &timeout_context,
                            format!(
                                "queue={} scenario={} mode={}",
                                queue.name(),
                                scenario_label(&scenario),
                                Mode::FillDrain.name()
                            ),
                        );
                        let expected_total = total_items(items_per_producer, scenario.producers);
                        let record = bench_fill_drain(queue, &scenario, items_per_producer);

                        assert_eq!(record.queue, queue.name());
                        assert_eq!(record.scenario, scenario.kind.name());
                        assert_eq!(record.mode, Mode::FillDrain.name());
                        assert_eq!(record.producers, scenario.producers);
                        assert_eq!(record.consumers, scenario.consumers);
                        assert_eq!(record.total_items, expected_total);
                        assert_eq!(record.consumed_items, expected_total);
                        assert!(record.push_elapsed_ns.is_none());
                        assert!(record.pop_elapsed_ns.is_none());
                        assert!(record.fill_elapsed_ns.is_some());
                        assert!(record.drain_elapsed_ns.is_some());
                    }
                }
                set_timeout_context(&timeout_context, "completed benchmark matrix");
            },
        );
    }

    #[test]
    fn throughput_integrity_smoke_all_paths() {
        let timeout_context = Arc::new(Mutex::new("starting integrity matrix".to_string()));
        run_with_timeout_context(
            "throughput_integrity_smoke_all_paths",
            Arc::clone(&timeout_context),
            move || {
                let items_per_producer = test_items_per_producer();
                for queue in test_queues() {
                    for scenario in test_scenarios() {
                        set_timeout_context(
                            &timeout_context,
                            format!(
                                "queue={} scenario={} mode={}",
                                queue.name(),
                                scenario_label(&scenario),
                                Mode::Throughput.name()
                            ),
                        );
                        run_throughput_integrity(queue, &scenario, items_per_producer);
                    }
                }
                set_timeout_context(&timeout_context, "completed integrity matrix");
            },
        );
    }

    #[test]
    fn fill_drain_integrity_smoke_all_paths() {
        let timeout_context = Arc::new(Mutex::new("starting integrity matrix".to_string()));
        run_with_timeout_context(
            "fill_drain_integrity_smoke_all_paths",
            Arc::clone(&timeout_context),
            move || {
                let items_per_producer = test_items_per_producer();
                for queue in test_queues() {
                    for scenario in test_scenarios() {
                        set_timeout_context(
                            &timeout_context,
                            format!(
                                "queue={} scenario={} mode={}",
                                queue.name(),
                                scenario_label(&scenario),
                                Mode::FillDrain.name()
                            ),
                        );
                        run_fill_drain_integrity(queue, &scenario, items_per_producer);
                    }
                }
                set_timeout_context(&timeout_context, "completed integrity matrix");
            },
        );
    }
}
