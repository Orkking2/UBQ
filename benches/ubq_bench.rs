use concurrent_queue::PopError;
use crossbeam_utils::Backoff;
use serde::Serialize;
use std::{
    fs,
    num::NonZero,
    path::PathBuf,
    sync::{
        Arc, Barrier, OnceLock,
        atomic::{AtomicU64, Ordering},
    },
    thread::{self, JoinHandle, available_parallelism},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use ubq::{ConfiguredUBQ, align, backoff};

const SENTINEL: u64 = u64::MAX;
#[cfg(test)]
#[allow(dead_code)]
const DEFAULT_UBQ_LABEL: &str = "balanced,1,2047,crossbeam";

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

trait BenchQueue: Send + Sync + 'static {
    fn new_queue() -> Arc<Self>
    where
        Self: Sized;

    fn send_value(&self, value: u64);
    fn recv_value(&self) -> u64;
}

impl<B, const POOL: usize, const BLOCK_SIZE: usize, A> BenchQueue
    for ConfiguredUBQ<u64, B, POOL, BLOCK_SIZE, A>
where
    B: backoff::BackoffPolicy + 'static,
    A: Send + Sync + 'static,
{
    fn new_queue() -> Arc<Self> {
        Arc::new(Self::new())
    }

    fn send_value(&self, value: u64) {
        self.push(value);
    }

    fn recv_value(&self) -> u64 {
        let backoff = Backoff::new();
        loop {
            if let Some(value) = self.pop() {
                return value;
            }
            backoff.snooze();
        }
    }
}

impl BenchQueue for crossbeam_queue::SegQueue<u64> {
    fn new_queue() -> Arc<Self> {
        Arc::new(Self::new())
    }

    fn send_value(&self, value: u64) {
        self.push(value);
    }

    fn recv_value(&self) -> u64 {
        let backoff = Backoff::new();
        loop {
            if let Some(value) = self.pop() {
                return value;
            }
            backoff.snooze();
        }
    }
}

impl BenchQueue for concurrent_queue::ConcurrentQueue<u64> {
    fn new_queue() -> Arc<Self> {
        Arc::new(Self::unbounded())
    }

    fn send_value(&self, value: u64) {
        self.push(value).expect("send failed");
    }

    fn recv_value(&self) -> u64 {
        let backoff = Backoff::new();
        loop {
            match self.pop() {
                Ok(value) => return value,
                Err(PopError::Empty) => {}
                Err(PopError::Closed) => panic!("recv failed: queue closed"),
            }
            backoff.snooze();
        }
    }
}

// fn supports_mutable_placeholder(queue: QueueKind) -> bool {
//     matches!(queue, QueueKind::Ubq | QueueKind::SegQueue)
// }

// fn send_mutable_placeholder(queue: &Queue, value: u64) {
//     match queue {
//         Queue::Ubq(q) => q.push(value),
//         Queue::SegQueue(q) => q.push(value),
//         Queue::ConcurrentQueue(_) => panic!("mutable placeholder queue unsupported"),
//     }
// }

// fn recv_mutable_placeholder(queue: &Queue) -> u64 {
//     let backoff = Backoff::new();
//     loop {
//         match queue {
//             Queue::Ubq(q) => {
//                 if let Some(value) = q.pop() {
//                     return value;
//                 }
//             }
//             Queue::SegQueue(q) => {
//                 if let Some(value) = q.pop() {
//                     return value;
//                 }
//             }
//             Queue::ConcurrentQueue(_) => panic!("mutable placeholder queue unsupported"),
//         }
//         backoff.snooze();
//     }
// }

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
    name: String,
    producers: usize,
    consumers: usize,
}

impl ScenarioConfig {
    fn new(producers: usize, consumers: usize) -> Self {
        Self {
            name: format!("{producers}p{consumers}c"),
            producers,
            consumers,
        }
    }
}

#[derive(Debug)]
struct BenchConfig {
    items_per_producer: u64,
    queues: Vec<QueueKind>,
    packed_scenarios: Vec<Vec<ScenarioConfig>>,
    modes: Vec<Mode>,
    block_cap: usize,
    ubq_label: String,
    machine_label: String,
    out_path: Option<PathBuf>,
    available_parallelism: usize,
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
    ubq_label: String,
    machine_label: String,
    queues: Vec<String>,
    scenarios: Vec<ScenarioMeta>,
    available_parallelism: usize,
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

struct UbqBenchEntry {
    label: &'static str,
    block_cap: usize,
    #[cfg(test)]
    #[allow(dead_code)]
    throughput: fn(&ScenarioConfig, u64) -> Record,
    #[cfg(test)]
    #[allow(dead_code)]
    fill_drain: fn(&ScenarioConfig, u64) -> Record,
    factory: fn(Arc<BenchConfig>, (usize, usize), u64) -> JoinHandle<Vec<Record>>,
}

macro_rules! ubq_bench_entry {
    ($label:expr, $backoff:path, $pool:literal, $block:literal, $align:path) => {
        UbqBenchEntry {
            label: $label,
            block_cap: $block,
            #[cfg(test)]
            throughput: |scenario, items_per_producer| {
                bench_throughput_for::<ConfiguredUBQ<u64, $backoff, $pool, $block, $align>>(
                    "ubq",
                    $block,
                    scenario,
                    items_per_producer,
                )
            },
            #[cfg(test)]
            fill_drain: |scenario, items_per_producer| {
                bench_fill_drain_for::<ConfiguredUBQ<u64, $backoff, $pool, $block, $align>>(
                    "ubq",
                    $block,
                    scenario,
                    items_per_producer,
                )
            },
            factory: |config, packed_indices, items_per_producer| {
                bench_factory_typed::<ConfiguredUBQ<u64, $backoff, $pool, $block, $align>>(
                    "ubq",
                    $block,
                    config,
                    packed_indices,
                    items_per_producer,
                )
            },
        }
    };
}

macro_rules! ubq_block_entries {
    ($preset_name:literal, $pool:literal, $backoff_name:literal, $backoff:path) => {
        vec![
            ubq_bench_entry!(
                concat!($preset_name, ",", stringify!($pool), ",31,", $backoff_name),
                $backoff,
                $pool,
                31,
                align::A64
            ),
            ubq_bench_entry!(
                concat!($preset_name, ",", stringify!($pool), ",63,", $backoff_name),
                $backoff,
                $pool,
                63,
                align::A128
            ),
            ubq_bench_entry!(
                concat!($preset_name, ",", stringify!($pool), ",127,", $backoff_name),
                $backoff,
                $pool,
                127,
                align::A256
            ),
            ubq_bench_entry!(
                concat!($preset_name, ",", stringify!($pool), ",255,", $backoff_name),
                $backoff,
                $pool,
                255,
                align::A512
            ),
            ubq_bench_entry!(
                concat!($preset_name, ",", stringify!($pool), ",511,", $backoff_name),
                $backoff,
                $pool,
                511,
                align::A1024
            ),
            ubq_bench_entry!(
                concat!(
                    $preset_name,
                    ",",
                    stringify!($pool),
                    ",1023,",
                    $backoff_name
                ),
                $backoff,
                $pool,
                1023,
                align::A2048
            ),
            ubq_bench_entry!(
                concat!(
                    $preset_name,
                    ",",
                    stringify!($pool),
                    ",2047,",
                    $backoff_name
                ),
                $backoff,
                $pool,
                2047,
                align::A4096
            ),
            ubq_bench_entry!(
                concat!(
                    $preset_name,
                    ",",
                    stringify!($pool),
                    ",4095,",
                    $backoff_name
                ),
                $backoff,
                $pool,
                4095,
                align::A8192
            ),
        ]
    };
}

macro_rules! ubq_backoff_entries {
    ($preset_name:literal, $pool:literal) => {{
        let mut entries = ubq_block_entries!($preset_name, $pool, "crossbeam", backoff::Crossbeam);
        entries.extend(ubq_block_entries!(
            $preset_name,
            $pool,
            "yield",
            backoff::Yield
        ));
        entries
    }};
}

static UBQ_BENCH_REGISTRY: OnceLock<Vec<UbqBenchEntry>> = OnceLock::new();

fn ubq_bench_registry() -> &'static [UbqBenchEntry] {
    UBQ_BENCH_REGISTRY
        .get_or_init(|| {
            let mut entries = Vec::new();
            entries.extend(ubq_backoff_entries!("balanced", 0));
            entries.extend(ubq_backoff_entries!("balanced", 1));
            entries.extend(ubq_backoff_entries!("balanced", 2));
            entries.extend(ubq_backoff_entries!("balanced", 4));
            entries.extend(ubq_backoff_entries!("balanced", 8));
            entries.extend(ubq_backoff_entries!("balanced", 16));
            entries.extend(ubq_backoff_entries!("balanced", 32));
            entries.extend(ubq_backoff_entries!("balanced", 64));
            entries
        })
        .as_slice()
}

fn normalize_ubq_label(input: &str) -> String {
    input.trim().to_ascii_lowercase()
}

fn find_ubq_entry(label: &str) -> Option<&'static UbqBenchEntry> {
    let normalized = normalize_ubq_label(label);
    ubq_bench_registry()
        .iter()
        .find(|entry| entry.label == normalized)
}

fn main() {
    let config = Arc::new(parse_args());
    let output = run_benches_parallel(config.clone());

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
    let mut scenarios = default_scenarios();
    let mut ubq_label = "".to_string();
    let mut machine_label = "".to_string();
    let mut out_path: Option<PathBuf> = None;

    let mut available_parallelism = available_parallelism().ok().map(NonZero::get);

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
                scenarios = parse_list(&value, parse_scenario_token, "scenarios");
            }
            "--out" => {
                let value = value
                    .or_else(|| args.next())
                    .unwrap_or_else(|| die("--out requires a value"));
                out_path = Some(PathBuf::from(value));
            }
            "--ubq-label" => {
                ubq_label = value
                    .or_else(|| args.next())
                    .unwrap_or_else(|| die("--ubq-label requires a value"));
                if ubq_label.trim().is_empty() {
                    die("ubq_label cannot be empty");
                }
            }
            "--machine-label" => {
                machine_label = value
                    .or_else(|| args.next())
                    .unwrap_or_else(|| die("--machine-label requires a value"));
                if machine_label.trim().is_empty() {
                    die("machine_label cannot be empty");
                }
            }
            "--available-parallelism" | "--ap" => {
                let value = value
                    .or_else(|| args.next())
                    .unwrap_or_else(|| die("--available-parallelism requires a value"));
                available_parallelism = Some(parse_u64(&value, "available_parallelism") as usize);
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

    if ubq_label.len() == 0 {
        die("please supply a non-empty UBQ label using --ubq-label=<preset,pool,block,backoff>");
    }

    if machine_label.len() == 0 {
        die("please supply a non-zero label using --machine-label={}");
    }

    let ubq_label = normalize_ubq_label(&ubq_label);
    let block_cap = find_ubq_entry(&ubq_label)
        .map(|entry| entry.block_cap)
        .unwrap_or_else(|| die(&format!("unsupported UBQ label: {ubq_label}")));

    let available_parallelism = available_parallelism.unwrap_or_else(|| die("unable to determine available parallelism, please pass a --available-parallelism value"));

    for scenario in &scenarios {
        if scenario.producers == 0 || scenario.consumers == 0 {
            die("producers and consumers must be > 0 for all scenarios");
        }
    }

    scenarios.sort_by(|lhs, rhs| scenario_total_threads(lhs).cmp(&scenario_total_threads(rhs)));

    let packed_scenarios = scenarios
        .into_iter()
        .filter(|scenario| scenario_total_threads(scenario) <= available_parallelism)
        .fold(
            Vec::new(),
            |mut vec: Vec<(usize, Vec<ScenarioConfig>)>, scenario| {
                if let Some((total_threads, scenario_pack)) = vec.last_mut() {
                    if total_threads
                        .checked_add(scenario_total_threads(&scenario))
                        .map(|total| total <= available_parallelism)
                        .unwrap_or(false)
                    {
                        *total_threads += scenario_total_threads(&scenario);

                        scenario_pack.push(scenario);
                        return vec;
                    }
                }

                vec.push((scenario_total_threads(&scenario), vec![scenario]));
                vec
            },
        )
        .into_iter()
        .map(|(.., v)| v)
        .collect();

    if items_per_producer == 0 {
        die("items_per_producer must be > 0");
    }

    BenchConfig {
        items_per_producer,
        queues,
        packed_scenarios,
        modes,
        block_cap,
        ubq_label,
        machine_label,
        out_path,
        available_parallelism,
    }
}

fn default_scenarios() -> Vec<ScenarioConfig> {
    vec![
        ScenarioConfig::new(1, 1),
        ScenarioConfig::new(1, 4),
        ScenarioConfig::new(1, 8),
        ScenarioConfig::new(1, 16),
        ScenarioConfig::new(1, 32),
        ScenarioConfig::new(1, 64),
        ScenarioConfig::new(4, 1),
        ScenarioConfig::new(4, 4),
        ScenarioConfig::new(4, 8),
        ScenarioConfig::new(8, 1),
        ScenarioConfig::new(8, 4),
        ScenarioConfig::new(8, 8),
        ScenarioConfig::new(8, 16),
        ScenarioConfig::new(16, 1),
        ScenarioConfig::new(16, 8),
        ScenarioConfig::new(16, 16),
        ScenarioConfig::new(16, 32),
        ScenarioConfig::new(32, 1),
        ScenarioConfig::new(32, 16),
        ScenarioConfig::new(32, 32),
        ScenarioConfig::new(32, 64),
        ScenarioConfig::new(64, 1),
        ScenarioConfig::new(64, 32),
        ScenarioConfig::new(64, 64),
    ]
}

fn scenario_total_threads(scenario: &ScenarioConfig) -> usize {
    scenario
        .producers
        .checked_add(scenario.consumers)
        .unwrap_or_else(|| die("total thread count overflow"))
}

fn parse_scenario_token(input: &str) -> Option<ScenarioConfig> {
    let token = input.trim().to_ascii_lowercase();
    let (producer_part, rest) = token.split_once('p')?;
    let consumer_part = rest.strip_suffix('c')?;

    if producer_part.is_empty() || consumer_part.is_empty() {
        return None;
    }
    if producer_part.starts_with('0') || consumer_part.starts_with('0') {
        return None;
    }
    if !producer_part.chars().all(|ch| ch.is_ascii_digit())
        || !consumer_part.chars().all(|ch| ch.is_ascii_digit())
    {
        return None;
    }

    let producers = producer_part.parse::<usize>().ok()?;
    let consumers = consumer_part.parse::<usize>().ok()?;
    if producers == 0 || consumers == 0 {
        return None;
    }

    Some(ScenarioConfig::new(producers, consumers))
}

fn run_benches_parallel(config: Arc<BenchConfig>) -> Output {
    let timestamp_unix_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_millis();

    let meta = Meta {
        timestamp_unix_ms,
        block_cap: config.block_cap,
        items_per_producer: config.items_per_producer,
        ubq_label: config.ubq_label.clone(),
        machine_label: config.machine_label.clone(),
        queues: config.queues.iter().map(|q| q.name().to_string()).collect(),
        scenarios: config
            .packed_scenarios
            .iter()
            .flat_map(|v| v)
            .map(|s| ScenarioMeta {
                name: s.name.clone(),
                producers: s.producers,
                consumers: s.consumers,
            })
            .collect(),
        available_parallelism: config.available_parallelism,
    };

    let mut results = Vec::new();

    for &queue in &config.queues {
        for (pack, scenario_pack) in config.packed_scenarios.iter().enumerate() {
            results.extend(
                scenario_pack
                    .iter()
                    .enumerate()
                    .map(|(subpack, scenario)| {
                        (
                            total_items(config.items_per_producer, scenario.producers),
                            bench_factory(
                                queue,
                                config.clone(),
                                (pack, subpack),
                                config.items_per_producer,
                            ),
                        )
                    })
                    .collect::<Vec<_>>()
                    .into_iter()
                    .flat_map(|(total, handle)| {
                        handle.join().unwrap().into_iter().map(move |record| {
                            if record.total_items != total {
                                die("internal error: total_items mismatch");
                            }

                            record
                        })
                    }),
            );
        }
    }

    Output { meta, results }
}

fn bench_factory(
    queue: QueueKind,
    config: Arc<BenchConfig>,
    (pack, subpack): (usize, usize),
    items_per_producer: u64,
) -> JoinHandle<Vec<Record>> {
    match queue {
        QueueKind::Ubq => (find_ubq_entry(&config.ubq_label)
            .unwrap_or_else(|| die(&format!("unsupported UBQ label: {}", config.ubq_label)))
            .factory)(config, (pack, subpack), items_per_producer),
        QueueKind::SegQueue => bench_factory_typed::<crossbeam_queue::SegQueue<u64>>(
            queue.name(),
            config.block_cap,
            config,
            (pack, subpack),
            items_per_producer,
        ),
        QueueKind::ConcurrentQueue => {
            bench_factory_typed::<concurrent_queue::ConcurrentQueue<u64>>(
                queue.name(),
                config.block_cap,
                config,
                (pack, subpack),
                items_per_producer,
            )
        }
    }
}

fn bench_factory_typed<Q: BenchQueue>(
    queue_name: &'static str,
    block_cap: usize,
    config: Arc<BenchConfig>,
    (pack, subpack): (usize, usize),
    items_per_producer: u64,
) -> JoinHandle<Vec<Record>> {
    thread::spawn(move || {
        config
            .modes
            .iter()
            .map(|mode| match mode {
                Mode::Throughput => bench_throughput_for::<Q>(
                    queue_name,
                    block_cap,
                    unsafe {
                        config
                            .packed_scenarios
                            .get_unchecked(pack)
                            .get_unchecked(subpack)
                    },
                    items_per_producer,
                ),
                Mode::FillDrain => bench_fill_drain_for::<Q>(
                    queue_name,
                    block_cap,
                    unsafe {
                        config
                            .packed_scenarios
                            .get_unchecked(pack)
                            .get_unchecked(subpack)
                    },
                    items_per_producer,
                ),
            })
            .collect()
    })
}

#[cfg(test)]
#[allow(dead_code)]
fn bench_throughput(
    queue: QueueKind,
    scenario: &ScenarioConfig,
    items_per_producer: u64,
) -> Record {
    bench_throughput_with_label(queue, DEFAULT_UBQ_LABEL, scenario, items_per_producer)
}

#[cfg(test)]
#[allow(dead_code)]
fn bench_throughput_with_label(
    queue: QueueKind,
    ubq_label: &str,
    scenario: &ScenarioConfig,
    items_per_producer: u64,
) -> Record {
    match queue {
        QueueKind::Ubq => (find_ubq_entry(ubq_label)
            .unwrap_or_else(|| die(&format!("unsupported UBQ label: {ubq_label}")))
            .throughput)(scenario, items_per_producer),
        QueueKind::SegQueue => bench_throughput_for::<crossbeam_queue::SegQueue<u64>>(
            queue.name(),
            find_ubq_entry(ubq_label)
                .map(|entry| entry.block_cap)
                .unwrap_or_else(|| die(&format!("unsupported UBQ label: {ubq_label}"))),
            scenario,
            items_per_producer,
        ),
        QueueKind::ConcurrentQueue => {
            bench_throughput_for::<concurrent_queue::ConcurrentQueue<u64>>(
                queue.name(),
                find_ubq_entry(ubq_label)
                    .map(|entry| entry.block_cap)
                    .unwrap_or_else(|| die(&format!("unsupported UBQ label: {ubq_label}"))),
                scenario,
                items_per_producer,
            )
        }
    }
}

fn bench_throughput_for<Q: BenchQueue>(
    queue_name: &'static str,
    block_cap: usize,
    scenario: &ScenarioConfig,
    items_per_producer: u64,
) -> Record {
    let total_items = total_items(items_per_producer, scenario.producers);

    let queue_handle = Q::new_queue();

    let total_threads = scenario_total_threads(scenario);
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
                queue_handle.send_value(value);
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
                let value = queue_handle.recv_value();
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
        queue_handle.send_value(SENTINEL);
    }

    for handle in consumer_handles {
        handle.join().expect("consumer join failed");
    }

    let elapsed_ns = start.get().expect("start set").elapsed().as_nanos() as u64;

    let consumed = consumed_total.load(Ordering::Relaxed);
    if consumed != total_items {
        warn_mismatch(queue_name, scenario, total_items, consumed);
    }

    let ops_per_sec = if elapsed_ns > 0 && consumed > 0 {
        Some(consumed as f64 / (elapsed_ns as f64 / 1_000_000_000.0))
    } else {
        None
    };

    Record {
        queue: queue_name.to_string(),
        scenario: scenario.name.clone(),
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
        block_cap,
    }
}

#[cfg(test)]
#[allow(dead_code)]
fn bench_fill_drain(
    queue: QueueKind,
    scenario: &ScenarioConfig,
    items_per_producer: u64,
) -> Record {
    bench_fill_drain_with_label(queue, DEFAULT_UBQ_LABEL, scenario, items_per_producer)
}

#[cfg(test)]
#[allow(dead_code)]
fn bench_fill_drain_with_label(
    queue: QueueKind,
    ubq_label: &str,
    scenario: &ScenarioConfig,
    items_per_producer: u64,
) -> Record {
    match queue {
        QueueKind::Ubq => (find_ubq_entry(ubq_label)
            .unwrap_or_else(|| die(&format!("unsupported UBQ label: {ubq_label}")))
            .fill_drain)(scenario, items_per_producer),
        QueueKind::SegQueue => bench_fill_drain_for::<crossbeam_queue::SegQueue<u64>>(
            queue.name(),
            find_ubq_entry(ubq_label)
                .map(|entry| entry.block_cap)
                .unwrap_or_else(|| die(&format!("unsupported UBQ label: {ubq_label}"))),
            scenario,
            items_per_producer,
        ),
        QueueKind::ConcurrentQueue => {
            bench_fill_drain_for::<concurrent_queue::ConcurrentQueue<u64>>(
                queue.name(),
                find_ubq_entry(ubq_label)
                    .map(|entry| entry.block_cap)
                    .unwrap_or_else(|| die(&format!("unsupported UBQ label: {ubq_label}"))),
                scenario,
                items_per_producer,
            )
        }
    }
}

fn bench_fill_drain_for<Q: BenchQueue>(
    queue_name: &'static str,
    block_cap: usize,
    scenario: &ScenarioConfig,
    items_per_producer: u64,
) -> Record {
    let total_items = total_items(items_per_producer, scenario.producers);

    let queue_handle = Q::new_queue();

    let fill_elapsed =
        run_producers_only_for(&queue_handle, scenario.producers, items_per_producer);

    for _ in 0..scenario.consumers {
        queue_handle.send_value(SENTINEL);
    }

    let (drain_elapsed, consumed) = run_consumers_only_for(&queue_handle, scenario.consumers);

    if consumed != total_items {
        warn_mismatch(queue_name, scenario, total_items, consumed);
    }

    let elapsed_ns = (fill_elapsed + drain_elapsed).as_nanos() as u64;

    Record {
        queue: queue_name.to_string(),
        scenario: scenario.name.clone(),
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
        block_cap,
    }
}

// fn bench_mutable_placeholder(
//     queue: QueueKind,
//     scenario: &ScenarioConfig,
//     items_per_producer: u64,
// ) -> Record {
//     if !supports_mutable_placeholder(queue) {
//         let total_items = total_items(items_per_producer, scenario.producers);
//         return skipped_record(
//             queue,
//             scenario,
//             Mode::MutablePlaceholder,
//             items_per_producer,
//             total_items,
//             "mutable_placeholder not supported for this queue".to_string(),
//         );
//     }

//     let total_items = total_items(items_per_producer, scenario.producers);
//     let queue_handle = make_queue(queue);

//     let total_threads = scenario_total_threads(scenario);
//     let ready = Arc::new(Barrier::new(total_threads + 1));
//     let start_gate = Arc::new(Barrier::new(total_threads + 1));
//     let start = Arc::new(OnceLock::new());

//     let producer_max = Arc::new(AtomicU64::new(0));
//     let consumer_max = Arc::new(AtomicU64::new(0));
//     let consumed_total = Arc::new(AtomicU64::new(0));

//     let mut producer_handles = Vec::with_capacity(scenario.producers);
//     for producer_id in 0..scenario.producers {
//         let queue_handle = queue_handle.clone();
//         let ready = ready.clone();
//         let start_gate = start_gate.clone();
//         let start = start.clone();
//         let producer_max = producer_max.clone();
//         producer_handles.push(thread::spawn(move || {
//             ready.wait();
//             start_gate.wait();
//             let start: Instant = *start.get().expect("start set");
//             let base = (producer_id as u64)
//                 .checked_mul(items_per_producer)
//                 .expect("item count overflow");
//             for offset in 0..items_per_producer {
//                 let value = base.checked_add(offset).expect("item count overflow");
//                 send_mutable_placeholder(&queue_handle, value);
//             }
//             let end_ns = start.elapsed().as_nanos() as u64;
//             producer_max.fetch_max(end_ns, Ordering::Relaxed);
//         }));
//     }

//     let mut consumer_handles = Vec::with_capacity(scenario.consumers);
//     for _ in 0..scenario.consumers {
//         let queue_handle = queue_handle.clone();
//         let ready = ready.clone();
//         let start_gate = start_gate.clone();
//         let start = start.clone();
//         let consumer_max = consumer_max.clone();
//         let consumed_total = consumed_total.clone();
//         consumer_handles.push(thread::spawn(move || {
//             ready.wait();
//             start_gate.wait();
//             let start = *start.get().expect("start set");
//             loop {
//                 let value = recv_mutable_placeholder(&queue_handle);
//                 if value == SENTINEL {
//                     break;
//                 }
//                 consumed_total.fetch_add(1, Ordering::Relaxed);
//             }
//             let end_ns = start.elapsed().as_nanos() as u64;
//             consumer_max.fetch_max(end_ns, Ordering::Relaxed);
//         }));
//     }

//     ready.wait();
//     start.set(Instant::now()).ok();
//     start_gate.wait();

//     for handle in producer_handles {
//         handle.join().expect("producer join failed");
//     }

//     for _ in 0..scenario.consumers {
//         send_mutable_placeholder(&queue_handle, SENTINEL);
//     }

//     for handle in consumer_handles {
//         handle.join().expect("consumer join failed");
//     }

//     let elapsed_ns = start.get().expect("start set").elapsed().as_nanos() as u64;

//     let consumed = consumed_total.load(Ordering::Relaxed);
//     if consumed != total_items {
//         warn_mismatch(queue, scenario, total_items, consumed);
//     }

//     let ops_per_sec = if elapsed_ns > 0 && consumed > 0 {
//         Some(consumed as f64 / (elapsed_ns as f64 / 1_000_000_000.0))
//     } else {
//         None
//     };

//     Record {
//         queue: queue.name().to_string(),
//         scenario: scenario.name.clone(),
//         mode: Mode::MutablePlaceholder.name().to_string(),
//         producers: scenario.producers,
//         consumers: scenario.consumers,
//         items_per_producer,
//         total_items,
//         consumed_items: consumed,
//         elapsed_ns,
//         ops_per_sec,
//         push_elapsed_ns: Some(producer_max.load(Ordering::Relaxed)),
//         pop_elapsed_ns: Some(consumer_max.load(Ordering::Relaxed)),
//         fill_elapsed_ns: None,
//         drain_elapsed_ns: None,
//         block_cap: BLOCK_LENGTH as usize,
//         skipped_reason: None,
//     }
// }

fn run_producers_only_for<Q: BenchQueue>(
    queue_handle: &Arc<Q>,
    producers: usize,
    items_per_producer: u64,
) -> Duration {
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
                queue_handle.send_value(value);
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

fn run_consumers_only_for<Q: BenchQueue>(
    queue_handle: &Arc<Q>,
    consumers: usize,
) -> (Duration, u64) {
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
                let value = queue_handle.recv_value();
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

fn die(message: &str) -> ! {
    eprintln!("{message}");
    std::process::exit(1);
}

fn warn_mismatch(queue_name: &str, scenario: &ScenarioConfig, expected: u64, got: u64) {
    eprintln!(
        "warning: {queue} {scenario} consumed count mismatch: expected {expected}, got {got}",
        queue = queue_name,
        scenario = scenario.name.as_str()
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
  --scenarios LIST          Comma list: 1p1c,4p1c,1p4c,4p4c,8p1c,8p4c,8p8c,1p8c,4p8c,16p1c,1p16c,8p16c,16p8c,16p16c,32p1c,1p32c,16p32c,32p16c,32p32c,64p1c,1p64c,32p64c,64p32c,64p64c
  --ubq-label LABEL         UBQ configuration label written to output metadata
  --machine-label LABEL     Machine label written to output metadata
  --out PATH                Write JSON output to PATH instead of stdout
  --only-ubq                Shortcut for --queues=ubq
  --throughput-only         Shortcut for --modes=throughput
"#;
    println!("{usage}");
    std::process::exit(0);
}

#[cfg(test)]
mod tests {
    // use super::*;
    // use std::panic::{AssertUnwindSafe, resume_unwind};
    // use std::sync::{
    //     Arc, Barrier, Mutex,
    //     atomic::{AtomicBool, AtomicUsize, Ordering},
    //     mpsc::{self, RecvTimeoutError},
    // };
    // use std::thread;
    // use std::time::Duration;

    // const DEFAULT_TEST_ITEMS_PER_PRODUCER: u64 = 200;
    // const DEFAULT_TEST_TIMEOUT_SECS: u64 = 30;

    // fn test_timeout() -> Duration {
    //     std::env::var("UBQ_TEST_TIMEOUT_SECS")
    //         .ok()
    //         .and_then(|value| value.parse::<u64>().ok())
    //         .map(Duration::from_secs)
    //         .unwrap_or(Duration::from_secs(DEFAULT_TEST_TIMEOUT_SECS))
    // }

    // fn run_with_timeout(name: &'static str, f: impl FnOnce() + Send + 'static) {
    //     run_with_timeout_context(
    //         name,
    //         Arc::new(Mutex::new("context unavailable".to_string())),
    //         f,
    //     );
    // }

    // fn run_with_timeout_context(
    //     name: &'static str,
    //     timeout_context: Arc<Mutex<String>>,
    //     f: impl FnOnce() + Send + 'static,
    // ) {
    //     let timeout = test_timeout();
    //     let (tx, rx) = mpsc::channel();

    //     thread::spawn(move || {
    //         let result = std::panic::catch_unwind(AssertUnwindSafe(f));
    //         let _ = tx.send(result);
    //     });

    //     match rx.recv_timeout(timeout) {
    //         Ok(Ok(())) => {}
    //         Ok(Err(payload)) => resume_unwind(payload),
    //         Err(RecvTimeoutError::Timeout) => {
    //             let context = timeout_context
    //                 .lock()
    //                 .map(|value| value.clone())
    //                 .unwrap_or_else(|_| "context unavailable (poisoned)".to_string());
    //             panic!(
    //                 "test `{name}` timed out after {}s (last context: {context})",
    //                 timeout.as_secs()
    //             )
    //         }
    //         Err(RecvTimeoutError::Disconnected) => {
    //             panic!("test `{name}` ended unexpectedly before reporting")
    //         }
    //     }
    // }

    // fn set_timeout_context(timeout_context: &Arc<Mutex<String>>, value: impl Into<String>) {
    //     if let Ok(mut slot) = timeout_context.lock() {
    //         *slot = value.into();
    //     }
    // }

    // fn test_items_per_producer() -> u64 {
    //     std::env::var("UBQ_BENCH_TEST_ITEMS")
    //         .ok()
    //         .and_then(|value| value.parse::<u64>().ok())
    //         .unwrap_or(DEFAULT_TEST_ITEMS_PER_PRODUCER)
    // }

    // fn test_scenarios() -> Vec<ScenarioConfig> {
    //     vec![
    //         ScenarioConfig::new(1, 1),
    //         ScenarioConfig::new(3, 1),
    //         ScenarioConfig::new(1, 3),
    //         ScenarioConfig::new(3, 3),
    //         ScenarioConfig::new(8, 8),
    //     ]
    // }

    // fn test_queues() -> [QueueKind; 3] {
    //     [
    //         QueueKind::Ubq,
    //         QueueKind::SegQueue,
    //         QueueKind::ConcurrentQueue,
    //     ]
    // }

    // fn scenario_label(scenario: &ScenarioConfig) -> String {
    //     format!(
    //         "{}({}p/{}c)",
    //         scenario.name, scenario.producers, scenario.consumers
    //     )
    // }

    // fn record_error(
    //     error_count: &AtomicUsize,
    //     first_error: &Mutex<Option<String>>,
    //     message: String,
    // ) {
    //     error_count.fetch_add(1, Ordering::Relaxed);
    //     let mut slot = first_error.lock().expect("first_error lock");
    //     if slot.is_none() {
    //         *slot = Some(message);
    //     }
    // }

    // fn assert_no_integrity_errors(
    //     queue: QueueKind,
    //     scenario: &ScenarioConfig,
    //     mode: Mode,
    //     seen: &[AtomicBool],
    //     error_count: &AtomicUsize,
    //     first_error: &Mutex<Option<String>>,
    //     expected_total: u64,
    //     consumed_total: u64,
    // ) {
    //     let label = scenario_label(scenario);
    //     assert_eq!(
    //         consumed_total,
    //         expected_total,
    //         "{} {} consumed mismatch in {} mode",
    //         queue.name(),
    //         label,
    //         mode.name()
    //     );

    //     let errors = error_count.load(Ordering::Relaxed);
    //     if errors > 0 {
    //         let first = first_error.lock().expect("first_error lock");
    //         let message = first.as_deref().unwrap_or("unknown consumer error");
    //         panic!(
    //             "{} {} integrity errors in {} mode: {errors} (first: {message})",
    //             queue.name(),
    //             label,
    //             mode.name()
    //         );
    //     }

    //     let mut missing_count = 0usize;
    //     let mut missing_samples = Vec::new();
    //     for (idx, seen_flag) in seen.iter().enumerate() {
    //         if !seen_flag.load(Ordering::Acquire) {
    //             missing_count += 1;
    //             if missing_samples.len() < 10 {
    //                 missing_samples.push(idx);
    //             }
    //         }
    //     }
    //     assert_eq!(
    //         missing_count,
    //         0,
    //         "{} {} missing values in {} mode: {} (samples: {:?})",
    //         queue.name(),
    //         label,
    //         mode.name(),
    //         missing_count,
    //         missing_samples
    //     );
    // }

    // fn run_throughput_integrity(
    //     queue: QueueKind,
    //     scenario: &ScenarioConfig,
    //     items_per_producer: u64,
    // ) {
    //     let total = total_items(items_per_producer, scenario.producers);
    //     let seen = Arc::new(
    //         (0..usize::try_from(total).expect("total items should fit usize"))
    //             .map(|_| AtomicBool::new(false))
    //             .collect::<Vec<_>>(),
    //     );
    //     let consumed_total = Arc::new(AtomicUsize::new(0));
    //     let error_count = Arc::new(AtomicUsize::new(0));
    //     let first_error = Arc::new(Mutex::new(None::<String>));

    //     let queue_handle = make_queue(queue);
    //     let total_threads = scenario.producers + scenario.consumers;
    //     let ready = Arc::new(Barrier::new(total_threads + 1));
    //     let start_gate = Arc::new(Barrier::new(total_threads + 1));

    //     let mut producer_handles = Vec::with_capacity(scenario.producers);
    //     for producer_id in 0..scenario.producers {
    //         let queue_handle = queue_handle.clone();
    //         let ready = Arc::clone(&ready);
    //         let start_gate = Arc::clone(&start_gate);
    //         producer_handles.push(thread::spawn(move || {
    //             ready.wait();
    //             start_gate.wait();
    //             let base = (producer_id as u64)
    //                 .checked_mul(items_per_producer)
    //                 .expect("item count overflow");
    //             for offset in 0..items_per_producer {
    //                 let value = base.checked_add(offset).expect("item count overflow");
    //                 send(&queue_handle, value);
    //             }
    //         }));
    //     }

    //     let mut consumer_handles = Vec::with_capacity(scenario.consumers);
    //     for _ in 0..scenario.consumers {
    //         let queue_handle = queue_handle.clone();
    //         let ready = Arc::clone(&ready);
    //         let start_gate = Arc::clone(&start_gate);
    //         let seen = Arc::clone(&seen);
    //         let consumed_total = Arc::clone(&consumed_total);
    //         let error_count = Arc::clone(&error_count);
    //         let first_error = Arc::clone(&first_error);
    //         consumer_handles.push(thread::spawn(move || {
    //             ready.wait();
    //             start_gate.wait();
    //             loop {
    //                 let value = recv(&queue_handle);
    //                 if value == SENTINEL {
    //                     break;
    //                 }
    //                 let idx = value as usize;
    //                 if idx >= seen.len() {
    //                     record_error(
    //                         &error_count,
    //                         &first_error,
    //                         format!("out-of-range value {value}"),
    //                     );
    //                     continue;
    //                 }
    //                 let already_seen = seen[idx].swap(true, Ordering::AcqRel);
    //                 if already_seen {
    //                     record_error(
    //                         &error_count,
    //                         &first_error,
    //                         format!("duplicate value {value}"),
    //                     );
    //                 }
    //                 consumed_total.fetch_add(1, Ordering::Relaxed);
    //             }
    //         }));
    //     }

    //     ready.wait();
    //     start_gate.wait();

    //     for handle in producer_handles {
    //         handle.join().expect("producer join failed");
    //     }

    //     for _ in 0..scenario.consumers {
    //         send(&queue_handle, SENTINEL);
    //     }

    //     for handle in consumer_handles {
    //         handle.join().expect("consumer join failed");
    //     }

    //     assert_no_integrity_errors(
    //         queue,
    //         scenario,
    //         Mode::Throughput,
    //         seen.as_slice(),
    //         &error_count,
    //         &first_error,
    //         total,
    //         consumed_total.load(Ordering::Relaxed) as u64,
    //     );
    // }

    // fn run_fill_drain_integrity(
    //     queue: QueueKind,
    //     scenario: &ScenarioConfig,
    //     items_per_producer: u64,
    // ) {
    //     let total = total_items(items_per_producer, scenario.producers);
    //     let seen = Arc::new(
    //         (0..usize::try_from(total).expect("total items should fit usize"))
    //             .map(|_| AtomicBool::new(false))
    //             .collect::<Vec<_>>(),
    //     );
    //     let consumed_total = Arc::new(AtomicUsize::new(0));
    //     let error_count = Arc::new(AtomicUsize::new(0));
    //     let first_error = Arc::new(Mutex::new(None::<String>));

    //     let queue_handle = make_queue(queue);
    //     run_producers_only(&queue_handle, scenario.producers, items_per_producer);

    //     for _ in 0..scenario.consumers {
    //         send(&queue_handle, SENTINEL);
    //     }

    //     let ready = Arc::new(Barrier::new(scenario.consumers + 1));
    //     let start_gate = Arc::new(Barrier::new(scenario.consumers + 1));

    //     let mut consumer_handles = Vec::with_capacity(scenario.consumers);
    //     for _ in 0..scenario.consumers {
    //         let queue_handle = queue_handle.clone();
    //         let ready = Arc::clone(&ready);
    //         let start_gate = Arc::clone(&start_gate);
    //         let seen = Arc::clone(&seen);
    //         let consumed_total = Arc::clone(&consumed_total);
    //         let error_count = Arc::clone(&error_count);
    //         let first_error = Arc::clone(&first_error);
    //         consumer_handles.push(thread::spawn(move || {
    //             ready.wait();
    //             start_gate.wait();
    //             loop {
    //                 let value = recv(&queue_handle);
    //                 if value == SENTINEL {
    //                     break;
    //                 }
    //                 let idx = value as usize;
    //                 if idx >= seen.len() {
    //                     record_error(
    //                         &error_count,
    //                         &first_error,
    //                         format!("out-of-range value {value}"),
    //                     );
    //                     continue;
    //                 }
    //                 let already_seen = seen[idx].swap(true, Ordering::AcqRel);
    //                 if already_seen {
    //                     record_error(
    //                         &error_count,
    //                         &first_error,
    //                         format!("duplicate value {value}"),
    //                     );
    //                 }
    //                 consumed_total.fetch_add(1, Ordering::Relaxed);
    //             }
    //         }));
    //     }

    //     ready.wait();
    //     start_gate.wait();

    //     for handle in consumer_handles {
    //         handle.join().expect("consumer join failed");
    //     }

    //     assert_no_integrity_errors(
    //         queue,
    //         scenario,
    //         Mode::FillDrain,
    //         seen.as_slice(),
    //         &error_count,
    //         &first_error,
    //         total,
    //         consumed_total.load(Ordering::Relaxed) as u64,
    //     );
    // }

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
                        assert_eq!(record.scenario, scenario.name.as_str());
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
                        assert_eq!(record.scenario, scenario.name.as_str());
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
    fn parse_scenario_token_accepts_xpxc() {
        let parsed = parse_scenario_token("8p8c").expect("8p8c should parse");
        assert_eq!(parsed.name, "8p8c");
        assert_eq!(parsed.producers, 8);
        assert_eq!(parsed.consumers, 8);
    }

    #[test]
    fn default_scenarios_include_extended_matrix() {
        let names = default_scenarios()
            .into_iter()
            .map(|scenario| scenario.name)
            .collect::<Vec<_>>();
        assert_eq!(
            names,
            vec![
                "1p1c", "4p1c", "1p4c", "4p4c", "8p1c", "8p4c", "8p8c", "1p8c", "4p8c", "16p1c",
                "1p16c", "8p16c", "16p8c", "16p16c", "32p1c", "1p32c", "16p32c", "32p16c",
                "32p32c", "64p1c", "1p64c", "32p64c", "64p32c", "64p64c",
            ]
        );
    }

    #[test]
    fn parallelism_skip_reason_respects_available_parallelism() {
        let scenario = ScenarioConfig::new(8, 16);
        let reason = parallelism_skip_reason(&scenario, Some(16)).expect("should skip");
        assert!(reason.contains("24 total threads"));
        assert!(reason.contains("available_parallelism is 16"));
        assert!(parallelism_skip_reason(&scenario, Some(24)).is_none());
        assert!(parallelism_skip_reason(&scenario, None).is_none());
    }

    #[test]
    fn run_benches_skips_insufficient_parallelism_scenarios() {
        let config = BenchConfig {
            items_per_producer: 1,
            queues: vec![QueueKind::Ubq],
            scenarios: vec![ScenarioConfig::new(4, 4)],
            modes: vec![Mode::Throughput, Mode::FillDrain, Mode::MutablePlaceholder],
            ubq_label: "test".to_string(),
            machine_label: "test".to_string(),
            out_path: None,
        };

        let output = run_benches_with_parallelism(&config, Some(4));
        assert_eq!(output.meta.available_parallelism, Some(4));
        assert_eq!(output.results.len(), 3);

        for record in output.results {
            assert_eq!(record.total_items, 4);
            assert_eq!(record.consumed_items, 0);
            assert_eq!(record.elapsed_ns, 0);
            assert!(record.skipped_reason.is_some());
        }
    }

    #[test]
    fn parse_scenario_token_rejects_legacy_and_invalid_values() {
        let invalid = [
            "spsc", "mpsc", "spmc", "mpmc", "0p1c", "1p0c", "01p1c", "1p01c", "1x1", "1p1",
            "1p1c1", "", "p1c", "1pc",
        ];
        for value in invalid {
            assert!(
                parse_scenario_token(value).is_none(),
                "unexpectedly parsed {value}"
            );
        }
    }

    #[test]
    fn mutable_placeholder_records_for_supported_queues() {
        let scenario = ScenarioConfig::new(2, 2);
        let items_per_producer = test_items_per_producer();
        let expected_total = total_items(items_per_producer, scenario.producers);

        for queue in [QueueKind::Ubq, QueueKind::SegQueue] {
            let record = bench_mutable_placeholder(queue, &scenario, items_per_producer);
            assert_eq!(record.queue, queue.name());
            assert_eq!(record.scenario, scenario.name.as_str());
            assert_eq!(record.mode, Mode::MutablePlaceholder.name());
            assert_eq!(record.consumed_items, expected_total);
            assert_eq!(record.total_items, expected_total);
            assert!(record.ops_per_sec.is_some());
            assert!(record.skipped_reason.is_none());
        }
    }

    #[test]
    fn mutable_placeholder_marks_unsupported_queues_as_skipped() {
        let scenario = ScenarioConfig::new(2, 2);
        let items_per_producer = test_items_per_producer();
        let expected_total = total_items(items_per_producer, scenario.producers);

        let record =
            bench_mutable_placeholder(QueueKind::ConcurrentQueue, &scenario, items_per_producer);
        assert_eq!(record.queue, QueueKind::ConcurrentQueue.name());
        assert_eq!(record.mode, Mode::MutablePlaceholder.name());
        assert_eq!(record.total_items, expected_total);
        assert_eq!(record.consumed_items, 0);
        assert!(record.ops_per_sec.is_none());
        assert!(record.skipped_reason.is_some());
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
