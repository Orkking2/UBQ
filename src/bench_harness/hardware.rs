//! Bounded, replayable hardware probes. No shared per-item counters in timing mode.
use super::*;
use serde_json::{Value, json};
use std::sync::atomic::{AtomicU8, AtomicUsize};

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    pub queue: String,
    pub scenario: String,
    pub batch_size: Option<usize>,
    pub backoff: String,
    pub mode: String,
    pub round_items: u64,
    pub rounds: Option<u64>,
    pub duration_secs: f64,
    pub warmup_rounds: u64,
    pub memory_depth: u64,
    pub idle_secs: Vec<u64>,
    pub rss_limit_mib: u64,
    pub allow_unpinned: bool,
    pub block_size: usize,
    pub capacity: usize,
    pub segment_size: usize,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            queue: "ubq".into(),
            scenario: "1p1c".into(),
            batch_size: None,
            backoff: "crossbeam".into(),
            mode: "handoff".into(),
            round_items: 8_388_608,
            rounds: None,
            duration_secs: 30.,
            warmup_rounds: 2,
            memory_depth: 65_536,
            idle_secs: vec![1, 10],
            rss_limit_mib: 8192,
            allow_unpinned: false,
            block_size: 256,
            capacity: 1_048_576,
            segment_size: 256,
        }
    }
}

/// Checkpoints report process footprint, not allocator-requested bytes.
pub fn memory_snapshot() -> Value {
    let mut fields = serde_json::Map::new();
    for (file, wanted) in [
        ("/proc/self/status", &["VmRSS", "VmHWM", "VmSize"][..]),
        (
            "/proc/self/smaps_rollup",
            &["Rss", "Pss", "Private_Clean", "Private_Dirty", "Anonymous"][..],
        ),
    ] {
        if let Ok(raw) = fs::read_to_string(file) {
            for line in raw.lines() {
                let Some((key, rest)) = line.split_once(':') else {
                    continue;
                };
                if wanted.contains(&key) {
                    if let Some(value) = rest
                        .split_whitespace()
                        .next()
                        .and_then(|v| v.parse::<u64>().ok())
                    {
                        fields.insert(format!("{key}_bytes"), json!(value * 1024));
                    }
                }
            }
        }
    }
    if fields.is_empty() {
        fields.insert("available".into(), json!(false));
    }
    Value::Object(fields)
}

struct Watchdog(Arc<AtomicBool>, Option<thread::JoinHandle<()>>);
impl Watchdog {
    fn new(limit: u64) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let flag = stop.clone();
        let handle = (limit > 0 && cfg!(target_os = "linux")).then(|| thread::spawn(move || {
            while !flag.load(AtomicOrdering::Relaxed) {
                if let Ok(raw) = fs::read_to_string("/proc/self/status") {
                    if let Some(kib) = raw.lines().find_map(|l| l.strip_prefix("VmRSS:")
                        .and_then(|s| s.split_whitespace().next()).and_then(|s| s.parse::<u64>().ok())) {
                        if kib > limit.saturating_mul(1024) {
                            eprintln!("bench_hardware: memory_budget_exceeded: RSS={kib} KiB limit={limit} MiB");
                            std::process::exit(75);
                        }
                    }
                }
                thread::sleep(Duration::from_millis(200));
            }
        }));
        Self(stop, handle)
    }
}
impl Drop for Watchdog {
    fn drop(&mut self) {
        self.0.store(true, AtomicOrdering::Relaxed);
        if let Some(h) = self.1.take() {
            let _ = h.join();
        }
    }
}

const STOP: usize = 0;
const HANDOFF: usize = 1;
const FILL: usize = 2;
const DRAIN: usize = 3;
const IDLE: usize = 4;

/// Memory-only scaffolding control: tracks occupancy without storing payloads.
/// Its timing is meaningless; it estimates process/thread measurement overhead.
struct MemoryScaffold(AtomicU64);
impl BenchQueueOps for MemoryScaffold {
    fn try_send_value(&self, _value: u64) -> bool {
        self.0.fetch_add(1, AtomicOrdering::Relaxed);
        true
    }
    fn try_recv_value(&self) -> Option<u64> {
        self.0
            .fetch_update(AtomicOrdering::Relaxed, AtomicOrdering::Relaxed, |count| {
                count.checked_sub(1)
            })
            .ok()
            .map(|_| 0)
    }
}

struct Workers {
    start: Arc<Barrier>,
    producers_finished: Arc<Barrier>,
    end: Arc<Barrier>,
    action: Arc<AtomicUsize>,
    done: Arc<AtomicBool>,
    slots: Arc<Vec<Mutex<(u64, u64)>>>,
    affinity: Arc<AtomicBool>,
    seen: Arc<Vec<AtomicU8>>,
    bad_value: Arc<AtomicBool>,
    handles: Vec<thread::JoinHandle<()>>,
    expected: u64,
}
impl Workers {
    fn new<Q: BenchQueueHandleFactory, const VERIFY: bool>(
        queue: &Arc<Q>,
        cfg: &Config,
        scenario: &ScenarioConfig,
        expected: u64,
    ) -> Self {
        let threads = scenario.total_threads();
        let mut workers = Self {
            start: Arc::new(Barrier::new(threads + 1)),
            producers_finished: Arc::new(Barrier::new(scenario.producers + 1)),
            end: Arc::new(Barrier::new(threads + 1)),
            action: Arc::new(AtomicUsize::new(STOP)),
            done: Arc::new(AtomicBool::new(false)),
            slots: Arc::new((0..threads).map(|_| Mutex::new((0, 0))).collect()),
            affinity: Arc::new(AtomicBool::new(true)),
            seen: Arc::new(if VERIFY {
                (0..expected).map(|_| AtomicU8::new(0)).collect()
            } else {
                vec![]
            }),
            bad_value: Arc::new(AtomicBool::new(false)),
            handles: vec![],
            expected,
        };
        let (p, c) = (scenario.producers, scenario.consumers);
        let ready = Arc::new(Barrier::new(threads + 1));
        for id in 0..threads {
            let ready = ready.clone();
            let producer = id < p;
            let index = if producer { id } else { id - p };
            let handle = if producer {
                queue.producer_thread_handle()
            } else {
                queue.consumer_thread_handle()
            };
            let start = workers.start.clone();
            let end = workers.end.clone();
            let pdone = workers.producers_finished.clone();
            let done = workers.done.clone();
            let action = workers.action.clone();
            let slots = workers.slots.clone();
            let affinity = workers.affinity.clone();
            let seen = workers.seen.clone();
            let bad = workers.bad_value.clone();
            let batch = cfg.batch_size;
            let core = if producer {
                producer_core_id(0, p, c, index)
            } else {
                consumer_core_id(0, p, c, index)
            };
            // Returning from a panicking worker would strand peers at barriers.
            // Abort lets the supervising process retain the panic and terminate the case.
            workers.handles.push(thread::spawn(move || {
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    if !pin_current_bench_thread(core) {
                        affinity.store(false, AtomicOrdering::Relaxed);
                    }
                    ready.wait();
                    loop {
                        start.wait();
                        let mode = action.load(AtomicOrdering::Acquire);
                        if mode == STOP {
                            break;
                        }
                        let clock = Instant::now();
                        let mut count = 0;
                        if producer {
                            if mode == HANDOFF || mode == FILL {
                                let quota = expected / p as u64
                                    + u64::from((index as u64) < expected % p as u64);
                                let base = index as u64 * (expected / p as u64)
                                    + (index as u64).min(expected % p as u64);
                                if let Some(size) = batch {
                                    for first in (0..quota as usize).step_by(size) {
                                        handle.send_batch(
                                            base,
                                            first..(first + size).min(quota as usize),
                                        );
                                    }
                                } else {
                                    for offset in 0..quota {
                                        handle.send_value(base + offset);
                                    }
                                }
                                count = quota;
                            }
                            *slots[id].lock().unwrap() = (count, clock.elapsed().as_nanos() as u64);
                            pdone.wait();
                        } else {
                            if mode == HANDOFF || mode == DRAIN {
                                let backoff = Backoff::new();
                                let mut after_done_empty = false;
                                loop {
                                    let received = if VERIFY {
                                        let mut visit = |value: u64| {
                                            if let Some(slot) = seen.get(value as usize) {
                                                if slot.swap(1, AtomicOrdering::Relaxed) != 0 {
                                                    bad.store(true, AtomicOrdering::Relaxed);
                                                }
                                            } else {
                                                bad.store(true, AtomicOrdering::Relaxed);
                                            }
                                        };
                                        if let Some(size) = batch {
                                            handle.visit_recv_batch(size, &mut visit)
                                        } else {
                                            handle
                                                .try_recv_value()
                                                .map(|v| {
                                                    visit(v);
                                                    1
                                                })
                                                .unwrap_or(0)
                                        }
                                    } else if let Some(size) = batch {
                                        handle.try_recv_batch(size)
                                    } else {
                                        handle
                                            .try_recv_value()
                                            .map(|v| {
                                                std::hint::black_box(v);
                                                1
                                            })
                                            .unwrap_or(0)
                                    };
                                    count += received as u64;
                                    if received != 0 {
                                        after_done_empty = false;
                                        continue;
                                    }
                                    // Recheck after acquiring the completion publication. An
                                    // empty observation made before it cannot establish drain.
                                    if done.load(AtomicOrdering::Acquire) {
                                        if after_done_empty {
                                            break;
                                        }
                                        after_done_empty = true;
                                    }
                                    backoff.snooze();
                                }
                            }
                            *slots[id].lock().unwrap() = (count, clock.elapsed().as_nanos() as u64);
                        }
                        end.wait();
                    }
                }));
                if result.is_err() {
                    std::process::abort();
                }
            }));
        }
        ready.wait();
        workers
    }
    fn round(&self, mode: usize, producers: usize) -> Result<u64, String> {
        self.bad_value.store(false, AtomicOrdering::Relaxed);
        for slot in self.seen.iter() {
            slot.store(0, AtomicOrdering::Relaxed);
        }
        self.done.store(mode == DRAIN, AtomicOrdering::Release);
        self.action.store(mode, AtomicOrdering::Release);
        let begin = Instant::now();
        self.start.wait();
        self.producers_finished.wait();
        self.done.store(true, AtomicOrdering::Release);
        self.end.wait();
        let elapsed = begin.elapsed().as_nanos() as u64;
        let sent: u64 = self.slots[..producers]
            .iter()
            .map(|s| s.lock().unwrap().0)
            .sum();
        let received: u64 = self.slots[producers..]
            .iter()
            .map(|s| s.lock().unwrap().0)
            .sum();
        if mode != IDLE
            && (mode != DRAIN && sent != self.expected || mode != FILL && received != self.expected)
        {
            return Err(format!(
                "integrity mismatch: expected {} sent {sent} consumed {received}",
                self.expected
            ));
        }
        if mode != FILL
            && mode != IDLE
            && !self.seen.is_empty()
            && (self.bad_value.load(AtomicOrdering::Relaxed)
                || self
                    .seen
                    .iter()
                    .any(|s| s.load(AtomicOrdering::Relaxed) != 1))
        {
            return Err(
                "exact-once verification failed (duplicate, missing or out-of-range value)".into(),
            );
        }
        Ok(elapsed)
    }
}
impl Drop for Workers {
    fn drop(&mut self) {
        self.action.store(STOP, AtomicOrdering::Release);
        self.start.wait();
        for handle in self.handles.drain(..) {
            let _ = handle.join();
        }
    }
}

fn run_with<Q: BenchQueueHandleFactory, const VERIFY: bool>(
    queue: Arc<Q>,
    cfg: &Config,
    scenario: &ScenarioConfig,
    before_queue: Value,
) -> Result<Value, String> {
    let memory = cfg.mode == "memory";
    let alignment = scenario.producers as u64 * cfg.batch_size.unwrap_or(1) as u64;
    let expected = if memory {
        cfg.memory_depth
    } else {
        cfg.round_items / alignment * alignment
    };
    if expected == 0 {
        return Err("round cap is smaller than one producer-aligned batch".into());
    }
    if memory
        && queue
            .bounded_capacity()
            .is_some_and(|cap| expected > cap as u64)
    {
        return Err("memory depth exceeds the queue's actual bounded capacity".into());
    }
    let workers = Workers::new::<Q, VERIFY>(&queue, cfg, scenario, expected);
    let mut checkpoints = vec![json!({"phase":"before_queue", "memory": before_queue})];
    let mut durations = vec![];
    if memory {
        // All persistent workers are pinned and parked at the start barrier.
        checkpoints.push(json!({"phase":"constructed_empty", "memory": memory_snapshot()}));
        for (phase, action) in [("full", FILL), ("drained", DRAIN)] {
            let os_before = profile_rusage_snapshot();
            durations.push(workers.round(action, scenario.producers)?);
            checkpoints.push(json!({"phase":phase,"items_resident":if action==FILL {expected} else {0},"memory":memory_snapshot(),
                "os":profile_rusage_delta(os_before, profile_rusage_snapshot())}));
        }
        let mut previous = 0;
        for &seconds in &cfg.idle_secs {
            thread::sleep(Duration::from_secs(seconds.saturating_sub(previous)));
            previous = seconds;
            checkpoints
                .push(json!({"phase":format!("idle_{seconds}s"),"memory":memory_snapshot()}));
        }
        let os_before = profile_rusage_snapshot();
        durations.push(workers.round(FILL, scenario.producers)?);
        checkpoints.push(
            json!({"phase":"refilled","items_resident":expected,"memory":memory_snapshot(),
            "os":profile_rusage_delta(os_before, profile_rusage_snapshot())}),
        );
        let os_before = profile_rusage_snapshot();
        durations.push(workers.round(DRAIN, scenario.producers)?);
        checkpoints.push(json!({"phase":"redrained","memory":memory_snapshot(),
            "os":profile_rusage_delta(os_before, profile_rusage_snapshot())}));
        let affinity = workers.affinity.load(AtomicOrdering::Relaxed);
        drop(workers);
        drop(queue);
        checkpoints.push(json!({"phase":"destroyed","memory":memory_snapshot()}));
        if !affinity && !cfg.allow_unpinned {
            return Err("worker affinity failed".into());
        }
        return Ok(
            json!({"benchmark":"queue_memory_lifecycle","schema_version":1,"queue":cfg.queue,
            "scenario":cfg.scenario,"batch_size":cfg.batch_size,"affinity_ok":affinity,
            "memory_depth":expected,"checkpoints":checkpoints,"phase_elapsed_ns":durations,
            "allocation_coverage":"process footprint; no allocator interception", "config":cfg}),
        );
    }
    let mut empty_rounds = (0..16)
        .map(|_| workers.round(IDLE, scenario.producers))
        .collect::<Result<Vec<_>, _>>()?;
    empty_rounds.sort_unstable();
    let empty_round_median_ns = empty_rounds[empty_rounds.len() / 2];
    for _ in 0..cfg.warmup_rounds {
        workers.round(HANDOFF, scenario.producers)?;
    }
    let os_before = profile_rusage_snapshot();
    let begin = Instant::now();
    let mut rounds = 0_u64;
    let mut elapsed = 0_u64;
    loop {
        elapsed = elapsed
            .checked_add(workers.round(HANDOFF, scenario.producers)?)
            .ok_or("time overflow")?;
        rounds += 1;
        if cfg.rounds.map_or(
            begin.elapsed().as_secs_f64() >= cfg.duration_secs,
            |target| rounds >= target,
        ) {
            break;
        }
    }
    let os = profile_rusage_delta(os_before, profile_rusage_snapshot());
    let affinity = workers.affinity.load(AtomicOrdering::Relaxed);
    if !affinity && !cfg.allow_unpinned {
        return Err("worker affinity failed".into());
    }
    let items = expected.checked_mul(rounds).ok_or("item count overflow")?;
    let result = json!({"benchmark":"queue_bounded_handoff","schema_version":1,
        "queue":cfg.queue,"queue_implementation":if cfg.queue=="segqueue" && cfg.batch_size.is_some() {"crossbeam-batchqueue"} else {&cfg.queue},
        "scenario":cfg.scenario,"batch_size":cfg.batch_size,"backoff":cfg.backoff,
        "completion_accounting":if cfg!(feature="probe_item_completion") {"item"} else {"segment"},
        "round_items":expected,"rounds":rounds,"warmup_rounds":cfg.warmup_rounds,
        "items":items,"elapsed_ns":elapsed,"ops_per_sec":items as f64*1e9/elapsed as f64,
        "fixed_work":cfg.rounds.is_some(),"affinity_ok":affinity,"exact_once_checked":VERIFY,
        "empty_round_median_ns":empty_round_median_ns,
        "barrier_overhead_proxy_fraction":empty_round_median_ns as f64*rounds as f64/elapsed as f64,
        "persistent_workers":true,"memory":memory_snapshot(),"os":os,"config":cfg});
    Ok(result)
}

pub fn run(cfg: &Config) -> Result<Value, String> {
    if !matches!(cfg.mode.as_str(), "handoff" | "verify" | "memory") {
        return Err("mode must be handoff, verify or memory".into());
    }
    if cfg.rounds == Some(0)
        || cfg.round_items == 0
        || cfg.memory_depth == 0
        || cfg.batch_size == Some(0)
        || !cfg.duration_secs.is_finite()
        || cfg.duration_secs <= 0.
        || !matches!(cfg.backoff.as_str(), "crossbeam" | "yield")
    {
        return Err("invalid workload parameters".into());
    }
    let mut scenarios = parse_scenarios_with_parallelism(Some(&cfg.scenario), usize::MAX)?;
    if scenarios.len() != 1 {
        return Err("exactly one scenario is required".into());
    }
    let scenario = scenarios.remove(0);
    if scenario.total_threads() > bench_core_ids().len() && !cfg.allow_unpinned {
        return Err("insufficient distinct worker CPUs".into());
    }
    let _watchdog = Watchdog::new(cfg.rss_limit_mib);
    let before = memory_snapshot();
    macro_rules! probe {
        ($queue:expr) => {{
            if cfg.mode == "verify" {
                run_with::<_, true>($queue, cfg, &scenario, before)
            } else {
                run_with::<_, false>($queue, cfg, &scenario, before)
            }
        }};
    }
    match cfg.queue.as_str() {
        "memory-scaffold" if cfg.mode == "memory" => {
            probe!(Arc::new(MemoryScaffold(AtomicU64::new(0))))
        }
        "ubq" if cfg.backoff == "yield" => probe!(UBQ::<u64, backoff::Yield>::new_queue()),
        "ubq" => probe!(UBQ::<u64, backoff::Crossbeam>::new_queue()),
        "lubq" if cfg.backoff == "yield" => probe!(LubqBenchQueue::<u64, backoff::Yield>::new(
            scenario.producers,
            scenario.consumers
        )),
        "lubq" => probe!(LubqBenchQueue::<u64, backoff::Crossbeam>::new(
            scenario.producers,
            scenario.consumers
        )),
        "segqueue" if cfg.batch_size.is_some() => probe!(BatchQueue::<u64>::new_queue()),
        "segqueue" => probe!(SegQueue::<u64>::new_queue()),
        "mutex-vecdeque" => probe!(MutexQueue::<u64>::new_queue()),
        "concurrent-queue" if cfg.batch_size.is_none() => {
            probe!(ConcurrentQueue::<u64>::new_queue())
        }
        "ms-queue" if cfg.batch_size.is_none() => probe!(MsQueue::<u64>::new_queue()),
        #[cfg(feature = "bench_moodycamel")]
        "moodycamel-cq" => probe!(MoodycamelQueue::new_handle()),
        #[cfg(feature = "bench_fastfifo")]
        "fastfifo" if cfg.batch_size.is_none() && cfg.block_size > 0 && cfg.capacity > 0 => {
            probe!(RbbqBenchQueue::new(cfg.block_size, cfg.capacity))
        }
        #[cfg(feature = "bench_lfqueue")]
        "lfqueue" if cfg.batch_size.is_none() && cfg.segment_size > 0 => {
            probe!(LfQueueBenchQueue::new(cfg.segment_size))
        }
        _ => Err(format!(
            "unsupported queue/configuration: {}; check compiled features",
            cfg.queue
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn config(queue: &str) -> Config {
        Config {
            queue: queue.into(),
            scenario: "2p2c".into(),
            batch_size: Some(64),
            round_items: 4096,
            rounds: Some(8),
            warmup_rounds: 2,
            allow_unpinned: true,
            rss_limit_mib: 0,
            ..Config::default()
        }
    }
    #[test]
    fn bounded_rounds_replay_exact_work_with_persistent_handles() {
        for queue in ["mutex-vecdeque", "lubq", "segqueue"] {
            let cfg = config(queue);
            let result = run(&cfg).unwrap();
            assert_eq!(result["items"], 32768);
            assert_eq!(result["rounds"], 8);
            assert_eq!(result["round_items"], 4096);
            assert_eq!(result["fixed_work"], true);
        }
    }
    #[test]
    fn native_batches_are_verified_exactly_once_across_reuse() {
        for queue in ["ubq", "lubq", "segqueue", "mutex-vecdeque"] {
            let mut cfg = config(queue);
            cfg.mode = "verify".into();
            assert_eq!(run(&cfg).unwrap()["exact_once_checked"], true);
        }
    }
    #[test]
    fn memory_depth_is_exact_even_when_not_producer_or_batch_aligned() {
        let cfg = Config {
            mode: "memory".into(),
            memory_depth: 4133,
            idle_secs: vec![],
            ..config("lubq")
        };
        let result = run(&cfg).unwrap();
        assert_eq!(result["memory_depth"], 4133);
        assert_eq!(result["checkpoints"][2]["items_resident"], 4133);
        assert_eq!(result["checkpoints"][3]["items_resident"], 0);
        assert_eq!(
            result["checkpoints"].as_array().unwrap().last().unwrap()["phase"],
            "destroyed"
        );
    }
}
