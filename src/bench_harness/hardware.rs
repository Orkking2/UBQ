//! Bounded, replayable hardware probes. No shared per-item counters in timing mode.
use super::*;
use crossbeam_utils::CachePadded;
use serde_json::{Value, json};
use std::sync::atomic::{AtomicU8, AtomicUsize};

/// Sense-reversing round barrier for pinned benchmark workers.
///
/// `std::sync::Barrier` rendezvouses through a mutex and condvar, so every
/// round wakes every worker through the scheduler. At 144 pinned threads that
/// costs roughly a millisecond per round, which dominated the measured time of
/// the fastest batch configurations in the v3 Grace campaign: an empty round
/// cost ~860 us while a full 8,388,608-item LUBQ batch-256 round at 72p72c took
/// ~1.4 ms, leaving 69% of the sample as harness control flow.
///
/// Workers spin briefly on a generation counter before falling back to
/// `yield_now`, so fast rounds rendezvous in user space while slow rounds
/// (seconds, for the contended scalar queues) still release their cores instead
/// of burning them.
pub struct RoundBarrier {
    participants: usize,
    /// Per-shard arrival counters. Sharding keeps 144 workers off a single
    /// contended cache line; only the last arrival in each shard touches the
    /// root counter.
    shards: Vec<CachePadded<AtomicUsize>>,
    shard_quota: Vec<usize>,
    root: CachePadded<AtomicUsize>,
    generation: CachePadded<AtomicUsize>,
}

/// Arrival shards. Grace exposes 72 cores per socket, so eight shards keep each
/// contended line to at most ~18 writers while the root sees only eight.
const BARRIER_SHARDS: usize = 8;

impl RoundBarrier {
    pub fn new(participants: usize) -> Self {
        let shards = BARRIER_SHARDS.min(participants.max(1));
        Self {
            participants,
            shards: (0..shards)
                .map(|_| CachePadded::new(AtomicUsize::new(0)))
                .collect(),
            shard_quota: (0..shards)
                .map(|shard| (shard..participants).step_by(shards).count())
                .collect(),
            root: CachePadded::new(AtomicUsize::new(0)),
            generation: CachePadded::new(AtomicUsize::new(0)),
        }
    }

    /// `id` must be a stable index below the participant count; each
    /// participant of a given barrier must always pass the same one.
    pub fn wait(&self, id: usize) {
        // A single participant has nobody to rendezvous with.
        if self.participants <= 1 {
            return;
        }
        let generation = self.generation.load(AtomicOrdering::Acquire);
        let shard = id % self.shards.len();
        // Each counter is reset by its own last arrival, before the generation
        // is published, so a fast worker re-entering `wait` cannot count itself
        // into the round it just left.
        if self.shards[shard].fetch_add(1, AtomicOrdering::AcqRel) + 1 == self.shard_quota[shard] {
            self.shards[shard].store(0, AtomicOrdering::Relaxed);
            if self.root.fetch_add(1, AtomicOrdering::AcqRel) + 1 == self.shards.len() {
                self.root.store(0, AtomicOrdering::Relaxed);
                self.generation.fetch_add(1, AtomicOrdering::Release);
                return;
            }
        }
        let mut spins = 0u32;
        while self.generation.load(AtomicOrdering::Acquire) == generation {
            if spins < SPIN_BEFORE_YIELD {
                spins += 1;
                std::hint::spin_loop();
            } else {
                thread::yield_now();
            }
        }
    }
}

/// Reports `pop_batch` over-reservation repair counts when the diagnostic
/// feature is compiled in. A nonzero count is evidence of a recycled block
/// address comparing equal to a stale reservation.
fn batch_repair_counts() -> Value {
    #[cfg(feature = "probe_batch_repair")]
    {
        let (reverts, false_fills, false_fill_slots) = crate::batch_repair_probe::counts();
        json!({
            "reverts": reverts,
            "false_fills": false_fills,
            "false_fill_slots": false_fill_slots,
        })
    }
    #[cfg(not(feature = "probe_batch_repair"))]
    {
        Value::Null
    }
}

/// Bounded spin before a waiting worker releases its core. Long enough to cover
/// a fast round's rendezvous, short enough that a multi-second round does not
/// spend meaningful CPU waiting.
const SPIN_BEFORE_YIELD: u32 = spin_budget();

const fn spin_budget() -> u32 {
    match option_env!("UBQ_ROUND_BARRIER_SPINS") {
        // `const` parsing keeps the budget a compile-time constant so the spin
        // loop stays branch-predictable.
        Some(raw) => konst_parse_u32(raw.as_bytes(), 0),
        // Measured on a 144-core Grace node: at 72p72c a 4096-iteration budget
        // gives a ~97 us empty round, while 65536 degrades it to ~1.25 ms and
        // cuts LUBQ batch throughput from 10.8 to 3.3 G handoffs/s. Long spins
        // keep every waiter hammering the shared arrival line, so the budget
        // stays short and the fallback does the waiting.
        None => 4_096,
    }
}

const fn konst_parse_u32(bytes: &[u8], acc: u32) -> u32 {
    match bytes {
        [] => acc,
        [digit, rest @ ..] if *digit >= b'0' && *digit <= b'9' => {
            konst_parse_u32(rest, acc * 10 + (*digit - b'0') as u32)
        }
        _ => acc,
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    pub queue: String,
    pub scenario: String,
    /// Items per `send_batch` call; `None` pushes one item per call.
    pub producer_batch: Option<usize>,
    /// Items requested per `try_recv_batch` call; `None` pops one item per call.
    /// The two sides are independent, so a workload can push singly and pop in
    /// batches, or the reverse.
    pub consumer_batch: Option<usize>,
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
            producer_batch: None,
            consumer_batch: None,
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
    start: Arc<RoundBarrier>,
    producers_finished: Arc<RoundBarrier>,
    end: Arc<RoundBarrier>,
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
            start: Arc::new(RoundBarrier::new(threads + 1)),
            producers_finished: Arc::new(RoundBarrier::new(scenario.producers + 1)),
            end: Arc::new(RoundBarrier::new(threads + 1)),
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
        let ready = Arc::new(RoundBarrier::new(threads + 1));
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
            let batch = if producer {
                cfg.producer_batch
            } else {
                cfg.consumer_batch
            };
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
                    ready.wait(id);
                    loop {
                        start.wait(id);
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
                            pdone.wait(index);
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
                        end.wait(id);
                    }
                }));
                if result.is_err() {
                    std::process::abort();
                }
            }));
        }
        ready.wait(threads);
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
        self.start.wait(self.slots.len());
        self.producers_finished.wait(producers);
        self.done.store(true, AtomicOrdering::Release);
        self.end.wait(self.slots.len());
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
            // When the exact-once map is live, say what the surplus or deficit
            // actually is: duplicated in-range values, or values never seen.
            let detail = if self.seen.is_empty() {
                String::new()
            } else {
                let missing = self
                    .seen
                    .iter()
                    .filter(|s| s.load(AtomicOrdering::Relaxed) == 0)
                    .count();
                let duplicated = self.bad_value.load(AtomicOrdering::Relaxed);
                format!(" (never-delivered values {missing}; duplicate or out-of-range observed {duplicated})")
            };
            return Err(format!(
                "integrity mismatch: expected {} sent {sent} consumed {received}{detail}",
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
        self.start.wait(self.slots.len());
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
    let alignment = scenario.producers as u64 * cfg.producer_batch.unwrap_or(1) as u64;
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
            "scenario":cfg.scenario,"producer_batch":cfg.producer_batch,"consumer_batch":cfg.consumer_batch,
            "affinity_ok":affinity,
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
        "queue":cfg.queue,"queue_implementation":if cfg.queue=="segqueue" && batched(cfg) {"crossbeam-batchqueue"} else {&cfg.queue},
        "scenario":cfg.scenario,"producer_batch":cfg.producer_batch,"consumer_batch":cfg.consumer_batch,
        "backoff":cfg.backoff,
        "completion_accounting":if cfg!(feature="probe_item_completion") {"item"} else {"segment"},
        "round_items":expected,"rounds":rounds,"warmup_rounds":cfg.warmup_rounds,
        "items":items,"elapsed_ns":elapsed,"ops_per_sec":items as f64*1e9/elapsed as f64,
        "fixed_work":cfg.rounds.is_some(),"affinity_ok":affinity,"exact_once_checked":VERIFY,
        "batch_repair_probe": batch_repair_counts(),
        "empty_round_median_ns":empty_round_median_ns,
        "barrier_overhead_proxy_fraction":empty_round_median_ns as f64*rounds as f64/elapsed as f64,
        "persistent_workers":true,"memory":memory_snapshot(),"os":os,"config":cfg});
    Ok(result)
}

// Module registration is generated from src/queue_variants.tsv. The adapters
// and measurement code remain identical across variants.
include!(concat!(env!("OUT_DIR"), "/queue_dispatch.rs"));

/// Parse a registered owned-buffer queue's optional VBlock geometry. Without
/// a suffix, both dimensions take the implementation's defaults.
fn parse_geometry(name: &str, prefix: &str) -> Result<(Option<usize>, Option<usize>), String> {
    let invalid = || format!("invalid queue geometry: {name}");
    if name == prefix {
        return Ok((None, None));
    }
    let geometry = name.strip_prefix(prefix).and_then(|s| s.strip_prefix("-s")).ok_or_else(invalid)?;
    let (slots, vblocks) = match geometry.split_once("-v") {
        Some((slots, vblocks)) => (slots, Some(vblocks.parse().map_err(|_| invalid())?)),
        None => (geometry, None),
    };
    Ok((Some(slots.parse().map_err(|_| invalid())?), vblocks))
}

/// Whether either side uses a batch API; queues without one run scalar only.
fn batched(cfg: &Config) -> bool {
    cfg.producer_batch.is_some() || cfg.consumer_batch.is_some()
}

pub fn run(cfg: &Config) -> Result<Value, String> {
    if !matches!(cfg.mode.as_str(), "handoff" | "verify" | "memory") {
        return Err("mode must be handoff, verify or memory".into());
    }
    if cfg.rounds == Some(0)
        || cfg.round_items == 0
        || cfg.memory_depth == 0
        || cfg.producer_batch == Some(0)
        || cfg.consumer_batch == Some(0)
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
    if let Some(result) = run_registered(cfg, &scenario, before.clone()) {
        return result;
    }
    match cfg.queue.as_str() {
        "memory-scaffold" if cfg.mode == "memory" => {
            probe!(Arc::new(MemoryScaffold(AtomicU64::new(0))))
        }
        "lubq" if cfg.backoff == "yield" => probe!(LubqBenchQueue::<u64, backoff::Yield>::new(
            scenario.producers,
            scenario.consumers
        )),
        "lubq" => probe!(LubqBenchQueue::<u64, backoff::Crossbeam>::new(
            scenario.producers,
            scenario.consumers
        )),
        "segqueue" if batched(cfg) => probe!(BatchQueue::<u64>::new_queue()),
        "segqueue" => probe!(SegQueue::<u64>::new_queue()),
        "mutex-vecdeque" => probe!(MutexQueue::<u64>::new_queue()),
        "concurrent-queue" if !batched(cfg) => {
            probe!(ConcurrentQueue::<u64>::new_queue())
        }
        "ms-queue" if !batched(cfg) => probe!(MsQueue::<u64>::new_queue()),
        #[cfg(feature = "bench_moodycamel")]
        "moodycamel-cq" => probe!(MoodycamelQueue::new_handle()),
        #[cfg(feature = "bench_fastfifo")]
        "fastfifo" if !batched(cfg) && cfg.block_size > 0 && cfg.capacity > 0 => {
            probe!(RbbqBenchQueue::new(cfg.block_size, cfg.capacity))
        }
        #[cfg(feature = "bench_lfqueue")]
        "lfqueue" if !batched(cfg) && cfg.segment_size > 0 => {
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
            producer_batch: Some(64),
            consumer_batch: Some(64),
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
        for queue in ["mutex-vecdeque", "lubq", "segqueue", "ubq-v8-s64", "ubq-v8-s64-v3"] {
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
        for queue in ["ubq", "lubq", "segqueue", "mutex-vecdeque", "ubq-v8-s64", "ubq-v8-s64-v3"] {
            let mut cfg = config(queue);
            cfg.mode = "verify".into();
            assert_eq!(run(&cfg).unwrap()["exact_once_checked"], true);
        }
    }
    #[test]
    fn producer_and_consumer_batches_are_independent() {
        // Scalar pushes against batched pops and the reverse: the round stays
        // aligned to the producer side, and every item still arrives once.
        for queue in ["ubq", "lubq", "segqueue", "mutex-vecdeque", "ubq-v8-s64-v3"] {
            for (producer, consumer) in [(None, Some(256)), (Some(256), None), (Some(16), Some(4096))] {
                let mut cfg = config(queue);
                cfg.mode = "verify".into();
                cfg.producer_batch = producer;
                cfg.consumer_batch = consumer;
                let result = run(&cfg).unwrap_or_else(|e| panic!("{queue} {producer:?}/{consumer:?}: {e}"));
                assert_eq!(result["exact_once_checked"], true, "{queue}");
                assert_eq!(result["items"], 32768, "{queue} {producer:?}/{consumer:?}");
                assert_eq!(result["producer_batch"], json!(producer));
                assert_eq!(result["consumer_batch"], json!(consumer));
            }
        }
        // A scalar-only queue accepts no batch on either side.
        let mut cfg = config("ms-queue");
        cfg.producer_batch = None;
        assert!(run(&cfg).is_err());
    }
    #[test]
    fn every_registered_queue_runs_under_both_backoff_policies() {
        for queue in ["ubq", "ubq-v8-s64-v3", "ubq-dynblock-s8-v3"] {
            for backoff in ["crossbeam", "yield"] {
                for mode in ["handoff", "verify"] {
                    let mut cfg = config(queue);
                    cfg.backoff = backoff.into();
                    cfg.mode = mode.into();
                    let result = run(&cfg).unwrap_or_else(|e| panic!("{queue} {backoff} {mode}: {e}"));
                    assert_eq!(result["backoff"], backoff, "{queue} {mode}");
                    assert_eq!(result["items"], 32768, "{queue} {backoff} {mode}");
                    if mode == "verify" {
                        assert_eq!(result["exact_once_checked"], true, "{queue} {backoff}");
                    }
                }
            }
        }
        let mut cfg = config("ubq-v8-s64");
        cfg.backoff = "spin".into();
        assert!(run(&cfg).is_err());
    }
    #[test]
    fn v8_geometry_names_select_slots_and_optional_vblocks() {
        assert_eq!(parse_geometry("ubq-v8", "ubq-v8"), Ok((None, None)));
        assert_eq!(parse_geometry("ubq-v8-s4096", "ubq-v8"), Ok((Some(4096), None)));
        assert_eq!(parse_geometry("ubq-v8-s256-v64", "ubq-v8"), Ok((Some(256), Some(64))));
        for bad in ["ubq-v8-s", "ubq-v8-sx", "ubq-v8-s64-v", "ubq-v8-s64-vx"] {
            assert!(parse_geometry(bad, "ubq-v8").is_err(), "{bad}");
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
