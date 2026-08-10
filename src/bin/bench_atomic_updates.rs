mod atomic_views;

use atomic_views::AtomicInt;
use clap::{Parser, ValueEnum};
use crossbeam_utils::{Backoff, CachePadded};
use portable_atomic::{AtomicBool, AtomicU64, AtomicU128, Ordering};
use serde::Serialize;
use std::collections::BTreeSet;
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Barrier, OnceLock};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const SCHEMA_VERSION: u32 = 3;
const UPDATE_CHUNK: usize = 4096;
const DEFAULT_RUNS_DIR: &str = "bench_results/runs";
const DEFAULT_BLOCK_SIZES: &str = "31,127,511,2047,4095";
const DEFAULT_ALIGNMENT: u64 = 4096;
const GENERATION_BITS: u32 = 32;
const INDEX_BITS: u32 = 32;
const INDEX_MASK: u64 = u32::MAX as u64;
// SegQueue reserves the low bit for metadata and advances its position by two.
const SEGQUEUE_SHIFT: u32 = 1;
const SEGQUEUE_INDEX_STEP: u64 = 1 << SEGQUEUE_SHIFT;

#[derive(Parser, Debug)]
#[command(name = "bench_atomic_updates")]
#[command(
    about = "Compare block-aware U64, SegQueue-style, and generation-cached U128/U64 updates"
)]
struct Args {
    /// Stable label used to group runs from the same machine.
    #[arg(long)]
    machine_label: String,

    /// Root directory for timestamped JSON results.
    #[arg(long, default_value = DEFAULT_RUNS_DIR)]
    runs_dir: PathBuf,

    /// Write to this exact path instead of the timestamped runs directory.
    #[arg(long)]
    output: Option<PathBuf>,

    /// Cap updater threads; powers of two at or below the cap are measured.
    #[arg(long)]
    max_updaters: Option<usize>,

    /// Comma-separated block sizes measured for every applicable layout and update method.
    #[arg(long, value_delimiter = ',', default_value = DEFAULT_BLOCK_SIZES)]
    block_sizes: Vec<u32>,

    /// Power-of-two spacing between synthetic block pointers.
    #[arg(long, default_value_t = DEFAULT_ALIGNMENT)]
    alignment: u64,

    /// Measurement duration for each block/layout/method/thread-count sample.
    #[arg(long, default_value_t = 500)]
    duration_ms: u64,

    /// Untimed warm-up before every measured sample.
    #[arg(long, default_value_t = 100)]
    warmup_ms: u64,

    /// Complete block/layout/method samples at each updater count.
    #[arg(long, default_value_t = 5)]
    repeats: usize,

    /// Atomic ordering profile to benchmark.
    #[arg(long, value_enum, default_value_t = OrderingProfile::Ubq)]
    ordering: OrderingProfile,

    /// Print the resolved plan without running or writing results.
    #[arg(long)]
    dry_run: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
enum OrderingProfile {
    /// Match UBQ's and SegQueue's producer-head load, RMW, and publication orderings.
    Ubq,
    /// Isolate the raw atomic mechanisms with relaxed ordering.
    Relaxed,
}

impl OrderingProfile {
    fn name(self) -> &'static str {
        match self {
            Self::Ubq => "ubq",
            Self::Relaxed => "relaxed",
        }
    }

    fn load_ordering(self) -> Ordering {
        match self {
            Self::Ubq => Ordering::Acquire,
            Self::Relaxed => Ordering::Relaxed,
        }
    }

    fn cas_success_ordering(self) -> Ordering {
        match self {
            Self::Ubq => Ordering::SeqCst,
            Self::Relaxed => Ordering::Relaxed,
        }
    }

    fn faa_ordering(self) -> Ordering {
        match self {
            Self::Ubq => Ordering::Acquire,
            Self::Relaxed => Ordering::Relaxed,
        }
    }

    fn failure_ordering(self) -> Ordering {
        match self {
            Self::Ubq => Ordering::Acquire,
            Self::Relaxed => Ordering::Relaxed,
        }
    }

    fn store_ordering(self) -> Ordering {
        match self {
            Self::Ubq => Ordering::Release,
            Self::Relaxed => Ordering::Relaxed,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum CounterLayout {
    U64,
    MixedU128U64,
}

impl CounterLayout {
    fn name(self) -> &'static str {
        match self {
            Self::U64 => "u64",
            Self::MixedU128U64 => "mixed",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum UpdateMethod {
    Cas,
    CasBackoff,
    Faa,
    #[serde(rename = "segqueue")]
    SegQueue,
}

impl UpdateMethod {
    fn name(self) -> &'static str {
        match self {
            Self::Cas => "cas",
            Self::CasBackoff => "casb",
            Self::Faa => "faa",
            Self::SegQueue => "sgq",
        }
    }

    fn uses_cas(self) -> bool {
        matches!(self, Self::Cas | Self::CasBackoff | Self::SegQueue)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct BenchmarkCase {
    layout: CounterLayout,
    method: UpdateMethod,
}

const CASES: [BenchmarkCase; 7] = [
    BenchmarkCase {
        layout: CounterLayout::U64,
        method: UpdateMethod::Cas,
    },
    BenchmarkCase {
        layout: CounterLayout::MixedU128U64,
        method: UpdateMethod::Cas,
    },
    BenchmarkCase {
        layout: CounterLayout::U64,
        method: UpdateMethod::CasBackoff,
    },
    BenchmarkCase {
        layout: CounterLayout::MixedU128U64,
        method: UpdateMethod::CasBackoff,
    },
    BenchmarkCase {
        layout: CounterLayout::U64,
        method: UpdateMethod::Faa,
    },
    BenchmarkCase {
        layout: CounterLayout::MixedU128U64,
        method: UpdateMethod::Faa,
    },
    BenchmarkCase {
        layout: CounterLayout::U64,
        method: UpdateMethod::SegQueue,
    },
];

fn case_order(offset: usize) -> [BenchmarkCase; CASES.len()] {
    let mut cases = CASES;
    let case_count = cases.len();
    cases.rotate_left(offset % case_count);
    cases
}

#[derive(Clone, Copy, Debug)]
struct BlockConfig {
    block_size: u32,
    alignment: u64,
}

impl BlockConfig {
    fn new(block_size: u32, alignment: u64) -> Result<Self, String> {
        if block_size == 0 {
            return Err("block sizes must be greater than zero".to_string());
        }
        if !alignment.is_power_of_two() {
            return Err("--alignment must be a nonzero power of two".to_string());
        }
        Ok(Self {
            block_size,
            alignment,
        })
    }

    #[inline]
    fn index(self, value: u64) -> u32 {
        value as u32
    }

    #[inline]
    fn generation(self, value: u64) -> u32 {
        (value >> INDEX_BITS) as u32
    }

    #[inline]
    fn pack_low(self, generation: u32, index: u32) -> u64 {
        (u64::from(generation) << INDEX_BITS) | u64::from(index)
    }

    #[inline]
    fn next_low(self, value: u64) -> u64 {
        self.pack_low(self.generation(value).wrapping_add(1), 0)
    }

    #[inline]
    fn pack_full(self, pointer: u64, low: u64) -> u128 {
        (u128::from(pointer) << 64) | u128::from(low)
    }

    fn expected_low(self, operations: u64) -> Result<u64, String> {
        let completed_blocks = operations / u64::from(self.block_size);
        let generation = u32::try_from(completed_blocks)
            .map_err(|_| "32-bit generation counter wrapped during one sample".to_string())?;
        let index = u32::try_from(operations % u64::from(self.block_size))
            .map_err(|_| "final index did not fit in u32".to_string())?;
        Ok(self.pack_low(generation, index))
    }

    fn expected_pointer(self, operations: u64) -> Result<u64, String> {
        let completed_blocks = operations / u64::from(self.block_size);
        completed_blocks
            .checked_add(1)
            .and_then(|blocks| blocks.checked_mul(self.alignment))
            .ok_or_else(|| "synthetic block pointer overflowed u64".to_string())
    }

    #[inline]
    fn segqueue_lap(self) -> u64 {
        // Like SegQueue's LAP/BLOCK_CAP pair, one offset per lap is a sentinel.
        u64::from(self.block_size) + 1
    }

    #[inline]
    fn segqueue_offset(self, value: u64) -> u32 {
        ((value >> SEGQUEUE_SHIFT) % self.segqueue_lap()) as u32
    }

    fn expected_segqueue_position(self, operations: u64) -> Result<u64, String> {
        let block_size = u64::from(self.block_size);
        let completed_blocks = operations / block_size;
        let index = operations % block_size;
        completed_blocks
            .checked_mul(self.segqueue_lap())
            .and_then(|position| position.checked_add(index))
            .and_then(|position| position.checked_mul(SEGQUEUE_INDEX_STEP))
            .ok_or_else(|| "SegQueue-style position overflowed u64".to_string())
    }
}

fn normalize_block_sizes(values: &[u32]) -> Result<Vec<u32>, String> {
    let sizes = values.iter().copied().collect::<BTreeSet<_>>();
    if sizes.is_empty() {
        return Err("--block-sizes must contain at least one size".to_string());
    }
    if sizes.contains(&0) {
        return Err("block sizes must be greater than zero".to_string());
    }
    Ok(sizes.into_iter().collect())
}

fn block_order(blocks: &[BlockConfig], repeat_index: usize) -> Vec<BlockConfig> {
    let mut ordered = blocks.to_vec();
    let block_count = ordered.len();
    ordered.rotate_left((repeat_index - 1) % block_count);
    ordered
}

#[derive(Clone, Copy, Default, Debug)]
struct CachedHead {
    generation: u32,
    pointer: u64,
}

#[derive(Default, Debug)]
struct UpdateStats {
    operations: u64,
    cas_failures: u64,
    boundary_stores: u64,
    boundary_waits: u64,
    invalid_faa_reservations: u64,
    wide_loads: u64,
    wide_stores: u64,
}

#[derive(Debug)]
struct ThreadOutcome {
    stats: UpdateStats,
    elapsed_ns: u64,
    pinned: bool,
}

#[derive(Debug, Serialize)]
struct RunMeta {
    timestamp_unix_ms: u128,
    machine_label: String,
    target_arch: &'static str,
    target_os: &'static str,
    target_endian: &'static str,
    pointer_width_bits: usize,
    available_parallelism: usize,
    effective_thread_limit: usize,
    enumerated_core_ids: Vec<usize>,
    updater_counts: Vec<usize>,
    block_sizes: Vec<u32>,
    alignment: u64,
    pointer_field_bits: u32,
    generation_field_bits: u32,
    index_field_bits: u32,
    duration_ms: u64,
    warmup_ms: u64,
    repeats: usize,
    ordering: OrderingProfile,
    update_chunk: usize,
    wide_atomic_lock_free: bool,
    mixed_width_memory_model_supported: bool,
}

#[derive(Debug, Serialize)]
struct Sample {
    repeat_index: usize,
    updater_count: usize,
    block_size: u32,
    layout: CounterLayout,
    method: UpdateMethod,
    operations: u64,
    elapsed_ns: u64,
    ops_per_sec: f64,
    cas_failures: u64,
    cas_attempts_per_update: Option<f64>,
    boundary_stores: u64,
    boundary_waits: u64,
    invalid_faa_reservations: u64,
    wide_loads: u64,
    wide_loads_per_update: Option<f64>,
    wide_stores: u64,
    final_generation: u32,
    final_index: u32,
    final_pointer: Option<u64>,
    final_low_value: u64,
    final_full_value_hex: Option<String>,
    assigned_core_ids: Vec<usize>,
    pinned_threads: usize,
}

#[derive(Debug, Serialize)]
struct OutputFile {
    benchmark: &'static str,
    schema_version: u32,
    meta: RunMeta,
    results: Vec<Sample>,
}

struct WordCounter {
    value: AtomicU64,
}

impl WordCounter {
    fn new(value: u64) -> Self {
        Self {
            value: AtomicU64::new(value),
        }
    }
}

trait BlockCounter {
    fn initialize(
        &self,
        ordering: Ordering,
        block: BlockConfig,
        cache: &mut CachedHead,
        stats: &mut UpdateStats,
    );

    fn narrow_load(&self, ordering: Ordering) -> u64;

    fn synchronize_generation(
        &self,
        observed: u64,
        ordering: Ordering,
        block: BlockConfig,
        cache: &mut CachedHead,
        stats: &mut UpdateStats,
    ) -> u64;

    fn compare_exchange_weak(
        &self,
        current: u64,
        new: u64,
        success: Ordering,
        failure: Ordering,
    ) -> Result<u64, u64>;

    fn fetch_add(&self, value: u64, ordering: Ordering) -> u64;

    fn narrow_store(&self, value: u64, ordering: Ordering);

    fn publish_next(
        &self,
        current: u64,
        ordering: Ordering,
        block: BlockConfig,
        cache: &mut CachedHead,
        stats: &mut UpdateStats,
    );

    fn final_values(&self) -> (u64, Option<u128>);
}

impl BlockCounter for WordCounter {
    fn initialize(
        &self,
        ordering: Ordering,
        block: BlockConfig,
        cache: &mut CachedHead,
        _stats: &mut UpdateStats,
    ) {
        cache.generation = block.generation(self.value.load(ordering));
    }

    #[inline]
    fn narrow_load(&self, ordering: Ordering) -> u64 {
        self.value.load(ordering)
    }

    #[inline]
    fn synchronize_generation(
        &self,
        observed: u64,
        _ordering: Ordering,
        block: BlockConfig,
        cache: &mut CachedHead,
        _stats: &mut UpdateStats,
    ) -> u64 {
        cache.generation = block.generation(observed);
        observed
    }

    #[inline]
    fn compare_exchange_weak(
        &self,
        current: u64,
        new: u64,
        success: Ordering,
        failure: Ordering,
    ) -> Result<u64, u64> {
        self.value
            .compare_exchange_weak(current, new, success, failure)
    }

    #[inline]
    fn fetch_add(&self, value: u64, ordering: Ordering) -> u64 {
        self.value.fetch_add(value, ordering)
    }

    #[inline]
    fn narrow_store(&self, value: u64, ordering: Ordering) {
        self.value.store(value, ordering);
    }

    #[inline]
    fn publish_next(
        &self,
        current: u64,
        ordering: Ordering,
        block: BlockConfig,
        cache: &mut CachedHead,
        stats: &mut UpdateStats,
    ) {
        let next = block.next_low(current);
        self.value.store(next, ordering);
        cache.generation = block.generation(next);
        stats.boundary_stores = stats.boundary_stores.wrapping_add(1);
    }

    fn final_values(&self) -> (u64, Option<u128>) {
        (self.value.load(Ordering::Relaxed), None)
    }
}

impl BlockCounter for AtomicInt {
    fn initialize(
        &self,
        ordering: Ordering,
        block: BlockConfig,
        cache: &mut CachedHead,
        stats: &mut UpdateStats,
    ) {
        let full = self.as_u128().load(ordering);
        let low = full as u64;
        cache.generation = block.generation(low);
        cache.pointer = (full >> 64) as u64;
        stats.wide_loads = stats.wide_loads.wrapping_add(1);
    }

    #[inline]
    fn narrow_load(&self, ordering: Ordering) -> u64 {
        self.as_u64().load(ordering)
    }

    #[inline]
    fn synchronize_generation(
        &self,
        observed: u64,
        ordering: Ordering,
        block: BlockConfig,
        cache: &mut CachedHead,
        stats: &mut UpdateStats,
    ) -> u64 {
        if block.generation(observed) == cache.generation {
            return observed;
        }

        let full = self.as_u128().load(ordering);
        let low = full as u64;
        cache.generation = block.generation(low);
        cache.pointer = (full >> 64) as u64;
        stats.wide_loads = stats.wide_loads.wrapping_add(1);
        low
    }

    #[inline]
    fn compare_exchange_weak(
        &self,
        current: u64,
        new: u64,
        success: Ordering,
        failure: Ordering,
    ) -> Result<u64, u64> {
        self.as_u64()
            .compare_exchange_weak(current, new, success, failure)
    }

    #[inline]
    fn fetch_add(&self, value: u64, ordering: Ordering) -> u64 {
        self.as_u64().fetch_add(value, ordering)
    }

    #[inline]
    fn narrow_store(&self, value: u64, ordering: Ordering) {
        self.as_u64().store(value, ordering);
    }

    #[inline]
    fn publish_next(
        &self,
        current: u64,
        ordering: Ordering,
        block: BlockConfig,
        cache: &mut CachedHead,
        stats: &mut UpdateStats,
    ) {
        let next_low = block.next_low(current);
        let next_pointer = cache.pointer.wrapping_add(block.alignment);
        self.as_u128()
            .store(block.pack_full(next_pointer, next_low), ordering);
        cache.generation = block.generation(next_low);
        cache.pointer = next_pointer;
        stats.boundary_stores = stats.boundary_stores.wrapping_add(1);
        stats.wide_stores = stats.wide_stores.wrapping_add(1);
    }

    fn final_values(&self) -> (u64, Option<u128>) {
        let full = self.as_u128().load(Ordering::Relaxed);
        (full as u64, Some(full))
    }
}

enum Counter {
    Word(WordCounter),
    Mixed(AtomicInt),
}

impl Counter {
    fn new(layout: CounterLayout, block: BlockConfig) -> Self {
        let low = block.pack_low(0, 0);
        match layout {
            CounterLayout::U64 => Self::Word(WordCounter::new(low)),
            CounterLayout::MixedU128U64 => {
                Self::Mixed(AtomicInt::new(block.pack_full(block.alignment, low)))
            }
        }
    }

    fn final_values(&self) -> (u64, Option<u128>) {
        match self {
            Self::Word(counter) => counter.final_values(),
            Self::Mixed(counter) => counter.final_values(),
        }
    }
}

fn updater_counts(limit: usize) -> Result<Vec<usize>, String> {
    if limit == 0 {
        return Err("effective updater limit must be greater than zero".to_string());
    }
    let mut counts = Vec::new();
    let mut count = 1_usize;
    while count <= limit {
        counts.push(count);
        let Some(next) = count.checked_mul(2) else {
            break;
        };
        count = next;
    }
    Ok(counts)
}

#[inline]
fn run_cas_chunk<C: BlockCounter>(
    atomic: &C,
    method: UpdateMethod,
    profile: OrderingProfile,
    block: BlockConfig,
    cache: &mut CachedHead,
    stats: &mut UpdateStats,
) {
    let backoff = Backoff::new();
    for _ in 0..UPDATE_CHUNK {
        let observed = atomic.narrow_load(profile.load_ordering());
        let mut current =
            atomic.synchronize_generation(observed, profile.load_ordering(), block, cache, stats);
        loop {
            let index = block.index(current);
            if index >= block.block_size {
                stats.boundary_waits = stats.boundary_waits.wrapping_add(1);
                backoff.snooze();
                let observed = atomic.narrow_load(profile.load_ordering());
                current = atomic.synchronize_generation(
                    observed,
                    profile.load_ordering(),
                    block,
                    cache,
                    stats,
                );
                continue;
            }

            match atomic.compare_exchange_weak(
                current,
                current.wrapping_add(1),
                profile.cas_success_ordering(),
                profile.failure_ordering(),
            ) {
                Ok(_) => {
                    if index + 1 == block.block_size {
                        atomic.publish_next(current, profile.store_ordering(), block, cache, stats);
                    }
                    stats.operations = stats.operations.wrapping_add(1);
                    backoff.reset();
                    break;
                }
                Err(actual) => {
                    current = atomic.synchronize_generation(
                        actual,
                        profile.load_ordering(),
                        block,
                        cache,
                        stats,
                    );
                    stats.cas_failures = stats.cas_failures.wrapping_add(1);
                    if method == UpdateMethod::CasBackoff {
                        backoff.spin();
                    }
                }
            }
        }
    }
}

#[inline]
fn run_faa_chunk<C: BlockCounter>(
    atomic: &C,
    profile: OrderingProfile,
    block: BlockConfig,
    cache: &mut CachedHead,
    stats: &mut UpdateStats,
) {
    let backoff = Backoff::new();
    for _ in 0..UPDATE_CHUNK {
        loop {
            let observed = atomic.narrow_load(profile.load_ordering());
            let current = atomic.synchronize_generation(
                observed,
                profile.load_ordering(),
                block,
                cache,
                stats,
            );
            if block.index(current) >= block.block_size {
                stats.boundary_waits = stats.boundary_waits.wrapping_add(1);
                backoff.snooze();
                continue;
            }

            let reserved = atomic.fetch_add(1, profile.faa_ordering());
            let reserved_generation = block.generation(reserved);
            if reserved_generation != cache.generation {
                let _ = atomic.synchronize_generation(
                    reserved,
                    profile.load_ordering(),
                    block,
                    cache,
                    stats,
                );
            }
            let index = block.index(reserved);
            if index >= block.block_size {
                stats.invalid_faa_reservations = stats.invalid_faa_reservations.wrapping_add(1);
                backoff.snooze();
                continue;
            }

            if index + 1 == block.block_size {
                atomic.publish_next(reserved, profile.store_ordering(), block, cache, stats);
            }
            stats.operations = stats.operations.wrapping_add(1);
            backoff.reset();
            break;
        }
    }
}

#[inline]
fn run_segqueue_chunk<C: BlockCounter>(
    atomic: &C,
    profile: OrderingProfile,
    block: BlockConfig,
    stats: &mut UpdateStats,
) {
    let backoff = Backoff::new();
    for _ in 0..UPDATE_CHUNK {
        let mut tail = atomic.narrow_load(profile.load_ordering());
        loop {
            let offset = block.segqueue_offset(tail);
            if offset == block.block_size {
                stats.boundary_waits = stats.boundary_waits.wrapping_add(1);
                backoff.snooze();
                tail = atomic.narrow_load(profile.load_ordering());
                continue;
            }

            let new_tail = tail.wrapping_add(SEGQUEUE_INDEX_STEP);
            match atomic.compare_exchange_weak(
                tail,
                new_tail,
                profile.cas_success_ordering(),
                profile.failure_ordering(),
            ) {
                Ok(_) => {
                    if offset + 1 == block.block_size {
                        atomic.narrow_store(
                            new_tail.wrapping_add(SEGQUEUE_INDEX_STEP),
                            profile.store_ordering(),
                        );
                        stats.boundary_stores = stats.boundary_stores.wrapping_add(1);
                    }
                    stats.operations = stats.operations.wrapping_add(1);
                    backoff.reset();
                    break;
                }
                Err(actual) => {
                    tail = actual;
                    stats.cas_failures = stats.cas_failures.wrapping_add(1);
                    backoff.spin();
                }
            }
        }
    }
}

fn run_worker<C: BlockCounter>(
    counter: &C,
    method: UpdateMethod,
    profile: OrderingProfile,
    block: BlockConfig,
    stop: &AtomicBool,
) -> UpdateStats {
    let mut stats = UpdateStats::default();
    let mut cache = CachedHead::default();
    counter.initialize(profile.load_ordering(), block, &mut cache, &mut stats);

    match method {
        UpdateMethod::Cas | UpdateMethod::CasBackoff => {
            while !stop.load(Ordering::Acquire) {
                run_cas_chunk(counter, method, profile, block, &mut cache, &mut stats);
            }
        }
        UpdateMethod::Faa => {
            while !stop.load(Ordering::Acquire) {
                run_faa_chunk(counter, profile, block, &mut cache, &mut stats);
            }
        }
        UpdateMethod::SegQueue => {
            while !stop.load(Ordering::Acquire) {
                run_segqueue_chunk(counter, profile, block, &mut stats);
            }
        }
    }
    stats
}

fn run_trial(
    updater_count: usize,
    case: BenchmarkCase,
    profile: OrderingProfile,
    block: BlockConfig,
    duration: Duration,
    core_ids: &[core_affinity::CoreId],
) -> Result<Sample, String> {
    let counter = Arc::new(CachePadded::new(Counter::new(case.layout, block)));
    let stop = Arc::new(CachePadded::new(AtomicBool::new(false)));
    let ready = Arc::new(Barrier::new(updater_count + 1));
    let start_gate = Arc::new(Barrier::new(updater_count + 1));
    let common_start = Arc::new(OnceLock::<Instant>::new());
    let mut handles = Vec::with_capacity(updater_count);

    for thread_index in 0..updater_count {
        let counter = Arc::clone(&counter);
        let stop = Arc::clone(&stop);
        let ready = Arc::clone(&ready);
        let start_gate = Arc::clone(&start_gate);
        let common_start = Arc::clone(&common_start);
        let core_id = core_ids.get(thread_index).copied();
        handles.push(
            thread::Builder::new()
                .name(format!(
                    "atomic-update-b{}-{}-{}-{thread_index}",
                    block.block_size,
                    case.layout.name(),
                    case.method.name()
                ))
                .spawn(move || {
                    let pinned = core_id.is_some_and(core_affinity::set_for_current);
                    ready.wait();
                    start_gate.wait();
                    let started = *common_start.get().expect("benchmark start initialized");
                    let stats = match &**counter {
                        Counter::Word(counter) => {
                            run_worker(counter, case.method, profile, block, &stop)
                        }
                        Counter::Mixed(counter) => {
                            run_worker(counter, case.method, profile, block, &stop)
                        }
                    };

                    ThreadOutcome {
                        stats,
                        elapsed_ns: started.elapsed().as_nanos().min(u64::MAX as u128) as u64,
                        pinned,
                    }
                })
                .map_err(|err| format!("failed to spawn updater thread {thread_index}: {err}"))?,
        );
    }

    ready.wait();
    common_start
        .set(Instant::now())
        .map_err(|_| "benchmark start was initialized twice".to_string())?;
    start_gate.wait();
    thread::sleep(duration);
    stop.store(true, Ordering::Release);

    let mut outcomes = Vec::with_capacity(updater_count);
    for handle in handles {
        outcomes.push(
            handle
                .join()
                .map_err(|_| "atomic updater thread panicked".to_string())?,
        );
    }

    let operations = outcomes
        .iter()
        .map(|outcome| outcome.stats.operations)
        .sum();
    let cas_failures = outcomes
        .iter()
        .map(|outcome| outcome.stats.cas_failures)
        .sum();
    let boundary_stores = outcomes
        .iter()
        .map(|outcome| outcome.stats.boundary_stores)
        .sum();
    let boundary_waits = outcomes
        .iter()
        .map(|outcome| outcome.stats.boundary_waits)
        .sum();
    let invalid_faa_reservations = outcomes
        .iter()
        .map(|outcome| outcome.stats.invalid_faa_reservations)
        .sum();
    let wide_loads = outcomes
        .iter()
        .map(|outcome| outcome.stats.wide_loads)
        .sum();
    let wide_stores = outcomes
        .iter()
        .map(|outcome| outcome.stats.wide_stores)
        .sum();
    let elapsed_ns = outcomes
        .iter()
        .map(|outcome| outcome.elapsed_ns)
        .max()
        .ok_or_else(|| "benchmark produced no worker timing".to_string())?;
    let (final_low_value, final_full_value) = counter.final_values();
    let expected_low = if case.method == UpdateMethod::SegQueue {
        block.expected_segqueue_position(operations)?
    } else {
        block.expected_low(operations)?
    };
    let expected_boundary_stores = operations / u64::from(block.block_size);
    if final_low_value != expected_low || boundary_stores != expected_boundary_stores {
        return Err(format!(
            "block {}/{}/{} validation failed: final low {final_low_value:#x} \
             (expected {expected_low:#x}), boundary stores {boundary_stores} \
             (expected {expected_boundary_stores})",
            block.block_size,
            case.layout.name(),
            case.method.name()
        ));
    }

    let final_pointer = final_full_value.map(|full| (full >> 64) as u64);
    if let Some(pointer) = final_pointer {
        let expected_pointer = block.expected_pointer(operations)?;
        if pointer != expected_pointer || wide_stores != boundary_stores {
            return Err(format!(
                "block {}/{}/{} validation failed: pointer {pointer:#x} \
                 (expected {expected_pointer:#x}), wide stores {wide_stores} \
                 (expected {boundary_stores})",
                block.block_size,
                case.layout.name(),
                case.method.name()
            ));
        }
    } else if wide_loads != 0 || wide_stores != 0 {
        return Err("U64 layout unexpectedly recorded wide atomic accesses".to_string());
    }

    let (final_generation, final_index) = if case.method == UpdateMethod::SegQueue {
        let completed_blocks = operations / u64::from(block.block_size);
        let generation = u32::try_from(completed_blocks)
            .map_err(|_| "SegQueue-style lap counter did not fit in u32".to_string())?;
        let index = u32::try_from(operations % u64::from(block.block_size))
            .map_err(|_| "SegQueue-style final offset did not fit in u32".to_string())?;
        (generation, index)
    } else {
        (
            block.generation(final_low_value),
            block.index(final_low_value),
        )
    };

    Ok(Sample {
        repeat_index: 0,
        updater_count,
        block_size: block.block_size,
        layout: case.layout,
        method: case.method,
        operations,
        elapsed_ns,
        ops_per_sec: operations as f64 * 1_000_000_000.0 / elapsed_ns as f64,
        cas_failures,
        cas_attempts_per_update: case
            .method
            .uses_cas()
            .then_some(1.0 + cas_failures as f64 / operations as f64),
        boundary_stores,
        boundary_waits,
        invalid_faa_reservations,
        wide_loads,
        wide_loads_per_update: (case.layout == CounterLayout::MixedU128U64)
            .then_some(wide_loads as f64 / operations as f64),
        wide_stores,
        final_generation,
        final_index,
        final_pointer,
        final_low_value,
        final_full_value_hex: final_full_value.map(|value| format!("{value:#034x}")),
        assigned_core_ids: core_ids
            .iter()
            .take(updater_count)
            .map(|core| core.id)
            .collect(),
        pinned_threads: outcomes.iter().filter(|outcome| outcome.pinned).count(),
    })
}

fn sanitize_component(value: &str) -> String {
    let sanitized = value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_') {
                ch
            } else {
                '_'
            }
        })
        .collect::<String>();
    if sanitized.is_empty() {
        "local".to_string()
    } else {
        sanitized
    }
}

fn timestamp_unix_ms() -> Result<u128, String> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .map_err(|err| format!("system clock is before the Unix epoch: {err}"))
}

fn output_path(args: &Args, timestamp: u128) -> PathBuf {
    args.output.clone().unwrap_or_else(|| {
        args.runs_dir
            .join(sanitize_component(&args.machine_label))
            .join("atomic_updates")
            .join(format!(
                "atomic-updates-{timestamp}-{}.json",
                std::process::id()
            ))
    })
}

fn atomic_write_json(path: &Path, output: &OutputFile) -> Result<(), String> {
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        fs::create_dir_all(parent)
            .map_err(|err| format!("failed to create {}: {err}", parent.display()))?;
    }
    let json = serde_json::to_string(output)
        .map_err(|err| format!("failed to serialize benchmark results: {err}"))?;
    let file_name = path
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_else(|| "atomic-updates.json".to_string());
    let temporary = path.with_file_name(format!(".{file_name}.tmp"));
    {
        let mut file = fs::File::create(&temporary)
            .map_err(|err| format!("failed to create {}: {err}", temporary.display()))?;
        file.write_all(json.as_bytes())
            .map_err(|err| format!("failed to write {}: {err}", temporary.display()))?;
        file.sync_all()
            .map_err(|err| format!("failed to flush {}: {err}", temporary.display()))?;
    }
    match fs::rename(&temporary, path) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == io::ErrorKind::AlreadyExists => {
            fs::remove_file(path).map_err(|remove_err| {
                format!("failed to replace {}: {remove_err}", path.display())
            })?;
            fs::rename(&temporary, path)
                .map_err(|rename_err| format!("failed to publish {}: {rename_err}", path.display()))
        }
        Err(err) => {
            let _ = fs::remove_file(&temporary);
            Err(format!("failed to publish {}: {err}", path.display()))
        }
    }
}

fn run(args: Args) -> Result<(), String> {
    if args.machine_label.trim().is_empty() {
        return Err("--machine-label must not be empty".to_string());
    }
    if args.duration_ms == 0 {
        return Err("--duration-ms must be greater than zero".to_string());
    }
    if args.repeats == 0 {
        return Err("--repeats must be greater than zero".to_string());
    }
    if args.max_updaters == Some(0) {
        return Err("--max-updaters must be greater than zero".to_string());
    }
    let block_sizes = normalize_block_sizes(&args.block_sizes)?;
    let blocks = block_sizes
        .iter()
        .map(|size| BlockConfig::new(*size, args.alignment))
        .collect::<Result<Vec<_>, _>>()?;
    if !AtomicU128::is_lock_free() {
        return Err(
            "AtomicU128 is not lock-free on this target; the mixed-width hardware \
             experiment cannot safely bypass portable-atomic's fallback lock"
                .to_string(),
        );
    }

    let available_parallelism = thread::available_parallelism()
        .map_err(|err| format!("unable to detect available parallelism: {err}"))?
        .get();
    let core_ids = core_affinity::get_core_ids().unwrap_or_default();
    let detected_limit = if core_ids.is_empty() {
        available_parallelism
    } else {
        available_parallelism.min(core_ids.len())
    };
    let effective_thread_limit = args
        .max_updaters
        .unwrap_or(detected_limit)
        .min(detected_limit);
    let counts = updater_counts(effective_thread_limit)?;
    let largest_block = u64::from(*block_sizes.last().expect("nonempty block sizes"));
    if largest_block
        .checked_add(u64::try_from(effective_thread_limit).unwrap_or(u64::MAX))
        .is_none_or(|limit| limit > INDEX_MASK)
    {
        return Err(
            "largest block plus possible in-flight FAA overshoot exceeds the u32 index field"
                .to_string(),
        );
    }

    println!("machine: {}", args.machine_label);
    println!("available parallelism: {available_parallelism}");
    println!("enumerated affinity cores: {}", core_ids.len());
    println!("effective thread limit: {effective_thread_limit}");
    println!(
        "updater counts: {}",
        counts
            .iter()
            .map(usize::to_string)
            .collect::<Vec<_>>()
            .join(",")
    );
    println!(
        "block sizes: {}",
        block_sizes
            .iter()
            .map(u32::to_string)
            .collect::<Vec<_>>()
            .join(",")
    );
    println!(
        "wide state: 64-bit pointer + {GENERATION_BITS}-bit generation + \
         {INDEX_BITS}-bit index; pointer alignment {}",
        args.alignment
    );
    println!("ordering: {}", args.ordering.name());
    println!("AtomicU128 lock-free: yes");
    println!(
        "sample timing: {}ms warm-up + {}ms measured, {} repeat(s)",
        args.warmup_ms, args.duration_ms, args.repeats
    );
    eprintln!(
        "warning: mixed_u128_u64 deliberately uses overlapping mixed-size atomics, \
         which Rust's memory model does not support"
    );
    if args.dry_run {
        return Ok(());
    }

    let measured_duration = Duration::from_millis(args.duration_ms);
    let warmup_duration = Duration::from_millis(args.warmup_ms);
    let mut results = Vec::with_capacity(counts.len() * args.repeats * blocks.len() * CASES.len());

    for &updaters in &counts {
        for repeat_index in 1..=args.repeats {
            for (block_position, block) in
                block_order(&blocks, repeat_index).into_iter().enumerate()
            {
                for case in case_order(repeat_index - 1 + block_position) {
                    if !warmup_duration.is_zero() {
                        run_trial(
                            updaters,
                            case,
                            args.ordering,
                            block,
                            warmup_duration,
                            &core_ids,
                        )?;
                    }
                    let mut sample = run_trial(
                        updaters,
                        case,
                        args.ordering,
                        block,
                        measured_duration,
                        &core_ids,
                    )?;
                    sample.repeat_index = repeat_index;
                    println!(
                        "{:>3} updater(s) | repeat {:>2} | block {:>4} | {:>15} | \
                         {:>11} | {:>11.3} Mops/s | wide loads/update {:>9}",
                        updaters,
                        repeat_index,
                        block.block_size,
                        case.layout.name(),
                        case.method.name().to_ascii_uppercase(),
                        sample.ops_per_sec / 1_000_000.0,
                        sample
                            .wide_loads_per_update
                            .map(|value| format!("{value:.6}"))
                            .unwrap_or_else(|| "-".to_string()),
                    );
                    results.push(sample);
                }
            }
        }
    }

    let timestamp = timestamp_unix_ms()?;
    let path = output_path(&args, timestamp);
    let output = OutputFile {
        benchmark: "atomic_updates",
        schema_version: SCHEMA_VERSION,
        meta: RunMeta {
            timestamp_unix_ms: timestamp,
            machine_label: args.machine_label.trim().to_string(),
            target_arch: std::env::consts::ARCH,
            target_os: std::env::consts::OS,
            target_endian: if cfg!(target_endian = "little") {
                "little"
            } else {
                "big"
            },
            pointer_width_bits: usize::BITS as usize,
            available_parallelism,
            effective_thread_limit,
            enumerated_core_ids: core_ids.iter().map(|core| core.id).collect(),
            updater_counts: counts,
            block_sizes,
            alignment: args.alignment,
            pointer_field_bits: 64,
            generation_field_bits: GENERATION_BITS,
            index_field_bits: INDEX_BITS,
            duration_ms: args.duration_ms,
            warmup_ms: args.warmup_ms,
            repeats: args.repeats,
            ordering: args.ordering,
            update_chunk: UPDATE_CHUNK,
            wide_atomic_lock_free: true,
            mixed_width_memory_model_supported: false,
        },
        results,
    };
    atomic_write_json(&path, &output)?;
    println!("wrote {}", path.display());
    Ok(())
}

fn main() {
    if let Err(err) = run(Args::parse()) {
        eprintln!("bench_atomic_updates: {err}");
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn powers_of_two_stop_at_the_effective_limit() {
        assert_eq!(updater_counts(1).unwrap(), vec![1]);
        assert_eq!(updater_counts(12).unwrap(), vec![1, 2, 4, 8]);
        assert_eq!(updater_counts(16).unwrap(), vec![1, 2, 4, 8, 16]);
        assert!(updater_counts(0).is_err());
    }

    #[test]
    fn case_order_rotates_all_cases_through_all_positions() {
        for case in CASES {
            for position in 0..CASES.len() {
                assert_eq!(
                    (0..CASES.len())
                        .filter(|offset| case_order(*offset)[position] == case)
                        .count(),
                    1
                );
            }
        }
    }

    #[test]
    fn block_sizes_are_sorted_and_deduplicated() {
        assert_eq!(
            normalize_block_sizes(&[511, 31, 511, 127]).unwrap(),
            vec![31, 127, 511]
        );
        assert!(normalize_block_sizes(&[]).is_err());
        assert!(normalize_block_sizes(&[0]).is_err());
    }

    #[test]
    fn state_encodes_pointer_generation_and_full_index_space() {
        let block = BlockConfig::new(511, 4096).unwrap();
        let low = block.pack_low(0xfeed_beef, 0xcafe_babe);
        assert_eq!(block.generation(low), 0xfeed_beef);
        assert_eq!(block.index(low), 0xcafe_babe);
        assert_eq!(block.next_low(low), block.pack_low(0xfeed_bef0, 0));
        let full = block.pack_full(0x1234_5000, low);
        assert_eq!((full >> 64) as u64, 0x1234_5000);
        assert_eq!(full as u64, low);
    }

    #[test]
    fn every_layout_and_method_preserves_block_traversal() {
        let block = BlockConfig::new(7, 64).unwrap();
        for case in CASES {
            let counter = Counter::new(case.layout, block);
            let mut stats = UpdateStats::default();
            let mut cache = CachedHead::default();
            match &counter {
                Counter::Word(counter) => {
                    counter.initialize(Ordering::Relaxed, block, &mut cache, &mut stats);
                    match case.method {
                        UpdateMethod::Cas | UpdateMethod::CasBackoff => run_cas_chunk(
                            counter,
                            case.method,
                            OrderingProfile::Relaxed,
                            block,
                            &mut cache,
                            &mut stats,
                        ),
                        UpdateMethod::Faa => run_faa_chunk(
                            counter,
                            OrderingProfile::Relaxed,
                            block,
                            &mut cache,
                            &mut stats,
                        ),
                        UpdateMethod::SegQueue => {
                            run_segqueue_chunk(counter, OrderingProfile::Relaxed, block, &mut stats)
                        }
                    }
                }
                Counter::Mixed(counter) => {
                    counter.initialize(Ordering::Relaxed, block, &mut cache, &mut stats);
                    match case.method {
                        UpdateMethod::Cas | UpdateMethod::CasBackoff => run_cas_chunk(
                            counter,
                            case.method,
                            OrderingProfile::Relaxed,
                            block,
                            &mut cache,
                            &mut stats,
                        ),
                        UpdateMethod::Faa => run_faa_chunk(
                            counter,
                            OrderingProfile::Relaxed,
                            block,
                            &mut cache,
                            &mut stats,
                        ),
                        UpdateMethod::SegQueue => {
                            run_segqueue_chunk(counter, OrderingProfile::Relaxed, block, &mut stats)
                        }
                    }
                }
            }
            assert_eq!(stats.operations, UPDATE_CHUNK as u64);
            assert_eq!(
                stats.boundary_stores,
                UPDATE_CHUNK as u64 / u64::from(block.block_size)
            );
            let expected = if case.method == UpdateMethod::SegQueue {
                block
                    .expected_segqueue_position(UPDATE_CHUNK as u64)
                    .unwrap()
            } else {
                block.expected_low(UPDATE_CHUNK as u64).unwrap()
            };
            assert_eq!(counter.final_values().0, expected);
            match case.layout {
                CounterLayout::U64 => {
                    assert_eq!(stats.wide_loads, 0);
                    assert_eq!(stats.wide_stores, 0);
                }
                CounterLayout::MixedU128U64 => {
                    assert_eq!(stats.wide_loads, 1);
                    assert_eq!(stats.wide_stores, stats.boundary_stores);
                    assert_eq!(
                        counter.final_values().1.unwrap() >> 64,
                        u128::from(block.expected_pointer(UPDATE_CHUNK as u64).unwrap())
                    );
                }
            }
        }
    }

    #[test]
    fn segqueue_reservation_skips_the_sentinel_between_laps() {
        let block = BlockConfig::new(7, 64).unwrap();
        let counter = WordCounter::new(0);
        let mut stats = UpdateStats::default();

        run_segqueue_chunk(&counter, OrderingProfile::Relaxed, block, &mut stats);

        let final_position = counter.final_values().0;
        assert_eq!(
            final_position,
            block
                .expected_segqueue_position(UPDATE_CHUNK as u64)
                .unwrap()
        );
        assert_eq!(block.segqueue_offset(final_position), 1);
        assert_eq!(stats.operations, UPDATE_CHUNK as u64);
        assert_eq!(stats.boundary_stores, UPDATE_CHUNK as u64 / 7);
        assert_eq!(stats.cas_failures, 0);
        assert_eq!(stats.boundary_waits, 0);
    }

    #[test]
    fn cli_defaults_to_ubq_block_sweep() {
        let args = Args::try_parse_from(["bench_atomic_updates", "--machine-label", "local"])
            .expect("arguments");
        assert_eq!(args.ordering, OrderingProfile::Ubq);
        assert_eq!(args.block_sizes, vec![31, 127, 511, 2047, 4095]);
        assert_eq!(args.alignment, DEFAULT_ALIGNMENT);
        assert_eq!(args.duration_ms, 500);
        assert_eq!(args.warmup_ms, 100);
        assert_eq!(args.repeats, 5);
    }
}
