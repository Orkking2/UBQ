mod atomic_views;

use atomic_views::AtomicInt;
use clap::{Parser, ValueEnum};
use crossbeam_utils::CachePadded;
use portable_atomic::{AtomicBool, AtomicU128, Ordering};
use serde::Serialize;
use std::collections::BTreeSet;
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const SCHEMA_VERSION: u32 = 1;
const DEFAULT_RUNS_DIR: &str = "bench_results/runs";
const DEFAULT_BATCH_SIZES: &str = "1,2,4,8,16,32,64,128,256,512,1024";
const UPDATE_CHUNK: usize = 256;
const TOKEN_BITS: u32 = 16;
const INDEX_BITS: u32 = u64::BITS - TOKEN_BITS;
const INDEX_MASK: u64 = (1_u64 << INDEX_BITS) - 1;
const TOKEN_SHIFT: u32 = INDEX_BITS;
const INITIAL_POINTER: u64 = 0x0000_1234_5678_9000;

#[derive(Parser, Debug)]
#[command(name = "bench_head_reload")]
#[command(about = "Measure the upper-bound benefit of a 16-bit low-word head token")]
struct Args {
    /// Stable label used to group runs from this machine.
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
    max_threads: Option<usize>,

    /// Comma-separated increments used by each successful reservation CAS.
    #[arg(long, value_delimiter = ',', default_value = DEFAULT_BATCH_SIZES)]
    batch_sizes: Vec<u64>,

    /// Measurement duration for each strategy/thread/batch sample.
    #[arg(long, default_value_t = 300)]
    duration_ms: u64,

    /// Untimed warm-up before every measured sample.
    #[arg(long, default_value_t = 50)]
    warmup_ms: u64,

    /// Complete samples for every strategy/thread/batch cell.
    #[arg(long, default_value_t = 5)]
    repeats: usize,

    /// Print the resolved grid without running or writing results.
    #[arg(long)]
    dry_run: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
enum ReloadStrategy {
    /// Reload the U128 head after every failed low-word CAS.
    AlwaysWide,
    /// Reuse a failed CAS's U64 result while its 16-bit token matches.
    TokenGated,
}

impl ReloadStrategy {
    fn name(self) -> &'static str {
        match self {
            Self::AlwaysWide => "always-wide",
            Self::TokenGated => "token-gated",
        }
    }
}

const STRATEGIES: [ReloadStrategy; 2] = [ReloadStrategy::AlwaysWide, ReloadStrategy::TokenGated];

#[derive(Clone, Copy, Default)]
struct CachedHead {
    token: u16,
    pointer: u64,
}

#[derive(Default)]
struct WorkerStats {
    reservations: u64,
    items: u128,
    cas_failures: u64,
    wide_loads: u64,
    token_hits: u64,
    token_misses: u64,
    checksum: u64,
}

struct WorkerOutcome {
    stats: WorkerStats,
    elapsed_ns: u64,
    pinned: bool,
}

#[derive(Debug, Serialize)]
struct RunMeta {
    timestamp_unix_ms: u128,
    machine_label: String,
    target_arch: &'static str,
    target_os: &'static str,
    available_parallelism: usize,
    effective_thread_limit: usize,
    enumerated_core_ids: Vec<usize>,
    thread_counts: Vec<usize>,
    batch_sizes: Vec<u64>,
    token_bits: u32,
    index_bits: u32,
    duration_ms: u64,
    warmup_ms: u64,
    repeats: usize,
    update_chunk: usize,
    wide_atomic_lock_free: bool,
    mixed_width_memory_model_supported: bool,
    token_changes_during_sample: bool,
}

#[derive(Debug, Serialize)]
struct Sample {
    repeat_index: usize,
    thread_count: usize,
    batch_size: u64,
    strategy: ReloadStrategy,
    reservations: u64,
    items: u128,
    elapsed_ns: u64,
    reservations_per_sec: f64,
    items_per_sec: f64,
    cas_failures: u64,
    cas_failures_per_reservation: f64,
    wide_loads: u64,
    wide_loads_per_reservation: f64,
    token_hits: u64,
    token_misses: u64,
    assigned_core_ids: Vec<usize>,
    pinned_threads: usize,
    checksum: u64,
}

#[derive(Debug, Serialize)]
struct OutputFile {
    benchmark: &'static str,
    schema_version: u32,
    meta: RunMeta,
    results: Vec<Sample>,
}

#[inline]
fn token(low: u64) -> u16 {
    (low >> TOKEN_SHIFT) as u16
}

#[inline]
fn load_full(atomic: &AtomicInt, cache: &mut CachedHead, stats: &mut WorkerStats) -> u64 {
    let full = atomic.as_u128().load(Ordering::Acquire);
    let low = full as u64;
    cache.token = token(low);
    cache.pointer = (full >> 64) as u64;
    stats.wide_loads = stats.wide_loads.wrapping_add(1);
    // Keep both halves of the wide load live, as a real head decoder would.
    stats.checksum = stats
        .checksum
        .rotate_left(7)
        .wrapping_add(cache.pointer ^ low);
    low
}

#[inline]
fn run_chunk(
    atomic: &AtomicInt,
    strategy: ReloadStrategy,
    batch_size: u64,
    stats: &mut WorkerStats,
) {
    for _ in 0..UPDATE_CHUNK {
        // pop_batch starts by acquiring the complete head. The optimization is
        // only about avoiding further wide loads after failed narrow CASes.
        let mut cache = CachedHead::default();
        let mut current = load_full(atomic, &mut cache, stats);

        loop {
            let index = current & INDEX_MASK;
            debug_assert!(index <= INDEX_MASK - batch_size);
            let next = current + batch_size;
            match atomic.as_u64().compare_exchange_weak(
                current,
                next,
                Ordering::Release,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    stats.reservations = stats.reservations.wrapping_add(1);
                    stats.items = stats.items.wrapping_add(u128::from(batch_size));
                    stats.checksum ^= cache.pointer.rotate_left((index & 63) as u32);
                    break;
                }
                Err(actual) => {
                    stats.cas_failures = stats.cas_failures.wrapping_add(1);
                    match strategy {
                        ReloadStrategy::AlwaysWide => {
                            current = load_full(atomic, &mut cache, stats);
                        }
                        ReloadStrategy::TokenGated if token(actual) == cache.token => {
                            stats.token_hits = stats.token_hits.wrapping_add(1);
                            current = actual;
                        }
                        ReloadStrategy::TokenGated => {
                            stats.token_misses = stats.token_misses.wrapping_add(1);
                            current = load_full(atomic, &mut cache, stats);
                        }
                    }
                }
            }
        }
    }
}

fn worker(
    atomic: Arc<CachePadded<AtomicInt>>,
    stop: Arc<AtomicBool>,
    barrier: Arc<Barrier>,
    strategy: ReloadStrategy,
    batch_size: u64,
    core_id: Option<core_affinity::CoreId>,
) -> WorkerOutcome {
    let pinned = core_id.is_some_and(core_affinity::set_for_current);
    let mut stats = WorkerStats::default();
    barrier.wait();
    let started = Instant::now();
    while !stop.load(Ordering::Relaxed) {
        run_chunk(&atomic, strategy, batch_size, &mut stats);
    }
    WorkerOutcome {
        stats,
        elapsed_ns: started.elapsed().as_nanos().min(u64::MAX as u128) as u64,
        pinned,
    }
}

fn run_trial(
    thread_count: usize,
    strategy: ReloadStrategy,
    batch_size: u64,
    duration: Duration,
    core_ids: &[core_affinity::CoreId],
) -> Result<Sample, String> {
    let atomic = Arc::new(CachePadded::new(AtomicInt::new(
        u128::from(INITIAL_POINTER) << 64,
    )));
    let stop = Arc::new(AtomicBool::new(false));
    let barrier = Arc::new(Barrier::new(thread_count + 1));
    let assigned_core_ids = core_ids
        .iter()
        .take(thread_count)
        .map(|core| core.id)
        .collect::<Vec<_>>();
    let mut handles = Vec::with_capacity(thread_count);

    for worker_index in 0..thread_count {
        let atomic = Arc::clone(&atomic);
        let stop = Arc::clone(&stop);
        let barrier = Arc::clone(&barrier);
        let core_id = core_ids.get(worker_index).copied();
        handles.push(thread::spawn(move || {
            worker(atomic, stop, barrier, strategy, batch_size, core_id)
        }));
    }

    barrier.wait();
    thread::sleep(duration);
    stop.store(true, Ordering::Relaxed);

    let outcomes = handles
        .into_iter()
        .map(|handle| {
            handle
                .join()
                .map_err(|_| "head reload benchmark worker panicked".to_string())
        })
        .collect::<Result<Vec<_>, _>>()?;

    let reservations = outcomes
        .iter()
        .map(|outcome| outcome.stats.reservations)
        .sum::<u64>();
    if reservations == 0 {
        return Err("sample completed without a successful reservation".to_string());
    }
    let items = outcomes
        .iter()
        .map(|outcome| outcome.stats.items)
        .sum::<u128>();
    let cas_failures = outcomes
        .iter()
        .map(|outcome| outcome.stats.cas_failures)
        .sum::<u64>();
    let wide_loads = outcomes
        .iter()
        .map(|outcome| outcome.stats.wide_loads)
        .sum::<u64>();
    let token_hits = outcomes
        .iter()
        .map(|outcome| outcome.stats.token_hits)
        .sum::<u64>();
    let token_misses = outcomes
        .iter()
        .map(|outcome| outcome.stats.token_misses)
        .sum::<u64>();
    let checksum = outcomes
        .iter()
        .fold(0_u64, |sum, outcome| sum ^ outcome.stats.checksum);
    let elapsed_ns = outcomes
        .iter()
        .map(|outcome| outcome.elapsed_ns)
        .max()
        .unwrap_or_default();
    let final_low = atomic.as_u64().load(Ordering::Relaxed);

    if token(final_low) != 0 {
        return Err("16-bit token changed during a steady-frontier sample".to_string());
    }
    if strategy == ReloadStrategy::AlwaysWide
        && wide_loads != reservations.wrapping_add(cas_failures)
    {
        return Err(format!(
            "always-wide load accounting mismatch: {wide_loads} != {reservations} + {cas_failures}"
        ));
    }
    if strategy == ReloadStrategy::TokenGated {
        if wide_loads != reservations {
            return Err(format!(
                "token-gated load accounting mismatch: {wide_loads} != {reservations}"
            ));
        }
        if token_hits != cas_failures || token_misses != 0 {
            return Err("steady token did not classify every failed CAS as a hit".to_string());
        }
    }

    Ok(Sample {
        repeat_index: 0,
        thread_count,
        batch_size,
        strategy,
        reservations,
        items,
        elapsed_ns,
        reservations_per_sec: reservations as f64 * 1_000_000_000.0 / elapsed_ns as f64,
        items_per_sec: items as f64 * 1_000_000_000.0 / elapsed_ns as f64,
        cas_failures,
        cas_failures_per_reservation: cas_failures as f64 / reservations as f64,
        wide_loads,
        wide_loads_per_reservation: wide_loads as f64 / reservations as f64,
        token_hits,
        token_misses,
        assigned_core_ids,
        pinned_threads: outcomes.iter().filter(|outcome| outcome.pinned).count(),
        checksum,
    })
}

fn normalize_batch_sizes(values: &[u64]) -> Result<Vec<u64>, String> {
    let values = values.iter().copied().collect::<BTreeSet<_>>();
    if values.is_empty() {
        return Err("--batch-sizes must contain at least one size".to_string());
    }
    if values.contains(&0) {
        return Err("batch sizes must be greater than zero".to_string());
    }
    if values.last().copied().unwrap_or_default() > INDEX_MASK {
        return Err(format!(
            "batch sizes must fit in the {INDEX_BITS}-bit index"
        ));
    }
    Ok(values.into_iter().collect())
}

fn thread_counts(limit: usize) -> Result<Vec<usize>, String> {
    if limit == 0 {
        return Err("effective thread limit must be greater than zero".to_string());
    }
    let mut values = Vec::new();
    let mut value = 1_usize;
    while value <= limit {
        values.push(value);
        let Some(next) = value.checked_mul(2) else {
            break;
        };
        value = next;
    }
    Ok(values)
}

fn timestamp_unix_ms() -> Result<u128, String> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .map_err(|err| format!("system clock is before the Unix epoch: {err}"))
}

fn sanitize_component(value: &str) -> String {
    let sanitized = value
        .trim()
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '-' | '_') {
                character
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

fn output_path(args: &Args, timestamp: u128) -> PathBuf {
    args.output.clone().unwrap_or_else(|| {
        args.runs_dir
            .join(sanitize_component(&args.machine_label))
            .join("head_reload")
            .join(format!(
                "head-reload-{timestamp}-{}.json",
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
        .unwrap_or_else(|| "head-reload.json".to_string());
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
    if args.duration_ms == 0 || args.repeats == 0 {
        return Err("--duration-ms and --repeats must be greater than zero".to_string());
    }
    if args.max_threads == Some(0) {
        return Err("--max-threads must be greater than zero".to_string());
    }
    if !AtomicU128::is_lock_free() {
        return Err(
            "AtomicU128 is not lock-free on this target; mixed-width measurements would include portable-atomic's fallback lock"
                .to_string(),
        );
    }

    let batch_sizes = normalize_batch_sizes(&args.batch_sizes)?;
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
        .max_threads
        .unwrap_or(detected_limit)
        .min(detected_limit);
    let counts = thread_counts(effective_thread_limit)?;

    println!("machine: {}", args.machine_label.trim());
    println!(
        "threads: {}",
        counts
            .iter()
            .map(usize::to_string)
            .collect::<Vec<_>>()
            .join(",")
    );
    println!(
        "batch sizes: {}",
        batch_sizes
            .iter()
            .map(u64::to_string)
            .collect::<Vec<_>>()
            .join(",")
    );
    println!("head low word: {TOKEN_BITS}-bit token + {INDEX_BITS}-bit index");
    println!("AtomicU128 lock-free: yes");
    println!(
        "sample timing: {}ms warm-up + {}ms measured, {} repeats",
        args.warmup_ms, args.duration_ms, args.repeats
    );
    eprintln!(
        "warning: this hardware experiment deliberately uses overlapping U128/U64 atomics, which Rust's memory model does not support"
    );
    if args.dry_run {
        return Ok(());
    }

    let warmup = Duration::from_millis(args.warmup_ms);
    let measured = Duration::from_millis(args.duration_ms);
    let mut results = Vec::with_capacity(counts.len() * batch_sizes.len() * args.repeats * 2);

    for &thread_count in &counts {
        for repeat_index in 1..=args.repeats {
            for (batch_position, &batch_size) in batch_sizes.iter().enumerate() {
                let mut strategies = STRATEGIES;
                if (repeat_index + batch_position) % 2 == 0 {
                    strategies.reverse();
                }
                for strategy in strategies {
                    if !warmup.is_zero() {
                        run_trial(thread_count, strategy, batch_size, warmup, &core_ids)?;
                    }
                    let mut sample =
                        run_trial(thread_count, strategy, batch_size, measured, &core_ids)?;
                    sample.repeat_index = repeat_index;
                    println!(
                        "{:>2} thread(s) | repeat {:>2} | batch {:>4} | {:>11} | {:>8.3} Mres/s | failures {:>7.3}/res | wide {:>7.3}/res",
                        thread_count,
                        repeat_index,
                        batch_size,
                        strategy.name(),
                        sample.reservations_per_sec / 1_000_000.0,
                        sample.cas_failures_per_reservation,
                        sample.wide_loads_per_reservation,
                    );
                    results.push(sample);
                }
            }
        }
    }

    let timestamp = timestamp_unix_ms()?;
    let path = output_path(&args, timestamp);
    let output = OutputFile {
        benchmark: "head_reload",
        schema_version: SCHEMA_VERSION,
        meta: RunMeta {
            timestamp_unix_ms: timestamp,
            machine_label: args.machine_label.trim().to_string(),
            target_arch: std::env::consts::ARCH,
            target_os: std::env::consts::OS,
            available_parallelism,
            effective_thread_limit,
            enumerated_core_ids: core_ids.iter().map(|core| core.id).collect(),
            thread_counts: counts,
            batch_sizes,
            token_bits: TOKEN_BITS,
            index_bits: INDEX_BITS,
            duration_ms: args.duration_ms,
            warmup_ms: args.warmup_ms,
            repeats: args.repeats,
            update_chunk: UPDATE_CHUNK,
            wide_atomic_lock_free: true,
            mixed_width_memory_model_supported: false,
            token_changes_during_sample: false,
        },
        results,
    };
    atomic_write_json(&path, &output)?;
    println!("wrote {}", path.display());
    Ok(())
}

fn main() {
    if let Err(err) = run(Args::parse()) {
        eprintln!("bench_head_reload: {err}");
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn low_word_reserves_sixteen_token_bits() {
        let low = (u64::from(0xbeef_u16) << TOKEN_SHIFT) | 0x1234_5678_9abc;
        assert_eq!(token(low), 0xbeef);
        assert_eq!(low & INDEX_MASK, 0x1234_5678_9abc);
    }

    #[test]
    fn thread_grid_uses_powers_of_two() {
        assert_eq!(thread_counts(1).unwrap(), vec![1]);
        assert_eq!(thread_counts(12).unwrap(), vec![1, 2, 4, 8]);
        assert!(thread_counts(0).is_err());
    }

    #[test]
    fn batch_grid_is_sorted_and_unique() {
        assert_eq!(
            normalize_batch_sizes(&[16, 1, 16, 4]).unwrap(),
            vec![1, 4, 16]
        );
        assert!(normalize_batch_sizes(&[0]).is_err());
    }
}
