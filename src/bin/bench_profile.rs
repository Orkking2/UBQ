use clap::Parser;
use std::time::Duration;
use ubq::bench_harness::{
    HandoffProfileConfig, LubqBackoff, QueueKind, parse_scenarios_with_parallelism,
    run_handoff_profile,
};

#[cfg(feature = "profile_allocator_probe")]
mod counting_allocator {
    use std::alloc::{GlobalAlloc, Layout, System};
    use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
    use ubq::bench_harness::ProfileProbeMetrics;

    struct CountingAllocator;

    static PROBE_ACTIVE: AtomicBool = AtomicBool::new(false);
    static PROBE_ALLOC_CALLS: AtomicU64 = AtomicU64::new(0);
    static PROBE_ALLOC_ZEROED_CALLS: AtomicU64 = AtomicU64::new(0);
    static PROBE_DEALLOC_CALLS: AtomicU64 = AtomicU64::new(0);
    static PROBE_REALLOC_CALLS: AtomicU64 = AtomicU64::new(0);
    static PROBE_ALLOCATED_BYTES: AtomicU64 = AtomicU64::new(0);
    static PROBE_DEALLOCATED_BYTES: AtomicU64 = AtomicU64::new(0);
    static PROBE_LIVE_DELTA: AtomicI64 = AtomicI64::new(0);
    static PROBE_PEAK_EXTRA: AtomicU64 = AtomicU64::new(0);

    #[global_allocator]
    static GLOBAL_ALLOCATOR: CountingAllocator = CountingAllocator;

    fn size_as_i64(size: usize) -> i64 {
        i64::try_from(size).unwrap_or(i64::MAX)
    }

    fn record_alloc(size: usize, zeroed: bool) {
        if !PROBE_ACTIVE.load(Ordering::Relaxed) {
            return;
        }
        PROBE_ALLOC_CALLS.fetch_add(1, Ordering::Relaxed);
        if zeroed {
            PROBE_ALLOC_ZEROED_CALLS.fetch_add(1, Ordering::Relaxed);
        }
        PROBE_ALLOCATED_BYTES.fetch_add(size as u64, Ordering::Relaxed);
        let live = PROBE_LIVE_DELTA
            .fetch_add(size_as_i64(size), Ordering::Relaxed)
            .saturating_add(size_as_i64(size));
        if live > 0 {
            PROBE_PEAK_EXTRA.fetch_max(live as u64, Ordering::Relaxed);
        }
    }

    fn record_dealloc(size: usize) {
        if !PROBE_ACTIVE.load(Ordering::Relaxed) {
            return;
        }
        PROBE_DEALLOC_CALLS.fetch_add(1, Ordering::Relaxed);
        PROBE_DEALLOCATED_BYTES.fetch_add(size as u64, Ordering::Relaxed);
        PROBE_LIVE_DELTA.fetch_sub(size_as_i64(size), Ordering::Relaxed);
    }

    unsafe impl GlobalAlloc for CountingAllocator {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            // SAFETY: forwarding the caller-provided layout to the system allocator.
            let pointer = unsafe { System.alloc(layout) };
            if !pointer.is_null() {
                record_alloc(layout.size(), false);
            }
            pointer
        }

        unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
            // SAFETY: forwarding the caller-provided layout to the system allocator.
            let pointer = unsafe { System.alloc_zeroed(layout) };
            if !pointer.is_null() {
                record_alloc(layout.size(), true);
            }
            pointer
        }

        unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
            record_dealloc(layout.size());
            // SAFETY: forwarding the pointer and its original layout to System.
            unsafe { System.dealloc(pointer, layout) };
        }

        unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
            // SAFETY: forwarding the pointer, original layout, and requested size.
            let replacement = unsafe { System.realloc(pointer, layout, new_size) };
            if !replacement.is_null() && PROBE_ACTIVE.load(Ordering::Relaxed) {
                PROBE_REALLOC_CALLS.fetch_add(1, Ordering::Relaxed);
                PROBE_ALLOCATED_BYTES.fetch_add(new_size as u64, Ordering::Relaxed);
                PROBE_DEALLOCATED_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
                if new_size >= layout.size() {
                    let growth = size_as_i64(new_size - layout.size());
                    let live = PROBE_LIVE_DELTA
                        .fetch_add(growth, Ordering::Relaxed)
                        .saturating_add(growth);
                    if live > 0 {
                        PROBE_PEAK_EXTRA.fetch_max(live as u64, Ordering::Relaxed);
                    }
                } else {
                    PROBE_LIVE_DELTA
                        .fetch_sub(size_as_i64(layout.size() - new_size), Ordering::Relaxed);
                }
            }
            replacement
        }
    }

    pub(super) fn start_allocator_probe() {
        PROBE_ALLOC_CALLS.store(0, Ordering::Relaxed);
        PROBE_ALLOC_ZEROED_CALLS.store(0, Ordering::Relaxed);
        PROBE_DEALLOC_CALLS.store(0, Ordering::Relaxed);
        PROBE_REALLOC_CALLS.store(0, Ordering::Relaxed);
        PROBE_ALLOCATED_BYTES.store(0, Ordering::Relaxed);
        PROBE_DEALLOCATED_BYTES.store(0, Ordering::Relaxed);
        PROBE_LIVE_DELTA.store(0, Ordering::Relaxed);
        PROBE_PEAK_EXTRA.store(0, Ordering::Relaxed);
        PROBE_ACTIVE.store(true, Ordering::Release);
    }

    pub(super) fn finish_allocator_probe() -> ProfileProbeMetrics {
        PROBE_ACTIVE.store(false, Ordering::Release);
        ProfileProbeMetrics {
            alloc_calls: PROBE_ALLOC_CALLS.load(Ordering::Relaxed),
            alloc_zeroed_calls: PROBE_ALLOC_ZEROED_CALLS.load(Ordering::Relaxed),
            dealloc_calls: PROBE_DEALLOC_CALLS.load(Ordering::Relaxed),
            realloc_calls: PROBE_REALLOC_CALLS.load(Ordering::Relaxed),
            allocated_bytes: PROBE_ALLOCATED_BYTES.load(Ordering::Relaxed),
            deallocated_bytes: PROBE_DEALLOCATED_BYTES.load(Ordering::Relaxed),
            live_bytes_delta: PROBE_LIVE_DELTA.load(Ordering::Relaxed),
            peak_additional_live_bytes: PROBE_PEAK_EXTRA.load(Ordering::Relaxed),
        }
    }
}

#[derive(Parser, Debug)]
#[command(name = "bench_profile")]
#[command(about = "Run one foreground queue handoff workload for a sampling profiler")]
struct Args {
    /// Queue to profile: ubq, lubq, segqueue, concurrent-queue,
    /// mutex-vecdeque, ms-queue, rbbq, lfqueue, or moodycamel-cq.
    #[arg(long, value_parser = parse_queue)]
    queue: QueueKind,

    /// Exactly one producer/consumer scenario, for example 1p1c or 72p72c.
    #[arg(long, default_value = "1p1c")]
    scenario: String,

    /// Native queue batch size. Omit for scalar operations.
    #[arg(long)]
    batch_size: Option<usize>,

    /// UBQ backoff configuration, for example balanced,1,page,crossbeam.
    #[arg(long)]
    ubq_label: Option<String>,

    /// LUBQ shard reservation backoff policy: crossbeam (default) or yield.
    /// Valid only with --queue lubq.
    #[arg(long, value_parser = parse_lubq_backoff)]
    lubq_backoff: Option<LubqBackoff>,

    /// RBBQ/BBQ block size. Valid only with --queue rbbq.
    #[arg(long)]
    fastfifo_block_size: Option<usize>,

    /// RBBQ/BBQ requested capacity. Valid only with --queue rbbq.
    #[arg(long)]
    fastfifo_capacity: Option<usize>,

    /// LSCQ segment size. Valid only with --queue lfqueue.
    #[arg(long)]
    lfqueue_segment_size: Option<usize>,

    /// Measured handoff duration.
    #[arg(long, default_value_t = 30)]
    duration_secs: u64,

    /// In-process warmup before the measured handoff.
    #[arg(long, default_value_t = 2)]
    warmup_secs: u64,

    /// Run exactly this many measured items per producer, skipping calibration.
    #[arg(long)]
    items_per_producer: Option<u64>,

    /// Optional excluded prewarm round before a fixed-work measurement.
    #[arg(long, requires = "items_per_producer")]
    prewarm_items_per_producer: Option<u64>,

    /// Enable the intrusive allocator counter probe for the measured round.
    #[arg(long)]
    allocator_probe: bool,

    /// Skip this many CPUs in the process affinity set before placing workers.
    #[arg(long, default_value_t = 0)]
    core_offset: usize,

    /// Continue if worker pinning is unavailable.
    #[arg(long)]
    allow_unpinned: bool,

    /// Print only the result JSON on stdout.
    #[arg(long)]
    json: bool,
}

fn parse_lubq_backoff(raw: &str) -> Result<LubqBackoff, String> {
    LubqBackoff::parse(raw).ok_or_else(|| format!("unknown lubq backoff policy: {raw}"))
}

fn parse_queue(raw: &str) -> Result<QueueKind, String> {
    let queue = QueueKind::parse(raw).ok_or_else(|| format!("unknown queue: {raw}"))?;
    match queue {
        QueueKind::Ubq
        | QueueKind::Lubq
        | QueueKind::SegQueue
        | QueueKind::ConcurrentQueue
        | QueueKind::MutexVecDeque
        | QueueKind::MsQueue
        | QueueKind::FastFifo
        | QueueKind::LfQueue
        | QueueKind::MoodycamelConcurrentQueue => Ok(queue),
        _ => Err(
            "bench_profile supports ubq, lubq, segqueue, concurrent-queue, mutex-vecdeque, \
             ms-queue, rbbq, lfqueue, and moodycamel-cq"
                .to_string(),
        ),
    }
}

fn main() {
    if let Err(err) = run() {
        eprintln!("bench_profile: {err}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let args = Args::parse();
    if args.duration_secs == 0 {
        return Err("--duration-secs must be greater than zero".to_string());
    }
    if args.batch_size.is_some_and(|size| size < 2) {
        return Err("--batch-size must be at least 2; omit it for scalar operations".to_string());
    }
    if args.items_per_producer == Some(0) {
        return Err("--items-per-producer must be greater than zero".to_string());
    }
    if args.prewarm_items_per_producer == Some(0) {
        return Err("--prewarm-items-per-producer must be greater than zero".to_string());
    }
    if args.batch_size.is_some()
        && matches!(
            args.queue,
            QueueKind::ConcurrentQueue
                | QueueKind::MsQueue
                | QueueKind::FastFifo
                | QueueKind::LfQueue
        )
    {
        return Err(format!(
            "{} has no native batch operation",
            args.queue.name()
        ));
    }
    if args.queue != QueueKind::Ubq && args.ubq_label.is_some() {
        return Err("--ubq-label is valid only with --queue ubq".to_string());
    }
    if args.queue != QueueKind::Lubq && args.lubq_backoff.is_some() {
        return Err("--lubq-backoff is valid only with --queue lubq".to_string());
    }
    if args.queue != QueueKind::FastFifo
        && (args.fastfifo_block_size.is_some() || args.fastfifo_capacity.is_some())
    {
        return Err(
            "--fastfifo-block-size/--fastfifo-capacity are valid only with --queue rbbq"
                .to_string(),
        );
    }
    if args.queue == QueueKind::FastFifo
        && (args.fastfifo_block_size.is_none() || args.fastfifo_capacity.is_none())
    {
        return Err(
            "--queue rbbq requires --fastfifo-block-size and --fastfifo-capacity".to_string(),
        );
    }
    if args.fastfifo_block_size.is_some_and(|value| value == 0)
        || args.fastfifo_capacity.is_some_and(|value| value == 0)
    {
        return Err("RBBQ block size and capacity must be greater than zero".to_string());
    }
    if args.queue != QueueKind::LfQueue && args.lfqueue_segment_size.is_some() {
        return Err("--lfqueue-segment-size is valid only with --queue lfqueue".to_string());
    }
    if args.queue == QueueKind::LfQueue && args.lfqueue_segment_size.is_none() {
        return Err("--queue lfqueue requires --lfqueue-segment-size".to_string());
    }
    if args.lfqueue_segment_size.is_some_and(|value| value == 0) {
        return Err("LSCQ segment size must be greater than zero".to_string());
    }

    let mut scenarios = parse_scenarios_with_parallelism(Some(&args.scenario), usize::MAX)?;
    if scenarios.len() != 1 {
        return Err("--scenario must select exactly one producer/consumer pair".to_string());
    }
    let scenario = scenarios.pop().expect("one scenario was validated");
    let measurement_probe = if args.allocator_probe {
        #[cfg(feature = "profile_allocator_probe")]
        {
            Some(ubq::bench_harness::ProfileMeasurementProbe {
                start: counting_allocator::start_allocator_probe,
                finish: counting_allocator::finish_allocator_probe,
            })
        }
        #[cfg(not(feature = "profile_allocator_probe"))]
        {
            return Err("--allocator-probe requires a binary built with --features \
                 profile_allocator_probe"
                .to_string());
        }
    } else {
        None
    };
    let config = HandoffProfileConfig {
        queue: args.queue,
        ubq_label: args.ubq_label,
        lubq_backoff: args.lubq_backoff,
        fastfifo_block_size: args.fastfifo_block_size,
        fastfifo_capacity: args.fastfifo_capacity,
        lfqueue_segment_size: args.lfqueue_segment_size,
        scenario,
        batch_size: args.batch_size,
        warmup: Duration::from_secs(args.warmup_secs),
        duration: Duration::from_secs(args.duration_secs),
        items_per_producer: args.items_per_producer,
        prewarm_items_per_producer: args.prewarm_items_per_producer,
        core_offset: args.core_offset,
        allow_unpinned: args.allow_unpinned,
        measurement_probe,
    };

    if !args.json {
        eprintln!(
            "profiling queue={} scenario={} batch={} warmup={}s duration={}s fixed_items={}",
            config.queue.name(),
            config.scenario.name,
            config
                .batch_size
                .map(|size| size.to_string())
                .unwrap_or_else(|| "scalar".to_string()),
            args.warmup_secs,
            args.duration_secs,
            args.items_per_producer
                .map(|items| items.to_string())
                .unwrap_or_else(|| "calibrated".to_string()),
        );
    }
    let result = run_handoff_profile(&config)?;
    if args.json {
        println!(
            "{}",
            serde_json::to_string(&result)
                .map_err(|err| format!("failed to serialize profile result: {err}"))?
        );
    } else {
        println!(
            "completed: {:.3} M items/s ({} items in {:.3}s, affinity_ok={})",
            result.ops_per_sec / 1_000_000.0,
            result.items,
            result.elapsed_ns as f64 / 1_000_000_000.0,
            result.affinity_ok,
        );
        println!(
            "{}",
            serde_json::to_string_pretty(&result)
                .map_err(|err| format!("failed to serialize profile result: {err}"))?
        );
    }
    Ok(())
}
