use clap::Parser;
use core_affinity::CoreId;
use crossbeam_utils::{Backoff, CachePadded};
#[cfg(feature = "bench_fastfifo")]
use rbbq::FastFifo;
use std::{
    cmp,
    collections::BTreeMap,
    fs::{self, File},
    io::{self, BufWriter, Write},
    path::{Path, PathBuf},
    sync::{
        Arc, Barrier,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    thread::{self, JoinHandle},
    time::{Instant, SystemTime, UNIX_EPOCH},
};
use ubq::{ConfiguredUBQ, align, backoff};

const DEFAULT_QUEUES: &str = "io-uring,bbq,ubq";
const DEFAULT_SQ_SIZES: &str = "32,1024";
const DEFAULT_BATCH_MODES: &str = "fixed1,random1to32";
const DEFAULT_UBQ_LABEL: &str = "balanced,1,2047,crossbeam";
const DEFAULT_REQUESTS: u64 = 1_000_000;
const DEFAULT_REPEATS: usize = 10;
const DEFAULT_BBQ_BLOCK_SIZE: usize = 64;
const DEFAULT_CQ_MULTIPLIER: usize = 2;

const UBQ_POOL_VALUES: [usize; 8] = [0, 1, 2, 4, 8, 16, 32, 64];
const UBQ_BLOCK_VALUES: [u16; 8] = [31, 63, 127, 255, 511, 1023, 2047, 4095];
const UBQ_BACKOFF_VALUES: [&str; 2] = ["crossbeam", "yield"];

#[derive(Parser, Debug)]
#[command(name = "io_uring_queue_bench")]
#[command(about = "Three-thread io_uring SQ/CQ queue replacement benchmark")]
struct Args {
    #[arg(long, default_value = DEFAULT_QUEUES)]
    queues: String,

    #[arg(long, default_value = DEFAULT_SQ_SIZES)]
    sq_sizes: String,

    #[arg(long, default_value = DEFAULT_BATCH_MODES)]
    batch_modes: String,

    #[arg(long, default_value_t = DEFAULT_REQUESTS)]
    requests: u64,

    #[arg(long, default_value_t = DEFAULT_REPEATS)]
    repeats: usize,

    #[arg(long, default_value_t = DEFAULT_CQ_MULTIPLIER)]
    cq_multiplier: usize,

    #[arg(long, default_value_t = DEFAULT_BBQ_BLOCK_SIZE)]
    bbq_block_size: usize,

    #[arg(long, default_value = DEFAULT_UBQ_LABEL)]
    ubq_label: String,

    #[arg(long)]
    out_dir: Option<PathBuf>,

    #[arg(long)]
    pin: bool,

    #[arg(long, default_value_t = 0)]
    core_offset: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
enum QueueKind {
    IoUring,
    Bbq,
    Ubq,
}

impl QueueKind {
    fn parse(token: &str) -> Option<Self> {
        match token.trim().to_ascii_lowercase().as_str() {
            "io-uring" | "io_uring" | "uring" | "linux" => Some(Self::IoUring),
            "bbq" | "rbbq" | "fastfifo" | "fast-fifo" => Some(Self::Bbq),
            "ubq" => Some(Self::Ubq),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
enum BatchMode {
    Fixed1,
    Random1To32,
}

impl BatchMode {
    fn parse(token: &str) -> Option<Self> {
        match token.trim().to_ascii_lowercase().as_str() {
            "fixed1" | "fixed-1" | "1" | "single" => Some(Self::Fixed1),
            "random1to32" | "random-1-to-32" | "random" | "rand" | "1..32" | "1-32" => {
                Some(Self::Random1To32)
            }
            _ => None,
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::Fixed1 => "fixed1",
            Self::Random1To32 => "random1to32",
        }
    }

    fn max(self) -> usize {
        match self {
            Self::Fixed1 => 1,
            Self::Random1To32 => 32,
        }
    }

    fn next(self, rng: &mut XorShift64) -> usize {
        match self {
            Self::Fixed1 => 1,
            Self::Random1To32 => (rng.next_u64() as usize % 32) + 1,
        }
    }
}

#[derive(Clone, Debug)]
struct UbqLabel {
    normalized: String,
    pool: usize,
    block: u16,
    backoff: String,
}

#[derive(Clone)]
struct RunContext {
    sq_sizes: Vec<usize>,
    batch_modes: Vec<BatchMode>,
    requests: u64,
    repeats: usize,
    cq_multiplier: usize,
    bbq_block_size: usize,
    pin_cores: Option<Vec<CoreId>>,
    core_offset: usize,
}

#[derive(Clone, Debug)]
struct Sample {
    queue: String,
    sq_size: usize,
    cq_size: usize,
    batch_mode: BatchMode,
    repeat: usize,
    requests: u64,
    submitted: u64,
    processed: u64,
    completed: u64,
    submit_elapsed_ns: u128,
    total_elapsed_ns: u128,
}

impl Sample {
    fn submit_ns_per_request(&self) -> f64 {
        self.submit_elapsed_ns as f64 / self.requests as f64
    }

    fn total_ns_per_request(&self) -> f64 {
        self.total_elapsed_ns as f64 / self.requests as f64
    }

    fn submit_requests_per_sec(&self) -> f64 {
        self.requests as f64 * 1_000_000_000.0 / self.submit_elapsed_ns as f64
    }

    fn total_requests_per_sec(&self) -> f64 {
        self.requests as f64 * 1_000_000_000.0 / self.total_elapsed_ns as f64
    }
}

#[derive(Clone, Copy)]
struct SummaryMedians {
    submit_ns_per_request: f64,
    total_ns_per_request: f64,
    submit_requests_per_sec: f64,
    total_requests_per_sec: f64,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
struct SummaryKey {
    queue: String,
    sq_size: usize,
    cq_size: usize,
    batch_mode: BatchMode,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
struct BaselineKey {
    sq_size: usize,
    cq_size: usize,
    batch_mode: BatchMode,
}

trait BenchQueue: Send + Sync + 'static {
    fn new(capacity: usize, bbq_block_size: usize) -> Result<Self, String>
    where
        Self: Sized;

    fn try_push(&self, value: u64) -> bool;

    fn try_pop(&self) -> Option<u64>;

    fn push_batch(&self, values: &[u64]) -> usize {
        let mut pushed = 0;
        for &value in values {
            if !self.try_push(value) {
                break;
            }
            pushed += 1;
        }
        pushed
    }

    fn push_sequential_batch(&self, start_value: u64, count: usize) -> usize {
        let mut pushed = 0;
        while pushed < count {
            if !self.try_push(start_value + pushed as u64) {
                break;
            }
            pushed += 1;
        }
        pushed
    }

    fn pop_batch(&self, out: &mut [u64]) -> usize {
        let mut popped = 0;
        for slot in out {
            let Some(value) = self.try_pop() else {
                break;
            };
            *slot = value;
            popped += 1;
        }
        popped
    }
}

struct IoUringQueue {
    capacity: usize,
    mask: usize,
    head: CachePadded<AtomicUsize>,
    tail: CachePadded<AtomicUsize>,
    array: Vec<AtomicUsize>,
    entries: Vec<AtomicU64>,
}

impl BenchQueue for IoUringQueue {
    fn new(capacity: usize, _bbq_block_size: usize) -> Result<Self, String> {
        if capacity == 0 {
            return Err("io_uring queue capacity must be > 0".to_string());
        }
        if !capacity.is_power_of_two() {
            return Err(format!(
                "io_uring queue capacity must be a power of two, got {capacity}"
            ));
        }
        Ok(Self {
            capacity,
            mask: capacity - 1,
            head: CachePadded::new(AtomicUsize::new(0)),
            tail: CachePadded::new(AtomicUsize::new(0)),
            array: (0..capacity).map(AtomicUsize::new).collect(),
            entries: (0..capacity).map(|_| AtomicU64::new(0)).collect(),
        })
    }

    fn try_push(&self, value: u64) -> bool {
        self.push_sequential_batch(value, 1) == 1
    }

    fn try_pop(&self) -> Option<u64> {
        let mut out = [0_u64; 1];
        (self.pop_batch(&mut out) == 1).then_some(out[0])
    }

    fn push_batch(&self, values: &[u64]) -> usize {
        let tail = self.tail.load(Ordering::Relaxed);
        let head = self.head.load(Ordering::Acquire);
        let available = self.capacity.saturating_sub(tail.wrapping_sub(head));
        let count = cmp::min(available, values.len());
        if count == 0 {
            return 0;
        }
        for (offset, &value) in values.iter().take(count).enumerate() {
            let slot = (tail + offset) & self.mask;
            self.entries[slot].store(value, Ordering::Relaxed);
            self.array[slot].store(slot, Ordering::Relaxed);
        }
        self.tail.store(tail + count, Ordering::Release);
        count
    }

    fn push_sequential_batch(&self, start_value: u64, count: usize) -> usize {
        let tail = self.tail.load(Ordering::Relaxed);
        let head = self.head.load(Ordering::Acquire);
        let available = self.capacity.saturating_sub(tail.wrapping_sub(head));
        let count = cmp::min(available, count);
        if count == 0 {
            return 0;
        }
        for offset in 0..count {
            let slot = (tail + offset) & self.mask;
            self.entries[slot].store(start_value + offset as u64, Ordering::Relaxed);
            self.array[slot].store(slot, Ordering::Relaxed);
        }
        self.tail.store(tail + count, Ordering::Release);
        count
    }

    fn pop_batch(&self, out: &mut [u64]) -> usize {
        let head = self.head.load(Ordering::Relaxed);
        let tail = self.tail.load(Ordering::Acquire);
        let available = tail.wrapping_sub(head);
        let count = cmp::min(available, out.len());
        if count == 0 {
            return 0;
        }
        for (offset, slot) in out.iter_mut().take(count).enumerate() {
            let ring_slot = (head + offset) & self.mask;
            let entry_index = self.array[ring_slot].load(Ordering::Relaxed) & self.mask;
            *slot = self.entries[entry_index].load(Ordering::Relaxed);
        }
        self.head.store(head + count, Ordering::Release);
        count
    }
}

struct BoundedUbq<Q> {
    inner: Q,
    capacity: usize,
    len: CachePadded<AtomicUsize>,
}

impl<B, const POOL: usize, const BLOCK: usize, A> BenchQueue
    for BoundedUbq<ConfiguredUBQ<u64, B, POOL, BLOCK, A>>
where
    B: backoff::BackoffPolicy + 'static,
    A: Send + Sync + 'static,
{
    fn new(capacity: usize, _bbq_block_size: usize) -> Result<Self, String> {
        if capacity == 0 {
            return Err("UBQ bounded wrapper capacity must be > 0".to_string());
        }
        Ok(Self {
            inner: ConfiguredUBQ::new(),
            capacity,
            len: CachePadded::new(AtomicUsize::new(0)),
        })
    }

    fn try_push(&self, value: u64) -> bool {
        let mut observed = self.len.load(Ordering::Relaxed);
        loop {
            if observed >= self.capacity {
                return false;
            }
            match self.len.compare_exchange_weak(
                observed,
                observed + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    self.inner.push(value);
                    return true;
                }
                Err(actual) => observed = actual,
            }
        }
    }

    fn try_pop(&self) -> Option<u64> {
        let value = self.inner.pop()?;
        self.len.fetch_sub(1, Ordering::AcqRel);
        Some(value)
    }
}

#[cfg(feature = "bench_fastfifo")]
struct BoundedRbbq {
    inner: FastFifo<u64>,
    capacity: usize,
    len: CachePadded<AtomicUsize>,
}

#[cfg(feature = "bench_fastfifo")]
impl BenchQueue for BoundedRbbq {
    fn new(capacity: usize, bbq_block_size: usize) -> Result<Self, String> {
        if capacity == 0 {
            return Err("BBQ bounded wrapper capacity must be > 0".to_string());
        }
        if bbq_block_size == 0 {
            return Err("BBQ block size must be > 0".to_string());
        }
        let num_blocks = capacity
            .div_ceil(bbq_block_size)
            .checked_add(2)
            .ok_or_else(|| "BBQ block count overflow".to_string())?
            .max(2);
        Ok(Self {
            inner: FastFifo::new(num_blocks, bbq_block_size),
            capacity,
            len: CachePadded::new(AtomicUsize::new(0)),
        })
    }

    fn try_push(&self, value: u64) -> bool {
        let mut observed = self.len.load(Ordering::Relaxed);
        loop {
            if observed >= self.capacity {
                return false;
            }
            match self.len.compare_exchange_weak(
                observed,
                observed + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    if self.inner.push(value).is_ok() {
                        return true;
                    }
                    self.len.fetch_sub(1, Ordering::AcqRel);
                    return false;
                }
                Err(actual) => observed = actual,
            }
        }
    }

    fn try_pop(&self) -> Option<u64> {
        let value = self.inner.pop().ok()?;
        self.len.fetch_sub(1, Ordering::AcqRel);
        Some(value)
    }
}

struct XorShift64(u64);

impl XorShift64 {
    fn new(seed: u64) -> Self {
        Self(seed.max(1))
    }

    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
}

fn parse_queue_kinds(raw: &str) -> Result<Vec<QueueKind>, String> {
    let mut queues = Vec::new();
    for token in raw.split(',') {
        if token.trim().is_empty() {
            continue;
        }
        let queue =
            QueueKind::parse(token).ok_or_else(|| format!("invalid queue kind '{token}'"))?;
        if !queues.contains(&queue) {
            queues.push(queue);
        }
    }
    if queues.is_empty() {
        return Err("at least one queue is required".to_string());
    }
    Ok(queues)
}

fn parse_batch_modes(raw: &str) -> Result<Vec<BatchMode>, String> {
    let mut modes = Vec::new();
    for token in raw.split(',') {
        if token.trim().is_empty() {
            continue;
        }
        let mode =
            BatchMode::parse(token).ok_or_else(|| format!("invalid batch mode '{token}'"))?;
        if !modes.contains(&mode) {
            modes.push(mode);
        }
    }
    if modes.is_empty() {
        return Err("at least one batch mode is required".to_string());
    }
    Ok(modes)
}

fn parse_sizes(raw: &str) -> Result<Vec<usize>, String> {
    let mut sizes = Vec::new();
    for token in raw.split(',') {
        if token.trim().is_empty() {
            continue;
        }
        let size = token
            .trim()
            .parse::<usize>()
            .map_err(|_| format!("invalid SQ size '{token}'"))?;
        if size == 0 {
            return Err("SQ size must be > 0".to_string());
        }
        if !sizes.contains(&size) {
            sizes.push(size);
        }
    }
    if sizes.is_empty() {
        return Err("at least one SQ size is required".to_string());
    }
    Ok(sizes)
}

fn parse_ubq_label(raw: &str) -> Result<UbqLabel, String> {
    let text = raw.trim().to_ascii_lowercase();
    let parts: Vec<&str> = text
        .split(',')
        .map(str::trim)
        .filter(|part| !part.is_empty())
        .collect();
    if parts.len() != 4 {
        return Err(format!("invalid UBQ label '{raw}'"));
    }
    let pool = parts[1]
        .parse::<usize>()
        .map_err(|_| format!("invalid UBQ label '{raw}'"))?;
    let block = parts[2]
        .parse::<u16>()
        .map_err(|_| format!("invalid UBQ label '{raw}'"))?;
    let backoff = parts[3].to_string();
    if parts[0] != "balanced"
        || !UBQ_POOL_VALUES.contains(&pool)
        || !UBQ_BLOCK_VALUES.contains(&block)
        || !UBQ_BACKOFF_VALUES.contains(&backoff.as_str())
    {
        return Err(format!("invalid UBQ label '{raw}'"));
    }
    Ok(UbqLabel {
        normalized: format!("balanced,{pool},{block},{backoff}"),
        pool,
        block,
        backoff,
    })
}

fn validate_args(args: &Args) -> Result<RunContext, String> {
    if args.requests == 0 {
        return Err("requests must be > 0".to_string());
    }
    if args.repeats == 0 {
        return Err("repeats must be > 0".to_string());
    }
    if args.cq_multiplier == 0 {
        return Err("cq-multiplier must be > 0".to_string());
    }
    if args.bbq_block_size == 0 {
        return Err("bbq-block-size must be > 0".to_string());
    }
    let sq_sizes = parse_sizes(&args.sq_sizes)?;
    for sq_size in &sq_sizes {
        sq_size
            .checked_mul(args.cq_multiplier)
            .ok_or_else(|| format!("CQ size overflow for SQ size {sq_size}"))?;
    }
    let batch_modes = parse_batch_modes(&args.batch_modes)?;
    let pin_cores = if args.pin {
        let cores =
            core_affinity::get_core_ids().ok_or_else(|| "failed to enumerate cores".to_string())?;
        let required = args
            .core_offset
            .checked_add(3)
            .ok_or_else(|| "core offset overflow".to_string())?;
        if cores.len() < required {
            return Err(format!(
                "--pin needs at least {required} core(s), but only {} were discovered",
                cores.len()
            ));
        }
        let probe_core = cores[args.core_offset];
        if affinity_supported(probe_core)? {
            Some(cores)
        } else {
            eprintln!(
                "warning: --pin requested, but setting thread affinity failed; continuing unpinned"
            );
            None
        }
    } else {
        None
    };
    Ok(RunContext {
        sq_sizes,
        batch_modes,
        requests: args.requests,
        repeats: args.repeats,
        cq_multiplier: args.cq_multiplier,
        bbq_block_size: args.bbq_block_size,
        pin_cores,
        core_offset: args.core_offset,
    })
}

fn affinity_supported(core: CoreId) -> Result<bool, String> {
    thread::Builder::new()
        .name("io_uring_affinity_probe".to_string())
        .spawn(move || core_affinity::set_for_current(core))
        .map_err(|err| format!("failed to spawn affinity probe: {err}"))?
        .join()
        .map_err(|_| "affinity probe thread panicked".to_string())
}

fn spawn_pinned<F, T>(
    name: &str,
    core: Option<CoreId>,
    f: F,
) -> Result<JoinHandle<Result<T, String>>, String>
where
    F: FnOnce() -> Result<T, String> + Send + 'static,
    T: Send + 'static,
{
    let thread_name = name.to_string();
    thread::Builder::new()
        .name(thread_name.clone())
        .spawn(move || {
            if let Some(core) = core {
                if !core_affinity::set_for_current(core) {
                    eprintln!("warning: failed to pin thread '{thread_name}'; continuing unpinned");
                }
            }
            f()
        })
        .map_err(|err| format!("failed to spawn thread '{name}': {err}"))
}

fn join_thread<T>(handle: JoinHandle<Result<T, String>>, role: &str) -> Result<T, String> {
    match handle.join() {
        Ok(result) => result,
        Err(payload) => {
            if let Some(message) = payload.downcast_ref::<&str>() {
                Err(format!("{role} thread panicked: {message}"))
            } else if let Some(message) = payload.downcast_ref::<String>() {
                Err(format!("{role} thread panicked: {message}"))
            } else {
                Err(format!("{role} thread panicked"))
            }
        }
    }
}

fn run_once<Q: BenchQueue>(
    queue_label: &str,
    sq_size: usize,
    batch_mode: BatchMode,
    repeat: usize,
    ctx: &RunContext,
) -> Result<Sample, String> {
    let cq_size = sq_size
        .checked_mul(ctx.cq_multiplier)
        .ok_or_else(|| format!("CQ size overflow for SQ size {sq_size}"))?;
    let sq = Arc::new(Q::new(sq_size, ctx.bbq_block_size)?);
    let cq = Arc::new(Q::new(cq_size, ctx.bbq_block_size)?);
    let ready = Arc::new(Barrier::new(4));
    let start = Arc::new(Barrier::new(4));
    let max_batch = batch_mode.max();

    let submit_core = ctx
        .pin_cores
        .as_ref()
        .and_then(|cores| cores.get(ctx.core_offset).cloned());
    let kernel_core = ctx
        .pin_cores
        .as_ref()
        .and_then(|cores| cores.get(ctx.core_offset + 1).cloned());
    let complete_core = ctx
        .pin_cores
        .as_ref()
        .and_then(|cores| cores.get(ctx.core_offset + 2).cloned());

    let submit_sq = Arc::clone(&sq);
    let submit_ready = Arc::clone(&ready);
    let submit_start = Arc::clone(&start);
    let requests = ctx.requests;
    let submitter = spawn_pinned("io_uring_submitter", submit_core, move || {
        submit_ready.wait();
        submit_start.wait();
        let mut rng = XorShift64::new(seed_for(repeat, sq_size, batch_mode, 1));
        let begin = Instant::now();
        let mut submitted = 0_u64;
        let backoff = Backoff::new();
        while submitted < requests {
            let remaining = (requests - submitted) as usize;
            let want = cmp::min(batch_mode.next(&mut rng), remaining);
            let mut pushed = 0_usize;
            while pushed < want {
                let count =
                    submit_sq.push_sequential_batch(submitted + pushed as u64, want - pushed);
                if count == 0 {
                    backoff.snooze();
                } else {
                    pushed += count;
                }
            }
            submitted += want as u64;
        }
        Ok((submitted, begin.elapsed().as_nanos()))
    })?;

    let kernel_sq = Arc::clone(&sq);
    let kernel_cq = Arc::clone(&cq);
    let kernel_ready = Arc::clone(&ready);
    let kernel_start = Arc::clone(&start);
    let kernel = spawn_pinned("io_uring_kernel", kernel_core, move || {
        kernel_ready.wait();
        kernel_start.wait();
        let mut rng = XorShift64::new(seed_for(repeat, sq_size, batch_mode, 2));
        let mut local = vec![0_u64; max_batch];
        let mut processed = 0_u64;
        let pop_backoff = Backoff::new();
        let push_backoff = Backoff::new();
        while processed < requests {
            let remaining = (requests - processed) as usize;
            let want = cmp::min(batch_mode.next(&mut rng), remaining);
            let mut popped = 0_usize;
            while popped < want {
                let count = kernel_sq.pop_batch(&mut local[popped..want]);
                if count == 0 {
                    pop_backoff.snooze();
                } else {
                    popped += count;
                }
            }
            let mut pushed = 0_usize;
            while pushed < want {
                let count = kernel_cq.push_batch(&local[pushed..want]);
                if count == 0 {
                    push_backoff.snooze();
                } else {
                    pushed += count;
                }
            }
            processed += want as u64;
        }
        Ok(processed)
    })?;

    let complete_cq = Arc::clone(&cq);
    let complete_ready = Arc::clone(&ready);
    let complete_start = Arc::clone(&start);
    let completer = spawn_pinned("io_uring_completer", complete_core, move || {
        complete_ready.wait();
        complete_start.wait();
        let mut rng = XorShift64::new(seed_for(repeat, sq_size, batch_mode, 3));
        let mut local = vec![0_u64; max_batch];
        let mut completed = 0_u64;
        let backoff = Backoff::new();
        while completed < requests {
            let remaining = (requests - completed) as usize;
            let want = cmp::min(batch_mode.next(&mut rng), remaining);
            let mut popped = 0_usize;
            while popped < want {
                let count = complete_cq.pop_batch(&mut local[popped..want]);
                if count == 0 {
                    backoff.snooze();
                } else {
                    for (idx, &value) in local[popped..popped + count].iter().enumerate() {
                        let expected = completed + popped as u64 + idx as u64;
                        if value != expected {
                            return Err(format!(
                                "completion order mismatch: expected {expected}, got {value}"
                            ));
                        }
                    }
                    popped += count;
                }
            }
            completed += want as u64;
        }
        Ok(completed)
    })?;

    ready.wait();
    let total_begin = Instant::now();
    start.wait();

    let (submitted, submit_elapsed_ns) = join_thread(submitter, "submitter")?;
    let processed = join_thread(kernel, "kernel")?;
    let completed = join_thread(completer, "completer")?;
    let total_elapsed_ns = total_begin.elapsed().as_nanos();

    Ok(Sample {
        queue: queue_label.to_string(),
        sq_size,
        cq_size,
        batch_mode,
        repeat,
        requests,
        submitted,
        processed,
        completed,
        submit_elapsed_ns,
        total_elapsed_ns,
    })
}

fn run_backend<Q: BenchQueue>(queue_label: &str, ctx: &RunContext) -> Result<Vec<Sample>, String> {
    let mut samples = Vec::new();
    for &sq_size in &ctx.sq_sizes {
        for &batch_mode in &ctx.batch_modes {
            for repeat in 1..=ctx.repeats {
                eprintln!(
                    "running queue={queue_label} sq={sq_size} cq={} batch={} repeat={repeat}/{}",
                    sq_size * ctx.cq_multiplier,
                    batch_mode.name(),
                    ctx.repeats
                );
                samples.push(run_once::<Q>(
                    queue_label,
                    sq_size,
                    batch_mode,
                    repeat,
                    ctx,
                )?);
            }
        }
    }
    Ok(samples)
}

macro_rules! dispatch_ubq_pool {
    ($pool:expr, $block:literal, $align_ty:ty, $backoff_ty:ty, $queue_label:expr, $ctx:expr) => {
        match $pool {
            0 => run_backend::<BoundedUbq<ConfiguredUBQ<u64, $backoff_ty, 0, $block, $align_ty>>>(
                $queue_label,
                $ctx,
            ),
            1 => run_backend::<BoundedUbq<ConfiguredUBQ<u64, $backoff_ty, 1, $block, $align_ty>>>(
                $queue_label,
                $ctx,
            ),
            2 => run_backend::<BoundedUbq<ConfiguredUBQ<u64, $backoff_ty, 2, $block, $align_ty>>>(
                $queue_label,
                $ctx,
            ),
            4 => run_backend::<BoundedUbq<ConfiguredUBQ<u64, $backoff_ty, 4, $block, $align_ty>>>(
                $queue_label,
                $ctx,
            ),
            8 => run_backend::<BoundedUbq<ConfiguredUBQ<u64, $backoff_ty, 8, $block, $align_ty>>>(
                $queue_label,
                $ctx,
            ),
            16 => {
                run_backend::<BoundedUbq<ConfiguredUBQ<u64, $backoff_ty, 16, $block, $align_ty>>>(
                    $queue_label,
                    $ctx,
                )
            }
            32 => {
                run_backend::<BoundedUbq<ConfiguredUBQ<u64, $backoff_ty, 32, $block, $align_ty>>>(
                    $queue_label,
                    $ctx,
                )
            }
            64 => {
                run_backend::<BoundedUbq<ConfiguredUBQ<u64, $backoff_ty, 64, $block, $align_ty>>>(
                    $queue_label,
                    $ctx,
                )
            }
            _ => Err(format!("unsupported UBQ pool size {}", $pool)),
        }
    };
}

macro_rules! dispatch_ubq_block {
    ($block:expr, $pool:expr, $backoff_ty:ty, $queue_label:expr, $ctx:expr) => {
        match $block {
            31 => dispatch_ubq_pool!($pool, 31, align::A64, $backoff_ty, $queue_label, $ctx),
            63 => dispatch_ubq_pool!($pool, 63, align::A128, $backoff_ty, $queue_label, $ctx),
            127 => dispatch_ubq_pool!($pool, 127, align::A256, $backoff_ty, $queue_label, $ctx),
            255 => dispatch_ubq_pool!($pool, 255, align::A512, $backoff_ty, $queue_label, $ctx),
            511 => dispatch_ubq_pool!($pool, 511, align::A1024, $backoff_ty, $queue_label, $ctx),
            1023 => dispatch_ubq_pool!($pool, 1023, align::A2048, $backoff_ty, $queue_label, $ctx),
            2047 => dispatch_ubq_pool!($pool, 2047, align::A4096, $backoff_ty, $queue_label, $ctx),
            4095 => dispatch_ubq_pool!($pool, 4095, align::A8192, $backoff_ty, $queue_label, $ctx),
            _ => Err(format!("unsupported UBQ block size {}", $block)),
        }
    };
}

fn run_ubq(label: &str, ctx: &RunContext) -> Result<Vec<Sample>, String> {
    let label = parse_ubq_label(label)?;
    let queue_label = format!("ubq_{}", label.normalized);
    match label.backoff.as_str() {
        "crossbeam" => dispatch_ubq_block!(
            label.block,
            label.pool,
            backoff::Crossbeam,
            &queue_label,
            ctx
        ),
        "yield" => dispatch_ubq_block!(label.block, label.pool, backoff::Yield, &queue_label, ctx),
        _ => Err(format!("unsupported UBQ backoff {}", label.backoff)),
    }
}

fn run_queue(queue: QueueKind, args: &Args, ctx: &RunContext) -> Result<Vec<Sample>, String> {
    match queue {
        QueueKind::IoUring => run_backend::<IoUringQueue>("io_uring", ctx),
        QueueKind::Bbq => run_bbq(ctx),
        QueueKind::Ubq => run_ubq(&args.ubq_label, ctx),
    }
}

#[cfg(feature = "bench_fastfifo")]
fn run_bbq(ctx: &RunContext) -> Result<Vec<Sample>, String> {
    let queue_label = format!("bbq_fastfifo_{}", ctx.bbq_block_size);
    run_backend::<BoundedRbbq>(&queue_label, ctx)
}

#[cfg(not(feature = "bench_fastfifo"))]
fn run_bbq(_ctx: &RunContext) -> Result<Vec<Sample>, String> {
    Err(
        "BBQ/RBBQ selected but the bench_rbbq feature is not enabled; rerun with --features bench_rbbq"
            .to_string(),
    )
}

fn seed_for(repeat: usize, sq_size: usize, batch_mode: BatchMode, role: u64) -> u64 {
    let mode = match batch_mode {
        BatchMode::Fixed1 => 0x9e37_79b9_7f4a_7c15,
        BatchMode::Random1To32 => 0xbf58_476d_1ce4_e5b9,
    };
    mode ^ ((repeat as u64) << 32) ^ ((sq_size as u64) << 11) ^ role
}

fn median(values: &mut [f64]) -> f64 {
    values.sort_by(f64::total_cmp);
    let mid = values.len() / 2;
    if values.len() % 2 == 0 {
        (values[mid - 1] + values[mid]) / 2.0
    } else {
        values[mid]
    }
}

fn csv_field(value: &str) -> String {
    if value.contains(',') || value.contains('"') || value.contains('\n') {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_string()
    }
}

fn write_row<W: Write>(writer: &mut W, fields: &[String]) -> io::Result<()> {
    for (idx, field) in fields.iter().enumerate() {
        if idx > 0 {
            writer.write_all(b",")?;
        }
        writer.write_all(csv_field(field).as_bytes())?;
    }
    writer.write_all(b"\n")
}

fn write_samples(path: &Path, samples: &[Sample]) -> Result<(), String> {
    let mut writer = BufWriter::new(
        File::create(path).map_err(|err| format!("failed to create {}: {err}", path.display()))?,
    );
    write_row(
        &mut writer,
        &[
            "queue".to_string(),
            "sq_size".to_string(),
            "cq_size".to_string(),
            "batch_mode".to_string(),
            "repeat".to_string(),
            "requests".to_string(),
            "submitted".to_string(),
            "processed".to_string(),
            "completed".to_string(),
            "submit_elapsed_ns".to_string(),
            "total_elapsed_ns".to_string(),
            "submit_ns_per_request".to_string(),
            "total_ns_per_request".to_string(),
            "submit_requests_per_sec".to_string(),
            "total_requests_per_sec".to_string(),
        ],
    )
    .map_err(|err| format!("failed to write {}: {err}", path.display()))?;
    for sample in samples {
        write_row(
            &mut writer,
            &[
                sample.queue.clone(),
                sample.sq_size.to_string(),
                sample.cq_size.to_string(),
                sample.batch_mode.name().to_string(),
                sample.repeat.to_string(),
                sample.requests.to_string(),
                sample.submitted.to_string(),
                sample.processed.to_string(),
                sample.completed.to_string(),
                sample.submit_elapsed_ns.to_string(),
                sample.total_elapsed_ns.to_string(),
                format!("{:.6}", sample.submit_ns_per_request()),
                format!("{:.6}", sample.total_ns_per_request()),
                format!("{:.6}", sample.submit_requests_per_sec()),
                format!("{:.6}", sample.total_requests_per_sec()),
            ],
        )
        .map_err(|err| format!("failed to write {}: {err}", path.display()))?;
    }
    writer
        .flush()
        .map_err(|err| format!("failed to flush {}: {err}", path.display()))
}

fn write_summary(path: &Path, samples: &[Sample]) -> Result<(), String> {
    let mut groups: BTreeMap<SummaryKey, Vec<&Sample>> = BTreeMap::new();
    for sample in samples {
        groups
            .entry(SummaryKey {
                queue: sample.queue.clone(),
                sq_size: sample.sq_size,
                cq_size: sample.cq_size,
                batch_mode: sample.batch_mode,
            })
            .or_default()
            .push(sample);
    }

    let mut medians: BTreeMap<SummaryKey, SummaryMedians> = BTreeMap::new();
    let mut latency_baselines: BTreeMap<BaselineKey, f64> = BTreeMap::new();
    let mut throughput_baselines: BTreeMap<BaselineKey, f64> = BTreeMap::new();
    for (key, group) in &groups {
        let mut submit_values: Vec<f64> = group
            .iter()
            .map(|sample| sample.submit_ns_per_request())
            .collect();
        let mut total_values: Vec<f64> = group
            .iter()
            .map(|sample| sample.total_ns_per_request())
            .collect();
        let mut submit_throughput_values: Vec<f64> = group
            .iter()
            .map(|sample| sample.submit_requests_per_sec())
            .collect();
        let mut total_throughput_values: Vec<f64> = group
            .iter()
            .map(|sample| sample.total_requests_per_sec())
            .collect();
        let submit_median = median(&mut submit_values);
        let total_median = median(&mut total_values);
        let submit_throughput_median = median(&mut submit_throughput_values);
        let total_throughput_median = median(&mut total_throughput_values);
        if key.queue == "io_uring" {
            let baseline_key = BaselineKey {
                sq_size: key.sq_size,
                cq_size: key.cq_size,
                batch_mode: key.batch_mode,
            };
            latency_baselines.insert(baseline_key.clone(), submit_median);
            throughput_baselines.insert(
                BaselineKey {
                    sq_size: key.sq_size,
                    cq_size: key.cq_size,
                    batch_mode: key.batch_mode,
                },
                submit_throughput_median,
            );
        }
        medians.insert(
            key.clone(),
            SummaryMedians {
                submit_ns_per_request: submit_median,
                total_ns_per_request: total_median,
                submit_requests_per_sec: submit_throughput_median,
                total_requests_per_sec: total_throughput_median,
            },
        );
    }

    let mut writer = BufWriter::new(
        File::create(path).map_err(|err| format!("failed to create {}: {err}", path.display()))?,
    );
    write_row(
        &mut writer,
        &[
            "queue".to_string(),
            "sq_size".to_string(),
            "cq_size".to_string(),
            "batch_mode".to_string(),
            "requests".to_string(),
            "repeats".to_string(),
            "submit_ns_per_request_median".to_string(),
            "total_ns_per_request_median".to_string(),
            "submit_requests_per_sec_median".to_string(),
            "total_requests_per_sec_median".to_string(),
            "submit_speedup_vs_io_uring".to_string(),
            "submit_throughput_speedup_vs_io_uring".to_string(),
        ],
    )
    .map_err(|err| format!("failed to write {}: {err}", path.display()))?;

    for (key, group) in groups {
        let median_values = medians
            .get(&key)
            .copied()
            .ok_or_else(|| "summary median lookup failed".to_string())?;
        let baseline_key = BaselineKey {
            sq_size: key.sq_size,
            cq_size: key.cq_size,
            batch_mode: key.batch_mode,
        };
        let latency_speedup = latency_baselines
            .get(&baseline_key)
            .map(|baseline| baseline / median_values.submit_ns_per_request);
        let throughput_speedup = throughput_baselines
            .get(&baseline_key)
            .map(|baseline| median_values.submit_requests_per_sec / baseline);
        let requests = group
            .first()
            .map(|sample| sample.requests)
            .ok_or_else(|| "empty summary group".to_string())?;
        write_row(
            &mut writer,
            &[
                key.queue,
                key.sq_size.to_string(),
                key.cq_size.to_string(),
                key.batch_mode.name().to_string(),
                requests.to_string(),
                group.len().to_string(),
                format!("{:.6}", median_values.submit_ns_per_request),
                format!("{:.6}", median_values.total_ns_per_request),
                format!("{:.6}", median_values.submit_requests_per_sec),
                format!("{:.6}", median_values.total_requests_per_sec),
                latency_speedup
                    .map(|value| format!("{value:.6}"))
                    .unwrap_or_default(),
                throughput_speedup
                    .map(|value| format!("{value:.6}"))
                    .unwrap_or_default(),
            ],
        )
        .map_err(|err| format!("failed to write {}: {err}", path.display()))?;
    }
    writer
        .flush()
        .map_err(|err| format!("failed to flush {}: {err}", path.display()))
}

fn default_out_dir() -> PathBuf {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0);
    let host = std::env::var("HOSTNAME")
        .or_else(|_| std::env::var("COMPUTERNAME"))
        .unwrap_or_else(|_| "local".to_string())
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
                ch
            } else {
                '-'
            }
        })
        .collect::<String>();
    PathBuf::from("bench_results")
        .join("io_uring_queue")
        .join(format!("{timestamp}-{host}"))
}

fn run(args: Args) -> Result<PathBuf, String> {
    let queues = parse_queue_kinds(&args.queues)?;
    let ctx = validate_args(&args)?;
    let mut samples = Vec::new();
    for queue in queues {
        samples.extend(run_queue(queue, &args, &ctx)?);
    }

    let out_dir = args.out_dir.clone().unwrap_or_else(default_out_dir);
    fs::create_dir_all(&out_dir)
        .map_err(|err| format!("failed to create {}: {err}", out_dir.display()))?;
    write_samples(&out_dir.join("samples.csv"), &samples)?;
    write_summary(&out_dir.join("summary.csv"), &samples)?;
    Ok(out_dir)
}

fn main() {
    let args = Args::parse();
    match run(args) {
        Ok(out_dir) => {
            println!("wrote {}", out_dir.join("samples.csv").display());
            println!("wrote {}", out_dir.join("summary.csv").display());
        }
        Err(err) => {
            eprintln!("{err}");
            std::process::exit(1);
        }
    }
}
