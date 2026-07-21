#![allow(missing_docs)]

#[cfg(feature = "bench_registry")]
use crate::align;
use crate::{ConfiguredUBQ, backoff};
use concurrent_queue::{ConcurrentQueue, PopError};
use crossbeam_queue::SegQueue;
use crossbeam_utils::Backoff;
#[cfg(feature = "bench_lfqueue")]
use lfqueue::UnboundedQueue as LfUnboundedQueue;
#[cfg(feature = "bench_fastfifo")]
use rbbq::FastFifo;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::ffi::OsStr;
use std::fmt;
use std::fs;
use std::io::{self, BufRead, BufReader, Write};
use std::num::NonZero;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::{
    Arc, Barrier, OnceLock,
    atomic::{AtomicU64, Ordering as AtomicOrdering},
    mpsc,
};
use std::thread;
use std::thread::available_parallelism;

fn bench_core_ids() -> &'static [core_affinity::CoreId] {
    static IDS: OnceLock<Vec<core_affinity::CoreId>> = OnceLock::new();
    IDS.get_or_init(|| core_affinity::get_core_ids().unwrap_or_default())
}
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::task::JoinSet;

pub const RUN_SCHEMA_VERSION: u32 = 3;
pub const PLAN_SCHEMA_VERSION: u32 = 2;
pub const DEFAULT_ITEMS_PER_PRODUCER: u64 = 1_000_000;
pub const DEFAULT_RUNS_DIR: &str = "bench_results/runs";
pub const DEFAULT_PLOTS_DIR: &str = "bench_results/plots";
pub const DEFAULT_SCENARIOS: &[&str] = &[
    "1p1c", "4p1c", "1p4c", "4p4c", "8p1c", "8p4c", "8p8c", "1p8c", "4p8c", "16p1c", "1p16c",
    "8p16c", "16p8c", "16p16c", "32p1c", "1p32c", "16p32c", "32p16c", "32p32c", "64p1c", "1p64c",
    "32p64c", "64p32c", "64p64c",
];
pub const BBQ_ATC22_X86_88T_SCENARIO_SUITE: &str = "bbq-atc22-x86-88t";
pub const BBQ_ATC22_OVERSUB_X86_12T_SCENARIO_SUITE: &str = "bbq-atc22-oversub-x86-12t";

const SENTINEL: u64 = u64::MAX;
const LOG_SENTINEL_META: u64 = u64::MAX;
const LOG_SINK_BUFFER_CAPACITY: usize = 1024 * 1024;
const LOG_SINK_FLUSH_THRESHOLD: usize = 256 * 1024;
const LOG_SINK_MAX_RECORD_BYTES: usize = 256;
const LOG_PRODUCER_ID_MASK: u64 = 0x00ff_ffff;
const LOG_SEQUENCE_MASK: u64 = 0xffff_ffff;
const LOG_MESSAGES: [&str; 8] = [
    "accepted client connection",
    "parsed request headers",
    "loaded tenant configuration",
    "queued background task",
    "completed cache lookup",
    "serialized response body",
    "updated metric counter",
    "released request context",
];
const DEFAULT_FASTFIFO_BLOCK_SIZES: [usize; 4] = [64, 256, 1024, 4096];
#[cfg(feature = "bench_fastfifo")]
const RBBQ_WAIT_TIMEOUT: Duration = Duration::from_secs(30);
#[cfg(feature = "bench_wcq")]
const WCQ_WAIT_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_BENCH_JOB_TIMEOUT_SECS: u64 = 300;
const UBQ_POOL_VALUES: [u8; 8] = [0, 1, 2, 4, 8, 16, 32, 64];
const UBQ_BLOCK_VALUES: [u16; 8] = [31, 63, 127, 255, 511, 1023, 2047, 4095];
const UBQ_BACKOFF_VALUES: [&str; 2] = ["crossbeam", "yield"];
const UBQ_SPARSE_POOL_VALUES: [u8; 4] = [0, 1, 8, 64];
const UBQ_SPARSE_BLOCK_VALUES: [u16; 5] = [31, 127, 511, 2047, 4095];
pub const DEFAULT_UBQ_BATCH_SIZES: [usize; 11] = [2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048];
const DEFAULT_LFQUEUE_SEGMENT_SIZES: [usize; 3] = [32, 256, 1024];
const DEFAULT_WCQ_CAPACITIES: [usize; 3] = [4096, 65536, 1048576];
const SUPPORTED_WCQ_CAPACITIES: [usize; 8] =
    [256, 1024, 4096, 16384, 65536, 262144, 1048576, 4194304];
const WCQ_MAX_THREADS: usize = 256;

thread_local! {
    static BENCH_JOB_DEADLINE: std::cell::Cell<Option<Instant>> =
        const { std::cell::Cell::new(None) };
}

fn bench_job_timeout() -> Duration {
    std::env::var("UBQ_BENCH_JOB_TIMEOUT_SECS")
        .ok()
        .and_then(|raw| raw.parse::<u64>().ok())
        .filter(|secs| *secs > 0)
        .map(Duration::from_secs)
        .unwrap_or(Duration::from_secs(DEFAULT_BENCH_JOB_TIMEOUT_SECS))
}

fn current_bench_job_deadline() -> Option<Instant> {
    BENCH_JOB_DEADLINE.with(std::cell::Cell::get)
}

fn with_bench_job_deadline<T>(deadline: Option<Instant>, f: impl FnOnce() -> T) -> T {
    struct DeadlineGuard(Option<Instant>);

    impl Drop for DeadlineGuard {
        fn drop(&mut self) {
            BENCH_JOB_DEADLINE.with(|slot| slot.set(self.0));
        }
    }

    BENCH_JOB_DEADLINE.with(|slot| {
        let previous = slot.replace(deadline);
        let _guard = DeadlineGuard(previous);
        f()
    })
}

fn check_bench_job_deadline(operation: &str) {
    let Some(deadline) = current_bench_job_deadline() else {
        return;
    };
    assert!(
        Instant::now() < deadline,
        "benchmark job timed out while {operation}"
    );
}

fn spawn_bench_thread<F, T>(f: F) -> thread::JoinHandle<T>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    let deadline = current_bench_job_deadline();
    thread::spawn(move || with_bench_job_deadline(deadline, f))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LogRecord {
    pub message: &'static str,
    pub meta: u64,
}

impl LogRecord {
    fn sentinel() -> Self {
        Self {
            message: "",
            meta: LOG_SENTINEL_META,
        }
    }

    fn is_sentinel(self) -> bool {
        self.meta == LOG_SENTINEL_META
    }
}

fn pack_log_meta(level: u8, producer_id: usize, sequence: u64) -> u64 {
    let producer_id = u64::try_from(producer_id).expect("producer id must fit u64");
    assert!(
        producer_id <= LOG_PRODUCER_ID_MASK,
        "producer id is too large"
    );
    assert!(sequence <= LOG_SEQUENCE_MASK, "log sequence is too large");
    ((level as u64) << 56) | (producer_id << 32) | sequence
}

fn unpack_log_level(meta: u64) -> u8 {
    (meta >> 56) as u8
}

fn unpack_log_producer_id(meta: u64) -> u64 {
    (meta >> 32) & LOG_PRODUCER_ID_MASK
}

fn unpack_log_sequence(meta: u64) -> u64 {
    meta & LOG_SEQUENCE_MASK
}

fn log_record_for(producer_id: usize, sequence: u64) -> LogRecord {
    let message_index = (producer_id ^ sequence as usize) & (LOG_MESSAGES.len() - 1);
    LogRecord {
        message: LOG_MESSAGES[message_index],
        meta: pack_log_meta((producer_id as u8) & 0x7, producer_id, sequence),
    }
}

pub trait LogQueueOps: Send + Sync + 'static {
    fn send_log(&self, record: LogRecord);
    fn recv_log(&self) -> LogRecord;
}

pub trait LogQueueThreadOps: Send + 'static {
    fn send_log(&self, record: LogRecord);
    fn recv_log(&self) -> LogRecord;
}

impl<Q: LogQueueOps> LogQueueThreadOps for Arc<Q> {
    fn send_log(&self, record: LogRecord) {
        (**self).send_log(record);
    }

    fn recv_log(&self) -> LogRecord {
        (**self).recv_log()
    }
}

pub trait LogQueueHandleFactory: Send + Sync + 'static {
    type ThreadHandle: LogQueueThreadOps;

    fn log_thread_handle(self: &Arc<Self>) -> Self::ThreadHandle;
}

impl<Q: LogQueueOps> LogQueueHandleFactory for Q {
    type ThreadHandle = Arc<Q>;

    fn log_thread_handle(self: &Arc<Self>) -> Self::ThreadHandle {
        self.clone()
    }
}

pub trait LogQueue: LogQueueOps {
    fn new_log_queue() -> Arc<Self>
    where
        Self: Sized;
}

pub trait BenchQueueOps: Send + Sync + 'static {
    fn send_value(&self, value: u64);
    fn send_batch(&self, base: u64, offsets: std::ops::Range<usize>) {
        for offset in offsets {
            self.send_value(base + offset as u64);
        }
    }
    fn recv_value(&self) -> u64;
}

pub trait BenchQueueThreadOps: Send + 'static {
    fn send_value(&self, value: u64);
    fn send_batch(&self, base: u64, offsets: std::ops::Range<usize>);
    fn recv_value(&self) -> u64;
}

impl<Q: BenchQueueOps> BenchQueueThreadOps for Arc<Q> {
    fn send_value(&self, value: u64) {
        (**self).send_value(value);
    }

    fn send_batch(&self, base: u64, offsets: std::ops::Range<usize>) {
        (**self).send_batch(base, offsets);
    }

    fn recv_value(&self) -> u64 {
        (**self).recv_value()
    }
}

pub trait BenchQueueHandleFactory: Send + Sync + 'static {
    type ThreadHandle: BenchQueueThreadOps;

    fn thread_handle(self: &Arc<Self>) -> Self::ThreadHandle;
}

impl<Q: BenchQueueOps> BenchQueueHandleFactory for Q {
    type ThreadHandle = Arc<Q>;

    fn thread_handle(self: &Arc<Self>) -> Self::ThreadHandle {
        self.clone()
    }
}

pub trait BenchQueue: BenchQueueOps {
    fn new_queue() -> Arc<Self>
    where
        Self: Sized;
}

impl<B, const POOL: usize, const BLOCK: usize, A> BenchQueueOps
    for ConfiguredUBQ<u64, B, POOL, BLOCK, A>
where
    B: backoff::BackoffPolicy + 'static,
    A: Send + Sync + 'static,
{
    fn send_value(&self, value: u64) {
        check_bench_job_deadline("pushing to UBQ");
        self.push(value);
    }

    fn send_batch(&self, base: u64, offsets: std::ops::Range<usize>) {
        check_bench_job_deadline("batch-pushing to UBQ");
        self.push_batch(offsets.map(move |offset| base + offset as u64));
    }

    fn recv_value(&self) -> u64 {
        let backoff = Backoff::new();
        loop {
            check_bench_job_deadline("popping from UBQ");
            if let Some(value) = self.pop() {
                return value;
            }
            backoff.snooze();
        }
    }
}

impl<B, const POOL: usize, const BLOCK: usize, A> BenchQueue
    for ConfiguredUBQ<u64, B, POOL, BLOCK, A>
where
    B: backoff::BackoffPolicy + 'static,
    A: Send + Sync + 'static,
{
    fn new_queue() -> Arc<Self> {
        Arc::new(Self::new())
    }
}

impl<B, const POOL: usize, const BLOCK: usize, A> LogQueueOps
    for ConfiguredUBQ<LogRecord, B, POOL, BLOCK, A>
where
    B: backoff::BackoffPolicy + 'static,
    A: Send + Sync + 'static,
{
    fn send_log(&self, record: LogRecord) {
        check_bench_job_deadline("pushing log record to UBQ");
        self.push(record);
    }

    fn recv_log(&self) -> LogRecord {
        let backoff = Backoff::new();
        loop {
            check_bench_job_deadline("popping log record from UBQ");
            if let Some(record) = self.pop() {
                return record;
            }
            backoff.snooze();
        }
    }
}

impl<B, const POOL: usize, const BLOCK: usize, A> LogQueue
    for ConfiguredUBQ<LogRecord, B, POOL, BLOCK, A>
where
    B: backoff::BackoffPolicy + 'static,
    A: Send + Sync + 'static,
{
    fn new_log_queue() -> Arc<Self> {
        Arc::new(Self::new())
    }
}

impl BenchQueueOps for SegQueue<u64> {
    fn send_value(&self, value: u64) {
        check_bench_job_deadline("pushing to SegQueue");
        self.push(value);
    }

    fn recv_value(&self) -> u64 {
        let backoff = Backoff::new();
        loop {
            check_bench_job_deadline("popping from SegQueue");
            if let Some(value) = self.pop() {
                return value;
            }
            backoff.snooze();
        }
    }
}

impl BenchQueue for SegQueue<u64> {
    fn new_queue() -> Arc<Self> {
        Arc::new(Self::new())
    }
}

impl LogQueueOps for SegQueue<LogRecord> {
    fn send_log(&self, record: LogRecord) {
        check_bench_job_deadline("pushing log record to SegQueue");
        self.push(record);
    }

    fn recv_log(&self) -> LogRecord {
        let backoff = Backoff::new();
        loop {
            check_bench_job_deadline("popping log record from SegQueue");
            if let Some(record) = self.pop() {
                return record;
            }
            backoff.snooze();
        }
    }
}

impl LogQueue for SegQueue<LogRecord> {
    fn new_log_queue() -> Arc<Self> {
        Arc::new(Self::new())
    }
}

impl BenchQueueOps for ConcurrentQueue<u64> {
    fn send_value(&self, value: u64) {
        check_bench_job_deadline("pushing to concurrent-queue");
        self.push(value).expect("send failed");
    }

    fn recv_value(&self) -> u64 {
        let backoff = Backoff::new();
        loop {
            check_bench_job_deadline("popping from concurrent-queue");
            match self.pop() {
                Ok(value) => return value,
                Err(PopError::Empty) => {}
                Err(PopError::Closed) => panic!("recv failed: queue closed"),
            }
            backoff.snooze();
        }
    }
}

impl BenchQueue for ConcurrentQueue<u64> {
    fn new_queue() -> Arc<Self> {
        Arc::new(Self::unbounded())
    }
}

impl LogQueueOps for ConcurrentQueue<LogRecord> {
    fn send_log(&self, record: LogRecord) {
        check_bench_job_deadline("pushing log record to concurrent-queue");
        self.push(record).expect("send failed");
    }

    fn recv_log(&self) -> LogRecord {
        let backoff = Backoff::new();
        loop {
            check_bench_job_deadline("popping log record from concurrent-queue");
            match self.pop() {
                Ok(record) => return record,
                Err(PopError::Empty) => {}
                Err(PopError::Closed) => panic!("recv failed: queue closed"),
            }
            backoff.snooze();
        }
    }
}

impl LogQueue for ConcurrentQueue<LogRecord> {
    fn new_log_queue() -> Arc<Self> {
        Arc::new(Self::unbounded())
    }
}

#[cfg(feature = "bench_lfqueue")]
struct LfQueueBenchQueue {
    inner: LfUnboundedQueue<u64>,
}

#[cfg(feature = "bench_lfqueue")]
impl LfQueueBenchQueue {
    fn new(segment_size: usize) -> Arc<Self> {
        Arc::new(Self {
            inner: LfUnboundedQueue::with_segment_size(segment_size),
        })
    }
}

#[cfg(feature = "bench_lfqueue")]
impl BenchQueueOps for LfQueueBenchQueue {
    fn send_value(&self, value: u64) {
        check_bench_job_deadline("pushing to lfqueue");
        self.inner.enqueue(value);
    }

    fn recv_value(&self) -> u64 {
        let backoff = Backoff::new();
        loop {
            check_bench_job_deadline("popping from lfqueue");
            if let Some(value) = self.inner.dequeue() {
                return value;
            }
            backoff.snooze();
        }
    }
}

#[cfg(feature = "bench_lfqueue")]
struct LogLfQueueBenchQueue {
    inner: LfUnboundedQueue<LogRecord>,
}

#[cfg(feature = "bench_lfqueue")]
impl LogLfQueueBenchQueue {
    fn new(segment_size: usize) -> Arc<Self> {
        Arc::new(Self {
            inner: LfUnboundedQueue::with_segment_size(segment_size),
        })
    }
}

#[cfg(feature = "bench_lfqueue")]
impl LogQueueOps for LogLfQueueBenchQueue {
    fn send_log(&self, record: LogRecord) {
        check_bench_job_deadline("pushing log record to lfqueue");
        self.inner.enqueue(record);
    }

    fn recv_log(&self) -> LogRecord {
        let backoff = Backoff::new();
        loop {
            check_bench_job_deadline("popping log record from lfqueue");
            if let Some(record) = self.inner.dequeue() {
                return record;
            }
            backoff.snooze();
        }
    }
}

#[cfg(feature = "bench_wcq")]
struct WcqBenchQueue<const CAPACITY: usize> {
    inner: wcq::Queue<u64, CAPACITY, WCQ_MAX_THREADS>,
}

#[cfg(feature = "bench_wcq")]
impl<const CAPACITY: usize> WcqBenchQueue<CAPACITY> {
    fn new() -> Arc<Self> {
        // Queue<u64, CAPACITY, WCQ_MAX_THREADS> is stored inline, so Arc::new()
        // must construct it on the calling thread's stack before promoting it to
        // the heap. For large CAPACITY values (e.g. 1M cells × ~48 B each = ~48 MB)
        // this overflows the default tokio spawn_blocking stack (8 MB on Linux/macOS).
        // Use a dedicated OS thread with a generous stack reservation whenever the
        // struct exceeds 4 MiB; virtual pages are committed lazily on Linux/macOS.
        const STACK_THRESHOLD: usize = 4 * 1024 * 1024;
        if std::mem::size_of::<Self>() <= STACK_THRESHOLD {
            return Arc::new(Self {
                inner: wcq::Queue::new(),
            });
        }
        std::thread::Builder::new()
            .stack_size(512 * 1024 * 1024)
            .spawn(|| {
                Arc::new(Self {
                    inner: wcq::Queue::new(),
                })
            })
            .expect("WCQ init thread spawn failed")
            .join()
            .expect("WCQ init thread panicked")
    }
}

#[cfg(feature = "bench_wcq")]
struct WcqThreadHandle<const CAPACITY: usize> {
    queue: Arc<WcqBenchQueue<CAPACITY>>,
    handle: wcq::ThreadHandle<WCQ_MAX_THREADS>,
}

#[cfg(feature = "bench_wcq")]
impl<const CAPACITY: usize> BenchQueueHandleFactory for WcqBenchQueue<CAPACITY> {
    type ThreadHandle = WcqThreadHandle<CAPACITY>;

    fn thread_handle(self: &Arc<Self>) -> Self::ThreadHandle {
        let handle = self
            .inner
            .register()
            .expect("wCQ thread handle registration failed");
        WcqThreadHandle {
            queue: self.clone(),
            handle,
        }
    }
}

#[cfg(feature = "bench_wcq")]
impl<const CAPACITY: usize> BenchQueueThreadOps for WcqThreadHandle<CAPACITY> {
    fn send_value(&self, value: u64) {
        let deadline = Instant::now() + WCQ_WAIT_TIMEOUT;
        let backoff = Backoff::new();
        loop {
            check_bench_job_deadline("pushing to wCQ");
            if self.queue.inner.enqueue(self.handle, value).is_ok() {
                return;
            }
            assert!(Instant::now() < deadline, "timed out pushing to wCQ");
            backoff.snooze();
        }
    }

    fn recv_value(&self) -> u64 {
        let backoff = Backoff::new();
        loop {
            check_bench_job_deadline("popping from wCQ");
            if let Some(value) = self.queue.inner.dequeue(self.handle) {
                return value;
            }
            backoff.snooze();
        }
    }
}

#[cfg(feature = "bench_fastfifo")]
struct RbbqBenchQueue {
    inner: FastFifo<u64>,
}

#[cfg(feature = "bench_fastfifo")]
impl RbbqBenchQueue {
    fn new(scenario: &ScenarioConfig, items_per_producer: u64, block_size: usize) -> Arc<Self> {
        let total_items = usize::try_from(total_items(items_per_producer, scenario.producers))
            .expect("total items must fit usize for RBBQ capacity");
        let required_capacity = total_items
            .checked_add(scenario.consumers)
            .and_then(|value| value.checked_add(block_size))
            .expect("RBBQ required capacity overflow");
        let num_blocks = required_capacity
            .div_ceil(block_size)
            .checked_add(2)
            .expect("RBBQ block count overflow")
            .max(2);
        Arc::new(Self {
            inner: FastFifo::new(num_blocks, block_size),
        })
    }
}

#[cfg(feature = "bench_fastfifo")]
impl BenchQueueOps for RbbqBenchQueue {
    fn send_value(&self, value: u64) {
        let deadline = Instant::now() + RBBQ_WAIT_TIMEOUT;
        let backoff = Backoff::new();
        loop {
            check_bench_job_deadline("pushing to RBBQ");
            if self.inner.push(value).is_ok() {
                return;
            }
            assert!(Instant::now() < deadline, "timed out pushing to RBBQ");
            backoff.snooze();
        }
    }

    fn recv_value(&self) -> u64 {
        let deadline = Instant::now() + RBBQ_WAIT_TIMEOUT;
        let backoff = Backoff::new();
        loop {
            check_bench_job_deadline("popping from RBBQ");
            if let Ok(value) = self.inner.pop() {
                return value;
            }
            assert!(Instant::now() < deadline, "timed out popping from RBBQ");
            backoff.snooze();
        }
    }
}

#[cfg(feature = "bench_fastfifo")]
struct LogRbbqBenchQueue {
    inner: FastFifo<LogRecord>,
}

#[cfg(feature = "bench_fastfifo")]
impl LogRbbqBenchQueue {
    fn new(scenario: &ScenarioConfig, items_per_producer: u64, block_size: usize) -> Arc<Self> {
        let total_items = usize::try_from(total_items(items_per_producer, scenario.producers))
            .expect("total items must fit usize for RBBQ capacity");
        let required_capacity = total_items
            .checked_add(scenario.consumers)
            .and_then(|value| value.checked_add(block_size))
            .expect("RBBQ required capacity overflow");
        let num_blocks = required_capacity
            .div_ceil(block_size)
            .checked_add(2)
            .expect("RBBQ block count overflow")
            .max(2);
        Arc::new(Self {
            inner: FastFifo::new(num_blocks, block_size),
        })
    }
}

#[cfg(feature = "bench_fastfifo")]
impl LogQueueOps for LogRbbqBenchQueue {
    fn send_log(&self, record: LogRecord) {
        let deadline = Instant::now() + RBBQ_WAIT_TIMEOUT;
        let backoff = Backoff::new();
        loop {
            check_bench_job_deadline("pushing log record to RBBQ");
            if self.inner.push(record).is_ok() {
                return;
            }
            assert!(Instant::now() < deadline, "timed out pushing to RBBQ");
            backoff.snooze();
        }
    }

    fn recv_log(&self) -> LogRecord {
        let deadline = Instant::now() + RBBQ_WAIT_TIMEOUT;
        let backoff = Backoff::new();
        loop {
            check_bench_job_deadline("popping log record from RBBQ");
            if let Ok(record) = self.inner.pop() {
                return record;
            }
            assert!(Instant::now() < deadline, "timed out popping from RBBQ");
            backoff.snooze();
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Mode {
    Throughput,
    ComplexThroughput,
    DataLatency,
    Fairness,
    FillDrain,
    AppLogFanIn,
    AppPipeline,
    AppTaskRoundtrip,
    AppLogMpscFile,
}

impl Mode {
    pub fn name(self) -> &'static str {
        match self {
            Mode::Throughput => "throughput",
            Mode::ComplexThroughput => "complex_throughput",
            Mode::DataLatency => "data_latency",
            Mode::Fairness => "fairness",
            Mode::FillDrain => "fill_drain",
            Mode::AppLogFanIn => "app_log_fan_in",
            Mode::AppPipeline => "app_pipeline",
            Mode::AppTaskRoundtrip => "app_task_roundtrip",
            Mode::AppLogMpscFile => "app_log_mpsc_file",
        }
    }

    pub fn parse(input: &str) -> Option<Self> {
        match input.trim().to_ascii_lowercase().as_str() {
            "throughput" => Some(Self::Throughput),
            "complex_throughput" | "complex-throughput" | "complex" => {
                Some(Self::ComplexThroughput)
            }
            "data_latency" | "data-latency" => Some(Self::DataLatency),
            "fairness" => Some(Self::Fairness),
            "fill_drain" | "fill-drain" => Some(Self::FillDrain),
            "app_log_fan_in" | "app-log-fan-in" => Some(Self::AppLogFanIn),
            "app_pipeline" | "app-pipeline" => Some(Self::AppPipeline),
            "app_task_roundtrip" | "app-task-roundtrip" => Some(Self::AppTaskRoundtrip),
            "app_log_mpsc_file" | "app-log-mpsc-file" => Some(Self::AppLogMpscFile),
            _ => None,
        }
    }

    fn extra_threads(self) -> usize {
        match self {
            Mode::AppPipeline => 1,
            _ => 0,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum QueueKind {
    Ubq,
    SegQueue,
    ConcurrentQueue,
    FastFifo,
    LfQueue,
    Wcq,
}

impl QueueKind {
    pub fn name(self) -> &'static str {
        match self {
            QueueKind::Ubq => "ubq",
            QueueKind::SegQueue => "segqueue",
            QueueKind::ConcurrentQueue => "concurrent-queue",
            QueueKind::FastFifo => "fastfifo",
            QueueKind::LfQueue => "lfqueue",
            QueueKind::Wcq => "wcq",
        }
    }

    pub fn parse(input: &str) -> Option<Self> {
        match input.trim().to_ascii_lowercase().as_str() {
            "ubq" => Some(Self::Ubq),
            "segqueue" | "crossbeam" | "crossbeam-segqueue" => Some(Self::SegQueue),
            "concurrent-queue" | "concurrent" => Some(Self::ConcurrentQueue),
            "fastfifo" | "fast-fifo" | "rbbq" | "bbq" => Some(Self::FastFifo),
            "lfqueue" | "lf-queue" | "lscq" | "scq" => Some(Self::LfQueue),
            "wcq" | "w-cq" | "wait-free-cq" | "wait-free-queue" => Some(Self::Wcq),
            _ => None,
        }
    }

    pub fn is_baseline(self) -> bool {
        !matches!(self, QueueKind::Ubq)
    }
}

impl Serialize for QueueKind {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.name())
    }
}

impl<'de> Deserialize<'de> for QueueKind {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        QueueKind::parse(&value)
            .ok_or_else(|| serde::de::Error::custom(format!("invalid queue kind: {value}")))
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub struct ScenarioConfig {
    pub name: String,
    pub producers: usize,
    pub consumers: usize,
}

impl ScenarioConfig {
    pub fn new(producers: usize, consumers: usize) -> Self {
        Self {
            name: format!("{producers}p{consumers}c"),
            producers,
            consumers,
        }
    }

    pub fn total_threads(&self) -> usize {
        self.producers
            .checked_add(self.consumers)
            .unwrap_or_else(|| panic!("scenario thread count overflow"))
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub struct UbqLabel {
    pub preset: String,
    pub pool: u8,
    pub block: u16,
    pub backoff: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UbqGrid {
    Sparse,
    Dense,
}

impl UbqGrid {
    pub fn name(self) -> &'static str {
        match self {
            Self::Sparse => "sparse",
            Self::Dense => "dense",
        }
    }

    pub fn labels(self) -> Vec<String> {
        let pools: &[u8] = match self {
            Self::Sparse => &UBQ_SPARSE_POOL_VALUES,
            Self::Dense => &UBQ_POOL_VALUES,
        };
        let blocks: &[u16] = match self {
            Self::Sparse => &UBQ_SPARSE_BLOCK_VALUES,
            Self::Dense => &UBQ_BLOCK_VALUES,
        };
        let mut labels = Vec::with_capacity(pools.len() * blocks.len() * UBQ_BACKOFF_VALUES.len());
        for pool in pools {
            for block in blocks {
                for backoff in UBQ_BACKOFF_VALUES {
                    labels.push(format!("balanced,{pool},{block},{backoff}"));
                }
            }
        }
        labels
    }
}

impl UbqLabel {
    pub fn text(&self) -> String {
        format!(
            "{},{},{},{}",
            self.preset, self.pool, self.block, self.backoff
        )
    }

    pub fn safe(&self) -> String {
        format!(
            "{}_{}_{}_{}",
            self.preset, self.pool, self.block, self.backoff
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub struct JobSpec {
    pub scenario: ScenarioConfig,
    pub repeat_index: usize,
    pub mode: Mode,
    pub items_per_producer: u64,
    pub queue: QueueKind,
    pub ubq_label: Option<String>,
    #[serde(default)]
    pub batch_size: Option<usize>,
    pub fastfifo_block_size: Option<usize>,
    #[serde(default)]
    pub lfqueue_segment_size: Option<usize>,
    #[serde(default)]
    pub wcq_capacity: Option<usize>,
}

impl JobSpec {
    pub fn queue_label(&self) -> String {
        match (
            &self.queue,
            &self.ubq_label,
            self.fastfifo_block_size,
            self.lfqueue_segment_size,
            self.wcq_capacity,
        ) {
            (QueueKind::Ubq, Some(label), _, _, _) => format!("ubq_{label}"),
            (QueueKind::FastFifo, _, Some(block_size), _, _) => fastfifo_queue_label(block_size),
            (QueueKind::LfQueue, _, _, Some(segment_size), _) => lfqueue_queue_label(segment_size),
            (QueueKind::Wcq, _, _, _, Some(capacity)) => wcq_queue_label(capacity),
            _ => self.queue.name().to_string(),
        }
    }

    pub fn thread_budget(&self) -> usize {
        self.scenario
            .total_threads()
            .checked_add(self.mode.extra_threads())
            .unwrap_or_else(|| panic!("job thread budget overflow"))
    }

    pub fn sort_key(
        &self,
    ) -> (
        std::cmp::Reverse<usize>,
        String,
        usize,
        String,
        u64,
        String,
        Option<usize>,
    ) {
        (
            std::cmp::Reverse(self.thread_budget()),
            self.scenario.name.clone(),
            self.repeat_index,
            self.mode.name().to_string(),
            self.items_per_producer,
            self.queue_label(),
            self.batch_size,
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct SampleKey {
    pub scenario: String,
    pub repeat_index: usize,
    pub mode: Mode,
    pub items_per_producer: u64,
    pub queue_label: String,
    pub batch_size: Option<usize>,
}

impl SampleKey {
    pub fn from_job(job: &JobSpec) -> Self {
        Self {
            scenario: job.scenario.name.clone(),
            repeat_index: job.repeat_index,
            mode: job.mode,
            items_per_producer: job.items_per_producer,
            queue_label: job.queue_label(),
            batch_size: job.batch_size,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PlanBundle {
    pub scenario: ScenarioConfig,
    pub repeat_index: usize,
    pub ubq_label: Option<String>,
    pub modes: Vec<Mode>,
    pub items_per_producer_values: Vec<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MatrixPlan {
    pub plan_schema_version: u32,
    pub machine_label: String,
    pub runs_dir: PathBuf,
    pub available_parallelism: usize,
    pub baseline_queues: Vec<QueueKind>,
    #[serde(default)]
    pub fastfifo_block_sizes: Vec<usize>,
    #[serde(default)]
    pub lfqueue_segment_sizes: Vec<usize>,
    #[serde(default)]
    pub wcq_capacities: Vec<usize>,
    #[serde(default)]
    pub ubq_grid: Option<UbqGrid>,
    #[serde(default)]
    pub ubq_batch_sizes: Vec<usize>,
    #[serde(default)]
    pub planned_repeats: usize,
    pub bundles: Vec<PlanBundle>,
    pub reuse_existing: bool,
}

#[derive(Clone, Debug)]
pub struct FrontierConfig {
    pub machine_label: String,
    pub runs_dir: PathBuf,
    pub scenarios: Vec<ScenarioConfig>,
    pub baseline_queues: Vec<QueueKind>,
    pub fastfifo_block_sizes: Vec<usize>,
    pub lfqueue_segment_sizes: Vec<usize>,
    pub wcq_capacities: Vec<usize>,
    pub seed_labels: Vec<String>,
    pub modes: Vec<Mode>,
    pub items_per_producer_values: Vec<u64>,
    pub repeats: usize,
    pub available_parallelism: usize,
}

/// Outcome returned by [`build_and_run_matrix_plan`] after the generated
/// scheduler subprocess exits. Infrastructure errors (compile failure, spawn
/// failure) still propagate as `Err`.
#[derive(Debug)]
pub struct BatchOutcome {
    /// `true` if the scheduler subprocess exited with a success code.
    pub exit_success: bool,
    /// If the scheduler crashed and a specific UBQ job was identified as the
    /// in-flight victim, its `(queue_label, scenario_name)` is stored here.
    /// `None` on success or when no UBQ victim could be identified.
    pub crashed_job: Option<(String, String)>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OutputMeta {
    pub timestamp_unix_ms: u128,
    pub machine_label: String,
    pub scenario: String,
    pub producers: usize,
    pub consumers: usize,
    pub repeat_index: usize,
    pub available_parallelism: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ubq_label: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ubq_block_size: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ubq_grid: Option<UbqGrid>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expected_ubq_configurations: Option<usize>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub ubq_batch_sizes: Vec<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub planned_repeats: Option<usize>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub planned_items_per_producer: Vec<u64>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BenchRecordStatus {
    Completed,
    Failed,
    TimedOut,
}

impl BenchRecordStatus {
    fn completed() -> Self {
        Self::Completed
    }
}

fn is_completed_status(status: &BenchRecordStatus) -> bool {
    matches!(status, BenchRecordStatus::Completed)
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BenchRecord {
    pub queue: String,
    pub mode: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub batch_size: Option<usize>,
    pub items_per_producer: u64,
    pub total_items: u64,
    pub consumed_items: u64,
    pub elapsed_ns: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ops_per_sec: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub producer_ops_per_sec: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub consumer_ops_per_sec: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub written_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub flush_count: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub push_elapsed_ns: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pop_elapsed_ns: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fill_elapsed_ns: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub drain_elapsed_ns: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub avg_data_latency_ns: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub producer_fairness_ratio: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub consumer_fairness_ratio: Option<f64>,
    #[serde(
        default = "BenchRecordStatus::completed",
        skip_serializing_if = "is_completed_status"
    )]
    pub status: BenchRecordStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failure_reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout_ns: Option<u64>,
}

impl BenchRecord {
    fn completed(&self) -> bool {
        self.status == BenchRecordStatus::Completed
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OutputFile {
    pub schema_version: u32,
    pub meta: OutputMeta,
    pub results: Vec<BenchRecord>,
}

#[derive(Clone)]
pub struct JobFactory {
    pub spec: JobSpec,
    pub run: Arc<dyn Fn(usize) -> BenchRecord + Send + Sync>,
}

impl fmt::Debug for JobFactory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("JobFactory")
            .field("spec", &self.spec)
            .finish_non_exhaustive()
    }
}

#[derive(Default)]
pub struct ExistingRunsIndex {
    pub records: BTreeMap<SampleKey, BenchRecord>,
}

#[derive(Clone)]
struct BundleOutputState {
    meta: OutputMeta,
    path: PathBuf,
    ordered_keys: Vec<SampleKey>,
    records: BTreeMap<SampleKey, BenchRecord>,
    dirty: bool,
}

impl BundleOutputState {
    fn new(plan: &MatrixPlan, bundle: &PlanBundle, run_id: &str) -> Result<Self, String> {
        Ok(Self {
            meta: bundle_output_meta(plan, bundle)?,
            path: output_path_for_bundle(plan, bundle, run_id),
            ordered_keys: expected_keys_for_bundle(plan, bundle),
            records: BTreeMap::new(),
            dirty: false,
        })
    }

    fn store_record(&mut self, key: &SampleKey, record: &BenchRecord) {
        self.records.insert(key.clone(), record.clone());
        self.dirty = true;
    }

    fn missing_keys(&self) -> impl Iterator<Item = &SampleKey> {
        self.ordered_keys
            .iter()
            .filter(|key| !self.records.contains_key(*key))
    }

    fn flush(&mut self) -> Result<bool, String> {
        if !self.dirty {
            return Ok(false);
        }

        self.meta.timestamp_unix_ms = now_unix_ms();
        let output = OutputFile {
            schema_version: RUN_SCHEMA_VERSION,
            meta: self.meta.clone(),
            results: self
                .ordered_keys
                .iter()
                .filter_map(|key| self.records.get(key).cloned())
                .collect(),
        };
        let json = serde_json::to_string_pretty(&output)
            .map_err(|err| format!("failed to serialize output: {err}"))?;
        atomic_write_string(&self.path, &json)?;
        self.dirty = false;
        Ok(true)
    }
}

struct IncrementalOutputWriter {
    bundles: Vec<BundleOutputState>,
    bundle_indices_by_key: BTreeMap<SampleKey, Vec<usize>>,
    write_count: usize,
}

impl IncrementalOutputWriter {
    fn new(plan: &MatrixPlan, cache: &ExistingRunsIndex) -> Result<Self, String> {
        let run_id = format!("{}", now_unix_nanos());
        let mut bundles = Vec::with_capacity(plan.bundles.len());
        let mut bundle_indices_by_key: BTreeMap<SampleKey, Vec<usize>> = BTreeMap::new();

        for bundle in &plan.bundles {
            let index = bundles.len();
            let state = BundleOutputState::new(plan, bundle, &run_id)?;
            for key in &state.ordered_keys {
                bundle_indices_by_key
                    .entry(key.clone())
                    .or_default()
                    .push(index);
            }
            bundles.push(state);
        }

        let mut writer = Self {
            bundles,
            bundle_indices_by_key,
            write_count: 0,
        };
        for (key, record) in &cache.records {
            writer.seed_cached_record(key, record);
        }
        Ok(writer)
    }

    fn seed_cached_record(&mut self, key: &SampleKey, record: &BenchRecord) {
        let Some(indices) = self.bundle_indices_by_key.get(key).cloned() else {
            return;
        };
        for index in indices {
            self.bundles[index]
                .records
                .insert(key.clone(), record.clone());
        }
    }

    fn handle_completed_record(
        &mut self,
        key: SampleKey,
        record: BenchRecord,
    ) -> Result<(), String> {
        let Some(indices) = self.bundle_indices_by_key.get(&key).cloned() else {
            return Err(format!(
                "missing output bundle mapping for {} scenario={} repeat={} mode={} items={}",
                key.queue_label,
                key.scenario,
                key.repeat_index,
                key.mode.name(),
                key.items_per_producer
            ));
        };

        for index in indices {
            let bundle = &mut self.bundles[index];
            bundle.store_record(&key, &record);
            if bundle.flush()? {
                self.write_count += 1;
            }
        }

        Ok(())
    }

    fn finish(mut self, expect_complete: bool) -> Result<usize, String> {
        if expect_complete {
            for bundle in &self.bundles {
                if let Some(missing) = bundle.missing_keys().next() {
                    return Err(format!(
                        "missing cached record for {} scenario={} repeat={} mode={} items={}",
                        missing.queue_label,
                        missing.scenario,
                        missing.repeat_index,
                        missing.mode.name(),
                        missing.items_per_producer
                    ));
                }
            }
        }

        for bundle in &mut self.bundles {
            if bundle.flush()? {
                self.write_count += 1;
            }
        }

        progress_line(format!(
            "scheduler: wrote {} output snapshot(s)",
            self.write_count
        ));
        Ok(self.write_count)
    }
}

enum OutputWriterMessage {
    Completed { key: SampleKey, record: BenchRecord },
    Finish { expect_complete: bool },
}

struct OutputWriterHandle {
    tx: Option<mpsc::Sender<OutputWriterMessage>>,
    join: Option<thread::JoinHandle<Result<usize, String>>>,
}

impl OutputWriterHandle {
    fn start(plan: &MatrixPlan, cache: &ExistingRunsIndex) -> Result<Self, String> {
        let writer = IncrementalOutputWriter::new(plan, cache)?;
        let (tx, rx) = mpsc::channel();
        let join = thread::spawn(move || -> Result<usize, String> {
            let mut writer = writer;
            while let Ok(message) = rx.recv() {
                match message {
                    OutputWriterMessage::Completed { key, record } => {
                        writer.handle_completed_record(key, record)?;
                    }
                    OutputWriterMessage::Finish { expect_complete } => {
                        return writer.finish(expect_complete);
                    }
                }
            }
            writer.finish(false)
        });

        Ok(Self {
            tx: Some(tx),
            join: Some(join),
        })
    }

    fn submit(&self, key: SampleKey, record: BenchRecord) -> Result<(), String> {
        let label = key.queue_label.clone();
        self.tx
            .as_ref()
            .expect("output writer sender available")
            .send(OutputWriterMessage::Completed { key, record })
            .map_err(|_| format!("output writer stopped before persisting {label}"))
    }

    fn close(mut self, expect_complete: bool) -> Result<usize, String> {
        if let Some(tx) = self.tx.take() {
            let _ = tx.send(OutputWriterMessage::Finish { expect_complete });
        }
        let Some(join) = self.join.take() else {
            return Ok(0);
        };
        join.join()
            .map_err(|_| "output writer thread panicked".to_string())?
    }
}

pub fn normalize_machine(name: &str) -> String {
    name.trim().to_ascii_lowercase()
}

pub fn parse_scenario_token(input: &str) -> Option<ScenarioConfig> {
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

pub fn parse_csv_list(raw: &str) -> Vec<String> {
    raw.split(',')
        .map(str::trim)
        .filter(|item| !item.is_empty())
        .map(ToString::to_string)
        .collect()
}

pub fn default_scenarios() -> Vec<ScenarioConfig> {
    DEFAULT_SCENARIOS
        .iter()
        .filter_map(|scenario| parse_scenario_token(scenario))
        .collect()
}

fn parse_positive_range(input: &str) -> Option<(usize, usize)> {
    let (start, end) = match input.split_once('-') {
        Some((start, end)) => (start.trim(), end.trim()),
        None => (input.trim(), input.trim()),
    };
    if start.is_empty() || end.is_empty() {
        return None;
    }
    let start = start.parse::<usize>().ok()?;
    let end = end.parse::<usize>().ok()?;
    if start == 0 || end == 0 || start > end {
        return None;
    }
    Some((start, end))
}

fn expand_family_range(
    family: &str,
    range: &str,
    out: &mut Vec<ScenarioConfig>,
) -> Result<bool, String> {
    let Some((start, end)) = parse_positive_range(range) else {
        return Err(format!("invalid {family} scenario range '{range}'"));
    };
    for threads in start..=end {
        let scenario = match family {
            "mpsc" => ScenarioConfig::new(threads, 1),
            "spmc" => ScenarioConfig::new(1, threads),
            "mpmc" => ScenarioConfig::new(threads, threads),
            _ => return Ok(false),
        };
        out.push(scenario);
    }
    Ok(true)
}

fn machine_mpsc_scenarios(available_parallelism: usize) -> Result<Vec<ScenarioConfig>, String> {
    if available_parallelism < 2 {
        return Err("mpsc:machine requires available_parallelism >= 2".to_string());
    }
    let max_producers = available_parallelism - 1;
    let mut producers = Vec::new();
    let mut value = 1usize;
    while value <= max_producers {
        producers.push(value);
        value = value
            .checked_mul(2)
            .ok_or_else(|| "mpsc:machine producer count overflow".to_string())?;
    }
    if producers.last().copied() != Some(max_producers) {
        producers.push(max_producers);
    }
    Ok(producers
        .into_iter()
        .map(|count| ScenarioConfig::new(count, 1))
        .collect())
}

fn expand_scenario_selector_with_parallelism(
    token: &str,
    machine_parallelism: Option<usize>,
) -> Result<Vec<ScenarioConfig>, String> {
    let normalized = token.trim().to_ascii_lowercase();
    if normalized == "spsc" {
        return Ok(vec![ScenarioConfig::new(1, 1)]);
    }
    if normalized == "mpsc:machine" {
        let parallelism = machine_parallelism
            .ok_or_else(|| "mpsc:machine requires detected machine parallelism".to_string())?;
        return machine_mpsc_scenarios(parallelism);
    }
    if normalized == BBQ_ATC22_X86_88T_SCENARIO_SUITE {
        let mut out = vec![ScenarioConfig::new(1, 1)];
        for producers in 1..=87 {
            out.push(ScenarioConfig::new(producers, 1));
        }
        for consumers in 1..=87 {
            out.push(ScenarioConfig::new(1, consumers));
        }
        return Ok(out);
    }
    if normalized == BBQ_ATC22_OVERSUB_X86_12T_SCENARIO_SUITE {
        let mut out = Vec::new();
        for producers in 1..=59 {
            out.push(ScenarioConfig::new(producers, 1));
        }
        for consumers in 1..=59 {
            out.push(ScenarioConfig::new(1, consumers));
        }
        return Ok(out);
    }
    for family in ["mpsc", "spmc", "mpmc"] {
        if let Some(range) = normalized.strip_prefix(&format!("{family}:")) {
            let mut out = Vec::new();
            expand_family_range(family, range, &mut out)?;
            return Ok(out);
        }
    }
    let parsed = parse_scenario_token(&normalized)
        .ok_or_else(|| format!("invalid scenario token '{token}'"))?;
    Ok(vec![parsed])
}

fn parse_scenarios_inner(
    raw: Option<&str>,
    machine_parallelism: Option<usize>,
) -> Result<Vec<ScenarioConfig>, String> {
    let source = raw.map(parse_csv_list).unwrap_or_else(|| {
        DEFAULT_SCENARIOS
            .iter()
            .map(|value| value.to_string())
            .collect()
    });
    let mut out = Vec::new();
    let mut seen = BTreeSet::new();
    for token in source {
        for parsed in expand_scenario_selector_with_parallelism(&token, machine_parallelism)? {
            if seen.insert(parsed.name.clone()) {
                out.push(parsed);
            }
        }
    }
    out.sort_by_key(|scenario| (scenario.total_threads(), scenario.name.clone()));
    Ok(out)
}

pub fn parse_scenarios(raw: Option<&str>) -> Result<Vec<ScenarioConfig>, String> {
    parse_scenarios_inner(raw, None)
}

pub fn parse_scenarios_with_parallelism(
    raw: Option<&str>,
    available_parallelism: usize,
) -> Result<Vec<ScenarioConfig>, String> {
    parse_scenarios_inner(raw, Some(available_parallelism))
}

pub fn parse_modes(raw: Option<&str>) -> Result<Vec<Mode>, String> {
    let source = raw
        .map(parse_csv_list)
        .unwrap_or_else(|| vec![Mode::Throughput.name().to_string()]);
    let mut out = Vec::new();
    let mut seen = BTreeSet::new();
    for token in source {
        let parsed = Mode::parse(&token).ok_or_else(|| format!("invalid mode '{token}'"))?;
        if seen.insert(parsed) {
            out.push(parsed);
        }
    }
    if out.is_empty() {
        return Err("at least one mode is required".to_string());
    }
    Ok(out)
}

pub fn parse_items_per_producer(raw: Option<&str>) -> Result<Vec<u64>, String> {
    let source = raw
        .map(parse_csv_list)
        .unwrap_or_else(|| vec![DEFAULT_ITEMS_PER_PRODUCER.to_string()]);
    let mut out = Vec::new();
    let mut seen = BTreeSet::new();
    for token in source {
        let parsed = token
            .parse::<u64>()
            .map_err(|_| format!("invalid items_per_producer '{token}'"))?;
        if parsed == 0 {
            return Err("items_per_producer must be > 0".to_string());
        }
        if seen.insert(parsed) {
            out.push(parsed);
        }
    }
    Ok(out)
}

pub fn parse_fastfifo_block_sizes(raw: Option<&str>) -> Result<Vec<usize>, String> {
    let source = raw.map(parse_csv_list).unwrap_or_else(|| {
        DEFAULT_FASTFIFO_BLOCK_SIZES
            .iter()
            .map(|value| value.to_string())
            .collect()
    });
    let mut out = Vec::new();
    let mut seen = BTreeSet::new();
    for token in source {
        let parsed = token
            .parse::<usize>()
            .map_err(|_| format!("invalid RBBQ block size '{token}'"))?;
        if parsed == 0 {
            return Err("RBBQ block sizes must be > 0".to_string());
        }
        if seen.insert(parsed) {
            out.push(parsed);
        }
    }
    if out.is_empty() {
        return Err("at least one RBBQ block size is required".to_string());
    }
    Ok(out)
}

fn fastfifo_queue_label(block_size: usize) -> String {
    format!("fastfifo_{block_size}")
}

pub fn parse_lfqueue_segment_sizes(raw: Option<&str>) -> Result<Vec<usize>, String> {
    let source = raw.map(parse_csv_list).unwrap_or_else(|| {
        DEFAULT_LFQUEUE_SEGMENT_SIZES
            .iter()
            .map(|value| value.to_string())
            .collect()
    });
    let mut out = Vec::new();
    let mut seen = BTreeSet::new();
    for token in source {
        let parsed = token
            .parse::<usize>()
            .map_err(|_| format!("invalid lfqueue segment size '{token}'"))?;
        if parsed == 0 {
            return Err("lfqueue segment sizes must be > 0".to_string());
        }
        if seen.insert(parsed) {
            out.push(parsed);
        }
    }
    if out.is_empty() {
        return Err("at least one lfqueue segment size is required".to_string());
    }
    Ok(out)
}

fn lfqueue_queue_label(segment_size: usize) -> String {
    format!("lfqueue_{segment_size}")
}

pub fn parse_wcq_capacities(raw: Option<&str>) -> Result<Vec<usize>, String> {
    let source = raw.map(parse_csv_list).unwrap_or_else(|| {
        DEFAULT_WCQ_CAPACITIES
            .iter()
            .map(|value| value.to_string())
            .collect()
    });
    let mut out = Vec::new();
    let mut seen = BTreeSet::new();
    for token in source {
        let parsed = token
            .parse::<usize>()
            .map_err(|_| format!("invalid wCQ capacity '{token}'"))?;
        if parsed == 0 {
            return Err("wCQ capacities must be > 0".to_string());
        }
        if !parsed.is_power_of_two() {
            return Err(format!("wCQ capacity '{parsed}' must be a power of two"));
        }
        if !SUPPORTED_WCQ_CAPACITIES.contains(&parsed) {
            return Err(format!(
                "unsupported wCQ capacity '{parsed}'; supported capacities are {}",
                SUPPORTED_WCQ_CAPACITIES
                    .iter()
                    .map(|value| value.to_string())
                    .collect::<Vec<_>>()
                    .join(",")
            ));
        }
        if seen.insert(parsed) {
            out.push(parsed);
        }
    }
    if out.is_empty() {
        return Err("at least one wCQ capacity is required".to_string());
    }
    Ok(out)
}

fn wcq_queue_label(capacity: usize) -> String {
    format!("wcq_{capacity}")
}

fn validate_mode_for_scenario(mode: Mode, scenario: &ScenarioConfig) -> Result<(), String> {
    if mode == Mode::AppLogMpscFile && scenario.consumers != 1 {
        return Err(format!(
            "mode {} requires exactly one consumer, got scenario {}",
            mode.name(),
            scenario.name
        ));
    }
    Ok(())
}

fn wcq_mode_supported(
    mode: Mode,
    capacity: usize,
    scenario: &ScenarioConfig,
    items_per_producer: u64,
) -> bool {
    match mode {
        // wCQ throughput mode is disabled due to two bugs in the wcq crate:
        //
        // 1. `Queue::len()` scans linearly from cell 0 and stops at the first
        //    empty cell.  Once the ring has wrapped (head past slot 0), cell 0
        //    can be empty while cells 1..N-1 hold valid items.  `is_empty()`
        //    then returns true for a non-empty queue.  The dequeue fast-path
        //    uses `!nonempty` to decide whether to short-circuit; with a
        //    spuriously empty-looking queue it advances head past valid items,
        //    permanently losing them, and producers eventually time out.
        //
        // 2. `AtomicPositionPair::compare_exchange` performs two *separate*
        //    atomic CAS operations (one on `entry`, one on `phase2`) with a
        //    non-atomic rollback when the second CAS fails.  Other threads can
        //    observe the inconsistent intermediate state, causing the slow-path
        //    helping mechanism to stall under concurrent producer/consumer load.
        //
        // Fill-drain mode is unaffected: the queue is fully loaded before any
        // consumer runs, so the ring never wraps during the fill phase, and
        // concurrency on head/tail is minimal during the drain phase.
        Mode::Throughput
        | Mode::ComplexThroughput
        | Mode::DataLatency
        | Mode::Fairness
        | Mode::AppLogFanIn
        | Mode::AppPipeline
        | Mode::AppTaskRoundtrip
        | Mode::AppLogMpscFile => false,
        Mode::FillDrain => {
            let Ok(total_items) =
                usize::try_from(total_items(items_per_producer, scenario.producers))
            else {
                return false;
            };
            total_items
                .checked_add(scenario.consumers)
                .is_some_and(|required| required <= capacity)
        }
    }
}

fn baseline_queue_labels_for_sample(
    baseline_queues: &[QueueKind],
    fastfifo_block_sizes: &[usize],
    lfqueue_segment_sizes: &[usize],
    wcq_capacities: &[usize],
    scenario: &ScenarioConfig,
    mode: Mode,
    items_per_producer: u64,
) -> Vec<String> {
    let mut labels = Vec::new();
    for queue in baseline_queues {
        match queue {
            QueueKind::FastFifo => {
                labels.extend(
                    fastfifo_block_sizes
                        .iter()
                        .copied()
                        .map(fastfifo_queue_label),
                );
            }
            QueueKind::LfQueue => {
                labels.extend(
                    lfqueue_segment_sizes
                        .iter()
                        .copied()
                        .map(lfqueue_queue_label),
                );
            }
            QueueKind::Wcq => {
                labels.extend(
                    wcq_capacities
                        .iter()
                        .copied()
                        .filter(|capacity| {
                            wcq_mode_supported(mode, *capacity, scenario, items_per_producer)
                        })
                        .map(wcq_queue_label),
                );
            }
            _ => labels.push(queue.name().to_string()),
        }
    }
    labels
}

pub fn parse_queue_kinds(raw: &str) -> Result<Vec<QueueKind>, String> {
    let mut out = Vec::new();
    let mut seen = BTreeSet::new();
    for token in parse_csv_list(raw) {
        let parsed =
            QueueKind::parse(&token).ok_or_else(|| format!("invalid queue kind '{token}'"))?;
        if seen.insert(parsed) {
            out.push(parsed);
        }
    }
    if out.is_empty() {
        return Err("at least one queue kind is required".to_string());
    }
    Ok(out)
}

pub fn parse_ubq_label(token: &str, require_valid: bool) -> Result<UbqLabel, String> {
    let text = token.trim().to_ascii_lowercase();
    let parts: Vec<&str> = text
        .split(',')
        .filter(|part| !part.trim().is_empty())
        .collect();
    if parts.len() != 4 {
        return Err(format!("invalid UBQ label '{token}'"));
    }
    let label = UbqLabel {
        preset: parts[0].to_string(),
        pool: parts[1]
            .parse::<u8>()
            .map_err(|_| format!("invalid UBQ label '{token}'"))?,
        block: parts[2]
            .parse::<u16>()
            .map_err(|_| format!("invalid UBQ label '{token}'"))?,
        backoff: parts[3].to_string(),
    };
    if require_valid && !is_valid_ubq_label(&label) {
        return Err(format!("invalid UBQ label '{token}'"));
    }
    Ok(label)
}

pub fn is_valid_ubq_label(label: &UbqLabel) -> bool {
    if label.preset != "balanced" {
        return false;
    }
    if !UBQ_BLOCK_VALUES.contains(&label.block) {
        return false;
    }
    if !UBQ_BACKOFF_VALUES.contains(&label.backoff.as_str()) {
        return false;
    }
    UBQ_POOL_VALUES.contains(&label.pool)
}

pub fn is_valid_ubq_label_for_scenario(label: &UbqLabel, scenario: &ScenarioConfig) -> bool {
    if !is_valid_ubq_label(label) {
        return false;
    }
    if usize::from(label.block) < scenario.producers {
        return false;
    }
    true
}

fn validate_ubq_label_for_scenario(
    label: &UbqLabel,
    scenario: &ScenarioConfig,
) -> Result<(), String> {
    if is_valid_ubq_label_for_scenario(label, scenario) {
        return Ok(());
    }
    Err(format!(
        "invalid UBQ label '{}' for scenario {}: block size {} is smaller than producer count {}",
        label.text(),
        scenario.name,
        label.block,
        scenario.producers
    ))
}

pub fn normalize_ubq_label(token: &str, require_valid: bool) -> Option<String> {
    parse_ubq_label(token, require_valid)
        .ok()
        .map(|value| value.text())
}

fn immediate_domain_neighbors_u8(value: u8, domain: &[u8]) -> Vec<u8> {
    if let Some(idx) = domain.iter().position(|candidate| *candidate == value) {
        let mut out = Vec::new();
        if idx > 0 {
            out.push(domain[idx - 1]);
        }
        if idx + 1 < domain.len() {
            out.push(domain[idx + 1]);
        }
        return out;
    }
    Vec::new()
}

fn immediate_domain_neighbors_u16(value: u16, domain: &[u16]) -> Vec<u16> {
    if let Some(idx) = domain.iter().position(|candidate| *candidate == value) {
        let mut out = Vec::new();
        if idx > 0 {
            out.push(domain[idx - 1]);
        }
        if idx + 1 < domain.len() {
            out.push(domain[idx + 1]);
        }
        return out;
    }
    Vec::new()
}

fn immediate_domain_neighbors_str<'a>(value: &str, domain: &'a [&str]) -> Vec<&'a str> {
    if let Some(idx) = domain.iter().position(|candidate| *candidate == value) {
        let mut out = Vec::new();
        if idx > 0 {
            out.push(domain[idx - 1]);
        }
        if idx + 1 < domain.len() {
            out.push(domain[idx + 1]);
        }
        return out;
    }
    Vec::new()
}

fn pool_neighbors(value: u8) -> Vec<u8> {
    let mut out = immediate_domain_neighbors_u8(value, &UBQ_POOL_VALUES);
    if value != 0 && UBQ_POOL_VALUES.contains(&0) && !out.contains(&0) {
        out.push(0);
    }
    out
}

fn immediate_neighbors(label: &UbqLabel, idx: usize) -> Vec<UbqLabel> {
    let mut out = Vec::new();
    match idx {
        0 => {
            for pool in pool_neighbors(label.pool) {
                out.push(UbqLabel {
                    preset: label.preset.clone(),
                    pool,
                    block: label.block,
                    backoff: label.backoff.clone(),
                });
            }
        }
        1 => {
            for block in immediate_domain_neighbors_u16(label.block, &UBQ_BLOCK_VALUES) {
                out.push(UbqLabel {
                    preset: label.preset.clone(),
                    pool: label.pool,
                    block,
                    backoff: label.backoff.clone(),
                });
            }
        }
        2 => {
            for backoff in immediate_domain_neighbors_str(&label.backoff, &UBQ_BACKOFF_VALUES) {
                out.push(UbqLabel {
                    preset: label.preset.clone(),
                    pool: label.pool,
                    block: label.block,
                    backoff: backoff.to_string(),
                });
            }
        }
        _ => {}
    }
    out
}

fn required_ubq_labels_for_center(label: &UbqLabel) -> BTreeSet<UbqLabel> {
    let mut required = BTreeSet::new();
    required.insert(label.clone());

    for idx in 0..3 {
        for candidate in immediate_neighbors(label, idx) {
            if is_valid_ubq_label(&candidate) {
                required.insert(candidate);
            }
        }
    }

    required
}

pub fn immediate_search_labels(label: &str) -> Result<BTreeSet<String>, String> {
    let parsed = parse_ubq_label(label, true)?;
    Ok(required_ubq_labels_for_center(&parsed)
        .into_iter()
        .map(|candidate| candidate.text())
        .collect())
}

pub fn immediate_search_labels_for_scenario(
    label: &str,
    scenario: &ScenarioConfig,
) -> Result<BTreeSet<String>, String> {
    let parsed = parse_ubq_label(label, true)?;
    validate_ubq_label_for_scenario(&parsed, scenario)?;
    Ok(required_ubq_labels_for_center(&parsed)
        .into_iter()
        .filter(|candidate| is_valid_ubq_label_for_scenario(candidate, scenario))
        .map(|candidate| candidate.text())
        .collect())
}

pub fn build_direct_matrix_plan(
    machine_label: &str,
    runs_dir: PathBuf,
    available_parallelism: usize,
    selected_queues: &[QueueKind],
    ubq_labels: &[String],
    fastfifo_block_sizes: &[usize],
    lfqueue_segment_sizes: &[usize],
    wcq_capacities: &[usize],
    scenarios: &[ScenarioConfig],
    modes: &[Mode],
    items_per_producer_values: &[u64],
    repeats: usize,
    reuse_existing: bool,
) -> Result<MatrixPlan, String> {
    if machine_label.trim().is_empty() {
        return Err("machine_label is required".to_string());
    }
    if available_parallelism == 0 {
        return Err("available_parallelism must be > 0".to_string());
    }
    let baseline_queues: Vec<QueueKind> = selected_queues
        .iter()
        .copied()
        .filter(|queue| queue.is_baseline())
        .collect();
    let include_ubq = selected_queues.iter().any(|queue| *queue == QueueKind::Ubq);
    let include_fastfifo = selected_queues
        .iter()
        .any(|queue| *queue == QueueKind::FastFifo);
    let include_lfqueue = selected_queues
        .iter()
        .any(|queue| *queue == QueueKind::LfQueue);
    let include_wcq = selected_queues.iter().any(|queue| *queue == QueueKind::Wcq);
    if include_ubq && ubq_labels.is_empty() {
        return Err("at least one --ubq-label is required when queue set includes ubq".to_string());
    }
    if include_fastfifo && fastfifo_block_sizes.is_empty() {
        return Err(
            "at least one --fastfifo-block-sizes/--rbbq-block-sizes value is required when queue set includes rbbq"
                .to_string(),
        );
    }
    if include_lfqueue && lfqueue_segment_sizes.is_empty() {
        return Err(
            "at least one --lfqueue-segment-sizes value is required when queue set includes lfqueue"
                .to_string(),
        );
    }
    if include_wcq && wcq_capacities.is_empty() {
        return Err(
            "at least one --wcq-capacities value is required when queue set includes wcq"
                .to_string(),
        );
    }
    for &block_size in fastfifo_block_sizes {
        if block_size == 0 {
            return Err("RBBQ block sizes must be > 0".to_string());
        }
    }
    for &segment_size in lfqueue_segment_sizes {
        if segment_size == 0 {
            return Err("lfqueue segment sizes must be > 0".to_string());
        }
    }
    for &capacity in wcq_capacities {
        if capacity == 0 {
            return Err("wCQ capacities must be > 0".to_string());
        }
        if !capacity.is_power_of_two() {
            return Err(format!("wCQ capacity '{capacity}' must be a power of two"));
        }
        if !SUPPORTED_WCQ_CAPACITIES.contains(&capacity) {
            return Err(format!(
                "unsupported wCQ capacity '{capacity}'; supported capacities are {}",
                SUPPORTED_WCQ_CAPACITIES
                    .iter()
                    .map(|value| value.to_string())
                    .collect::<Vec<_>>()
                    .join(",")
            ));
        }
    }
    let normalized_fastfifo_block_sizes = if include_fastfifo {
        let mut out = Vec::new();
        let mut seen = BTreeSet::new();
        for &block_size in fastfifo_block_sizes {
            if seen.insert(block_size) {
                out.push(block_size);
            }
        }
        out
    } else {
        Vec::new()
    };
    let normalized_lfqueue_segment_sizes = if include_lfqueue {
        let mut out = Vec::new();
        let mut seen = BTreeSet::new();
        for &segment_size in lfqueue_segment_sizes {
            if seen.insert(segment_size) {
                out.push(segment_size);
            }
        }
        out
    } else {
        Vec::new()
    };
    let normalized_wcq_capacities = if include_wcq {
        let mut out = Vec::new();
        let mut seen = BTreeSet::new();
        for &capacity in wcq_capacities {
            if seen.insert(capacity) {
                out.push(capacity);
            }
        }
        out
    } else {
        Vec::new()
    };
    let parsed_ubq_labels = if include_ubq {
        let mut parsed = Vec::with_capacity(ubq_labels.len());
        for label in ubq_labels {
            parsed.push(parse_ubq_label(label, true)?);
        }
        parsed
    } else {
        Vec::new()
    };
    let normalized_ubq_labels: Vec<String> =
        parsed_ubq_labels.iter().map(|label| label.text()).collect();
    for scenario in scenarios {
        if scenario.total_threads() > available_parallelism {
            return Err(format!(
                "scenario {} requires {} threads but available_parallelism is {}",
                scenario.name,
                scenario.total_threads(),
                available_parallelism
            ));
        }
        if include_wcq && scenario.total_threads().saturating_add(1) > WCQ_MAX_THREADS {
            return Err(format!(
                "scenario {} requires {} wCQ thread handles including sentinel sender but \
                 this harness supports {}",
                scenario.name,
                scenario.total_threads() + 1,
                WCQ_MAX_THREADS
            ));
        }
        for &mode in modes {
            validate_mode_for_scenario(mode, scenario)?;
            let required_threads = scenario
                .total_threads()
                .checked_add(mode.extra_threads())
                .ok_or_else(|| "scenario thread count overflow".to_string())?;
            if required_threads > available_parallelism {
                return Err(format!(
                    "scenario {} mode {} requires {} threads but available_parallelism is {}",
                    scenario.name,
                    mode.name(),
                    required_threads,
                    available_parallelism
                ));
            }
        }
    }
    if repeats == 0 {
        return Err("repeats must be > 0".to_string());
    }

    let mut bundles = Vec::new();
    for scenario in scenarios {
        for repeat_index in 1..=repeats {
            if include_ubq {
                for (normalized, parsed) in
                    normalized_ubq_labels.iter().zip(parsed_ubq_labels.iter())
                {
                    if !is_valid_ubq_label_for_scenario(parsed, scenario) {
                        continue;
                    }
                    bundles.push(PlanBundle {
                        scenario: scenario.clone(),
                        repeat_index,
                        ubq_label: Some(normalized.clone()),
                        modes: modes.to_vec(),
                        items_per_producer_values: items_per_producer_values.to_vec(),
                    });
                }
            } else {
                bundles.push(PlanBundle {
                    scenario: scenario.clone(),
                    repeat_index,
                    ubq_label: None,
                    modes: modes.to_vec(),
                    items_per_producer_values: items_per_producer_values.to_vec(),
                });
            }
        }
    }

    Ok(MatrixPlan {
        plan_schema_version: PLAN_SCHEMA_VERSION,
        machine_label: normalize_machine(machine_label),
        runs_dir,
        available_parallelism,
        baseline_queues,
        fastfifo_block_sizes: normalized_fastfifo_block_sizes,
        lfqueue_segment_sizes: normalized_lfqueue_segment_sizes,
        wcq_capacities: normalized_wcq_capacities,
        ubq_grid: None,
        ubq_batch_sizes: Vec::new(),
        planned_repeats: repeats,
        bundles,
        reuse_existing,
    })
}

pub fn build_grid_matrix_plan(
    machine_label: &str,
    runs_dir: PathBuf,
    available_parallelism: usize,
    selected_queues: &[QueueKind],
    grid: UbqGrid,
    batch_sizes: &[usize],
    fastfifo_block_sizes: &[usize],
    lfqueue_segment_sizes: &[usize],
    wcq_capacities: &[usize],
    scenarios: &[ScenarioConfig],
    modes: &[Mode],
    items_per_producer_values: &[u64],
    repeats: usize,
    reuse_existing: bool,
) -> Result<MatrixPlan, String> {
    let mut normalized_batch_sizes = Vec::with_capacity(batch_sizes.len());
    let mut seen = BTreeSet::new();
    for &batch_size in batch_sizes {
        if batch_size < 2 {
            return Err(
                "UBQ batch sizes must be >= 2; scalar push is measured separately".to_string(),
            );
        }
        if seen.insert(batch_size) {
            normalized_batch_sizes.push(batch_size);
        }
    }
    let labels = grid.labels();
    let mut plan = build_direct_matrix_plan(
        machine_label,
        runs_dir,
        available_parallelism,
        selected_queues,
        &labels,
        fastfifo_block_sizes,
        lfqueue_segment_sizes,
        wcq_capacities,
        scenarios,
        modes,
        items_per_producer_values,
        repeats,
        reuse_existing,
    )?;
    plan.ubq_grid = Some(grid);
    plan.ubq_batch_sizes = normalized_batch_sizes;
    plan.planned_repeats = repeats;
    if !plan.baseline_queues.is_empty() {
        for scenario in scenarios {
            for repeat_index in 1..=repeats {
                plan.bundles.push(PlanBundle {
                    scenario: scenario.clone(),
                    repeat_index,
                    ubq_label: None,
                    modes: modes.to_vec(),
                    items_per_producer_values: items_per_producer_values.to_vec(),
                });
            }
        }
    }
    Ok(plan)
}

pub fn parse_embedded_plan(raw: &str) -> Result<MatrixPlan, String> {
    let plan: MatrixPlan =
        serde_json::from_str(raw).map_err(|err| format!("invalid embedded matrix plan: {err}"))?;
    if plan.plan_schema_version != PLAN_SCHEMA_VERSION {
        return Err(format!(
            "unsupported plan schema version: {}",
            plan.plan_schema_version
        ));
    }
    Ok(plan)
}

pub fn load_existing_runs(
    runs_dir: &Path,
    machine_label: &str,
) -> Result<ExistingRunsIndex, String> {
    let mut files = Vec::new();
    collect_run_jsons_recursive(runs_dir, &mut files)?;
    files.sort();
    let machine_label = normalize_machine(machine_label);
    let mut index = ExistingRunsIndex::default();

    for path in files {
        let raw = match fs::read_to_string(&path) {
            Ok(value) => value,
            Err(_) => continue,
        };
        let parsed: OutputFile = match serde_json::from_str(&raw) {
            Ok(value) => value,
            Err(_) => continue,
        };
        if parsed.schema_version != RUN_SCHEMA_VERSION {
            continue;
        }
        if normalize_machine(&parsed.meta.machine_label) != machine_label {
            continue;
        }
        for record in parsed.results {
            if !record.completed() {
                continue;
            }
            let queue_label = if record.queue == "ubq" {
                match parsed.meta.ubq_label.as_deref() {
                    Some(label) => format!("ubq_{label}"),
                    None => continue,
                }
            } else {
                record.queue.clone()
            };
            let key = SampleKey {
                scenario: parsed.meta.scenario.clone(),
                repeat_index: parsed.meta.repeat_index,
                mode: Mode::parse(&record.mode).unwrap_or(Mode::Throughput),
                items_per_producer: record.items_per_producer,
                queue_label,
                batch_size: record.batch_size,
            };
            index.records.entry(key).or_insert(record);
        }
    }

    Ok(index)
}

fn collect_run_jsons_recursive(dir: &Path, files: &mut Vec<PathBuf>) -> Result<(), String> {
    if !dir.exists() {
        return Ok(());
    }
    let entries = fs::read_dir(dir)
        .map_err(|err| format!("failed to read runs dir {}: {err}", dir.display()))?;
    for entry in entries {
        let entry = entry.map_err(|err| format!("failed to read runs dir entry: {err}"))?;
        let path = entry.path();
        if path.is_dir() {
            collect_run_jsons_recursive(&path, files)?;
            continue;
        }
        if path.is_file() && path.extension() == Some(OsStr::new("json")) {
            files.push(path);
        }
    }
    Ok(())
}

fn required_job_specs(plan: &MatrixPlan) -> BTreeSet<JobSpec> {
    let mut out = BTreeSet::new();
    for bundle in &plan.bundles {
        for mode in &bundle.modes {
            for &items_per_producer in &bundle.items_per_producer_values {
                for &baseline_queue in &plan.baseline_queues {
                    match baseline_queue {
                        QueueKind::FastFifo => {
                            for &block_size in &plan.fastfifo_block_sizes {
                                out.insert(JobSpec {
                                    scenario: bundle.scenario.clone(),
                                    repeat_index: bundle.repeat_index,
                                    mode: *mode,
                                    items_per_producer,
                                    queue: baseline_queue,
                                    ubq_label: None,
                                    batch_size: None,
                                    fastfifo_block_size: Some(block_size),
                                    lfqueue_segment_size: None,
                                    wcq_capacity: None,
                                });
                            }
                        }
                        QueueKind::LfQueue => {
                            for &segment_size in &plan.lfqueue_segment_sizes {
                                out.insert(JobSpec {
                                    scenario: bundle.scenario.clone(),
                                    repeat_index: bundle.repeat_index,
                                    mode: *mode,
                                    items_per_producer,
                                    queue: baseline_queue,
                                    ubq_label: None,
                                    batch_size: None,
                                    fastfifo_block_size: None,
                                    lfqueue_segment_size: Some(segment_size),
                                    wcq_capacity: None,
                                });
                            }
                        }
                        QueueKind::Wcq => {
                            for &capacity in &plan.wcq_capacities {
                                if wcq_mode_supported(
                                    *mode,
                                    capacity,
                                    &bundle.scenario,
                                    items_per_producer,
                                ) {
                                    out.insert(JobSpec {
                                        scenario: bundle.scenario.clone(),
                                        repeat_index: bundle.repeat_index,
                                        mode: *mode,
                                        items_per_producer,
                                        queue: baseline_queue,
                                        ubq_label: None,
                                        batch_size: None,
                                        fastfifo_block_size: None,
                                        lfqueue_segment_size: None,
                                        wcq_capacity: Some(capacity),
                                    });
                                }
                            }
                        }
                        _ => {
                            out.insert(JobSpec {
                                scenario: bundle.scenario.clone(),
                                repeat_index: bundle.repeat_index,
                                mode: *mode,
                                items_per_producer,
                                queue: baseline_queue,
                                ubq_label: None,
                                batch_size: None,
                                fastfifo_block_size: None,
                                lfqueue_segment_size: None,
                                wcq_capacity: None,
                            });
                        }
                    }
                }
                if let Some(label) = bundle.ubq_label.as_ref() {
                    out.insert(JobSpec {
                        scenario: bundle.scenario.clone(),
                        repeat_index: bundle.repeat_index,
                        mode: *mode,
                        items_per_producer,
                        queue: QueueKind::Ubq,
                        ubq_label: Some(label.clone()),
                        batch_size: None,
                        fastfifo_block_size: None,
                        lfqueue_segment_size: None,
                        wcq_capacity: None,
                    });
                    if *mode == Mode::Throughput {
                        for &batch_size in &plan.ubq_batch_sizes {
                            out.insert(JobSpec {
                                scenario: bundle.scenario.clone(),
                                repeat_index: bundle.repeat_index,
                                mode: *mode,
                                items_per_producer,
                                queue: QueueKind::Ubq,
                                ubq_label: Some(label.clone()),
                                batch_size: Some(batch_size),
                                fastfifo_block_size: None,
                                lfqueue_segment_size: None,
                                wcq_capacity: None,
                            });
                        }
                    }
                }
            }
        }
    }
    out
}

pub fn make_ubq_job_factory<Q: BenchQueue, L: LogQueue>(
    label: &str,
    scenario: ScenarioConfig,
    repeat_index: usize,
    mode: Mode,
    items_per_producer: u64,
    batch_size: Option<usize>,
) -> JobFactory {
    let parsed = parse_ubq_label(label, true).expect("valid UBQ label");
    let normalized = parsed.text();
    validate_ubq_label_for_scenario(&parsed, &scenario).unwrap_or_else(|err| panic!("{err}"));
    let spec = JobSpec {
        scenario: scenario.clone(),
        repeat_index,
        mode,
        items_per_producer,
        queue: QueueKind::Ubq,
        ubq_label: Some(normalized),
        batch_size,
        fastfifo_block_size: None,
        lfqueue_segment_size: None,
        wcq_capacity: None,
    };
    let queue_name = "ubq".to_string();
    let run_scenario = scenario.clone();
    let run = Arc::new(move |core_offset: usize| match mode {
        Mode::Throughput => match batch_size {
            Some(batch_size) => bench_throughput_batched_for::<Q>(
                &queue_name,
                &run_scenario,
                items_per_producer,
                batch_size,
                core_offset,
            ),
            None => bench_throughput_for::<Q>(
                &queue_name,
                &run_scenario,
                items_per_producer,
                core_offset,
            ),
        },
        Mode::ComplexThroughput => bench_complex_throughput_for::<Q>(
            &queue_name,
            &run_scenario,
            items_per_producer,
            core_offset,
        ),
        Mode::DataLatency => {
            bench_data_latency_for::<Q>(&queue_name, &run_scenario, items_per_producer, core_offset)
        }
        Mode::Fairness => {
            bench_fairness_for::<Q>(&queue_name, &run_scenario, items_per_producer, core_offset)
        }
        Mode::FillDrain => {
            bench_fill_drain_for::<Q>(&queue_name, &run_scenario, items_per_producer, core_offset)
        }
        Mode::AppLogFanIn => bench_app_log_fan_in_for::<Q>(
            &queue_name,
            &run_scenario,
            items_per_producer,
            core_offset,
        ),
        Mode::AppPipeline => {
            bench_app_pipeline_for::<Q>(&queue_name, &run_scenario, items_per_producer, core_offset)
        }
        Mode::AppTaskRoundtrip => bench_app_task_roundtrip_for::<Q>(
            &queue_name,
            &run_scenario,
            items_per_producer,
            core_offset,
        ),
        Mode::AppLogMpscFile => bench_app_log_mpsc_file_for::<L>(
            &queue_name,
            &run_scenario,
            items_per_producer,
            core_offset,
        ),
    });
    JobFactory { spec, run }
}

pub fn make_segqueue_job_factory(
    scenario: ScenarioConfig,
    repeat_index: usize,
    mode: Mode,
    items_per_producer: u64,
) -> JobFactory {
    let spec = JobSpec {
        scenario: scenario.clone(),
        repeat_index,
        mode,
        items_per_producer,
        queue: QueueKind::SegQueue,
        ubq_label: None,
        batch_size: None,
        fastfifo_block_size: None,
        lfqueue_segment_size: None,
        wcq_capacity: None,
    };
    let queue_name = QueueKind::SegQueue.name().to_string();
    let run_scenario = scenario.clone();
    let run = Arc::new(move |core_offset: usize| match mode {
        Mode::Throughput => bench_throughput_for::<SegQueue<u64>>(
            &queue_name,
            &run_scenario,
            items_per_producer,
            core_offset,
        ),
        Mode::ComplexThroughput => bench_complex_throughput_for::<SegQueue<u64>>(
            &queue_name,
            &run_scenario,
            items_per_producer,
            core_offset,
        ),
        Mode::DataLatency => bench_data_latency_for::<SegQueue<u64>>(
            &queue_name,
            &run_scenario,
            items_per_producer,
            core_offset,
        ),
        Mode::Fairness => bench_fairness_for::<SegQueue<u64>>(
            &queue_name,
            &run_scenario,
            items_per_producer,
            core_offset,
        ),
        Mode::FillDrain => bench_fill_drain_for::<SegQueue<u64>>(
            &queue_name,
            &run_scenario,
            items_per_producer,
            core_offset,
        ),
        Mode::AppLogFanIn => bench_app_log_fan_in_for::<SegQueue<u64>>(
            &queue_name,
            &run_scenario,
            items_per_producer,
            core_offset,
        ),
        Mode::AppPipeline => bench_app_pipeline_for::<SegQueue<u64>>(
            &queue_name,
            &run_scenario,
            items_per_producer,
            core_offset,
        ),
        Mode::AppTaskRoundtrip => bench_app_task_roundtrip_for::<SegQueue<u64>>(
            &queue_name,
            &run_scenario,
            items_per_producer,
            core_offset,
        ),
        Mode::AppLogMpscFile => bench_app_log_mpsc_file_for::<SegQueue<LogRecord>>(
            &queue_name,
            &run_scenario,
            items_per_producer,
            core_offset,
        ),
    });
    JobFactory { spec, run }
}

pub fn make_concurrent_queue_job_factory(
    scenario: ScenarioConfig,
    repeat_index: usize,
    mode: Mode,
    items_per_producer: u64,
) -> JobFactory {
    let spec = JobSpec {
        scenario: scenario.clone(),
        repeat_index,
        mode,
        items_per_producer,
        queue: QueueKind::ConcurrentQueue,
        ubq_label: None,
        batch_size: None,
        fastfifo_block_size: None,
        lfqueue_segment_size: None,
        wcq_capacity: None,
    };
    let queue_name = QueueKind::ConcurrentQueue.name().to_string();
    let run_scenario = scenario.clone();
    let run = Arc::new(move |core_offset: usize| match mode {
        Mode::Throughput => bench_throughput_for::<ConcurrentQueue<u64>>(
            &queue_name,
            &run_scenario,
            items_per_producer,
            core_offset,
        ),
        Mode::ComplexThroughput => bench_complex_throughput_for::<ConcurrentQueue<u64>>(
            &queue_name,
            &run_scenario,
            items_per_producer,
            core_offset,
        ),
        Mode::DataLatency => bench_data_latency_for::<ConcurrentQueue<u64>>(
            &queue_name,
            &run_scenario,
            items_per_producer,
            core_offset,
        ),
        Mode::Fairness => bench_fairness_for::<ConcurrentQueue<u64>>(
            &queue_name,
            &run_scenario,
            items_per_producer,
            core_offset,
        ),
        Mode::FillDrain => bench_fill_drain_for::<ConcurrentQueue<u64>>(
            &queue_name,
            &run_scenario,
            items_per_producer,
            core_offset,
        ),
        Mode::AppLogFanIn => bench_app_log_fan_in_for::<ConcurrentQueue<u64>>(
            &queue_name,
            &run_scenario,
            items_per_producer,
            core_offset,
        ),
        Mode::AppPipeline => bench_app_pipeline_for::<ConcurrentQueue<u64>>(
            &queue_name,
            &run_scenario,
            items_per_producer,
            core_offset,
        ),
        Mode::AppTaskRoundtrip => bench_app_task_roundtrip_for::<ConcurrentQueue<u64>>(
            &queue_name,
            &run_scenario,
            items_per_producer,
            core_offset,
        ),
        Mode::AppLogMpscFile => bench_app_log_mpsc_file_for::<ConcurrentQueue<LogRecord>>(
            &queue_name,
            &run_scenario,
            items_per_producer,
            core_offset,
        ),
    });
    JobFactory { spec, run }
}

#[cfg(feature = "bench_lfqueue")]
pub fn make_lfqueue_job_factory(
    segment_size: usize,
    scenario: ScenarioConfig,
    repeat_index: usize,
    mode: Mode,
    items_per_producer: u64,
) -> JobFactory {
    assert!(segment_size > 0, "lfqueue segment size must be > 0");
    let spec = JobSpec {
        scenario: scenario.clone(),
        repeat_index,
        mode,
        items_per_producer,
        queue: QueueKind::LfQueue,
        ubq_label: None,
        batch_size: None,
        fastfifo_block_size: None,
        lfqueue_segment_size: Some(segment_size),
        wcq_capacity: None,
    };
    let queue_name = lfqueue_queue_label(segment_size);
    let run_scenario = scenario.clone();
    let run = Arc::new(move |core_offset: usize| {
        let queue_handle = LfQueueBenchQueue::new(segment_size);
        match mode {
            Mode::Throughput => bench_throughput_with_queue(
                queue_handle,
                &queue_name,
                &run_scenario,
                items_per_producer,
                core_offset,
            ),
            Mode::ComplexThroughput => bench_complex_throughput_with_queue(
                queue_handle,
                &queue_name,
                &run_scenario,
                items_per_producer,
                core_offset,
            ),
            Mode::DataLatency => bench_data_latency_with_queue(
                queue_handle,
                &queue_name,
                &run_scenario,
                items_per_producer,
                core_offset,
            ),
            Mode::Fairness => bench_fairness_with_queue(
                queue_handle,
                &queue_name,
                &run_scenario,
                items_per_producer,
                core_offset,
            ),
            Mode::FillDrain => bench_fill_drain_with_queue(
                queue_handle,
                &queue_name,
                &run_scenario,
                items_per_producer,
                core_offset,
            ),
            Mode::AppLogFanIn => bench_app_log_fan_in_with_queue(
                queue_handle,
                &queue_name,
                &run_scenario,
                items_per_producer,
                core_offset,
            ),
            Mode::AppPipeline => bench_app_pipeline_with_queues(
                queue_handle,
                LfQueueBenchQueue::new(segment_size),
                &queue_name,
                &run_scenario,
                items_per_producer,
                core_offset,
            ),
            Mode::AppTaskRoundtrip => bench_app_task_roundtrip_with_queues(
                queue_handle,
                LfQueueBenchQueue::new(segment_size),
                &queue_name,
                &run_scenario,
                items_per_producer,
                core_offset,
            ),
            Mode::AppLogMpscFile => bench_app_log_mpsc_file_with_queue(
                LogLfQueueBenchQueue::new(segment_size),
                &queue_name,
                &run_scenario,
                items_per_producer,
                core_offset,
            ),
        }
    });
    JobFactory { spec, run }
}

#[cfg(feature = "bench_wcq")]
fn make_wcq_job_factory_typed<const CAPACITY: usize>(
    scenario: ScenarioConfig,
    repeat_index: usize,
    mode: Mode,
    items_per_producer: u64,
) -> JobFactory {
    let spec = JobSpec {
        scenario: scenario.clone(),
        repeat_index,
        mode,
        items_per_producer,
        queue: QueueKind::Wcq,
        ubq_label: None,
        batch_size: None,
        fastfifo_block_size: None,
        lfqueue_segment_size: None,
        wcq_capacity: Some(CAPACITY),
    };
    let queue_name = wcq_queue_label(CAPACITY);
    let run_scenario = scenario.clone();
    let run = Arc::new(move |core_offset: usize| {
        let queue_handle = WcqBenchQueue::<CAPACITY>::new();
        match mode {
            Mode::Throughput => bench_throughput_with_queue(
                queue_handle,
                &queue_name,
                &run_scenario,
                items_per_producer,
                core_offset,
            ),
            Mode::ComplexThroughput => bench_complex_throughput_with_queue(
                queue_handle,
                &queue_name,
                &run_scenario,
                items_per_producer,
                core_offset,
            ),
            Mode::DataLatency => bench_data_latency_with_queue(
                queue_handle,
                &queue_name,
                &run_scenario,
                items_per_producer,
                core_offset,
            ),
            Mode::Fairness => bench_fairness_with_queue(
                queue_handle,
                &queue_name,
                &run_scenario,
                items_per_producer,
                core_offset,
            ),
            Mode::FillDrain => bench_fill_drain_with_queue(
                queue_handle,
                &queue_name,
                &run_scenario,
                items_per_producer,
                core_offset,
            ),
            Mode::AppLogFanIn => bench_app_log_fan_in_with_queue(
                queue_handle,
                &queue_name,
                &run_scenario,
                items_per_producer,
                core_offset,
            ),
            Mode::AppPipeline => bench_app_pipeline_with_queues(
                queue_handle,
                WcqBenchQueue::<CAPACITY>::new(),
                &queue_name,
                &run_scenario,
                items_per_producer,
                core_offset,
            ),
            Mode::AppTaskRoundtrip => bench_app_task_roundtrip_with_queues(
                queue_handle,
                WcqBenchQueue::<CAPACITY>::new(),
                &queue_name,
                &run_scenario,
                items_per_producer,
                core_offset,
            ),
            Mode::AppLogMpscFile => failed_runtime_bench_record(
                &queue_name,
                Mode::AppLogMpscFile,
                &run_scenario,
                items_per_producer,
                "wCQ does not support app_log_mpsc_file in this harness".to_string(),
                0,
            ),
        }
    });
    JobFactory { spec, run }
}

#[cfg(feature = "bench_wcq")]
pub fn make_wcq_job_factory(
    capacity: usize,
    scenario: ScenarioConfig,
    repeat_index: usize,
    mode: Mode,
    items_per_producer: u64,
) -> Option<JobFactory> {
    match capacity {
        256 => Some(make_wcq_job_factory_typed::<256>(
            scenario,
            repeat_index,
            mode,
            items_per_producer,
        )),
        1024 => Some(make_wcq_job_factory_typed::<1024>(
            scenario,
            repeat_index,
            mode,
            items_per_producer,
        )),
        4096 => Some(make_wcq_job_factory_typed::<4096>(
            scenario,
            repeat_index,
            mode,
            items_per_producer,
        )),
        16384 => Some(make_wcq_job_factory_typed::<16384>(
            scenario,
            repeat_index,
            mode,
            items_per_producer,
        )),
        65536 => Some(make_wcq_job_factory_typed::<65536>(
            scenario,
            repeat_index,
            mode,
            items_per_producer,
        )),
        262144 => Some(make_wcq_job_factory_typed::<262144>(
            scenario,
            repeat_index,
            mode,
            items_per_producer,
        )),
        1048576 => Some(make_wcq_job_factory_typed::<1048576>(
            scenario,
            repeat_index,
            mode,
            items_per_producer,
        )),
        4194304 => Some(make_wcq_job_factory_typed::<4194304>(
            scenario,
            repeat_index,
            mode,
            items_per_producer,
        )),
        _ => None,
    }
}

#[cfg(feature = "bench_fastfifo")]
pub fn make_fastfifo_job_factory(
    block_size: usize,
    scenario: ScenarioConfig,
    repeat_index: usize,
    mode: Mode,
    items_per_producer: u64,
) -> JobFactory {
    assert!(block_size > 0, "RBBQ block size must be > 0");
    let spec = JobSpec {
        scenario: scenario.clone(),
        repeat_index,
        mode,
        items_per_producer,
        queue: QueueKind::FastFifo,
        ubq_label: None,
        batch_size: None,
        fastfifo_block_size: Some(block_size),
        lfqueue_segment_size: None,
        wcq_capacity: None,
    };
    let queue_name = fastfifo_queue_label(block_size);
    let run_scenario = scenario.clone();
    let run = Arc::new(move |core_offset: usize| {
        let queue_handle = RbbqBenchQueue::new(&run_scenario, items_per_producer, block_size);
        match mode {
            Mode::Throughput => bench_throughput_with_queue(
                queue_handle,
                &queue_name,
                &run_scenario,
                items_per_producer,
                core_offset,
            ),
            Mode::ComplexThroughput => bench_complex_throughput_with_queue(
                queue_handle,
                &queue_name,
                &run_scenario,
                items_per_producer,
                core_offset,
            ),
            Mode::DataLatency => bench_data_latency_with_queue(
                queue_handle,
                &queue_name,
                &run_scenario,
                items_per_producer,
                core_offset,
            ),
            Mode::Fairness => bench_fairness_with_queue(
                queue_handle,
                &queue_name,
                &run_scenario,
                items_per_producer,
                core_offset,
            ),
            Mode::FillDrain => bench_fill_drain_with_queue(
                queue_handle,
                &queue_name,
                &run_scenario,
                items_per_producer,
                core_offset,
            ),
            Mode::AppLogFanIn => bench_app_log_fan_in_with_queue(
                queue_handle,
                &queue_name,
                &run_scenario,
                items_per_producer,
                core_offset,
            ),
            Mode::AppPipeline => bench_app_pipeline_with_queues(
                queue_handle,
                RbbqBenchQueue::new(&run_scenario, items_per_producer, block_size),
                &queue_name,
                &run_scenario,
                items_per_producer,
                core_offset,
            ),
            Mode::AppTaskRoundtrip => bench_app_task_roundtrip_with_queues(
                queue_handle,
                RbbqBenchQueue::new(&run_scenario, items_per_producer, block_size),
                &queue_name,
                &run_scenario,
                items_per_producer,
                core_offset,
            ),
            Mode::AppLogMpscFile => bench_app_log_mpsc_file_with_queue(
                LogRbbqBenchQueue::new(&run_scenario, items_per_producer, block_size),
                &queue_name,
                &run_scenario,
                items_per_producer,
                core_offset,
            ),
        }
    });
    JobFactory { spec, run }
}

pub fn run_embedded_scheduler(plan: MatrixPlan, factories: Vec<JobFactory>) -> Result<(), String> {
    let mut required = required_job_specs(&plan);
    let mut factory_by_key: BTreeMap<SampleKey, JobFactory> = BTreeMap::new();
    for factory in factories {
        let key = SampleKey::from_job(&factory.spec);
        if required.contains(&factory.spec) {
            factory_by_key.insert(key, factory);
        }
    }

    let mut cache = if plan.reuse_existing {
        load_existing_runs(&plan.runs_dir, &plan.machine_label)?
    } else {
        ExistingRunsIndex::default()
    };

    let mut pending = Vec::new();
    for spec in required.iter() {
        let key = SampleKey::from_job(spec);
        if cache.records.contains_key(&key) {
            continue;
        }
        let factory = factory_by_key
            .remove(&key)
            .ok_or_else(|| format!("missing generated job factory for {}", spec.queue_label()))?;
        pending.push(factory);
    }

    progress_line(format!(
        "scheduler: {} bundle(s), {} required sample(s), {} cached, {} pending",
        plan.bundles.len(),
        required.len(),
        required.len().saturating_sub(pending.len()),
        pending.len()
    ));
    let (executed, crashed_job) =
        execute_job_factories(&plan, &cache, pending, plan.available_parallelism)?;
    cache.records.extend(executed);
    required.clear();
    if let Some((queue_label, scenario)) = crashed_job {
        return Err(format!(
            "scheduler crashed while running ({queue_label}, scenario={scenario})"
        ));
    }
    Ok(())
}

fn execute_job_factories(
    plan: &MatrixPlan,
    cache: &ExistingRunsIndex,
    pending: Vec<JobFactory>,
    available_parallelism: usize,
) -> Result<(BTreeMap<SampleKey, BenchRecord>, Option<(String, String)>), String> {
    execute_job_factories_with_timeout(
        plan,
        cache,
        pending,
        available_parallelism,
        bench_job_timeout(),
    )
}

fn execute_job_factories_with_timeout(
    plan: &MatrixPlan,
    cache: &ExistingRunsIndex,
    mut pending: Vec<JobFactory>,
    available_parallelism: usize,
    job_timeout: Duration,
) -> Result<(BTreeMap<SampleKey, BenchRecord>, Option<(String, String)>), String> {
    for job in &pending {
        if job.spec.thread_budget() > available_parallelism {
            return Err(format!(
                "job {} requires {} threads but available_parallelism is {}",
                job.spec.queue_label(),
                job.spec.thread_budget(),
                available_parallelism
            ));
        }
    }

    pending.sort_by(|lhs, rhs| lhs.spec.sort_key().cmp(&rhs.spec.sort_key()));
    let required_specs = required_job_specs(plan);
    let total_jobs = required_specs.len();
    let progress_layout = ProgressLayout::new(required_specs.iter(), available_parallelism);
    let initially_complete = total_jobs.saturating_sub(pending.len());
    progress_line(format!(
        "scheduler: starting {} pending benchmark job(s); {}/{} ({:.2}%) already complete; thread budget {}",
        pending.len(),
        initially_complete,
        total_jobs,
        completion_percent(initially_complete, total_jobs),
        available_parallelism
    ));
    let writer = OutputWriterHandle::start(plan, cache)?;
    if pending.is_empty() {
        writer.close(true)?;
        return Ok((BTreeMap::new(), None));
    }
    progress_layout.print_header();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|err| format!("failed to build scheduler runtime: {err}"))?;

    // Each task returns (key, record, budget). Panics and timeouts become
    // failed records so one bad queue does not abort the whole batch.
    let execution_result: Result<
        (BTreeMap<SampleKey, BenchRecord>, Option<(String, String)>),
        String,
    > = runtime.block_on(async {
        let mut results = BTreeMap::new();
        let mut running: JoinSet<Result<(SampleKey, BenchRecord, usize), String>> = JoinSet::new();
        let mut used_threads = 0_usize;
        let mut completed = initially_complete;

        while !pending.is_empty() || !running.is_empty() {
            let mut started = false;
            loop {
                let Some(index) = pending
                    .iter()
                    .position(|job| can_start_job(&job.spec, used_threads, available_parallelism))
                else {
                    break;
                };
                let job = pending.remove(index);
                let key = SampleKey::from_job(&job.spec);
                let budget = job.spec.thread_budget();
                used_threads += budget;
                started = true;
                progress_layout.print_row(
                    "start",
                    &key,
                    budget,
                    used_threads,
                    pending.len(),
                    completed,
                    total_jobs,
                );
                let core_offset = used_threads - budget;
                running.spawn(async move {
                    let timeout_ns = job_timeout.as_nanos() as u64;
                    let started_at = Instant::now();
                    let deadline = started_at.checked_add(job_timeout);
                    let timeout_spec = job.spec.clone();
                    let panic_spec = job.spec.clone();
                    let mut handle = tokio::task::spawn_blocking(move || {
                        with_bench_job_deadline(deadline, || (job.run)(core_offset))
                    });
                    let mut timed_out = false;
                    let join_result = match tokio::time::timeout(job_timeout, &mut handle).await {
                        Ok(result) => result,
                        Err(_) => {
                            timed_out = true;
                            handle.await
                        }
                    };
                    match join_result {
                        Ok(_record) if timed_out => {
                            let elapsed_ns = started_at.elapsed().as_nanos() as u64;
                            let record = failed_bench_record(
                                &timeout_spec,
                                BenchRecordStatus::TimedOut,
                                format!(
                                    "benchmark job exceeded {}s timeout",
                                    job_timeout.as_secs()
                                ),
                                elapsed_ns,
                                Some(timeout_ns),
                            );
                            Ok((key, record, budget))
                        }
                        Ok(record) => Ok((key, record, budget)),
                        Err(err) if err.is_panic() => {
                            let elapsed_ns = started_at.elapsed().as_nanos() as u64;
                            let reason = panic_payload_message(err.into_panic());
                            let (status, timeout) =
                                status_and_timeout_for_failure(&reason, elapsed_ns);
                            let record = failed_bench_record(
                                &panic_spec,
                                status,
                                reason,
                                elapsed_ns,
                                timeout,
                            );
                            Ok((key, record, budget))
                        }
                        Err(err) => Err(format!("benchmark task join failed: {err}")),
                    }
                });
            }

            if running.is_empty() && !pending.is_empty() {
                return Err("scheduler stalled with pending work".to_string());
            }

            if !started || pending.is_empty() {
                if let Some(joined) = running.join_next().await {
                    let (key, record, budget) =
                        joined.map_err(|err| format!("scheduler task failed: {err}"))??;
                    used_threads = used_threads.saturating_sub(budget);
                    completed += 1;
                    if record.completed() {
                        progress_layout.print_row(
                            "done",
                            &key,
                            budget,
                            used_threads,
                            pending.len(),
                            completed,
                            total_jobs,
                        );
                    } else {
                        let state = match &record.status {
                            BenchRecordStatus::Completed => "done",
                            BenchRecordStatus::Failed => "failed",
                            BenchRecordStatus::TimedOut => "timed out",
                        };
                        progress_layout.print_row(
                            state,
                            &key,
                            budget,
                            used_threads,
                            pending.len(),
                            completed,
                            total_jobs,
                        );
                        progress_line(format!(
                            "scheduler: reason {:<width$} | {}",
                            "",
                            record.failure_reason.as_deref().unwrap_or("unknown"),
                            width = progress_layout.state_width
                        ));
                    }
                    writer.submit(key.clone(), record.clone())?;
                    results.insert(key, record);
                }
            }
        }

        Ok((results, None))
    });
    runtime.shutdown_timeout(Duration::from_secs(1));
    let is_fully_complete = matches!(&execution_result, Ok((_, None)));
    let writer_result = writer.close(is_fully_complete);
    match (execution_result, writer_result) {
        (Ok(pair), Ok(_)) => Ok(pair),
        (Err(exec_err), Ok(_)) => Err(exec_err),
        (Ok(_), Err(writer_err)) => Err(writer_err),
        (Err(exec_err), Err(writer_err)) => {
            Err(format!("{exec_err}; output writer error: {writer_err}"))
        }
    }
}

fn can_start_job(spec: &JobSpec, used_threads: usize, available_parallelism: usize) -> bool {
    used_threads + spec.thread_budget() <= available_parallelism
}

fn result_key_sort(lhs: &SampleKey, rhs: &SampleKey) -> Ordering {
    let queue_order = |label: &str| match label {
        value if value.starts_with("ubq_") => 0_u8,
        "segqueue" => 1,
        "concurrent-queue" => 2,
        value if value.starts_with("fastfifo_") => 3,
        value if value.starts_with("lfqueue_") => 4,
        value if value.starts_with("wcq_") => 5,
        _ => 99,
    };
    let queue_variant = |label: &str| {
        label
            .strip_prefix("fastfifo_")
            .or_else(|| label.strip_prefix("lfqueue_"))
            .or_else(|| label.strip_prefix("wcq_"))
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(usize::MAX)
    };
    (
        lhs.mode.name().to_string(),
        lhs.items_per_producer,
        queue_order(&lhs.queue_label),
        queue_variant(&lhs.queue_label),
        lhs.queue_label.clone(),
        lhs.batch_size,
    )
        .cmp(&(
            rhs.mode.name().to_string(),
            rhs.items_per_producer,
            queue_order(&rhs.queue_label),
            queue_variant(&rhs.queue_label),
            rhs.queue_label.clone(),
            rhs.batch_size,
        ))
}

fn expected_keys_for_bundle(plan: &MatrixPlan, bundle: &PlanBundle) -> Vec<SampleKey> {
    let mut keys = Vec::new();
    for mode in &bundle.modes {
        for &items_per_producer in &bundle.items_per_producer_values {
            let baseline_queues = if plan.ubq_grid.is_some() && bundle.ubq_label.is_some() {
                &[][..]
            } else {
                plan.baseline_queues.as_slice()
            };
            for baseline_queue in baseline_queues {
                match baseline_queue {
                    QueueKind::FastFifo => {
                        for &block_size in &plan.fastfifo_block_sizes {
                            let spec = JobSpec {
                                scenario: bundle.scenario.clone(),
                                repeat_index: bundle.repeat_index,
                                mode: *mode,
                                items_per_producer,
                                queue: *baseline_queue,
                                ubq_label: None,
                                batch_size: None,
                                fastfifo_block_size: Some(block_size),
                                lfqueue_segment_size: None,
                                wcq_capacity: None,
                            };
                            keys.push(SampleKey::from_job(&spec));
                        }
                    }
                    QueueKind::LfQueue => {
                        for &segment_size in &plan.lfqueue_segment_sizes {
                            let spec = JobSpec {
                                scenario: bundle.scenario.clone(),
                                repeat_index: bundle.repeat_index,
                                mode: *mode,
                                items_per_producer,
                                queue: *baseline_queue,
                                ubq_label: None,
                                batch_size: None,
                                fastfifo_block_size: None,
                                lfqueue_segment_size: Some(segment_size),
                                wcq_capacity: None,
                            };
                            keys.push(SampleKey::from_job(&spec));
                        }
                    }
                    QueueKind::Wcq => {
                        for &capacity in &plan.wcq_capacities {
                            if wcq_mode_supported(
                                *mode,
                                capacity,
                                &bundle.scenario,
                                items_per_producer,
                            ) {
                                let spec = JobSpec {
                                    scenario: bundle.scenario.clone(),
                                    repeat_index: bundle.repeat_index,
                                    mode: *mode,
                                    items_per_producer,
                                    queue: *baseline_queue,
                                    ubq_label: None,
                                    batch_size: None,
                                    fastfifo_block_size: None,
                                    lfqueue_segment_size: None,
                                    wcq_capacity: Some(capacity),
                                };
                                keys.push(SampleKey::from_job(&spec));
                            }
                        }
                    }
                    _ => {
                        let spec = JobSpec {
                            scenario: bundle.scenario.clone(),
                            repeat_index: bundle.repeat_index,
                            mode: *mode,
                            items_per_producer,
                            queue: *baseline_queue,
                            ubq_label: None,
                            batch_size: None,
                            fastfifo_block_size: None,
                            lfqueue_segment_size: None,
                            wcq_capacity: None,
                        };
                        keys.push(SampleKey::from_job(&spec));
                    }
                }
            }
            if let Some(label) = bundle.ubq_label.as_ref() {
                let spec = JobSpec {
                    scenario: bundle.scenario.clone(),
                    repeat_index: bundle.repeat_index,
                    mode: *mode,
                    items_per_producer,
                    queue: QueueKind::Ubq,
                    ubq_label: Some(label.clone()),
                    batch_size: None,
                    fastfifo_block_size: None,
                    lfqueue_segment_size: None,
                    wcq_capacity: None,
                };
                keys.push(SampleKey::from_job(&spec));
                if *mode == Mode::Throughput {
                    for &batch_size in &plan.ubq_batch_sizes {
                        let spec = JobSpec {
                            scenario: bundle.scenario.clone(),
                            repeat_index: bundle.repeat_index,
                            mode: *mode,
                            items_per_producer,
                            queue: QueueKind::Ubq,
                            ubq_label: Some(label.clone()),
                            batch_size: Some(batch_size),
                            fastfifo_block_size: None,
                            lfqueue_segment_size: None,
                            wcq_capacity: None,
                        };
                        keys.push(SampleKey::from_job(&spec));
                    }
                }
            }
        }
    }
    keys.sort_by(result_key_sort);
    keys
}

fn bundle_output_meta(plan: &MatrixPlan, bundle: &PlanBundle) -> Result<OutputMeta, String> {
    let ubq_label = bundle.ubq_label.clone();
    let ubq_block_size = match ubq_label.as_deref() {
        Some(label) => Some(parse_ubq_label(label, true)?.block),
        None => None,
    };
    let expected_ubq_configurations = plan.ubq_grid.map(|_| {
        plan.bundles
            .iter()
            .filter(|candidate| candidate.scenario == bundle.scenario)
            .filter_map(|candidate| candidate.ubq_label.as_deref())
            .collect::<BTreeSet<_>>()
            .len()
    });
    let planned_items_per_producer = if plan.ubq_grid.is_some() {
        plan.bundles
            .iter()
            .filter(|candidate| candidate.scenario == bundle.scenario)
            .flat_map(|candidate| candidate.items_per_producer_values.iter().copied())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect()
    } else {
        Vec::new()
    };
    Ok(OutputMeta {
        timestamp_unix_ms: now_unix_ms(),
        machine_label: plan.machine_label.clone(),
        scenario: bundle.scenario.name.clone(),
        producers: bundle.scenario.producers,
        consumers: bundle.scenario.consumers,
        repeat_index: bundle.repeat_index,
        available_parallelism: plan.available_parallelism,
        ubq_label,
        ubq_block_size,
        ubq_grid: plan.ubq_grid,
        expected_ubq_configurations,
        ubq_batch_sizes: plan.ubq_batch_sizes.clone(),
        planned_repeats: plan.ubq_grid.map(|_| plan.planned_repeats),
        planned_items_per_producer,
    })
}

fn atomic_write_string(path: &Path, contents: &str) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .map_err(|err| format!("failed to create output dir {}: {err}", parent.display()))?;
    }
    let file_name = path
        .file_name()
        .map(|value| value.to_string_lossy().into_owned())
        .unwrap_or_else(|| "snapshot.json".to_string());
    let tmp_path = path.with_file_name(format!(".{file_name}.tmp"));
    {
        let mut file = fs::File::create(&tmp_path)
            .map_err(|err| format!("failed to create temp output {}: {err}", tmp_path.display()))?;
        file.write_all(contents.as_bytes())
            .map_err(|err| format!("failed to write temp output {}: {err}", tmp_path.display()))?;
        file.sync_all()
            .map_err(|err| format!("failed to flush temp output {}: {err}", tmp_path.display()))?;
    }
    match fs::rename(&tmp_path, path) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == io::ErrorKind::AlreadyExists => {
            fs::remove_file(path).map_err(|remove_err| {
                format!("failed to replace output {}: {remove_err}", path.display())
            })?;
            fs::rename(&tmp_path, path).map_err(|rename_err| {
                format!("failed to replace output {}: {rename_err}", path.display())
            })
        }
        Err(err) => {
            let _ = fs::remove_file(&tmp_path);
            Err(format!(
                "failed to publish output {}: {err}",
                path.display()
            ))
        }
    }
}

fn output_path_for_bundle(plan: &MatrixPlan, bundle: &PlanBundle, run_id: &str) -> PathBuf {
    let label = bundle
        .ubq_label
        .as_deref()
        .map(|value| parse_ubq_label(value, true).expect("valid label").safe())
        .unwrap_or_else(|| "baseline".to_string());
    plan.runs_dir
        .join(sanitize_name(&plan.machine_label))
        .join(sanitize_name(&bundle.scenario.name))
        .join(label)
        .join(format!("{run_id}_r{}.json", bundle.repeat_index))
}

/// Parses a scheduler stdout tracking line of the form
/// `"scheduler: start <label> scenario=<s> repeat=<n> ..."` or
/// `"scheduler: done <label> scenario=<s> repeat=<n> ..."`.
/// Returns `("start"|"done", queue_label, scenario, repeat_index)` or `None`.
fn parse_scheduler_tracking_line(line: &str) -> Option<(&'static str, String, String, usize)> {
    let (verb, rest) = if let Some(rest) = line.strip_prefix("scheduler: start ") {
        ("start", rest)
    } else if let Some(rest) = line.strip_prefix("scheduler: done ") {
        ("done", rest)
    } else {
        return None;
    };
    let mut parts = rest.split_ascii_whitespace();
    let queue_label = parts.next()?.to_string();
    let mut scenario = String::new();
    let mut repeat_index: usize = 0;
    for field in parts {
        if let Some(v) = field.strip_prefix("scenario=") {
            scenario = v.to_string();
        } else if let Some(v) = field.strip_prefix("repeat=") {
            repeat_index = v.parse().unwrap_or(0);
        }
    }
    if scenario.is_empty() {
        return None;
    }
    Some((verb, queue_label, scenario, repeat_index))
}

pub fn build_and_run_matrix_plan(
    plan: &MatrixPlan,
    dry_run: bool,
) -> Result<(PathBuf, BatchOutcome), String> {
    let repo_root =
        std::env::current_dir().map_err(|err| format!("failed to read current dir: {err}"))?;
    let run_id = format!("{}", now_unix_nanos());
    let generated_root = repo_root.join("target").join("bench_harness").join(run_id);
    let src_dir = generated_root.join("src");
    fs::create_dir_all(&src_dir).map_err(|err| {
        format!(
            "failed to create generated src dir {}: {err}",
            src_dir.display()
        )
    })?;

    let cargo_toml = generated_root.join("Cargo.toml");
    let main_rs = src_dir.join("main.rs");
    let plan_json = serde_json::to_string(plan)
        .map_err(|err| format!("failed to serialize matrix plan: {err}"))?;
    fs::write(&cargo_toml, generated_cargo_toml(&repo_root))
        .map_err(|err| format!("failed to write {}: {err}", cargo_toml.display()))?;
    fs::write(&main_rs, generated_main_source(plan, &plan_json))
        .map_err(|err| format!("failed to write {}: {err}", main_rs.display()))?;

    let required_jobs = required_job_specs(plan).len();
    progress_line(format!(
        "bench_matrix: prepared {} bundle(s), {} unique job(s), generated root {}",
        plan.bundles.len(),
        required_jobs,
        generated_root.display()
    ));
    let mut cmd = Command::new("cargo");
    cmd.arg("run")
        .arg("--offline")
        .arg("--release")
        .arg("--manifest-path")
        .arg(&cargo_toml);
    if std::env::var("RUSTFLAGS").map_or(false, |f| f.contains("sanitizer")) {
        cmd.arg("--target").arg(host_target());
    }
    if dry_run {
        progress_line(format!(
            "bench_matrix dry-run: cargo run --offline --release --manifest-path {}",
            cargo_toml.display()
        ));
        return Ok((
            generated_root,
            BatchOutcome {
                exit_success: true,
                crashed_job: None,
            },
        ));
    }

    progress_line(format!(
        "bench_matrix: building and running generated scheduler {}",
        cargo_toml.display()
    ));
    let mut child = cmd
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .map_err(|err| format!("failed to launch generated scheduler: {err}"))?;

    let child_stdout = child.stdout.take().expect("stdout was piped");
    // Channel for tracking events: (verb, queue_label, scenario, repeat_index).
    let (tracking_tx, tracking_rx) = mpsc::channel::<(&'static str, String, String, usize)>();
    let stdout_thread = thread::spawn(move || {
        let reader = BufReader::new(child_stdout);
        for line in reader.lines().map_while(Result::ok) {
            println!("{line}");
            let _ = io::stdout().flush();
            if let Some((verb, queue_label, scenario, repeat_index)) =
                parse_scheduler_tracking_line(&line)
            {
                let _ = tracking_tx.send((verb, queue_label, scenario, repeat_index));
            }
        }
    });

    let status = child
        .wait()
        .map_err(|err| format!("failed to wait on generated scheduler: {err}"))?;
    let _ = stdout_thread.join();

    let mut started: BTreeSet<(String, String, usize)> = BTreeSet::new();
    let mut done: BTreeSet<(String, String, usize)> = BTreeSet::new();
    for (verb, queue_label, scenario, repeat_index) in tracking_rx.try_iter() {
        match verb {
            "start" => {
                started.insert((queue_label, scenario, repeat_index));
            }
            "done" => {
                done.insert((queue_label, scenario, repeat_index));
            }
            _ => {}
        }
    }

    if status.success() {
        return Ok((
            generated_root,
            BatchOutcome {
                exit_success: true,
                crashed_job: None,
            },
        ));
    }

    // Crashed. Find the UBQ job that started but never completed.
    let crashed_job = started
        .difference(&done)
        .find(|(label, _, _)| label.starts_with("ubq_"))
        .map(|(label, scenario, _)| (label.clone(), scenario.clone()));

    Ok((
        generated_root,
        BatchOutcome {
            exit_success: false,
            crashed_job,
        },
    ))
}

fn generated_cargo_toml(repo_root: &Path) -> String {
    format!(
        "[package]\nname = \"ubq_generated_scheduler\"\nversion = \"0.0.0\"\nedition = \"2024\"\n\n[dependencies]\nubq = {{ path = {:?}, features = [\"bench_rbbq\", \"bench_lfqueue\", \"bench_wcq\"] }}\n",
        repo_root.display().to_string()
    )
}

fn generated_main_source(plan: &MatrixPlan, plan_json: &str) -> String {
    let mut out = String::new();
    out.push_str("use ubq::bench_harness;\n");
    out.push_str("use ubq::{ConfiguredUBQ, align, backoff};\n\n");
    out.push_str("fn main() {\n");
    out.push_str(
        "    let plan = bench_harness::parse_embedded_plan(PLAN_JSON).expect(\"plan\");\n",
    );
    out.push_str("    let mut jobs = Vec::new();\n");

    for spec in required_job_specs(plan) {
        let scenario_expr = format!(
            "bench_harness::ScenarioConfig::new({}, {})",
            spec.scenario.producers, spec.scenario.consumers
        );
        match spec.queue {
            QueueKind::SegQueue => {
                out.push_str(&format!(
                    "    jobs.push(bench_harness::make_segqueue_job_factory({scenario_expr}, {}, bench_harness::Mode::{:?}, {}));\n",
                    spec.repeat_index, spec.mode, spec.items_per_producer
                ));
            }
            QueueKind::ConcurrentQueue => {
                out.push_str(&format!(
                    "    jobs.push(bench_harness::make_concurrent_queue_job_factory({scenario_expr}, {}, bench_harness::Mode::{:?}, {}));\n",
                    spec.repeat_index, spec.mode, spec.items_per_producer
                ));
            }
            QueueKind::FastFifo => {
                let block_size = spec
                    .fastfifo_block_size
                    .expect("RBBQ block size must be present");
                out.push_str(&format!(
                    "    jobs.push(bench_harness::make_fastfifo_job_factory({}, {scenario_expr}, {}, bench_harness::Mode::{:?}, {}));\n",
                    block_size, spec.repeat_index, spec.mode, spec.items_per_producer
                ));
            }
            QueueKind::LfQueue => {
                let segment_size = spec
                    .lfqueue_segment_size
                    .expect("lfqueue segment size must be present");
                out.push_str(&format!(
                    "    jobs.push(bench_harness::make_lfqueue_job_factory({}, {scenario_expr}, {}, bench_harness::Mode::{:?}, {}));\n",
                    segment_size, spec.repeat_index, spec.mode, spec.items_per_producer
                ));
            }
            QueueKind::Wcq => {
                let capacity = spec.wcq_capacity.expect("wCQ capacity must be present");
                out.push_str(&format!(
                    "    jobs.push(bench_harness::make_wcq_job_factory({}, {scenario_expr}, {}, bench_harness::Mode::{:?}, {}).expect(\"supported wCQ capacity\"));\n",
                    capacity, spec.repeat_index, spec.mode, spec.items_per_producer
                ));
            }
            QueueKind::Ubq => {
                let label = parse_ubq_label(
                    spec.ubq_label
                        .as_deref()
                        .expect("ubq labels must be present"),
                    true,
                )
                .expect("valid label");
                out.push_str(&format!(
                    "    jobs.push(bench_harness::make_ubq_job_factory::<{}, {}>(\"{}\", {scenario_expr}, {}, bench_harness::Mode::{:?}, {}, {:?}));\n",
                    ubq_type_expr(&label, "u64"),
                    ubq_type_expr(&label, "bench_harness::LogRecord"),
                    label.text(),
                    spec.repeat_index,
                    spec.mode,
                    spec.items_per_producer,
                    spec.batch_size
                ));
            }
        }
    }

    out.push_str(
        "    bench_harness::run_embedded_scheduler(plan, jobs).expect(\"run scheduler\");\n",
    );
    out.push_str("}\n\n");
    out.push_str("const PLAN_JSON: &str = r####\"");
    out.push_str(plan_json);
    out.push_str("\"####;\n");
    out
}

fn progress_line(message: impl AsRef<str>) {
    println!("{}", message.as_ref());
    let _ = io::stdout().flush();
}

fn decimal_width(value: usize) -> usize {
    value.max(1).ilog10() as usize + 1
}

fn completion_percent(completed: usize, total: usize) -> f64 {
    if total == 0 {
        100.0
    } else {
        100.0 * completed as f64 / total as f64
    }
}

struct ProgressLayout {
    state_width: usize,
    queue_width: usize,
    scenario_width: usize,
    repeat_width: usize,
    mode_width: usize,
    items_width: usize,
    batch_width: usize,
    thread_width: usize,
    count_width: usize,
    available_parallelism: usize,
}

impl ProgressLayout {
    fn new<'a>(specs: impl IntoIterator<Item = &'a JobSpec>, available_parallelism: usize) -> Self {
        let mut layout = Self {
            state_width: "timed out".len(),
            queue_width: "queue".len(),
            scenario_width: 1,
            repeat_width: 1,
            mode_width: 1,
            items_width: 1,
            batch_width: "scalar".len(),
            thread_width: 1,
            count_width: 1,
            available_parallelism,
        };
        let mut count = 0_usize;
        for spec in specs {
            count += 1;
            layout.queue_width = layout.queue_width.max(spec.queue_label().len());
            layout.scenario_width = layout.scenario_width.max(spec.scenario.name.len());
            layout.repeat_width = layout.repeat_width.max(decimal_width(spec.repeat_index));
            layout.mode_width = layout.mode_width.max(spec.mode.name().len());
            layout.items_width = layout
                .items_width
                .max(spec.items_per_producer.to_string().len());
            layout.batch_width = layout.batch_width.max(
                spec.batch_size
                    .map(|value| value.to_string().len())
                    .unwrap_or("scalar".len()),
            );
            layout.thread_width = layout.thread_width.max(decimal_width(spec.thread_budget()));
        }
        layout.count_width = decimal_width(count);
        layout.thread_width = layout
            .thread_width
            .max(decimal_width(available_parallelism));
        layout
    }

    fn print_header(&self) {
        progress_line(format!(
            "scheduler: {:<state$} {:<queue$} | {:>8} | {:<scenario_col$} | {:<repeat_col$} | {:<mode_col$} | {:<items_col$} | {:<batch_col$} | {:<threads_col$} | {:<active_col$} | {:<pending_col$} | {:<completed_col$}",
            "state",
            "queue",
            "progress",
            "scenario",
            "repeat",
            "mode",
            "items",
            "batch",
            "threads",
            "active",
            "pending",
            "completed",
            state = self.state_width,
            queue = self.queue_width,
            scenario_col = self.scenario_width + "scenario=".len(),
            repeat_col = self.repeat_width + "repeat=".len(),
            mode_col = self.mode_width + "mode=".len(),
            items_col = self.items_width + "items=".len(),
            batch_col = self.batch_width + "batch=".len(),
            threads_col = self.thread_width + "threads=".len(),
            active_col = self.thread_width * 2 + 1 + "active=".len(),
            pending_col = self.count_width + "pending=".len(),
            completed_col = self.count_width * 2 + 1 + "completed=".len(),
        ));
    }

    fn print_row(
        &self,
        state: &str,
        key: &SampleKey,
        threads: usize,
        active: usize,
        pending: usize,
        completed: usize,
        total: usize,
    ) {
        let batch = key
            .batch_size
            .map(|value| value.to_string())
            .unwrap_or_else(|| "scalar".to_string());
        progress_line(format!(
            "scheduler: {:<state$} {:<queue$} | {:>7.2}% | scenario={:<scenario$} | repeat={:>repeat$} | mode={:<mode$} | items={:>items$} | batch={:>batch$} | threads={:>threads$} | active={:>active_width$} | pending={:>pending_width$} | completed={:>completed_width$}",
            state,
            key.queue_label,
            completion_percent(completed, total),
            key.scenario,
            key.repeat_index,
            key.mode.name(),
            key.items_per_producer,
            batch,
            threads,
            format!(
                "{:>width$}/{:<width$}",
                active,
                self.available_parallelism,
                width = self.thread_width
            ),
            pending,
            format!(
                "{:>width$}/{:<width$}",
                completed,
                total,
                width = self.count_width
            ),
            state = self.state_width,
            queue = self.queue_width,
            scenario = self.scenario_width,
            repeat = self.repeat_width,
            mode = self.mode_width,
            items = self.items_width,
            batch = self.batch_width,
            threads = self.thread_width,
            active_width = self.thread_width * 2 + 1,
            pending_width = self.count_width,
            completed_width = self.count_width * 2 + 1,
        ));
    }
}

fn ubq_type_expr(label: &UbqLabel, value_type: &str) -> String {
    let backoff_ty = match label.backoff.as_str() {
        "crossbeam" => "backoff::Crossbeam",
        "yield" => "backoff::Yield",
        _ => panic!("unsupported backoff {}", label.backoff),
    };
    let align_ty = match label.block {
        31 => "align::A64",
        63 => "align::A128",
        127 => "align::A256",
        255 => "align::A512",
        511 => "align::A1024",
        1023 => "align::A2048",
        2047 => "align::A4096",
        4095 => "align::A8192",
        _ => panic!("unsupported block size {}", label.block),
    };
    format!(
        "ConfiguredUBQ<{value_type}, {backoff_ty}, {}, {}, {align_ty}>",
        label.pool, label.block
    )
}

pub fn frontier_search(config: &FrontierConfig, dry_run: bool) -> Result<(), String> {
    let mut round = 0_usize;
    let mut failed_attempts: BTreeMap<(String, String), usize> = BTreeMap::new();
    let mut incompletable: BTreeSet<(String, String)> = BTreeSet::new();

    loop {
        round += 1;
        let index = load_existing_runs(&config.runs_dir, &config.machine_label)?;
        let plan = compute_frontier_round_plan(config, &index, &incompletable)?;
        if plan.bundles.is_empty() {
            if incompletable.is_empty() {
                progress_line(format!(
                    "bench_frontier frontier-complete after {round} rounds"
                ));
            } else {
                progress_line(format!(
                    "bench_frontier frontier-complete after {round} rounds \
                     ({} scenario(s) marked incompletable)",
                    incompletable.len()
                ));
            }
            return Ok(());
        }
        let required_jobs = required_job_specs(&plan).len();
        progress_line(format!(
            "bench_frontier round {}: scheduling {} bundle(s), {} unique job(s)",
            round,
            plan.bundles.len(),
            required_jobs
        ));
        if dry_run {
            for bundle in &plan.bundles {
                progress_line(format!(
                    "  scenario={} repeat={} label={}",
                    bundle.scenario.name,
                    bundle.repeat_index,
                    bundle.ubq_label.as_deref().unwrap_or("baseline")
                ));
            }
            return Ok(());
        }
        let outcome = run_matrix_plan_in_process(&plan, false)?;
        if !outcome.exit_success {
            match outcome.crashed_job {
                Some((queue_label, scenario)) => {
                    let key = (queue_label.clone(), scenario.clone());
                    let count = failed_attempts.entry(key.clone()).or_insert(0);
                    *count += 1;
                    if *count >= config.repeats {
                        incompletable.insert(key);
                        progress_line(format!(
                            "bench_frontier: marking ({queue_label}, {scenario}) incompletable \
                             after {count} failed attempt(s)"
                        ));
                    } else {
                        progress_line(format!(
                            "bench_frontier: ({queue_label}, {scenario}) crashed \
                             ({count}/{} attempts), will retry",
                            config.repeats
                        ));
                    }
                }
                None => {
                    return Err("generated scheduler crashed but no in-flight UBQ job \
                         could be identified; check stderr for details"
                        .to_string());
                }
            }
        }
    }
}

pub fn compute_frontier_round_plan(
    config: &FrontierConfig,
    index: &ExistingRunsIndex,
    incompletable: &BTreeSet<(String, String)>,
) -> Result<MatrixPlan, String> {
    let mut normalized_seed_labels = Vec::with_capacity(config.seed_labels.len());
    for seed in &config.seed_labels {
        normalized_seed_labels.push(parse_ubq_label(seed, true)?);
    }

    let mut desired: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for scenario in &config.scenarios {
        let entry = desired.entry(scenario.name.clone()).or_default();
        for seed in &normalized_seed_labels {
            if is_valid_ubq_label_for_scenario(seed, scenario) {
                entry.insert(seed.text());
            }
        }
        if entry.is_empty() {
            return Err(format!(
                "scenario {} has no valid seed labels after applying the FAA block-size constraint",
                scenario.name
            ));
        }
    }

    let present_labels = collect_present_ubq_labels(index);
    let globally_desired_winners = collect_global_winner_labels(
        index,
        &config.scenarios,
        &present_labels,
        &config.baseline_queues,
        &config.fastfifo_block_sizes,
        &config.lfqueue_segment_sizes,
        &config.wcq_capacities,
        &config.modes,
        &config.items_per_producer_values,
        config.repeats,
    );
    for label in globally_desired_winners {
        let Ok(parsed) = parse_ubq_label(&label, true) else {
            continue;
        };
        for scenario in &config.scenarios {
            if is_valid_ubq_label_for_scenario(&parsed, scenario) {
                desired
                    .entry(scenario.name.clone())
                    .or_default()
                    .insert(label.clone());
            }
        }
    }
    let local_best_labels = collect_local_best_ubq_labels(
        index,
        &config.scenarios,
        &present_labels,
        &config.baseline_queues,
        &config.fastfifo_block_sizes,
        &config.lfqueue_segment_sizes,
        &config.wcq_capacities,
        &config.modes,
        &config.items_per_producer_values,
        config.repeats,
    );
    for scenario in &config.scenarios {
        let Some(labels) = desired.get_mut(&scenario.name) else {
            continue;
        };
        let Some(scenario_local_best_labels) = local_best_labels.get(&scenario.name) else {
            continue;
        };
        for label in scenario_local_best_labels {
            for neighbor in immediate_search_labels_for_scenario(label, scenario)? {
                labels.insert(neighbor);
            }
        }
    }

    let mut bundles = Vec::new();
    for scenario in &config.scenarios {
        let labels = desired.get(&scenario.name).cloned().unwrap_or_default();
        for label in labels {
            let queue_label = format!("ubq_{label}");
            if incompletable.contains(&(queue_label, scenario.name.clone())) {
                continue;
            }
            for repeat_index in 1..=config.repeats {
                if bundle_complete(
                    index,
                    scenario,
                    repeat_index,
                    Some(label.as_str()),
                    &config.baseline_queues,
                    &config.fastfifo_block_sizes,
                    &config.lfqueue_segment_sizes,
                    &config.wcq_capacities,
                    &config.modes,
                    &config.items_per_producer_values,
                ) {
                    continue;
                }
                bundles.push(PlanBundle {
                    scenario: scenario.clone(),
                    repeat_index,
                    ubq_label: Some(label.clone()),
                    modes: config.modes.clone(),
                    items_per_producer_values: config.items_per_producer_values.clone(),
                });
            }
        }
    }

    Ok(MatrixPlan {
        plan_schema_version: PLAN_SCHEMA_VERSION,
        machine_label: config.machine_label.clone(),
        runs_dir: config.runs_dir.clone(),
        available_parallelism: config.available_parallelism,
        baseline_queues: config.baseline_queues.clone(),
        fastfifo_block_sizes: config.fastfifo_block_sizes.clone(),
        lfqueue_segment_sizes: config.lfqueue_segment_sizes.clone(),
        wcq_capacities: config.wcq_capacities.clone(),
        ubq_grid: None,
        ubq_batch_sizes: Vec::new(),
        planned_repeats: config.repeats,
        bundles,
        reuse_existing: true,
    })
}

fn collect_present_ubq_labels(index: &ExistingRunsIndex) -> BTreeSet<String> {
    index
        .records
        .keys()
        .filter_map(|key| {
            key.queue_label
                .strip_prefix("ubq_")
                .map(ToString::to_string)
        })
        .collect()
}

fn collect_global_winner_labels(
    index: &ExistingRunsIndex,
    scenarios: &[ScenarioConfig],
    present_labels: &BTreeSet<String>,
    baseline_queues: &[QueueKind],
    fastfifo_block_sizes: &[usize],
    lfqueue_segment_sizes: &[usize],
    wcq_capacities: &[usize],
    modes: &[Mode],
    items_per_producer_values: &[u64],
    repeats: usize,
) -> BTreeSet<String> {
    let mut winners = BTreeSet::new();
    for scenario in scenarios {
        for mode in modes {
            for &items_per_producer in items_per_producer_values {
                let baseline_labels = baseline_queue_labels_for_sample(
                    baseline_queues,
                    fastfifo_block_sizes,
                    lfqueue_segment_sizes,
                    wcq_capacities,
                    scenario,
                    *mode,
                    items_per_producer,
                );
                let best_baseline = baseline_labels
                    .iter()
                    .filter_map(|queue_label| {
                        mean_ops(
                            index,
                            &scenario.name,
                            queue_label,
                            *mode,
                            items_per_producer,
                            repeats,
                        )
                    })
                    .max_by(|lhs, rhs| lhs.partial_cmp(rhs).unwrap_or(Ordering::Equal));
                let Some(best_baseline) = best_baseline else {
                    continue;
                };

                let best_label = present_labels
                    .iter()
                    .filter_map(|label| {
                        let parsed = parse_ubq_label(label, true).ok()?;
                        is_valid_ubq_label_for_scenario(&parsed, scenario).then_some(label)
                    })
                    .filter(|label| {
                        is_complete_coverage(
                            index,
                            scenario,
                            label,
                            baseline_queues,
                            fastfifo_block_sizes,
                            lfqueue_segment_sizes,
                            wcq_capacities,
                            modes,
                            items_per_producer_values,
                            repeats,
                        )
                    })
                    .filter_map(|label| {
                        mean_ops(
                            index,
                            &scenario.name,
                            label,
                            *mode,
                            items_per_producer,
                            repeats,
                        )
                        .map(|ops| (label.clone(), ops))
                    })
                    .max_by(|lhs, rhs| lhs.1.partial_cmp(&rhs.1).unwrap_or(Ordering::Equal));

                if let Some((label, best_ubq_ops)) = best_label {
                    if best_ubq_ops > best_baseline {
                        winners.insert(label);
                    }
                }
            }
        }
    }
    winners
}

fn collect_local_best_ubq_labels(
    index: &ExistingRunsIndex,
    scenarios: &[ScenarioConfig],
    present_labels: &BTreeSet<String>,
    baseline_queues: &[QueueKind],
    fastfifo_block_sizes: &[usize],
    lfqueue_segment_sizes: &[usize],
    wcq_capacities: &[usize],
    modes: &[Mode],
    items_per_producer_values: &[u64],
    repeats: usize,
) -> BTreeMap<String, BTreeSet<String>> {
    let mut winners = BTreeMap::new();
    for scenario in scenarios {
        let mut scenario_winners = BTreeSet::new();
        for mode in modes {
            for &items_per_producer in items_per_producer_values {
                let best_label = present_labels
                    .iter()
                    .filter_map(|label| {
                        let parsed = parse_ubq_label(label, true).ok()?;
                        is_valid_ubq_label_for_scenario(&parsed, scenario).then_some(label)
                    })
                    .filter(|label| {
                        is_complete_coverage(
                            index,
                            scenario,
                            label,
                            baseline_queues,
                            fastfifo_block_sizes,
                            lfqueue_segment_sizes,
                            wcq_capacities,
                            modes,
                            items_per_producer_values,
                            repeats,
                        )
                    })
                    .filter_map(|label| {
                        mean_ops(
                            index,
                            &scenario.name,
                            label,
                            *mode,
                            items_per_producer,
                            repeats,
                        )
                        .map(|ops| (label.clone(), ops))
                    })
                    .max_by(|lhs, rhs| lhs.1.partial_cmp(&rhs.1).unwrap_or(Ordering::Equal));
                if let Some((label, _)) = best_label {
                    scenario_winners.insert(label);
                }
            }
        }
        if !scenario_winners.is_empty() {
            winners.insert(scenario.name.clone(), scenario_winners);
        }
    }
    winners
}

fn bundle_complete(
    index: &ExistingRunsIndex,
    scenario: &ScenarioConfig,
    repeat_index: usize,
    label: Option<&str>,
    baseline_queues: &[QueueKind],
    fastfifo_block_sizes: &[usize],
    lfqueue_segment_sizes: &[usize],
    wcq_capacities: &[usize],
    modes: &[Mode],
    items_per_producer_values: &[u64],
) -> bool {
    for mode in modes {
        for &items in items_per_producer_values {
            let baseline_labels = baseline_queue_labels_for_sample(
                baseline_queues,
                fastfifo_block_sizes,
                lfqueue_segment_sizes,
                wcq_capacities,
                scenario,
                *mode,
                items,
            );
            for baseline_label in &baseline_labels {
                let key = SampleKey {
                    scenario: scenario.name.clone(),
                    repeat_index,
                    mode: *mode,
                    items_per_producer: items,
                    queue_label: baseline_label.clone(),
                    batch_size: None,
                };
                if !index.records.contains_key(&key) {
                    return false;
                }
            }
            if let Some(label) = label {
                let key = SampleKey {
                    scenario: scenario.name.clone(),
                    repeat_index,
                    mode: *mode,
                    items_per_producer: items,
                    queue_label: format!("ubq_{label}"),
                    batch_size: None,
                };
                if !index.records.contains_key(&key) {
                    return false;
                }
            }
        }
    }
    true
}

fn is_complete_coverage(
    index: &ExistingRunsIndex,
    scenario: &ScenarioConfig,
    label: &str,
    baseline_queues: &[QueueKind],
    fastfifo_block_sizes: &[usize],
    lfqueue_segment_sizes: &[usize],
    wcq_capacities: &[usize],
    modes: &[Mode],
    items_per_producer_values: &[u64],
    repeats: usize,
) -> bool {
    (1..=repeats).all(|repeat_index| {
        for mode in modes {
            for &items in items_per_producer_values {
                let baseline_labels = baseline_queue_labels_for_sample(
                    baseline_queues,
                    fastfifo_block_sizes,
                    lfqueue_segment_sizes,
                    wcq_capacities,
                    scenario,
                    *mode,
                    items,
                );
                for baseline_label in baseline_labels {
                    let key = SampleKey {
                        scenario: scenario.name.clone(),
                        repeat_index,
                        mode: *mode,
                        items_per_producer: items,
                        queue_label: baseline_label,
                        batch_size: None,
                    };
                    if !index.records.contains_key(&key) {
                        return false;
                    }
                }
                let key = SampleKey {
                    scenario: scenario.name.clone(),
                    repeat_index,
                    mode: *mode,
                    items_per_producer: items,
                    queue_label: format!("ubq_{label}"),
                    batch_size: None,
                };
                if !index.records.contains_key(&key) {
                    return false;
                }
            }
        }
        true
    })
}

fn mean_ops(
    index: &ExistingRunsIndex,
    scenario: &str,
    queue_label: &str,
    mode: Mode,
    items_per_producer: u64,
    repeats: usize,
) -> Option<f64> {
    let mut values = Vec::new();
    let lookup_label = if queue_label.starts_with("ubq_")
        || queue_label == "segqueue"
        || queue_label == "concurrent-queue"
        || queue_label.starts_with("fastfifo_")
        || queue_label.starts_with("lfqueue_")
        || queue_label.starts_with("wcq_")
    {
        queue_label.to_string()
    } else {
        format!("ubq_{queue_label}")
    };
    for repeat_index in 1..=repeats {
        let key = SampleKey {
            scenario: scenario.to_string(),
            repeat_index,
            mode,
            items_per_producer,
            queue_label: lookup_label.clone(),
            batch_size: None,
        };
        let record = index.records.get(&key)?;
        values.push(record.ops_per_sec?);
    }
    Some(values.iter().sum::<f64>() / values.len() as f64)
}

fn bench_throughput_for<Q: BenchQueue>(
    queue_name: &str,
    scenario: &ScenarioConfig,
    items_per_producer: u64,
    core_offset: usize,
) -> BenchRecord {
    bench_throughput_with_queue(
        Q::new_queue(),
        queue_name,
        scenario,
        items_per_producer,
        core_offset,
    )
}

fn bench_throughput_batched_for<Q: BenchQueue>(
    queue_name: &str,
    scenario: &ScenarioConfig,
    items_per_producer: u64,
    batch_size: usize,
    core_offset: usize,
) -> BenchRecord {
    assert!(batch_size >= 2, "batch size must be >= 2");
    bench_throughput_with_queue_variant(
        Q::new_queue(),
        queue_name,
        scenario,
        items_per_producer,
        Some(batch_size),
        core_offset,
    )
}

fn bench_throughput_with_queue<Q: BenchQueueHandleFactory>(
    queue_handle: Arc<Q>,
    queue_name: &str,
    scenario: &ScenarioConfig,
    items_per_producer: u64,
    core_offset: usize,
) -> BenchRecord {
    bench_throughput_with_queue_variant(
        queue_handle,
        queue_name,
        scenario,
        items_per_producer,
        None,
        core_offset,
    )
}

fn bench_throughput_with_queue_variant<Q: BenchQueueHandleFactory>(
    queue_handle: Arc<Q>,
    queue_name: &str,
    scenario: &ScenarioConfig,
    items_per_producer: u64,
    batch_size: Option<usize>,
    core_offset: usize,
) -> BenchRecord {
    let total_items = total_items(items_per_producer, scenario.producers);
    let total_threads = scenario.total_threads();
    let ready = Arc::new(Barrier::new(total_threads + 1));
    let start_gate = Arc::new(Barrier::new(total_threads + 1));
    let start = Arc::new(OnceLock::new());
    let producer_max = Arc::new(AtomicU64::new(0));
    let consumer_max = Arc::new(AtomicU64::new(0));
    let consumed_total = Arc::new(AtomicU64::new(0));

    let mut producer_handles = Vec::with_capacity(scenario.producers);
    for producer_id in 0..scenario.producers {
        let queue_thread = queue_handle.thread_handle();
        let ready = ready.clone();
        let start_gate = start_gate.clone();
        let start = start.clone();
        let producer_max = producer_max.clone();
        let core_id = bench_core_ids().get(core_offset + producer_id).copied();
        producer_handles.push(spawn_bench_thread(move || {
            if let Some(id) = core_id {
                core_affinity::set_for_current(id);
            }
            ready.wait();
            start_gate.wait();
            let start: Instant = *start.get().expect("start set");
            let base = (producer_id as u64)
                .checked_mul(items_per_producer)
                .expect("item count overflow");
            let end = base
                .checked_add(items_per_producer)
                .expect("item count overflow");
            if let Some(batch_size) = batch_size {
                let item_count =
                    usize::try_from(items_per_producer).expect("batched item count must fit usize");
                let mut first = 0_usize;
                while first < item_count {
                    let next = first.saturating_add(batch_size).min(item_count);
                    queue_thread.send_batch(base, first..next);
                    first = next;
                }
            } else {
                for value in base..end {
                    queue_thread.send_value(value);
                }
            }
            let end_ns = start.elapsed().as_nanos() as u64;
            producer_max.fetch_max(end_ns, AtomicOrdering::Relaxed);
        }));
    }

    let mut consumer_handles = Vec::with_capacity(scenario.consumers);
    for consumer_id in 0..scenario.consumers {
        let queue_thread = queue_handle.thread_handle();
        let ready = ready.clone();
        let start_gate = start_gate.clone();
        let start = start.clone();
        let consumer_max = consumer_max.clone();
        let consumed_total = consumed_total.clone();
        let core_id = bench_core_ids()
            .get(core_offset + scenario.producers + consumer_id)
            .copied();
        consumer_handles.push(spawn_bench_thread(move || {
            if let Some(id) = core_id {
                core_affinity::set_for_current(id);
            }
            ready.wait();
            start_gate.wait();
            let start: Instant = *start.get().expect("start set");
            loop {
                let value = queue_thread.recv_value();
                if value == SENTINEL {
                    break;
                }
                consumed_total.fetch_add(1, AtomicOrdering::Relaxed);
            }
            let end_ns = start.elapsed().as_nanos() as u64;
            consumer_max.fetch_max(end_ns, AtomicOrdering::Relaxed);
        }));
    }

    ready.wait();
    start.set(Instant::now()).ok();
    start_gate.wait();

    let mut failure_reason = None;
    if let Err(err) = join_bench_threads(producer_handles, "producer") {
        failure_reason.get_or_insert(err);
    }
    if let Err(err) = send_sentinels(queue_handle.thread_handle(), scenario.consumers, queue_name) {
        failure_reason.get_or_insert(err);
    }
    if let Err(err) = join_bench_threads(consumer_handles, "consumer") {
        failure_reason.get_or_insert(err);
    }
    if let Some(reason) = failure_reason {
        let elapsed_ns = start.get().expect("start set").elapsed().as_nanos() as u64;
        return failed_runtime_bench_record(
            queue_name,
            Mode::Throughput,
            scenario,
            items_per_producer,
            reason,
            elapsed_ns,
        );
    }

    let elapsed_ns = start.get().expect("start set").elapsed().as_nanos() as u64;
    let consumed = consumed_total.load(AtomicOrdering::Relaxed);
    let ops_per_sec = throughput_ops(consumed, elapsed_ns);

    BenchRecord {
        queue: queue_name.to_string(),
        mode: Mode::Throughput.name().to_string(),
        batch_size,
        items_per_producer,
        total_items,
        consumed_items: consumed,
        elapsed_ns,
        ops_per_sec,
        producer_ops_per_sec: None,
        consumer_ops_per_sec: None,
        written_bytes: None,
        flush_count: None,
        push_elapsed_ns: Some(producer_max.load(AtomicOrdering::Relaxed)),
        pop_elapsed_ns: Some(consumer_max.load(AtomicOrdering::Relaxed)),
        fill_elapsed_ns: None,
        drain_elapsed_ns: None,
        avg_data_latency_ns: None,
        producer_fairness_ratio: None,
        consumer_fairness_ratio: None,
        status: BenchRecordStatus::Completed,
        failure_reason: None,
        timeout_ns: None,
    }
}

fn deterministic_busy(thread_id: usize, op_index: u64) {
    let mut value = op_index
        .wrapping_mul(0x9e37_79b9_7f4a_7c15)
        .wrapping_add(thread_id as u64);
    value ^= value >> 33;
    value = value.wrapping_mul(0xff51_afd7_ed55_8ccd);
    let iterations = (value % 101) as usize;
    for _ in 0..iterations {
        std::hint::spin_loop();
    }
}

struct LogSink {
    file: fs::File,
    buffer: Vec<u8>,
    written_bytes: u64,
    flush_count: u64,
}

impl LogSink {
    fn new(
        queue_name: &str,
        scenario: &ScenarioConfig,
        items_per_producer: u64,
    ) -> io::Result<Self> {
        let dir = Path::new("target").join("bench-log-sink");
        fs::create_dir_all(&dir)?;
        let file_name = format!(
            "{}_{}_{}_{}.log",
            sanitize_name(queue_name),
            scenario.name,
            items_per_producer,
            now_unix_nanos()
        );
        Ok(Self {
            file: fs::File::create(dir.join(file_name))?,
            buffer: Vec::with_capacity(LOG_SINK_BUFFER_CAPACITY),
            written_bytes: 0,
            flush_count: 0,
        })
    }

    fn write_record(&mut self, record: LogRecord) -> io::Result<()> {
        if self.buffer.capacity().saturating_sub(self.buffer.len()) < LOG_SINK_MAX_RECORD_BYTES {
            self.flush_buffer()?;
        }
        let before = self.buffer.len();
        append_log_record_line(&mut self.buffer, record);
        debug_assert!(self.buffer.len() - before <= LOG_SINK_MAX_RECORD_BYTES);
        if self.buffer.len() >= LOG_SINK_FLUSH_THRESHOLD {
            self.flush_buffer()?;
        }
        Ok(())
    }

    fn flush_buffer(&mut self) -> io::Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        self.file.write_all(&self.buffer)?;
        self.written_bytes = self
            .written_bytes
            .checked_add(self.buffer.len() as u64)
            .expect("written byte count overflow");
        self.buffer.clear();
        self.flush_count = self
            .flush_count
            .checked_add(1)
            .expect("flush count overflow");
        Ok(())
    }

    fn finish(mut self) -> io::Result<(u64, u64)> {
        self.flush_buffer()?;
        self.file.flush()?;
        Ok((self.written_bytes, self.flush_count))
    }
}

fn append_log_record_line(out: &mut Vec<u8>, record: LogRecord) {
    out.extend_from_slice(b"level=");
    append_decimal_u64(out, unpack_log_level(record.meta) as u64);
    out.extend_from_slice(b" producer=");
    append_decimal_u64(out, unpack_log_producer_id(record.meta));
    out.extend_from_slice(b" seq=");
    append_decimal_u64(out, unpack_log_sequence(record.meta));
    out.extend_from_slice(b" message=");
    out.extend_from_slice(record.message.as_bytes());
    out.push(b'\n');
}

fn append_decimal_u64(out: &mut Vec<u8>, mut value: u64) {
    let mut buf = [0u8; 20];
    let mut index = buf.len();
    loop {
        index -= 1;
        buf[index] = b'0' + (value % 10) as u8;
        value /= 10;
        if value == 0 {
            break;
        }
    }
    out.extend_from_slice(&buf[index..]);
}

fn bench_app_log_mpsc_file_for<Q: LogQueue>(
    queue_name: &str,
    scenario: &ScenarioConfig,
    items_per_producer: u64,
    core_offset: usize,
) -> BenchRecord {
    bench_app_log_mpsc_file_with_queue(
        Q::new_log_queue(),
        queue_name,
        scenario,
        items_per_producer,
        core_offset,
    )
}

fn bench_app_log_mpsc_file_with_queue<Q: LogQueueHandleFactory>(
    queue_handle: Arc<Q>,
    queue_name: &str,
    scenario: &ScenarioConfig,
    items_per_producer: u64,
    core_offset: usize,
) -> BenchRecord {
    if let Err(reason) = validate_mode_for_scenario(Mode::AppLogMpscFile, scenario) {
        return failed_runtime_bench_record(
            queue_name,
            Mode::AppLogMpscFile,
            scenario,
            items_per_producer,
            reason,
            0,
        );
    }

    let total_items = total_items(items_per_producer, scenario.producers);
    let total_threads = scenario.total_threads();
    let ready = Arc::new(Barrier::new(total_threads + 1));
    let start_gate = Arc::new(Barrier::new(total_threads + 1));
    let start = Arc::new(OnceLock::new());
    let producer_max = Arc::new(AtomicU64::new(0));

    let mut producer_handles = Vec::with_capacity(scenario.producers);
    for producer_id in 0..scenario.producers {
        let queue_thread = queue_handle.log_thread_handle();
        let ready = ready.clone();
        let start_gate = start_gate.clone();
        let start = start.clone();
        let producer_max = producer_max.clone();
        let core_id = bench_core_ids().get(core_offset + producer_id).copied();
        producer_handles.push(spawn_bench_thread(move || {
            if let Some(id) = core_id {
                core_affinity::set_for_current(id);
            }
            ready.wait();
            start_gate.wait();
            let start: Instant = *start.get().expect("start set");
            for sequence in 0..items_per_producer {
                queue_thread.send_log(log_record_for(producer_id, sequence));
            }
            producer_max.fetch_max(start.elapsed().as_nanos() as u64, AtomicOrdering::Relaxed);
        }));
    }

    let queue_thread = queue_handle.log_thread_handle();
    let ready_consumer = ready.clone();
    let start_gate_consumer = start_gate.clone();
    let start_consumer = start.clone();
    let queue_name_consumer = queue_name.to_string();
    let scenario_consumer = scenario.clone();
    let core_id = bench_core_ids()
        .get(core_offset + scenario.producers)
        .copied();
    let consumer_handle = spawn_bench_thread(move || -> (u64, u64, u64, u64) {
        if let Some(id) = core_id {
            core_affinity::set_for_current(id);
        }
        let mut sink = LogSink::new(&queue_name_consumer, &scenario_consumer, items_per_producer)
            .expect("failed to create log sink");
        ready_consumer.wait();
        start_gate_consumer.wait();
        let start: Instant = *start_consumer.get().expect("start set");
        let mut consumed = 0u64;
        loop {
            let record = queue_thread.recv_log();
            if record.is_sentinel() {
                break;
            }
            sink.write_record(record)
                .expect("failed to write log record");
            consumed = consumed.checked_add(1).expect("consumed count overflow");
        }
        let (written_bytes, flush_count) = sink.finish().expect("failed to flush log sink");
        let end_ns = start.elapsed().as_nanos() as u64;
        (end_ns, consumed, written_bytes, flush_count)
    });

    ready.wait();
    start.set(Instant::now()).ok();
    start_gate.wait();

    let mut failure_reason = None;
    if let Err(err) = join_bench_threads(producer_handles, "producer") {
        failure_reason.get_or_insert(err);
    }
    if let Err(err) = send_log_sentinel(queue_handle.log_thread_handle(), queue_name) {
        failure_reason.get_or_insert(err);
    }
    let consumer_result = match join_bench_thread(consumer_handle, "consumer") {
        Ok(value) => Some(value),
        Err(err) => {
            failure_reason.get_or_insert(err);
            None
        }
    };
    if let Some(reason) = failure_reason {
        let elapsed_ns = start.get().expect("start set").elapsed().as_nanos() as u64;
        return failed_runtime_bench_record(
            queue_name,
            Mode::AppLogMpscFile,
            scenario,
            items_per_producer,
            reason,
            elapsed_ns,
        );
    }

    let elapsed_ns = start.get().expect("start set").elapsed().as_nanos() as u64;
    let producer_elapsed_ns = producer_max.load(AtomicOrdering::Relaxed);
    let (consumer_elapsed_ns, consumed, written_bytes, flush_count) =
        consumer_result.expect("consumer completed");
    let consumer_ops_per_sec = throughput_ops(consumed, consumer_elapsed_ns);

    BenchRecord {
        queue: queue_name.to_string(),
        mode: Mode::AppLogMpscFile.name().to_string(),
        batch_size: None,
        items_per_producer,
        total_items,
        consumed_items: consumed,
        elapsed_ns,
        ops_per_sec: consumer_ops_per_sec,
        producer_ops_per_sec: throughput_ops(total_items, producer_elapsed_ns),
        consumer_ops_per_sec,
        written_bytes: Some(written_bytes),
        flush_count: Some(flush_count),
        push_elapsed_ns: Some(producer_elapsed_ns),
        pop_elapsed_ns: Some(consumer_elapsed_ns),
        fill_elapsed_ns: None,
        drain_elapsed_ns: None,
        avg_data_latency_ns: None,
        producer_fairness_ratio: None,
        consumer_fairness_ratio: None,
        status: BenchRecordStatus::Completed,
        failure_reason: None,
        timeout_ns: None,
    }
}

struct AppRecord {
    created_ns: u64,
    id: u64,
    hash: u64,
}

fn app_record_ptr(record: AppRecord) -> u64 {
    Box::into_raw(Box::new(record)) as usize as u64
}

unsafe fn app_record_from_ptr(ptr: u64) -> Box<AppRecord> {
    unsafe { Box::from_raw(ptr as usize as *mut AppRecord) }
}

fn app_hash(mut value: u64) -> u64 {
    value ^= value >> 30;
    value = value.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value ^= value >> 27;
    value = value.wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

fn app_work(thread_id: usize, record_id: u64) -> u64 {
    let mixed = app_hash(record_id ^ ((thread_id as u64) << 32));
    deterministic_busy(thread_id, mixed);
    app_hash(mixed)
}

fn bench_app_log_fan_in_for<Q: BenchQueue>(
    queue_name: &str,
    scenario: &ScenarioConfig,
    items_per_producer: u64,
    core_offset: usize,
) -> BenchRecord {
    bench_app_log_fan_in_with_queue(
        Q::new_queue(),
        queue_name,
        scenario,
        items_per_producer,
        core_offset,
    )
}

fn bench_app_log_fan_in_with_queue<Q: BenchQueueHandleFactory>(
    queue_handle: Arc<Q>,
    queue_name: &str,
    scenario: &ScenarioConfig,
    items_per_producer: u64,
    core_offset: usize,
) -> BenchRecord {
    let total_items = total_items(items_per_producer, scenario.producers);
    let total_threads = scenario.total_threads();
    let ready = Arc::new(Barrier::new(total_threads + 1));
    let start_gate = Arc::new(Barrier::new(total_threads + 1));
    let start = Arc::new(OnceLock::new());
    let producer_max = Arc::new(AtomicU64::new(0));
    let consumer_max = Arc::new(AtomicU64::new(0));
    let consumed_total = Arc::new(AtomicU64::new(0));
    let latency_total = Arc::new(AtomicU64::new(0));
    let producer_count = scenario.producers;

    let mut producer_handles = Vec::with_capacity(scenario.producers);
    for producer_id in 0..scenario.producers {
        let queue_thread = queue_handle.thread_handle();
        let ready = ready.clone();
        let start_gate = start_gate.clone();
        let start = start.clone();
        let producer_max = producer_max.clone();
        let core_id = bench_core_ids().get(core_offset + producer_id).copied();
        producer_handles.push(spawn_bench_thread(move || {
            if let Some(id) = core_id {
                core_affinity::set_for_current(id);
            }
            ready.wait();
            start_gate.wait();
            let start: Instant = *start.get().expect("start set");
            let base = (producer_id as u64)
                .checked_mul(items_per_producer)
                .expect("item count overflow");
            for offset in 0..items_per_producer {
                let id = base.checked_add(offset).expect("item count overflow");
                let created_ns = start.elapsed().as_nanos() as u64;
                let hash = app_work(producer_id, id);
                queue_thread.send_value(app_record_ptr(AppRecord {
                    created_ns,
                    id,
                    hash,
                }));
            }
            producer_max.fetch_max(start.elapsed().as_nanos() as u64, AtomicOrdering::Relaxed);
        }));
    }

    let mut consumer_handles = Vec::with_capacity(scenario.consumers);
    for consumer_id in 0..scenario.consumers {
        let queue_thread = queue_handle.thread_handle();
        let ready = ready.clone();
        let start_gate = start_gate.clone();
        let start = start.clone();
        let consumer_max = consumer_max.clone();
        let consumed_total = consumed_total.clone();
        let latency_total = latency_total.clone();
        let core_id = bench_core_ids()
            .get(core_offset + scenario.producers + consumer_id)
            .copied();
        consumer_handles.push(spawn_bench_thread(move || {
            if let Some(id) = core_id {
                core_affinity::set_for_current(id);
            }
            ready.wait();
            start_gate.wait();
            let start: Instant = *start.get().expect("start set");
            loop {
                let ptr = queue_thread.recv_value();
                if ptr == SENTINEL {
                    break;
                }
                let now_ns = start.elapsed().as_nanos() as u64;
                let record = unsafe { app_record_from_ptr(ptr) };
                let digest = app_work(producer_count + consumer_id, record.id ^ record.hash);
                std::hint::black_box(digest);
                latency_total.fetch_add(
                    now_ns.saturating_sub(record.created_ns),
                    AtomicOrdering::Relaxed,
                );
                consumed_total.fetch_add(1, AtomicOrdering::Relaxed);
            }
            consumer_max.fetch_max(start.elapsed().as_nanos() as u64, AtomicOrdering::Relaxed);
        }));
    }

    ready.wait();
    start.set(Instant::now()).ok();
    start_gate.wait();

    let mut failure_reason = None;
    if let Err(err) = join_bench_threads(producer_handles, "producer") {
        failure_reason.get_or_insert(err);
    }
    if let Err(err) = send_sentinels(queue_handle.thread_handle(), scenario.consumers, queue_name) {
        failure_reason.get_or_insert(err);
    }
    if let Err(err) = join_bench_threads(consumer_handles, "consumer") {
        failure_reason.get_or_insert(err);
    }
    if let Some(reason) = failure_reason {
        let elapsed_ns = start.get().expect("start set").elapsed().as_nanos() as u64;
        return failed_runtime_bench_record(
            queue_name,
            Mode::AppLogFanIn,
            scenario,
            items_per_producer,
            reason,
            elapsed_ns,
        );
    }

    let elapsed_ns = start.get().expect("start set").elapsed().as_nanos() as u64;
    let consumed = consumed_total.load(AtomicOrdering::Relaxed);
    BenchRecord {
        queue: queue_name.to_string(),
        mode: Mode::AppLogFanIn.name().to_string(),
        batch_size: None,
        items_per_producer,
        total_items,
        consumed_items: consumed,
        elapsed_ns,
        ops_per_sec: throughput_ops(consumed, elapsed_ns),
        producer_ops_per_sec: None,
        consumer_ops_per_sec: None,
        written_bytes: None,
        flush_count: None,
        push_elapsed_ns: Some(producer_max.load(AtomicOrdering::Relaxed)),
        pop_elapsed_ns: Some(consumer_max.load(AtomicOrdering::Relaxed)),
        fill_elapsed_ns: None,
        drain_elapsed_ns: None,
        avg_data_latency_ns: average_latency_ns(&latency_total, consumed),
        producer_fairness_ratio: None,
        consumer_fairness_ratio: None,
        status: BenchRecordStatus::Completed,
        failure_reason: None,
        timeout_ns: None,
    }
}

fn bench_app_pipeline_for<Q: BenchQueue>(
    queue_name: &str,
    scenario: &ScenarioConfig,
    items_per_producer: u64,
    core_offset: usize,
) -> BenchRecord {
    bench_app_pipeline_with_queues(
        Q::new_queue(),
        Q::new_queue(),
        queue_name,
        scenario,
        items_per_producer,
        core_offset,
    )
}

fn bench_app_pipeline_with_queues<Q: BenchQueueHandleFactory>(
    stage1: Arc<Q>,
    stage2: Arc<Q>,
    queue_name: &str,
    scenario: &ScenarioConfig,
    items_per_producer: u64,
    core_offset: usize,
) -> BenchRecord {
    let total_items = total_items(items_per_producer, scenario.producers);
    let total_threads = scenario
        .total_threads()
        .checked_add(1)
        .expect("pipeline thread count overflow");
    let ready = Arc::new(Barrier::new(total_threads + 1));
    let start_gate = Arc::new(Barrier::new(total_threads + 1));
    let start = Arc::new(OnceLock::new());
    let producer_max = Arc::new(AtomicU64::new(0));
    let collector_end = Arc::new(AtomicU64::new(0));
    let consumed_total = Arc::new(AtomicU64::new(0));
    let latency_total = Arc::new(AtomicU64::new(0));
    let producer_count = scenario.producers;
    let consumer_count = scenario.consumers;

    let mut producer_handles = Vec::with_capacity(scenario.producers);
    for producer_id in 0..scenario.producers {
        let queue_thread = stage1.thread_handle();
        let ready = ready.clone();
        let start_gate = start_gate.clone();
        let start = start.clone();
        let producer_max = producer_max.clone();
        let core_id = bench_core_ids().get(core_offset + producer_id).copied();
        producer_handles.push(spawn_bench_thread(move || {
            if let Some(id) = core_id {
                core_affinity::set_for_current(id);
            }
            ready.wait();
            start_gate.wait();
            let start: Instant = *start.get().expect("start set");
            let base = (producer_id as u64)
                .checked_mul(items_per_producer)
                .expect("item count overflow");
            for offset in 0..items_per_producer {
                let id = base.checked_add(offset).expect("item count overflow");
                let created_ns = start.elapsed().as_nanos() as u64;
                queue_thread.send_value(app_record_ptr(AppRecord {
                    created_ns,
                    id,
                    hash: app_work(producer_id, id),
                }));
            }
            producer_max.fetch_max(start.elapsed().as_nanos() as u64, AtomicOrdering::Relaxed);
        }));
    }

    let mut worker_handles = Vec::with_capacity(scenario.consumers);
    for worker_id in 0..scenario.consumers {
        let input_thread = stage1.thread_handle();
        let output_thread = stage2.thread_handle();
        let ready = ready.clone();
        let start_gate = start_gate.clone();
        let start = start.clone();
        let core_id = bench_core_ids()
            .get(core_offset + producer_count + worker_id)
            .copied();
        worker_handles.push(spawn_bench_thread(move || {
            if let Some(id) = core_id {
                core_affinity::set_for_current(id);
            }
            ready.wait();
            start_gate.wait();
            let _start: Instant = *start.get().expect("start set");
            loop {
                let ptr = input_thread.recv_value();
                if ptr == SENTINEL {
                    break;
                }
                let mut record = unsafe { app_record_from_ptr(ptr) };
                record.hash ^= app_work(producer_count + worker_id, record.id);
                output_thread.send_value(Box::into_raw(record) as usize as u64);
            }
        }));
    }

    let collector = {
        let output_thread = stage2.thread_handle();
        let ready = ready.clone();
        let start_gate = start_gate.clone();
        let start = start.clone();
        let collector_end = collector_end.clone();
        let consumed_total = consumed_total.clone();
        let latency_total = latency_total.clone();
        let core_id = bench_core_ids()
            .get(core_offset + scenario.producers + scenario.consumers)
            .copied();
        spawn_bench_thread(move || {
            if let Some(id) = core_id {
                core_affinity::set_for_current(id);
            }
            ready.wait();
            start_gate.wait();
            let start: Instant = *start.get().expect("start set");
            for _ in 0..total_items {
                let ptr = output_thread.recv_value();
                let now_ns = start.elapsed().as_nanos() as u64;
                let record = unsafe { app_record_from_ptr(ptr) };
                std::hint::black_box(record.hash);
                latency_total.fetch_add(
                    now_ns.saturating_sub(record.created_ns),
                    AtomicOrdering::Relaxed,
                );
                consumed_total.fetch_add(1, AtomicOrdering::Relaxed);
            }
            collector_end.store(start.elapsed().as_nanos() as u64, AtomicOrdering::Relaxed);
        })
    };

    ready.wait();
    start.set(Instant::now()).ok();
    start_gate.wait();

    let mut failure_reason = None;
    if let Err(err) = join_bench_threads(producer_handles, "producer") {
        failure_reason.get_or_insert(err);
    }
    if let Err(err) = send_sentinels(stage1.thread_handle(), consumer_count, queue_name) {
        failure_reason.get_or_insert(err);
    }
    if let Err(err) = join_bench_threads(worker_handles, "worker") {
        failure_reason.get_or_insert(err);
    }
    if let Err(err) = join_bench_thread(collector, "collector") {
        failure_reason.get_or_insert(err);
    }
    if let Some(reason) = failure_reason {
        let elapsed_ns = start.get().expect("start set").elapsed().as_nanos() as u64;
        return failed_runtime_bench_record(
            queue_name,
            Mode::AppPipeline,
            scenario,
            items_per_producer,
            reason,
            elapsed_ns,
        );
    }

    let elapsed_ns = start.get().expect("start set").elapsed().as_nanos() as u64;
    let consumed = consumed_total.load(AtomicOrdering::Relaxed);
    BenchRecord {
        queue: queue_name.to_string(),
        mode: Mode::AppPipeline.name().to_string(),
        batch_size: None,
        items_per_producer,
        total_items,
        consumed_items: consumed,
        elapsed_ns,
        ops_per_sec: throughput_ops(consumed, elapsed_ns),
        producer_ops_per_sec: None,
        consumer_ops_per_sec: None,
        written_bytes: None,
        flush_count: None,
        push_elapsed_ns: Some(producer_max.load(AtomicOrdering::Relaxed)),
        pop_elapsed_ns: Some(collector_end.load(AtomicOrdering::Relaxed)),
        fill_elapsed_ns: None,
        drain_elapsed_ns: None,
        avg_data_latency_ns: average_latency_ns(&latency_total, consumed),
        producer_fairness_ratio: None,
        consumer_fairness_ratio: None,
        status: BenchRecordStatus::Completed,
        failure_reason: None,
        timeout_ns: None,
    }
}

fn bench_app_task_roundtrip_for<Q: BenchQueue>(
    queue_name: &str,
    scenario: &ScenarioConfig,
    items_per_producer: u64,
    core_offset: usize,
) -> BenchRecord {
    bench_app_task_roundtrip_with_queues(
        Q::new_queue(),
        Q::new_queue(),
        queue_name,
        scenario,
        items_per_producer,
        core_offset,
    )
}

fn bench_app_task_roundtrip_with_queues<Q: BenchQueueHandleFactory>(
    request_queue: Arc<Q>,
    response_queue: Arc<Q>,
    queue_name: &str,
    scenario: &ScenarioConfig,
    items_per_producer: u64,
    core_offset: usize,
) -> BenchRecord {
    let total_items = total_items(items_per_producer, scenario.producers);
    let total_threads = scenario.total_threads();
    let ready = Arc::new(Barrier::new(total_threads + 1));
    let start_gate = Arc::new(Barrier::new(total_threads + 1));
    let start = Arc::new(OnceLock::new());
    let client_max = Arc::new(AtomicU64::new(0));
    let worker_max = Arc::new(AtomicU64::new(0));
    let consumed_total = Arc::new(AtomicU64::new(0));
    let latency_total = Arc::new(AtomicU64::new(0));

    let mut worker_handles = Vec::with_capacity(scenario.consumers);
    for worker_id in 0..scenario.consumers {
        let request_thread = request_queue.thread_handle();
        let response_thread = response_queue.thread_handle();
        let ready = ready.clone();
        let start_gate = start_gate.clone();
        let start = start.clone();
        let worker_max = worker_max.clone();
        let core_id = bench_core_ids()
            .get(core_offset + scenario.producers + worker_id)
            .copied();
        worker_handles.push(spawn_bench_thread(move || {
            if let Some(id) = core_id {
                core_affinity::set_for_current(id);
            }
            ready.wait();
            start_gate.wait();
            let start: Instant = *start.get().expect("start set");
            loop {
                let ptr = request_thread.recv_value();
                if ptr == SENTINEL {
                    break;
                }
                let mut record = unsafe { app_record_from_ptr(ptr) };
                record.hash ^= app_work(worker_id, record.id);
                response_thread.send_value(Box::into_raw(record) as usize as u64);
            }
            worker_max.fetch_max(start.elapsed().as_nanos() as u64, AtomicOrdering::Relaxed);
        }));
    }

    let mut client_handles = Vec::with_capacity(scenario.producers);
    for client_id in 0..scenario.producers {
        let request_thread = request_queue.thread_handle();
        let response_thread = response_queue.thread_handle();
        let ready = ready.clone();
        let start_gate = start_gate.clone();
        let start = start.clone();
        let client_max = client_max.clone();
        let consumed_total = consumed_total.clone();
        let latency_total = latency_total.clone();
        let core_id = bench_core_ids().get(core_offset + client_id).copied();
        client_handles.push(spawn_bench_thread(move || {
            if let Some(id) = core_id {
                core_affinity::set_for_current(id);
            }
            ready.wait();
            start_gate.wait();
            let start: Instant = *start.get().expect("start set");
            let base = (client_id as u64)
                .checked_mul(items_per_producer)
                .expect("item count overflow");
            for offset in 0..items_per_producer {
                let id = base.checked_add(offset).expect("item count overflow");
                let created_ns = start.elapsed().as_nanos() as u64;
                request_thread.send_value(app_record_ptr(AppRecord {
                    created_ns,
                    id,
                    hash: app_work(client_id, id),
                }));
                let ptr = response_thread.recv_value();
                let now_ns = start.elapsed().as_nanos() as u64;
                let record = unsafe { app_record_from_ptr(ptr) };
                std::hint::black_box(record.hash);
                latency_total.fetch_add(
                    now_ns.saturating_sub(record.created_ns),
                    AtomicOrdering::Relaxed,
                );
                consumed_total.fetch_add(1, AtomicOrdering::Relaxed);
            }
            client_max.fetch_max(start.elapsed().as_nanos() as u64, AtomicOrdering::Relaxed);
        }));
    }

    ready.wait();
    start.set(Instant::now()).ok();
    start_gate.wait();

    let mut failure_reason = None;
    if let Err(err) = join_bench_threads(client_handles, "client") {
        failure_reason.get_or_insert(err);
    }
    if let Err(err) = send_sentinels(
        request_queue.thread_handle(),
        scenario.consumers,
        queue_name,
    ) {
        failure_reason.get_or_insert(err);
    }
    if let Err(err) = join_bench_threads(worker_handles, "worker") {
        failure_reason.get_or_insert(err);
    }
    if let Some(reason) = failure_reason {
        let elapsed_ns = start.get().expect("start set").elapsed().as_nanos() as u64;
        return failed_runtime_bench_record(
            queue_name,
            Mode::AppTaskRoundtrip,
            scenario,
            items_per_producer,
            reason,
            elapsed_ns,
        );
    }

    let elapsed_ns = start.get().expect("start set").elapsed().as_nanos() as u64;
    let consumed = consumed_total.load(AtomicOrdering::Relaxed);
    BenchRecord {
        queue: queue_name.to_string(),
        mode: Mode::AppTaskRoundtrip.name().to_string(),
        batch_size: None,
        items_per_producer,
        total_items,
        consumed_items: consumed,
        elapsed_ns,
        ops_per_sec: throughput_ops(consumed, elapsed_ns),
        producer_ops_per_sec: None,
        consumer_ops_per_sec: None,
        written_bytes: None,
        flush_count: None,
        push_elapsed_ns: Some(client_max.load(AtomicOrdering::Relaxed)),
        pop_elapsed_ns: Some(worker_max.load(AtomicOrdering::Relaxed)),
        fill_elapsed_ns: None,
        drain_elapsed_ns: None,
        avg_data_latency_ns: average_latency_ns(&latency_total, consumed),
        producer_fairness_ratio: None,
        consumer_fairness_ratio: None,
        status: BenchRecordStatus::Completed,
        failure_reason: None,
        timeout_ns: None,
    }
}

fn average_latency_ns(latency_total: &AtomicU64, consumed: u64) -> Option<f64> {
    if consumed == 0 {
        None
    } else {
        Some(latency_total.load(AtomicOrdering::Relaxed) as f64 / consumed as f64)
    }
}

fn bench_complex_throughput_for<Q: BenchQueue>(
    queue_name: &str,
    scenario: &ScenarioConfig,
    items_per_producer: u64,
    core_offset: usize,
) -> BenchRecord {
    bench_complex_throughput_with_queue(
        Q::new_queue(),
        queue_name,
        scenario,
        items_per_producer,
        core_offset,
    )
}

fn bench_complex_throughput_with_queue<Q: BenchQueueHandleFactory>(
    queue_handle: Arc<Q>,
    queue_name: &str,
    scenario: &ScenarioConfig,
    items_per_producer: u64,
    core_offset: usize,
) -> BenchRecord {
    let total_items = total_items(items_per_producer, scenario.producers);
    let total_threads = scenario.total_threads();
    let ready = Arc::new(Barrier::new(total_threads + 1));
    let start_gate = Arc::new(Barrier::new(total_threads + 1));
    let start = Arc::new(OnceLock::new());
    let producer_max = Arc::new(AtomicU64::new(0));
    let consumer_max = Arc::new(AtomicU64::new(0));
    let consumed_total = Arc::new(AtomicU64::new(0));
    let producer_count = scenario.producers;

    let mut producer_handles = Vec::with_capacity(scenario.producers);
    for producer_id in 0..scenario.producers {
        let queue_thread = queue_handle.thread_handle();
        let ready = ready.clone();
        let start_gate = start_gate.clone();
        let start = start.clone();
        let producer_max = producer_max.clone();
        let core_id = bench_core_ids().get(core_offset + producer_id).copied();
        producer_handles.push(spawn_bench_thread(move || {
            if let Some(id) = core_id {
                core_affinity::set_for_current(id);
            }
            ready.wait();
            start_gate.wait();
            let start: Instant = *start.get().expect("start set");
            let base = (producer_id as u64)
                .checked_mul(items_per_producer)
                .expect("item count overflow");
            for offset in 0..items_per_producer {
                deterministic_busy(producer_id, offset);
                let value = base.checked_add(offset).expect("item count overflow");
                let ptr = Box::into_raw(Box::new(value)) as usize as u64;
                queue_thread.send_value(ptr);
            }
            let end_ns = start.elapsed().as_nanos() as u64;
            producer_max.fetch_max(end_ns, AtomicOrdering::Relaxed);
        }));
    }

    let mut consumer_handles = Vec::with_capacity(scenario.consumers);
    for consumer_id in 0..scenario.consumers {
        let queue_thread = queue_handle.thread_handle();
        let ready = ready.clone();
        let start_gate = start_gate.clone();
        let start = start.clone();
        let consumer_max = consumer_max.clone();
        let consumed_total = consumed_total.clone();
        let core_id = bench_core_ids()
            .get(core_offset + scenario.producers + consumer_id)
            .copied();
        consumer_handles.push(spawn_bench_thread(move || {
            if let Some(id) = core_id {
                core_affinity::set_for_current(id);
            }
            ready.wait();
            start_gate.wait();
            let start: Instant = *start.get().expect("start set");
            loop {
                let ptr = queue_thread.recv_value();
                if ptr == SENTINEL {
                    break;
                }
                deterministic_busy(producer_count + consumer_id, ptr);
                let boxed = unsafe { Box::from_raw(ptr as usize as *mut u64) };
                std::hint::black_box(*boxed);
                consumed_total.fetch_add(1, AtomicOrdering::Relaxed);
            }
            let end_ns = start.elapsed().as_nanos() as u64;
            consumer_max.fetch_max(end_ns, AtomicOrdering::Relaxed);
        }));
    }

    ready.wait();
    start.set(Instant::now()).ok();
    start_gate.wait();

    let mut failure_reason = None;
    if let Err(err) = join_bench_threads(producer_handles, "producer") {
        failure_reason.get_or_insert(err);
    }
    if let Err(err) = send_sentinels(queue_handle.thread_handle(), scenario.consumers, queue_name) {
        failure_reason.get_or_insert(err);
    }
    if let Err(err) = join_bench_threads(consumer_handles, "consumer") {
        failure_reason.get_or_insert(err);
    }
    if let Some(reason) = failure_reason {
        let elapsed_ns = start.get().expect("start set").elapsed().as_nanos() as u64;
        return failed_runtime_bench_record(
            queue_name,
            Mode::ComplexThroughput,
            scenario,
            items_per_producer,
            reason,
            elapsed_ns,
        );
    }

    let elapsed_ns = start.get().expect("start set").elapsed().as_nanos() as u64;
    let consumed = consumed_total.load(AtomicOrdering::Relaxed);
    let ops_per_sec = throughput_ops(consumed, elapsed_ns);

    BenchRecord {
        queue: queue_name.to_string(),
        mode: Mode::ComplexThroughput.name().to_string(),
        batch_size: None,
        items_per_producer,
        total_items,
        consumed_items: consumed,
        elapsed_ns,
        ops_per_sec,
        producer_ops_per_sec: None,
        consumer_ops_per_sec: None,
        written_bytes: None,
        flush_count: None,
        push_elapsed_ns: Some(producer_max.load(AtomicOrdering::Relaxed)),
        pop_elapsed_ns: Some(consumer_max.load(AtomicOrdering::Relaxed)),
        fill_elapsed_ns: None,
        drain_elapsed_ns: None,
        avg_data_latency_ns: None,
        producer_fairness_ratio: None,
        consumer_fairness_ratio: None,
        status: BenchRecordStatus::Completed,
        failure_reason: None,
        timeout_ns: None,
    }
}

fn bench_data_latency_for<Q: BenchQueue>(
    queue_name: &str,
    scenario: &ScenarioConfig,
    items_per_producer: u64,
    core_offset: usize,
) -> BenchRecord {
    bench_data_latency_with_queue(
        Q::new_queue(),
        queue_name,
        scenario,
        items_per_producer,
        core_offset,
    )
}

fn bench_data_latency_with_queue<Q: BenchQueueHandleFactory>(
    queue_handle: Arc<Q>,
    queue_name: &str,
    scenario: &ScenarioConfig,
    items_per_producer: u64,
    core_offset: usize,
) -> BenchRecord {
    let total_items = total_items(items_per_producer, scenario.producers);
    let total_threads = scenario.total_threads();
    let ready = Arc::new(Barrier::new(total_threads + 1));
    let start_gate = Arc::new(Barrier::new(total_threads + 1));
    let start = Arc::new(OnceLock::new());
    let producer_max = Arc::new(AtomicU64::new(0));
    let consumer_max = Arc::new(AtomicU64::new(0));
    let consumed_total = Arc::new(AtomicU64::new(0));
    let latency_total = Arc::new(AtomicU64::new(0));
    let producer_count = scenario.producers;

    let mut producer_handles = Vec::with_capacity(scenario.producers);
    for producer_id in 0..scenario.producers {
        let queue_thread = queue_handle.thread_handle();
        let ready = ready.clone();
        let start_gate = start_gate.clone();
        let start = start.clone();
        let producer_max = producer_max.clone();
        let core_id = bench_core_ids().get(core_offset + producer_id).copied();
        producer_handles.push(spawn_bench_thread(move || {
            if let Some(id) = core_id {
                core_affinity::set_for_current(id);
            }
            ready.wait();
            start_gate.wait();
            let start: Instant = *start.get().expect("start set");
            for offset in 0..items_per_producer {
                deterministic_busy(producer_id, offset);
                let mut boxed = Box::new(0_u64);
                let enqueue_ns = start.elapsed().as_nanos() as u64;
                *boxed = enqueue_ns;
                let ptr = Box::into_raw(boxed) as usize as u64;
                queue_thread.send_value(ptr);
            }
            let end_ns = start.elapsed().as_nanos() as u64;
            producer_max.fetch_max(end_ns, AtomicOrdering::Relaxed);
        }));
    }

    let mut consumer_handles = Vec::with_capacity(scenario.consumers);
    for consumer_id in 0..scenario.consumers {
        let queue_thread = queue_handle.thread_handle();
        let ready = ready.clone();
        let start_gate = start_gate.clone();
        let start = start.clone();
        let consumer_max = consumer_max.clone();
        let consumed_total = consumed_total.clone();
        let latency_total = latency_total.clone();
        let core_id = bench_core_ids()
            .get(core_offset + scenario.producers + consumer_id)
            .copied();
        consumer_handles.push(spawn_bench_thread(move || {
            if let Some(id) = core_id {
                core_affinity::set_for_current(id);
            }
            ready.wait();
            start_gate.wait();
            let start: Instant = *start.get().expect("start set");
            loop {
                let ptr = queue_thread.recv_value();
                if ptr == SENTINEL {
                    break;
                }
                let now_ns = start.elapsed().as_nanos() as u64;
                deterministic_busy(producer_count + consumer_id, ptr);
                let enqueue_ns = unsafe { *Box::from_raw(ptr as usize as *mut u64) };
                latency_total.fetch_add(now_ns.saturating_sub(enqueue_ns), AtomicOrdering::Relaxed);
                consumed_total.fetch_add(1, AtomicOrdering::Relaxed);
            }
            let end_ns = start.elapsed().as_nanos() as u64;
            consumer_max.fetch_max(end_ns, AtomicOrdering::Relaxed);
        }));
    }

    ready.wait();
    start.set(Instant::now()).ok();
    start_gate.wait();

    let mut failure_reason = None;
    if let Err(err) = join_bench_threads(producer_handles, "producer") {
        failure_reason.get_or_insert(err);
    }
    if let Err(err) = send_sentinels(queue_handle.thread_handle(), scenario.consumers, queue_name) {
        failure_reason.get_or_insert(err);
    }
    if let Err(err) = join_bench_threads(consumer_handles, "consumer") {
        failure_reason.get_or_insert(err);
    }
    if let Some(reason) = failure_reason {
        let elapsed_ns = start.get().expect("start set").elapsed().as_nanos() as u64;
        return failed_runtime_bench_record(
            queue_name,
            Mode::DataLatency,
            scenario,
            items_per_producer,
            reason,
            elapsed_ns,
        );
    }

    let elapsed_ns = start.get().expect("start set").elapsed().as_nanos() as u64;
    let consumed = consumed_total.load(AtomicOrdering::Relaxed);
    let ops_per_sec = throughput_ops(consumed, elapsed_ns);
    let avg_data_latency_ns = if consumed == 0 {
        None
    } else {
        Some(latency_total.load(AtomicOrdering::Relaxed) as f64 / consumed as f64)
    };

    BenchRecord {
        queue: queue_name.to_string(),
        mode: Mode::DataLatency.name().to_string(),
        batch_size: None,
        items_per_producer,
        total_items,
        consumed_items: consumed,
        elapsed_ns,
        ops_per_sec,
        producer_ops_per_sec: None,
        consumer_ops_per_sec: None,
        written_bytes: None,
        flush_count: None,
        push_elapsed_ns: Some(producer_max.load(AtomicOrdering::Relaxed)),
        pop_elapsed_ns: Some(consumer_max.load(AtomicOrdering::Relaxed)),
        fill_elapsed_ns: None,
        drain_elapsed_ns: None,
        avg_data_latency_ns,
        producer_fairness_ratio: None,
        consumer_fairness_ratio: None,
        status: BenchRecordStatus::Completed,
        failure_reason: None,
        timeout_ns: None,
    }
}

fn bench_fairness_for<Q: BenchQueue>(
    queue_name: &str,
    scenario: &ScenarioConfig,
    items_per_producer: u64,
    core_offset: usize,
) -> BenchRecord {
    bench_fairness_with_queue(
        Q::new_queue(),
        queue_name,
        scenario,
        items_per_producer,
        core_offset,
    )
}

fn bench_fairness_with_queue<Q: BenchQueueHandleFactory>(
    queue_handle: Arc<Q>,
    queue_name: &str,
    scenario: &ScenarioConfig,
    items_per_producer: u64,
    core_offset: usize,
) -> BenchRecord {
    let total_items = total_items(items_per_producer, scenario.producers);
    let total_threads = scenario.total_threads();
    let ready = Arc::new(Barrier::new(total_threads + 1));
    let start_gate = Arc::new(Barrier::new(total_threads + 1));
    let start = Arc::new(OnceLock::new());

    let mut producer_handles = Vec::with_capacity(scenario.producers);
    for producer_id in 0..scenario.producers {
        let queue_thread = queue_handle.thread_handle();
        let ready = ready.clone();
        let start_gate = start_gate.clone();
        let start = start.clone();
        let core_id = bench_core_ids().get(core_offset + producer_id).copied();
        producer_handles.push(spawn_bench_thread(move || -> u64 {
            if let Some(id) = core_id {
                core_affinity::set_for_current(id);
            }
            ready.wait();
            start_gate.wait();
            let start: Instant = *start.get().expect("start set");
            let base = (producer_id as u64)
                .checked_mul(items_per_producer)
                .expect("item count overflow");
            for offset in 0..items_per_producer {
                let value = base.checked_add(offset).expect("item count overflow");
                queue_thread.send_value(value);
            }
            start.elapsed().as_nanos() as u64
        }));
    }

    let mut consumer_handles = Vec::with_capacity(scenario.consumers);
    for consumer_id in 0..scenario.consumers {
        let queue_thread = queue_handle.thread_handle();
        let ready = ready.clone();
        let start_gate = start_gate.clone();
        let start = start.clone();
        let core_id = bench_core_ids()
            .get(core_offset + scenario.producers + consumer_id)
            .copied();
        consumer_handles.push(spawn_bench_thread(move || -> (u64, u64) {
            if let Some(id) = core_id {
                core_affinity::set_for_current(id);
            }
            ready.wait();
            start_gate.wait();
            let start: Instant = *start.get().expect("start set");
            let mut consumed = 0_u64;
            loop {
                let value = queue_thread.recv_value();
                if value == SENTINEL {
                    break;
                }
                consumed += 1;
            }
            (start.elapsed().as_nanos() as u64, consumed)
        }));
    }

    ready.wait();
    start.set(Instant::now()).ok();
    start_gate.wait();

    let mut failure_reason = None;
    let producer_end_ns = match join_bench_threads(producer_handles, "producer") {
        Ok(values) => values,
        Err(err) => {
            failure_reason.get_or_insert(err);
            Vec::new()
        }
    };
    if let Err(err) = send_sentinels(queue_handle.thread_handle(), scenario.consumers, queue_name) {
        failure_reason.get_or_insert(err);
    }
    let consumer_results = match join_bench_threads(consumer_handles, "consumer") {
        Ok(values) => values,
        Err(err) => {
            failure_reason.get_or_insert(err);
            Vec::new()
        }
    };
    if let Some(reason) = failure_reason {
        let elapsed_ns = start.get().expect("start set").elapsed().as_nanos() as u64;
        return failed_runtime_bench_record(
            queue_name,
            Mode::Fairness,
            scenario,
            items_per_producer,
            reason,
            elapsed_ns,
        );
    }

    let elapsed_ns = start.get().expect("start set").elapsed().as_nanos() as u64;
    let consumed = consumer_results.iter().map(|(_, count)| *count).sum();
    let ops_per_sec = throughput_ops(consumed, elapsed_ns);
    let producer_rates = producer_end_ns
        .iter()
        .filter_map(|&end_ns| throughput_ops(items_per_producer, end_ns))
        .collect::<Vec<_>>();
    let consumer_rates = consumer_results
        .iter()
        .filter_map(|&(end_ns, count)| throughput_ops(count, end_ns))
        .collect::<Vec<_>>();

    BenchRecord {
        queue: queue_name.to_string(),
        mode: Mode::Fairness.name().to_string(),
        batch_size: None,
        items_per_producer,
        total_items,
        consumed_items: consumed,
        elapsed_ns,
        ops_per_sec,
        producer_ops_per_sec: None,
        consumer_ops_per_sec: None,
        written_bytes: None,
        flush_count: None,
        push_elapsed_ns: producer_end_ns.iter().copied().max(),
        pop_elapsed_ns: consumer_results.iter().map(|(end_ns, _)| *end_ns).max(),
        fill_elapsed_ns: None,
        drain_elapsed_ns: None,
        avg_data_latency_ns: None,
        producer_fairness_ratio: fairness_ratio(&producer_rates),
        consumer_fairness_ratio: fairness_ratio(&consumer_rates),
        status: BenchRecordStatus::Completed,
        failure_reason: None,
        timeout_ns: None,
    }
}

fn bench_fill_drain_for<Q: BenchQueue>(
    queue_name: &str,
    scenario: &ScenarioConfig,
    items_per_producer: u64,
    core_offset: usize,
) -> BenchRecord {
    bench_fill_drain_with_queue(
        Q::new_queue(),
        queue_name,
        scenario,
        items_per_producer,
        core_offset,
    )
}

fn bench_fill_drain_with_queue<Q: BenchQueueHandleFactory>(
    queue_handle: Arc<Q>,
    queue_name: &str,
    scenario: &ScenarioConfig,
    items_per_producer: u64,
    core_offset: usize,
) -> BenchRecord {
    let total_items = total_items(items_per_producer, scenario.producers);
    let fill_elapsed = run_producers_only_for(
        &queue_handle,
        scenario.producers,
        items_per_producer,
        core_offset,
    );
    let sentinel_sender = queue_handle.thread_handle();
    for _ in 0..scenario.consumers {
        sentinel_sender.send_value(SENTINEL);
    }
    let (drain_elapsed, consumed) =
        run_consumers_only_for(&queue_handle, scenario.consumers, core_offset);
    let elapsed_ns = (fill_elapsed + drain_elapsed).as_nanos() as u64;
    let ops_per_sec = throughput_ops(consumed, elapsed_ns);
    BenchRecord {
        queue: queue_name.to_string(),
        mode: Mode::FillDrain.name().to_string(),
        batch_size: None,
        items_per_producer,
        total_items,
        consumed_items: consumed,
        elapsed_ns,
        ops_per_sec,
        producer_ops_per_sec: None,
        consumer_ops_per_sec: None,
        written_bytes: None,
        flush_count: None,
        push_elapsed_ns: None,
        pop_elapsed_ns: None,
        fill_elapsed_ns: Some(fill_elapsed.as_nanos() as u64),
        drain_elapsed_ns: Some(drain_elapsed.as_nanos() as u64),
        avg_data_latency_ns: None,
        producer_fairness_ratio: None,
        consumer_fairness_ratio: None,
        status: BenchRecordStatus::Completed,
        failure_reason: None,
        timeout_ns: None,
    }
}

fn fairness_ratio(values: &[f64]) -> Option<f64> {
    let mut min = f64::INFINITY;
    let mut max = 0.0_f64;
    for &value in values {
        if value > 0.0 {
            min = min.min(value);
            max = max.max(value);
        }
    }
    min.is_finite().then_some(max / min)
}

fn throughput_ops(consumed: u64, elapsed_ns: u64) -> Option<f64> {
    if elapsed_ns == 0 || consumed == 0 {
        None
    } else {
        Some(consumed as f64 / (elapsed_ns as f64 / 1_000_000_000.0))
    }
}

fn record_queue_name_for_spec(spec: &JobSpec) -> String {
    match spec.queue {
        QueueKind::Ubq => QueueKind::Ubq.name().to_string(),
        _ => spec.queue_label(),
    }
}

fn failed_bench_record(
    spec: &JobSpec,
    status: BenchRecordStatus,
    reason: String,
    elapsed_ns: u64,
    timeout_ns: Option<u64>,
) -> BenchRecord {
    BenchRecord {
        queue: record_queue_name_for_spec(spec),
        mode: spec.mode.name().to_string(),
        batch_size: spec.batch_size,
        items_per_producer: spec.items_per_producer,
        total_items: total_items(spec.items_per_producer, spec.scenario.producers),
        consumed_items: 0,
        elapsed_ns,
        ops_per_sec: None,
        producer_ops_per_sec: None,
        consumer_ops_per_sec: None,
        written_bytes: None,
        flush_count: None,
        push_elapsed_ns: None,
        pop_elapsed_ns: None,
        fill_elapsed_ns: None,
        drain_elapsed_ns: None,
        avg_data_latency_ns: None,
        producer_fairness_ratio: None,
        consumer_fairness_ratio: None,
        status,
        failure_reason: Some(reason),
        timeout_ns,
    }
}

fn status_and_timeout_for_failure(
    reason: &str,
    elapsed_ns: u64,
) -> (BenchRecordStatus, Option<u64>) {
    let normalized = reason.to_ascii_lowercase();
    if normalized.contains("timed out") || normalized.contains("timeout") {
        (BenchRecordStatus::TimedOut, Some(elapsed_ns))
    } else {
        (BenchRecordStatus::Failed, None)
    }
}

fn failed_runtime_bench_record(
    queue_name: &str,
    mode: Mode,
    scenario: &ScenarioConfig,
    items_per_producer: u64,
    reason: String,
    elapsed_ns: u64,
) -> BenchRecord {
    let (status, timeout_ns) = status_and_timeout_for_failure(&reason, elapsed_ns);
    BenchRecord {
        queue: queue_name.to_string(),
        mode: mode.name().to_string(),
        batch_size: None,
        items_per_producer,
        total_items: total_items(items_per_producer, scenario.producers),
        consumed_items: 0,
        elapsed_ns,
        ops_per_sec: None,
        producer_ops_per_sec: None,
        consumer_ops_per_sec: None,
        written_bytes: None,
        flush_count: None,
        push_elapsed_ns: None,
        pop_elapsed_ns: None,
        fill_elapsed_ns: None,
        drain_elapsed_ns: None,
        avg_data_latency_ns: None,
        producer_fairness_ratio: None,
        consumer_fairness_ratio: None,
        status,
        failure_reason: Some(reason),
        timeout_ns,
    }
}

fn join_bench_thread<T>(handle: thread::JoinHandle<T>, role: &str) -> Result<T, String> {
    handle
        .join()
        .map_err(|payload| format!("{role} join failed: {}", panic_payload_message(payload)))
}

fn join_bench_threads<T>(
    handles: Vec<thread::JoinHandle<T>>,
    role: &str,
) -> Result<Vec<T>, String> {
    let mut values = Vec::with_capacity(handles.len());
    let mut first_error = None;
    for handle in handles {
        match join_bench_thread(handle, role) {
            Ok(value) => values.push(value),
            Err(err) => {
                if first_error.is_none() {
                    first_error = Some(err);
                }
            }
        }
    }
    match first_error {
        Some(err) => Err(err),
        None => Ok(values),
    }
}

fn send_sentinels<T: BenchQueueThreadOps>(
    sender: T,
    consumers: usize,
    queue_name: &str,
) -> Result<(), String> {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
        for _ in 0..consumers {
            sender.send_value(SENTINEL);
        }
    }))
    .map_err(|payload| {
        format!(
            "sending sentinels to {queue_name} failed: {}",
            panic_payload_message(payload)
        )
    })
}

fn send_log_sentinel<T: LogQueueThreadOps>(sender: T, queue_name: &str) -> Result<(), String> {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
        sender.send_log(LogRecord::sentinel());
    }))
    .map_err(|payload| {
        format!(
            "sending log sentinel to {queue_name} failed: {}",
            panic_payload_message(payload)
        )
    })
}

fn panic_payload_message(payload: Box<dyn std::any::Any + Send + 'static>) -> String {
    if let Some(message) = payload.downcast_ref::<&'static str>() {
        (*message).to_string()
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else {
        "benchmark job panicked".to_string()
    }
}

fn run_producers_only_for<Q: BenchQueueHandleFactory>(
    queue_handle: &Arc<Q>,
    producers: usize,
    items_per_producer: u64,
    core_offset: usize,
) -> Duration {
    let ready = Arc::new(Barrier::new(producers + 1));
    let start_gate = Arc::new(Barrier::new(producers + 1));
    let start = Arc::new(OnceLock::new());
    let max_end = Arc::new(AtomicU64::new(0));
    let mut handles = Vec::with_capacity(producers);

    for producer_id in 0..producers {
        let queue_thread = queue_handle.thread_handle();
        let ready = ready.clone();
        let start_gate = start_gate.clone();
        let start = start.clone();
        let max_end = max_end.clone();
        let core_id = bench_core_ids().get(core_offset + producer_id).copied();
        handles.push(spawn_bench_thread(move || {
            if let Some(id) = core_id {
                core_affinity::set_for_current(id);
            }
            ready.wait();
            start_gate.wait();
            let start: Instant = *start.get().expect("start set");
            let base = (producer_id as u64)
                .checked_mul(items_per_producer)
                .expect("item count overflow");
            for offset in 0..items_per_producer {
                let value = base.checked_add(offset).expect("item count overflow");
                queue_thread.send_value(value);
            }
            let end_ns = start.elapsed().as_nanos() as u64;
            max_end.fetch_max(end_ns, AtomicOrdering::Relaxed);
        }));
    }

    ready.wait();
    start.set(Instant::now()).ok();
    start_gate.wait();
    if let Err(err) = join_bench_threads(handles, "producer") {
        panic!("{err}");
    }
    Duration::from_nanos(max_end.load(AtomicOrdering::Relaxed))
}

fn run_consumers_only_for<Q: BenchQueueHandleFactory>(
    queue_handle: &Arc<Q>,
    consumers: usize,
    core_offset: usize,
) -> (Duration, u64) {
    let ready = Arc::new(Barrier::new(consumers + 1));
    let start_gate = Arc::new(Barrier::new(consumers + 1));
    let start = Arc::new(OnceLock::new());
    let max_end = Arc::new(AtomicU64::new(0));
    let consumed_total = Arc::new(AtomicU64::new(0));
    let mut handles = Vec::with_capacity(consumers);

    for consumer_id in 0..consumers {
        let queue_thread = queue_handle.thread_handle();
        let ready = ready.clone();
        let start_gate = start_gate.clone();
        let start = start.clone();
        let max_end = max_end.clone();
        let consumed_total = consumed_total.clone();
        let core_id = bench_core_ids().get(core_offset + consumer_id).copied();
        handles.push(spawn_bench_thread(move || {
            if let Some(id) = core_id {
                core_affinity::set_for_current(id);
            }
            ready.wait();
            start_gate.wait();
            let start: Instant = *start.get().expect("start set");
            loop {
                let value = queue_thread.recv_value();
                if value == SENTINEL {
                    break;
                }
                consumed_total.fetch_add(1, AtomicOrdering::Relaxed);
            }
            let end_ns = start.elapsed().as_nanos() as u64;
            max_end.fetch_max(end_ns, AtomicOrdering::Relaxed);
        }));
    }

    ready.wait();
    start.set(Instant::now()).ok();
    start_gate.wait();
    if let Err(err) = join_bench_threads(handles, "consumer") {
        panic!("{err}");
    }
    (
        Duration::from_nanos(max_end.load(AtomicOrdering::Relaxed)),
        consumed_total.load(AtomicOrdering::Relaxed),
    )
}

fn total_items(items_per_producer: u64, producers: usize) -> u64 {
    let total = items_per_producer
        .checked_mul(producers as u64)
        .unwrap_or_else(|| panic!("total items overflow"));
    if total == SENTINEL {
        panic!("total items must be < u64::MAX");
    }
    total
}

fn now_unix_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_millis()
}

fn host_target() -> String {
    let arch = std::env::consts::ARCH;
    match std::env::consts::OS {
        "macos" => format!("{arch}-apple-darwin"),
        "linux" => format!("{arch}-unknown-linux-gnu"),
        os => format!("{arch}-unknown-{os}"),
    }
}

fn now_unix_nanos() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos()
}

pub fn sanitize_name(raw: &str) -> String {
    let mut out = String::new();
    for ch in raw.chars() {
        if ch.is_ascii_alphanumeric() || matches!(ch, '.' | '_' | '-') {
            out.push(ch.to_ascii_lowercase());
        } else {
            out.push('_');
        }
    }
    out.trim_matches('_').to_string()
}

pub fn detect_available_parallelism() -> Result<usize, String> {
    available_parallelism()
        .ok()
        .map(NonZero::get)
        .ok_or_else(|| "unable to determine available_parallelism".to_string())
}

/// Run a [`MatrixPlan`] fully in-process using the static UBQ registry compiled
/// by the build script (requires the `bench_registry` feature).
///
/// This replaces the old two-process approach (`build_and_run_matrix_plan`) that
/// generated a temporary Cargo project and compiled it at runtime.  All UBQ
/// configurations are monomorphised once at build time, so grid and direct-matrix
/// plans dispatch through precompiled functions with no subprocess overhead.
///
/// Panics inside individual benchmark jobs are caught via
/// `tokio::task::spawn_blocking` join errors and reported as a [`BatchOutcome`]
/// with `exit_success = false` and the crashing job identified, matching the
/// contract expected by the benchmark runners.
pub fn run_matrix_plan_in_process(
    plan: &MatrixPlan,
    dry_run: bool,
) -> Result<BatchOutcome, String> {
    let required_specs = required_job_specs(plan);
    progress_line(format!(
        "bench_matrix: {} bundle(s), {} unique job(s) [in-process]",
        plan.bundles.len(),
        required_specs.len(),
    ));

    if dry_run {
        return Ok(BatchOutcome {
            exit_success: true,
            crashed_job: None,
        });
    }

    // Build a JobFactory for every required spec using the compile-time registry.
    let mut factories: Vec<JobFactory> = Vec::with_capacity(required_specs.len());
    for spec in &required_specs {
        let factory = match spec.queue {
            QueueKind::SegQueue => make_segqueue_job_factory(
                spec.scenario.clone(),
                spec.repeat_index,
                spec.mode,
                spec.items_per_producer,
            ),
            QueueKind::ConcurrentQueue => make_concurrent_queue_job_factory(
                spec.scenario.clone(),
                spec.repeat_index,
                spec.mode,
                spec.items_per_producer,
            ),
            QueueKind::FastFifo => {
                let block_size = spec
                    .fastfifo_block_size
                    .ok_or_else(|| "RBBQ job spec is missing block size".to_string())?;
                #[cfg(feature = "bench_fastfifo")]
                {
                    make_fastfifo_job_factory(
                        block_size,
                        spec.scenario.clone(),
                        spec.repeat_index,
                        spec.mode,
                        spec.items_per_producer,
                    )
                }
                #[cfg(not(feature = "bench_fastfifo"))]
                {
                    let _ = block_size;
                    return Err(
                        "RBBQ selected but the bench_fastfifo/bench_rbbq feature is not enabled; \
                         rebuild with --features bench_registry,bench_rbbq"
                            .to_string(),
                    );
                }
            }
            QueueKind::LfQueue => {
                let segment_size = spec
                    .lfqueue_segment_size
                    .ok_or_else(|| "lfqueue job spec is missing segment size".to_string())?;
                #[cfg(feature = "bench_lfqueue")]
                {
                    make_lfqueue_job_factory(
                        segment_size,
                        spec.scenario.clone(),
                        spec.repeat_index,
                        spec.mode,
                        spec.items_per_producer,
                    )
                }
                #[cfg(not(feature = "bench_lfqueue"))]
                {
                    let _ = segment_size;
                    return Err(
                        "lfqueue selected but the bench_lfqueue feature is not enabled; \
                         rebuild with --features bench_registry,bench_lfqueue"
                            .to_string(),
                    );
                }
            }
            QueueKind::Wcq => {
                let capacity = spec
                    .wcq_capacity
                    .ok_or_else(|| "wCQ job spec is missing capacity".to_string())?;
                #[cfg(feature = "bench_wcq")]
                {
                    make_wcq_job_factory(
                        capacity,
                        spec.scenario.clone(),
                        spec.repeat_index,
                        spec.mode,
                        spec.items_per_producer,
                    )
                    .ok_or_else(|| {
                        format!(
                            "unsupported wCQ capacity {capacity}; supported capacities are \
                             256,1024,4096,16384,65536,262144,1048576,4194304"
                        )
                    })?
                }
                #[cfg(not(feature = "bench_wcq"))]
                {
                    let _ = capacity;
                    return Err("wCQ selected but the bench_wcq feature is not enabled; \
                         rebuild with --features bench_registry,bench_wcq"
                        .to_string());
                }
            }
            QueueKind::Ubq => {
                let label = spec
                    .ubq_label
                    .as_deref()
                    .ok_or_else(|| "UBQ job spec is missing its label".to_string())?;
                lookup_ubq_job_factory(
                    label,
                    spec.scenario.clone(),
                    spec.repeat_index,
                    spec.mode,
                    spec.items_per_producer,
                    spec.batch_size,
                )
                .ok_or_else(|| {
                    format!(
                        "no compiled UBQ configuration for label '{label}'; \
                         rebuild with --features bench_registry"
                    )
                })?
            }
        };
        factories.push(factory);
    }

    let cache = if plan.reuse_existing {
        load_existing_runs(&plan.runs_dir, &plan.machine_label)?
    } else {
        ExistingRunsIndex::default()
    };

    // Drop already-cached specs from the pending list.
    let pending: Vec<JobFactory> = factories
        .into_iter()
        .filter(|f| !cache.records.contains_key(&SampleKey::from_job(&f.spec)))
        .collect();

    progress_line(format!(
        "scheduler: {} bundle(s), {} required, {} cached, {} pending",
        plan.bundles.len(),
        required_specs.len(),
        required_specs.len().saturating_sub(pending.len()),
        pending.len(),
    ));

    let (_, crashed_job) =
        execute_job_factories(plan, &cache, pending, plan.available_parallelism)?;

    Ok(BatchOutcome {
        exit_success: crashed_job.is_none(),
        crashed_job,
    })
}

// Static UBQ registry — generated by build.rs.
// Defines: fn lookup_ubq_job_factory(
//     label, scenario, repeat_index, mode, items_per_producer, batch_size,
// )
//          -> Option<JobFactory>
include!(concat!(env!("OUT_DIR"), "/bench_registry.rs"));

#[cfg(test)]
mod tests {
    use super::*;

    fn test_record(queue: &str, mode: Mode, items_per_producer: u64) -> BenchRecord {
        BenchRecord {
            queue: queue.to_string(),
            mode: mode.name().to_string(),
            batch_size: None,
            items_per_producer,
            total_items: items_per_producer,
            consumed_items: items_per_producer,
            elapsed_ns: 1,
            ops_per_sec: Some(items_per_producer as f64),
            producer_ops_per_sec: None,
            consumer_ops_per_sec: None,
            written_bytes: None,
            flush_count: None,
            push_elapsed_ns: None,
            pop_elapsed_ns: None,
            fill_elapsed_ns: None,
            drain_elapsed_ns: None,
            avg_data_latency_ns: None,
            producer_fairness_ratio: None,
            consumer_fairness_ratio: None,
            status: BenchRecordStatus::Completed,
            failure_reason: None,
            timeout_ns: None,
        }
    }

    #[test]
    fn sparse_and_dense_grids_have_the_approved_dimensions() {
        let sparse = UbqGrid::Sparse.labels();
        let dense = UbqGrid::Dense.labels();

        assert_eq!(sparse.len(), 40);
        assert_eq!(dense.len(), 128);
        assert!(sparse.contains(&"balanced,0,31,crossbeam".to_string()));
        assert!(sparse.contains(&"balanced,64,4095,yield".to_string()));
        assert!(!sparse.contains(&"balanced,2,63,crossbeam".to_string()));
        assert!(dense.contains(&"balanced,2,63,crossbeam".to_string()));
    }

    #[test]
    fn grid_plan_adds_every_batch_size_only_to_ubq_throughput() {
        let plan = build_grid_matrix_plan(
            "local",
            PathBuf::from("runs"),
            2,
            &[
                QueueKind::Ubq,
                QueueKind::SegQueue,
                QueueKind::ConcurrentQueue,
            ],
            UbqGrid::Sparse,
            &DEFAULT_UBQ_BATCH_SIZES,
            &[],
            &[],
            &[],
            &[ScenarioConfig::new(1, 1)],
            &[Mode::Throughput],
            &[10],
            1,
            true,
        )
        .expect("grid plan");
        let specs = required_job_specs(&plan);

        assert_eq!(
            plan.bundles
                .iter()
                .filter(|bundle| bundle.ubq_label.is_some())
                .count(),
            40
        );
        assert_eq!(
            plan.bundles
                .iter()
                .filter(|bundle| bundle.ubq_label.is_none())
                .count(),
            1
        );
        assert_eq!(specs.len(), 482);
        assert_eq!(
            specs
                .iter()
                .filter(|spec| spec.queue == QueueKind::Ubq && spec.batch_size.is_none())
                .count(),
            40
        );
        assert_eq!(
            specs
                .iter()
                .filter(|spec| spec.queue == QueueKind::Ubq && spec.batch_size.is_some())
                .count(),
            440
        );
        assert!(
            specs
                .iter()
                .filter(|spec| spec.queue.is_baseline())
                .all(|spec| spec.batch_size.is_none())
        );
        let baseline_bundle = plan
            .bundles
            .iter()
            .find(|bundle| bundle.ubq_label.is_none())
            .expect("baseline bundle");
        assert_eq!(expected_keys_for_bundle(&plan, baseline_bundle).len(), 2);
        let ubq_bundle = plan
            .bundles
            .iter()
            .find(|bundle| bundle.ubq_label.is_some())
            .expect("UBQ bundle");
        let ubq_keys = expected_keys_for_bundle(&plan, ubq_bundle);
        assert_eq!(ubq_keys.len(), 12);
        assert!(
            ubq_keys
                .iter()
                .all(|key| key.queue_label.starts_with("ubq_"))
        );
    }

    #[test]
    fn scenario_constraints_reduce_the_grid_before_counting_jobs() {
        let plan = build_grid_matrix_plan(
            "local",
            PathBuf::from("runs"),
            65,
            &[QueueKind::Ubq],
            UbqGrid::Sparse,
            &DEFAULT_UBQ_BATCH_SIZES,
            &[],
            &[],
            &[],
            &[ScenarioConfig::new(64, 1)],
            &[Mode::Throughput],
            &[10],
            1,
            true,
        )
        .expect("grid plan");

        assert_eq!(plan.bundles.len(), 32);
        assert_eq!(required_job_specs(&plan).len(), 384);
    }

    #[test]
    fn batched_throughput_job_preserves_values_and_records_its_batch_size() {
        type Queue = ConfiguredUBQ<u64, backoff::Crossbeam, 1, 31, align::A64>;
        let record =
            bench_throughput_batched_for::<Queue>("ubq", &ScenarioConfig::new(1, 1), 257, 16, 0);

        assert_eq!(record.status, BenchRecordStatus::Completed);
        assert_eq!(record.batch_size, Some(16));
        assert_eq!(record.total_items, 257);
        assert_eq!(record.consumed_items, 257);
    }

    #[test]
    fn completion_percentage_includes_cached_jobs() {
        assert_eq!(completion_percent(0, 480), 0.0);
        assert_eq!(completion_percent(120, 480), 25.0);
        assert_eq!(completion_percent(480, 480), 100.0);
    }

    #[test]
    fn scalar_and_batched_samples_have_distinct_cache_keys() {
        let scalar = JobSpec {
            scenario: ScenarioConfig::new(1, 1),
            repeat_index: 1,
            mode: Mode::Throughput,
            items_per_producer: 10,
            queue: QueueKind::Ubq,
            ubq_label: Some("balanced,1,31,crossbeam".to_string()),
            batch_size: None,
            fastfifo_block_size: None,
            lfqueue_segment_size: None,
            wcq_capacity: None,
        };
        let batched = JobSpec {
            batch_size: Some(16),
            ..scalar.clone()
        };

        assert_ne!(SampleKey::from_job(&scalar), SampleKey::from_job(&batched));
        assert_eq!(scalar.queue_label(), batched.queue_label());
    }

    #[test]
    fn parses_four_part_ubq_labels() {
        let parsed = parse_ubq_label("balanced,8,127,crossbeam", true).expect("label");
        assert_eq!(parsed.preset, "balanced");
        assert_eq!(parsed.pool, 8);
        assert_eq!(parsed.block, 127);
        assert_eq!(parsed.backoff, "crossbeam");
    }

    #[test]
    fn scenario_search_excludes_small_blocks_for_high_producer_count() {
        let scenario = ScenarioConfig::new(64, 1);
        let labels = immediate_search_labels_for_scenario("balanced,8,127,crossbeam", &scenario)
            .expect("scenario labels");
        assert!(labels.contains("balanced,8,127,crossbeam"));
        assert!(!labels.contains("balanced,8,63,crossbeam"));
    }

    #[test]
    fn scenario_search_includes_zero_pool_counterpart_for_nonzero_pool() {
        let scenario = ScenarioConfig::new(1, 1);
        let labels = immediate_search_labels_for_scenario("balanced,8,127,crossbeam", &scenario)
            .expect("scenario labels");

        assert!(labels.contains("balanced,0,127,crossbeam"));
    }

    #[test]
    fn parses_fastfifo_aliases_and_block_sizes() {
        assert_eq!(QueueKind::parse("fastfifo"), Some(QueueKind::FastFifo));
        assert_eq!(QueueKind::parse("rbbq"), Some(QueueKind::FastFifo));
        assert_eq!(QueueKind::parse("bbq"), Some(QueueKind::FastFifo));
        assert_eq!(
            parse_fastfifo_block_sizes(Some("64,256,64")).expect("block sizes"),
            vec![64, 256]
        );
        assert_eq!(
            parse_fastfifo_block_sizes(None).expect("default block sizes"),
            vec![64, 256, 1024, 4096]
        );
    }

    #[test]
    fn parses_publication_queue_aliases_and_sizes() {
        assert_eq!(QueueKind::parse("lfqueue"), Some(QueueKind::LfQueue));
        assert_eq!(QueueKind::parse("lscq"), Some(QueueKind::LfQueue));
        assert_eq!(QueueKind::parse("wcq"), Some(QueueKind::Wcq));
        assert_eq!(
            parse_lfqueue_segment_sizes(Some("32,256,32")).expect("segment sizes"),
            vec![32, 256]
        );
        assert_eq!(
            parse_wcq_capacities(Some("4096,65536,4096")).expect("capacities"),
            vec![4096, 65536]
        );
        assert!(parse_wcq_capacities(Some("8192")).is_err());
    }

    #[test]
    fn parses_bbq_atc22_scenario_selectors() {
        let scenarios = parse_scenarios(Some("spsc,mpsc:2-3,spmc:2-3")).expect("scenarios");
        let names = scenarios
            .into_iter()
            .map(|scenario| scenario.name)
            .collect::<Vec<_>>();
        assert_eq!(names, vec!["1p1c", "1p2c", "2p1c", "1p3c", "3p1c"]);

        let full = parse_scenarios(Some(BBQ_ATC22_X86_88T_SCENARIO_SUITE)).expect("suite");
        assert!(full.iter().any(|scenario| scenario.name == "87p1c"));
        assert!(full.iter().any(|scenario| scenario.name == "1p87c"));

        let oversub =
            parse_scenarios(Some(BBQ_ATC22_OVERSUB_X86_12T_SCENARIO_SUITE)).expect("suite");
        assert!(oversub.iter().any(|scenario| scenario.name == "59p1c"));
        assert!(oversub.iter().any(|scenario| scenario.name == "1p59c"));
    }

    #[test]
    fn parses_bbq_atc22_metric_modes() {
        assert_eq!(Mode::parse("complex"), Some(Mode::ComplexThroughput));
        assert_eq!(
            Mode::parse("complex-throughput"),
            Some(Mode::ComplexThroughput)
        );
        assert_eq!(Mode::parse("data-latency"), Some(Mode::DataLatency));
        assert_eq!(Mode::parse("fairness"), Some(Mode::Fairness));
    }

    #[test]
    fn parses_application_metric_modes() {
        assert_eq!(Mode::parse("app_log_fan_in"), Some(Mode::AppLogFanIn));
        assert_eq!(Mode::parse("app-log-fan-in"), Some(Mode::AppLogFanIn));
        assert_eq!(Mode::parse("app_pipeline"), Some(Mode::AppPipeline));
        assert_eq!(Mode::parse("app-pipeline"), Some(Mode::AppPipeline));
        assert_eq!(
            Mode::parse("app_task_roundtrip"),
            Some(Mode::AppTaskRoundtrip)
        );
        assert_eq!(
            Mode::parse("app-task-roundtrip"),
            Some(Mode::AppTaskRoundtrip)
        );
        assert_eq!(Mode::parse("app_log_mpsc_file"), Some(Mode::AppLogMpscFile));
        assert_eq!(Mode::parse("app-log-mpsc-file"), Some(Mode::AppLogMpscFile));
    }

    #[test]
    fn parses_machine_mpsc_scenarios_with_reserved_writer() {
        let names_16 = parse_scenarios_with_parallelism(Some("mpsc:machine"), 16)
            .expect("machine scenarios")
            .into_iter()
            .map(|scenario| scenario.name)
            .collect::<Vec<_>>();
        assert_eq!(names_16, vec!["1p1c", "2p1c", "4p1c", "8p1c", "15p1c"]);

        let names_160 = parse_scenarios_with_parallelism(Some("mpsc:machine"), 160)
            .expect("machine scenarios")
            .into_iter()
            .map(|scenario| scenario.name)
            .collect::<Vec<_>>();
        assert_eq!(
            names_160,
            vec![
                "1p1c", "2p1c", "4p1c", "8p1c", "16p1c", "32p1c", "64p1c", "128p1c", "159p1c"
            ]
        );
    }

    #[test]
    fn app_log_mpsc_file_requires_one_consumer() {
        let err = build_direct_matrix_plan(
            "local",
            PathBuf::from(DEFAULT_RUNS_DIR),
            8,
            &[QueueKind::SegQueue],
            &[],
            &[],
            &[],
            &[],
            &[ScenarioConfig::new(2, 2)],
            &[Mode::AppLogMpscFile],
            &[1],
            1,
            false,
        )
        .expect_err("expected MPSC validation error");
        assert!(err.contains("exactly one consumer"));
    }

    #[test]
    fn log_meta_roundtrips_and_writer_renders_line() {
        let record = LogRecord {
            message: "static message",
            meta: pack_log_meta(3, 42, 99),
        };
        assert_eq!(unpack_log_level(record.meta), 3);
        assert_eq!(unpack_log_producer_id(record.meta), 42);
        assert_eq!(unpack_log_sequence(record.meta), 99);

        let mut out = Vec::with_capacity(LOG_SINK_MAX_RECORD_BYTES);
        append_log_record_line(&mut out, record);
        assert_eq!(
            std::str::from_utf8(&out).expect("utf8"),
            "level=3 producer=42 seq=99 message=static message\n"
        );
    }

    #[test]
    fn app_log_mpsc_file_completes_small_segqueue_run() {
        let scenario = ScenarioConfig::new(2, 1);
        let record =
            bench_app_log_mpsc_file_for::<SegQueue<LogRecord>>("segqueue", &scenario, 16, 0);
        assert_eq!(record.mode, Mode::AppLogMpscFile.name());
        assert_eq!(record.consumed_items, record.total_items);
        assert!(record.ops_per_sec.is_some());
        assert!(record.producer_ops_per_sec.is_some());
        assert!(record.consumer_ops_per_sec.is_some());
        assert!(record.written_bytes.is_some_and(|value| value > 0));
        assert!(record.flush_count.is_some_and(|value| value > 0));
        assert!(record.push_elapsed_ns.is_some());
        assert!(record.pop_elapsed_ns.is_some());
    }

    #[test]
    fn application_modes_complete_small_segqueue_runs() {
        for scenario in [ScenarioConfig::new(1, 1), ScenarioConfig::new(2, 2)] {
            for mode in [Mode::AppLogFanIn, Mode::AppPipeline, Mode::AppTaskRoundtrip] {
                let record = match mode {
                    Mode::AppLogFanIn => {
                        bench_app_log_fan_in_for::<SegQueue<u64>>("segqueue", &scenario, 32, 0)
                    }
                    Mode::AppPipeline => {
                        bench_app_pipeline_for::<SegQueue<u64>>("segqueue", &scenario, 32, 0)
                    }
                    Mode::AppTaskRoundtrip => {
                        bench_app_task_roundtrip_for::<SegQueue<u64>>("segqueue", &scenario, 32, 0)
                    }
                    _ => unreachable!(),
                };
                assert_eq!(record.mode, mode.name());
                assert_eq!(record.consumed_items, record.total_items);
                assert!(record.ops_per_sec.is_some());
                assert!(record.avg_data_latency_ns.is_some());
                assert!(record.push_elapsed_ns.is_some());
                assert!(record.pop_elapsed_ns.is_some());
            }
        }
    }

    #[test]
    fn direct_plan_expands_fastfifo_block_variants() {
        let plan = build_direct_matrix_plan(
            "local",
            PathBuf::from(DEFAULT_RUNS_DIR),
            16,
            &[QueueKind::FastFifo],
            &[],
            &[64, 256],
            &[],
            &[],
            &[ScenarioConfig::new(1, 1)],
            &[Mode::Throughput],
            &[1],
            1,
            false,
        )
        .expect("plan");
        assert_eq!(plan.fastfifo_block_sizes, vec![64, 256]);
        let keys = expected_keys_for_bundle(&plan, &plan.bundles[0]);
        let labels = keys
            .into_iter()
            .map(|key| key.queue_label)
            .collect::<Vec<_>>();
        assert_eq!(labels, vec!["fastfifo_64", "fastfifo_256"]);
    }

    #[test]
    fn direct_plan_expands_publication_queue_variants() {
        let plan = build_direct_matrix_plan(
            "local",
            PathBuf::from(DEFAULT_RUNS_DIR),
            16,
            &[QueueKind::LfQueue, QueueKind::Wcq],
            &[],
            &[],
            &[32, 256],
            &[4096, 65536],
            &[ScenarioConfig::new(1, 1)],
            &[Mode::FillDrain],
            &[1],
            1,
            false,
        )
        .expect("plan");
        let keys = expected_keys_for_bundle(&plan, &plan.bundles[0]);
        let labels = keys
            .into_iter()
            .map(|key| key.queue_label)
            .collect::<Vec<_>>();
        assert_eq!(
            labels,
            vec!["lfqueue_32", "lfqueue_256", "wcq_4096", "wcq_65536"]
        );
    }

    #[test]
    fn direct_plan_schedules_application_modes_but_excludes_wcq() {
        let plan = build_direct_matrix_plan(
            "local",
            PathBuf::from(DEFAULT_RUNS_DIR),
            8,
            &[QueueKind::SegQueue, QueueKind::LfQueue, QueueKind::Wcq],
            &[],
            &[],
            &[32],
            &[4096],
            &[ScenarioConfig::new(2, 2)],
            &[Mode::AppLogFanIn, Mode::AppPipeline, Mode::AppTaskRoundtrip],
            &[1],
            1,
            false,
        )
        .expect("plan");
        let keys = expected_keys_for_bundle(&plan, &plan.bundles[0]);
        assert_eq!(keys.len(), 6);
        assert!(keys.iter().all(|key| !key.queue_label.starts_with("wcq_")));
        for mode in [Mode::AppLogFanIn, Mode::AppPipeline, Mode::AppTaskRoundtrip] {
            assert!(
                keys.iter()
                    .any(|key| key.mode == mode && key.queue_label == "segqueue")
            );
            assert!(
                keys.iter()
                    .any(|key| key.mode == mode && key.queue_label == "lfqueue_32")
            );
        }
    }

    #[test]
    fn direct_plan_omits_wcq_fill_drain_when_capacity_cannot_hold_prefill() {
        let plan = build_direct_matrix_plan(
            "local",
            PathBuf::from(DEFAULT_RUNS_DIR),
            16,
            &[QueueKind::Wcq],
            &[],
            &[],
            &[],
            &[4096],
            &[ScenarioConfig::new(8, 8)],
            &[Mode::FillDrain],
            &[1000],
            1,
            false,
        )
        .expect("plan");
        let keys = expected_keys_for_bundle(&plan, &plan.bundles[0]);
        assert!(keys.is_empty());
    }

    #[test]
    fn direct_plan_requires_ubq_labels_if_ubq_selected() {
        let err = build_direct_matrix_plan(
            "local",
            PathBuf::from(DEFAULT_RUNS_DIR),
            16,
            &[QueueKind::Ubq, QueueKind::SegQueue],
            &[],
            &[],
            &[],
            &[],
            &[ScenarioConfig::new(1, 1)],
            &[Mode::Throughput],
            &[1],
            1,
            false,
        )
        .expect_err("expected validation error");
        assert!(err.contains("--ubq-label"));
    }

    #[test]
    fn direct_plan_skips_labels_incompatible_with_scenario() {
        // block=63 is too small for 64 producers — the incompatible combo must be
        // silently skipped rather than aborting the whole matrix.
        let plan = build_direct_matrix_plan(
            "local",
            PathBuf::from(DEFAULT_RUNS_DIR),
            128,
            &[QueueKind::Ubq],
            &["balanced,8,63,crossbeam".to_string()],
            &[],
            &[],
            &[],
            &[ScenarioConfig::new(64, 1)],
            &[Mode::Throughput],
            &[1],
            1,
            false,
        )
        .expect("plan must succeed even when some labels are incompatible with some scenarios");
        assert!(
            plan.bundles.is_empty(),
            "no bundles should be emitted for the incompatible (block=63, 64p1c) pair"
        );
    }

    #[test]
    fn serialization_omits_none_fields() {
        let output = OutputFile {
            schema_version: RUN_SCHEMA_VERSION,
            meta: OutputMeta {
                timestamp_unix_ms: 1,
                machine_label: "local".to_string(),
                scenario: "1p1c".to_string(),
                producers: 1,
                consumers: 1,
                repeat_index: 1,
                available_parallelism: 2,
                ubq_label: None,
                ubq_block_size: None,
                ubq_grid: None,
                expected_ubq_configurations: None,
                ubq_batch_sizes: Vec::new(),
                planned_repeats: None,
                planned_items_per_producer: Vec::new(),
            },
            results: vec![BenchRecord {
                queue: "segqueue".to_string(),
                mode: "throughput".to_string(),
                batch_size: None,
                items_per_producer: 1,
                total_items: 1,
                consumed_items: 1,
                elapsed_ns: 1,
                ops_per_sec: Some(1.0),
                producer_ops_per_sec: None,
                consumer_ops_per_sec: None,
                written_bytes: None,
                flush_count: None,
                push_elapsed_ns: None,
                pop_elapsed_ns: None,
                fill_elapsed_ns: None,
                drain_elapsed_ns: None,
                avg_data_latency_ns: None,
                producer_fairness_ratio: None,
                consumer_fairness_ratio: None,
                status: BenchRecordStatus::Completed,
                failure_reason: None,
                timeout_ns: None,
            }],
        };
        let json = serde_json::to_string(&output).expect("json");
        assert!(!json.contains("null"));
        assert!(!json.contains("ubq_label"));
        assert!(!json.contains("fill_elapsed_ns"));
        assert!(!json.contains("status"));
    }

    #[test]
    fn incremental_writer_persists_partial_bundle_snapshots() {
        let root =
            std::env::temp_dir().join(format!("ubq_partial_snapshot_test_{}", now_unix_nanos()));
        let runs_dir = root.join("runs");
        fs::create_dir_all(&runs_dir).expect("mkdir");

        let plan = MatrixPlan {
            plan_schema_version: PLAN_SCHEMA_VERSION,
            machine_label: "local".to_string(),
            runs_dir: runs_dir.clone(),
            available_parallelism: 2,
            baseline_queues: vec![QueueKind::SegQueue],
            fastfifo_block_sizes: Vec::new(),
            lfqueue_segment_sizes: Vec::new(),
            wcq_capacities: Vec::new(),
            ubq_grid: None,
            ubq_batch_sizes: Vec::new(),
            planned_repeats: 1,
            bundles: vec![PlanBundle {
                scenario: ScenarioConfig::new(1, 1),
                repeat_index: 1,
                ubq_label: Some("balanced,1,31,crossbeam".to_string()),
                modes: vec![Mode::Throughput],
                items_per_producer_values: vec![1],
            }],
            reuse_existing: true,
        };

        let key = SampleKey {
            scenario: "1p1c".to_string(),
            repeat_index: 1,
            mode: Mode::Throughput,
            items_per_producer: 1,
            queue_label: "segqueue".to_string(),
            batch_size: None,
        };

        let writer =
            IncrementalOutputWriter::new(&plan, &ExistingRunsIndex::default()).expect("writer");
        let writer = {
            let mut writer = writer;
            writer
                .handle_completed_record(key.clone(), test_record("segqueue", Mode::Throughput, 1))
                .expect("write partial snapshot");
            writer
        };

        let loaded = load_existing_runs(&runs_dir, "local").expect("load");
        assert_eq!(
            loaded.records.get(&key).expect("cached record").queue,
            "segqueue"
        );
        assert_eq!(loaded.records.len(), 1);

        writer.finish(false).expect("finish writer");
        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn execute_job_factories_does_not_duplicate_cached_bundles() {
        let root =
            std::env::temp_dir().join(format!("ubq_cached_bundle_test_{}", now_unix_nanos()));
        let runs_dir = root.join("runs");
        fs::create_dir_all(&runs_dir).expect("mkdir");

        let scenario = ScenarioConfig::new(1, 1);
        let plan = MatrixPlan {
            plan_schema_version: PLAN_SCHEMA_VERSION,
            machine_label: "local".to_string(),
            runs_dir: runs_dir.clone(),
            available_parallelism: 2,
            baseline_queues: vec![QueueKind::SegQueue, QueueKind::ConcurrentQueue],
            fastfifo_block_sizes: Vec::new(),
            lfqueue_segment_sizes: Vec::new(),
            wcq_capacities: Vec::new(),
            ubq_grid: None,
            ubq_batch_sizes: Vec::new(),
            planned_repeats: 1,
            bundles: vec![PlanBundle {
                scenario: scenario.clone(),
                repeat_index: 1,
                ubq_label: None,
                modes: vec![Mode::Throughput],
                items_per_producer_values: vec![1],
            }],
            reuse_existing: true,
        };

        let segqueue_key = SampleKey {
            scenario: scenario.name.clone(),
            repeat_index: 1,
            mode: Mode::Throughput,
            items_per_producer: 1,
            queue_label: "segqueue".to_string(),
            batch_size: None,
        };
        let concurrent_key = SampleKey {
            scenario: scenario.name.clone(),
            repeat_index: 1,
            mode: Mode::Throughput,
            items_per_producer: 1,
            queue_label: "concurrent-queue".to_string(),
            batch_size: None,
        };
        let mut cache = ExistingRunsIndex::default();
        cache.records.insert(
            segqueue_key.clone(),
            test_record("segqueue", Mode::Throughput, 1),
        );
        cache.records.insert(
            concurrent_key.clone(),
            test_record("concurrent-queue", Mode::Throughput, 1),
        );

        let (executed, crashed) =
            execute_job_factories(&plan, &cache, Vec::new(), 2).expect("execute");
        assert!(executed.is_empty());
        assert!(crashed.is_none());

        let mut files = Vec::new();
        collect_run_jsons_recursive(&runs_dir, &mut files).expect("scan runs");
        assert!(
            files.is_empty(),
            "a no-op resume must not rewrite cached samples"
        );

        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn execute_job_factories_can_run_multiple_ubq_jobs_concurrently() {
        let root = std::env::temp_dir().join(format!("ubq_parallel_ubq_test_{}", now_unix_nanos()));
        let runs_dir = root.join("runs");
        fs::create_dir_all(&runs_dir).expect("mkdir");

        let scenario = ScenarioConfig::new(1, 1);
        let ubq_label = "balanced,1,31,crossbeam".to_string();
        let plan = MatrixPlan {
            plan_schema_version: PLAN_SCHEMA_VERSION,
            machine_label: "local".to_string(),
            runs_dir: runs_dir.clone(),
            available_parallelism: 4,
            baseline_queues: Vec::new(),
            fastfifo_block_sizes: Vec::new(),
            lfqueue_segment_sizes: Vec::new(),
            wcq_capacities: Vec::new(),
            ubq_grid: None,
            ubq_batch_sizes: Vec::new(),
            planned_repeats: 1,
            bundles: vec![
                PlanBundle {
                    scenario: scenario.clone(),
                    repeat_index: 1,
                    ubq_label: Some(ubq_label.clone()),
                    modes: vec![Mode::Throughput],
                    items_per_producer_values: vec![1],
                },
                PlanBundle {
                    scenario: scenario.clone(),
                    repeat_index: 2,
                    ubq_label: Some(ubq_label.clone()),
                    modes: vec![Mode::Throughput],
                    items_per_producer_values: vec![1],
                },
            ],
            reuse_existing: false,
        };

        let active = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let max_active = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let start_count = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let make_job = |repeat_index| {
            let active = std::sync::Arc::clone(&active);
            let max_active = std::sync::Arc::clone(&max_active);
            let start_count = std::sync::Arc::clone(&start_count);
            JobFactory {
                spec: JobSpec {
                    scenario: scenario.clone(),
                    repeat_index,
                    mode: Mode::Throughput,
                    items_per_producer: 1,
                    queue: QueueKind::Ubq,
                    ubq_label: Some(ubq_label.clone()),
                    batch_size: None,
                    fastfifo_block_size: None,
                    lfqueue_segment_size: None,
                    wcq_capacity: None,
                },
                run: std::sync::Arc::new(move |_| {
                    start_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    let now_active = active.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
                    max_active.fetch_max(now_active, std::sync::atomic::Ordering::SeqCst);

                    let deadline =
                        std::time::Instant::now() + std::time::Duration::from_millis(200);
                    while start_count.load(std::sync::atomic::Ordering::SeqCst) < 2
                        && std::time::Instant::now() < deadline
                    {
                        std::thread::sleep(std::time::Duration::from_millis(5));
                    }
                    std::thread::sleep(std::time::Duration::from_millis(20));

                    active.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
                    test_record("ubq", Mode::Throughput, 1)
                }),
            }
        };

        let pending = vec![make_job(1), make_job(2)];
        let (executed, crashed) = execute_job_factories(
            &plan,
            &ExistingRunsIndex::default(),
            pending,
            plan.available_parallelism,
        )
        .expect("execute");

        assert!(crashed.is_none());
        assert_eq!(executed.len(), 2);
        assert_eq!(start_count.load(std::sync::atomic::Ordering::SeqCst), 2);
        assert!(
            max_active.load(std::sync::atomic::Ordering::SeqCst) >= 2,
            "expected overlapping UBQ execution when thread budget allows it"
        );

        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn execute_job_factories_records_timeout_and_continues() {
        let root =
            std::env::temp_dir().join(format!("ubq_timeout_record_test_{}", now_unix_nanos()));
        let runs_dir = root.join("runs");
        fs::create_dir_all(&runs_dir).expect("mkdir");

        let scenario = ScenarioConfig::new(1, 1);
        let plan = MatrixPlan {
            plan_schema_version: PLAN_SCHEMA_VERSION,
            machine_label: "local".to_string(),
            runs_dir: runs_dir.clone(),
            available_parallelism: 4,
            baseline_queues: vec![QueueKind::SegQueue, QueueKind::ConcurrentQueue],
            fastfifo_block_sizes: Vec::new(),
            lfqueue_segment_sizes: Vec::new(),
            wcq_capacities: Vec::new(),
            ubq_grid: None,
            ubq_batch_sizes: Vec::new(),
            planned_repeats: 1,
            bundles: vec![PlanBundle {
                scenario: scenario.clone(),
                repeat_index: 1,
                ubq_label: None,
                modes: vec![Mode::Throughput],
                items_per_producer_values: vec![1],
            }],
            reuse_existing: false,
        };

        let slow_spec = JobSpec {
            scenario: scenario.clone(),
            repeat_index: 1,
            mode: Mode::Throughput,
            items_per_producer: 1,
            queue: QueueKind::SegQueue,
            ubq_label: None,
            batch_size: None,
            fastfifo_block_size: None,
            lfqueue_segment_size: None,
            wcq_capacity: None,
        };
        let fast_spec = JobSpec {
            queue: QueueKind::ConcurrentQueue,
            ..slow_spec.clone()
        };
        let pending = vec![
            JobFactory {
                spec: slow_spec.clone(),
                run: Arc::new(move |_| {
                    std::thread::sleep(Duration::from_millis(80));
                    test_record("segqueue", Mode::Throughput, 1)
                }),
            },
            JobFactory {
                spec: fast_spec.clone(),
                run: Arc::new(move |_| test_record("concurrent-queue", Mode::Throughput, 1)),
            },
        ];

        let (executed, crashed) = execute_job_factories_with_timeout(
            &plan,
            &ExistingRunsIndex::default(),
            pending,
            plan.available_parallelism,
            Duration::from_millis(10),
        )
        .expect("execute");

        assert!(crashed.is_none());
        let slow_key = SampleKey::from_job(&slow_spec);
        let fast_key = SampleKey::from_job(&fast_spec);
        let slow_record = executed.get(&slow_key).expect("timeout record");
        assert_eq!(slow_record.status, BenchRecordStatus::TimedOut);
        assert!(
            slow_record
                .failure_reason
                .as_deref()
                .unwrap()
                .contains("timeout")
        );
        assert!(slow_record.ops_per_sec.is_none());
        assert_eq!(
            executed.get(&fast_key).expect("fast record").status,
            BenchRecordStatus::Completed
        );

        let loaded = load_existing_runs(&runs_dir, "local").expect("load");
        assert!(
            !loaded.records.contains_key(&slow_key),
            "timed-out jobs must remain eligible for retry after resume"
        );
        assert!(loaded.records.contains_key(&fast_key));

        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn frontier_bootstraps_seed_across_all_scenarios() {
        let config = FrontierConfig {
            machine_label: "local".to_string(),
            runs_dir: PathBuf::from(DEFAULT_RUNS_DIR),
            scenarios: vec![ScenarioConfig::new(1, 1), ScenarioConfig::new(1, 4)],
            baseline_queues: vec![QueueKind::SegQueue, QueueKind::ConcurrentQueue],
            fastfifo_block_sizes: Vec::new(),
            lfqueue_segment_sizes: Vec::new(),
            wcq_capacities: Vec::new(),
            seed_labels: vec!["balanced,8,127,crossbeam".to_string()],
            modes: vec![Mode::Throughput],
            items_per_producer_values: vec![1],
            repeats: 2,
            available_parallelism: 8,
        };
        let plan =
            compute_frontier_round_plan(&config, &ExistingRunsIndex::default(), &BTreeSet::new())
                .expect("plan");
        assert_eq!(plan.bundles.len(), 4);
    }

    #[test]
    fn frontier_expands_local_winners_only() {
        let scenario = ScenarioConfig::new(1, 1);
        let mut index = ExistingRunsIndex::default();
        for repeat_index in 1..=2 {
            index.records.insert(
                SampleKey {
                    scenario: scenario.name.clone(),
                    repeat_index,
                    mode: Mode::Throughput,
                    items_per_producer: 1,
                    queue_label: "segqueue".to_string(),
                    batch_size: None,
                },
                BenchRecord {
                    queue: "segqueue".to_string(),
                    mode: "throughput".to_string(),
                    batch_size: None,
                    items_per_producer: 1,
                    total_items: 1,
                    consumed_items: 1,
                    elapsed_ns: 10,
                    ops_per_sec: Some(10.0),
                    producer_ops_per_sec: None,
                    consumer_ops_per_sec: None,
                    written_bytes: None,
                    flush_count: None,
                    push_elapsed_ns: None,
                    pop_elapsed_ns: None,
                    fill_elapsed_ns: None,
                    drain_elapsed_ns: None,
                    avg_data_latency_ns: None,
                    producer_fairness_ratio: None,
                    consumer_fairness_ratio: None,
                    status: BenchRecordStatus::Completed,
                    failure_reason: None,
                    timeout_ns: None,
                },
            );
            index.records.insert(
                SampleKey {
                    scenario: scenario.name.clone(),
                    repeat_index,
                    mode: Mode::Throughput,
                    items_per_producer: 1,
                    queue_label: "concurrent-queue".to_string(),
                    batch_size: None,
                },
                BenchRecord {
                    queue: "concurrent-queue".to_string(),
                    mode: "throughput".to_string(),
                    batch_size: None,
                    items_per_producer: 1,
                    total_items: 1,
                    consumed_items: 1,
                    elapsed_ns: 9,
                    ops_per_sec: Some(9.0),
                    producer_ops_per_sec: None,
                    consumer_ops_per_sec: None,
                    written_bytes: None,
                    flush_count: None,
                    push_elapsed_ns: None,
                    pop_elapsed_ns: None,
                    fill_elapsed_ns: None,
                    drain_elapsed_ns: None,
                    avg_data_latency_ns: None,
                    producer_fairness_ratio: None,
                    consumer_fairness_ratio: None,
                    status: BenchRecordStatus::Completed,
                    failure_reason: None,
                    timeout_ns: None,
                },
            );
            index.records.insert(
                SampleKey {
                    scenario: scenario.name.clone(),
                    repeat_index,
                    mode: Mode::Throughput,
                    items_per_producer: 1,
                    queue_label: "ubq_balanced,8,127,crossbeam".to_string(),
                    batch_size: None,
                },
                BenchRecord {
                    queue: "ubq".to_string(),
                    mode: "throughput".to_string(),
                    batch_size: None,
                    items_per_producer: 1,
                    total_items: 1,
                    consumed_items: 1,
                    elapsed_ns: 20,
                    ops_per_sec: Some(20.0),
                    producer_ops_per_sec: None,
                    consumer_ops_per_sec: None,
                    written_bytes: None,
                    flush_count: None,
                    push_elapsed_ns: None,
                    pop_elapsed_ns: None,
                    fill_elapsed_ns: None,
                    drain_elapsed_ns: None,
                    avg_data_latency_ns: None,
                    producer_fairness_ratio: None,
                    consumer_fairness_ratio: None,
                    status: BenchRecordStatus::Completed,
                    failure_reason: None,
                    timeout_ns: None,
                },
            );
        }

        let config = FrontierConfig {
            machine_label: "local".to_string(),
            runs_dir: PathBuf::from(DEFAULT_RUNS_DIR),
            scenarios: vec![scenario.clone()],
            baseline_queues: vec![QueueKind::SegQueue, QueueKind::ConcurrentQueue],
            fastfifo_block_sizes: Vec::new(),
            lfqueue_segment_sizes: Vec::new(),
            wcq_capacities: Vec::new(),
            seed_labels: vec!["balanced,8,127,crossbeam".to_string()],
            modes: vec![Mode::Throughput],
            items_per_producer_values: vec![1],
            repeats: 2,
            available_parallelism: 8,
        };
        let plan = compute_frontier_round_plan(&config, &index, &BTreeSet::new()).expect("plan");
        assert!(
            plan.bundles
                .iter()
                .any(|bundle| bundle.ubq_label.as_deref() == Some("balanced,8,127,yield"))
        );
        assert!(
            plan.bundles
                .iter()
                .any(|bundle| bundle.ubq_label.as_deref() == Some("balanced,0,127,crossbeam"))
        );
    }

    #[test]
    fn frontier_expands_local_best_ubq_even_when_baseline_wins() {
        let scenario = ScenarioConfig::new(1, 1);
        let mut index = ExistingRunsIndex::default();
        for repeat_index in 1..=2 {
            index.records.insert(
                SampleKey {
                    scenario: scenario.name.clone(),
                    repeat_index,
                    mode: Mode::Throughput,
                    items_per_producer: 1,
                    queue_label: "segqueue".to_string(),
                    batch_size: None,
                },
                BenchRecord {
                    queue: "segqueue".to_string(),
                    mode: "throughput".to_string(),
                    batch_size: None,
                    items_per_producer: 1,
                    total_items: 1,
                    consumed_items: 1,
                    elapsed_ns: 10,
                    ops_per_sec: Some(30.0),
                    producer_ops_per_sec: None,
                    consumer_ops_per_sec: None,
                    written_bytes: None,
                    flush_count: None,
                    push_elapsed_ns: None,
                    pop_elapsed_ns: None,
                    fill_elapsed_ns: None,
                    drain_elapsed_ns: None,
                    avg_data_latency_ns: None,
                    producer_fairness_ratio: None,
                    consumer_fairness_ratio: None,
                    status: BenchRecordStatus::Completed,
                    failure_reason: None,
                    timeout_ns: None,
                },
            );
            index.records.insert(
                SampleKey {
                    scenario: scenario.name.clone(),
                    repeat_index,
                    mode: Mode::Throughput,
                    items_per_producer: 1,
                    queue_label: "concurrent-queue".to_string(),
                    batch_size: None,
                },
                BenchRecord {
                    queue: "concurrent-queue".to_string(),
                    mode: "throughput".to_string(),
                    batch_size: None,
                    items_per_producer: 1,
                    total_items: 1,
                    consumed_items: 1,
                    elapsed_ns: 11,
                    ops_per_sec: Some(29.0),
                    producer_ops_per_sec: None,
                    consumer_ops_per_sec: None,
                    written_bytes: None,
                    flush_count: None,
                    push_elapsed_ns: None,
                    pop_elapsed_ns: None,
                    fill_elapsed_ns: None,
                    drain_elapsed_ns: None,
                    avg_data_latency_ns: None,
                    producer_fairness_ratio: None,
                    consumer_fairness_ratio: None,
                    status: BenchRecordStatus::Completed,
                    failure_reason: None,
                    timeout_ns: None,
                },
            );
            index.records.insert(
                SampleKey {
                    scenario: scenario.name.clone(),
                    repeat_index,
                    mode: Mode::Throughput,
                    items_per_producer: 1,
                    queue_label: "ubq_balanced,8,127,crossbeam".to_string(),
                    batch_size: None,
                },
                BenchRecord {
                    queue: "ubq".to_string(),
                    mode: "throughput".to_string(),
                    batch_size: None,
                    items_per_producer: 1,
                    total_items: 1,
                    consumed_items: 1,
                    elapsed_ns: 20,
                    ops_per_sec: Some(20.0),
                    producer_ops_per_sec: None,
                    consumer_ops_per_sec: None,
                    written_bytes: None,
                    flush_count: None,
                    push_elapsed_ns: None,
                    pop_elapsed_ns: None,
                    fill_elapsed_ns: None,
                    drain_elapsed_ns: None,
                    avg_data_latency_ns: None,
                    producer_fairness_ratio: None,
                    consumer_fairness_ratio: None,
                    status: BenchRecordStatus::Completed,
                    failure_reason: None,
                    timeout_ns: None,
                },
            );
        }

        let config = FrontierConfig {
            machine_label: "local".to_string(),
            runs_dir: PathBuf::from(DEFAULT_RUNS_DIR),
            scenarios: vec![scenario.clone()],
            baseline_queues: vec![QueueKind::SegQueue, QueueKind::ConcurrentQueue],
            fastfifo_block_sizes: Vec::new(),
            lfqueue_segment_sizes: Vec::new(),
            wcq_capacities: Vec::new(),
            seed_labels: vec!["balanced,8,127,crossbeam".to_string()],
            modes: vec![Mode::Throughput],
            items_per_producer_values: vec![1],
            repeats: 2,
            available_parallelism: 8,
        };
        let plan = compute_frontier_round_plan(&config, &index, &BTreeSet::new()).expect("plan");
        assert!(
            plan.bundles
                .iter()
                .any(|bundle| bundle.ubq_label.as_deref() == Some("balanced,8,127,yield"))
        );
    }

    #[test]
    fn frontier_expands_when_wcq_is_unsupported_for_one_mode() {
        let scenario = ScenarioConfig::new(1, 1);
        let mut index = ExistingRunsIndex::default();
        for mode in [Mode::Throughput, Mode::FillDrain] {
            index.records.insert(
                SampleKey {
                    scenario: scenario.name.clone(),
                    repeat_index: 1,
                    mode,
                    items_per_producer: 1,
                    queue_label: "segqueue".to_string(),
                    batch_size: None,
                },
                test_record("segqueue", mode, 1),
            );
            index.records.insert(
                SampleKey {
                    scenario: scenario.name.clone(),
                    repeat_index: 1,
                    mode,
                    items_per_producer: 1,
                    queue_label: "ubq_balanced,8,127,crossbeam".to_string(),
                    batch_size: None,
                },
                test_record("ubq", mode, 1),
            );
        }
        index.records.insert(
            SampleKey {
                scenario: scenario.name.clone(),
                repeat_index: 1,
                mode: Mode::FillDrain,
                items_per_producer: 1,
                queue_label: "wcq_4096".to_string(),
                batch_size: None,
            },
            test_record("wcq", Mode::FillDrain, 1),
        );

        let config = FrontierConfig {
            machine_label: "local".to_string(),
            runs_dir: PathBuf::from(DEFAULT_RUNS_DIR),
            scenarios: vec![scenario],
            baseline_queues: vec![QueueKind::SegQueue, QueueKind::Wcq],
            fastfifo_block_sizes: Vec::new(),
            lfqueue_segment_sizes: Vec::new(),
            wcq_capacities: vec![4096],
            seed_labels: vec!["balanced,8,127,crossbeam".to_string()],
            modes: vec![Mode::Throughput, Mode::FillDrain],
            items_per_producer_values: vec![1],
            repeats: 1,
            available_parallelism: 8,
        };
        let plan = compute_frontier_round_plan(&config, &index, &BTreeSet::new()).expect("plan");
        assert!(
            plan.bundles
                .iter()
                .any(|bundle| bundle.ubq_label.as_deref() == Some("balanced,8,127,yield"))
        );
        assert!(
            plan.bundles
                .iter()
                .any(|bundle| bundle.ubq_label.as_deref() == Some("balanced,0,127,crossbeam"))
        );
    }

    #[test]
    fn frontier_does_not_expand_nonbest_baseline_beater() {
        let scenario = ScenarioConfig::new(1, 1);
        let weaker_label = "balanced,8,127,crossbeam";
        let best_label = "balanced,16,127,crossbeam";
        let best_only_neighbor = "balanced,32,127,crossbeam";
        let weaker_only_neighbor = "balanced,4,127,crossbeam";
        let mut index = ExistingRunsIndex::default();

        for repeat_index in 1..=2 {
            index.records.insert(
                SampleKey {
                    scenario: scenario.name.clone(),
                    repeat_index,
                    mode: Mode::Throughput,
                    items_per_producer: 1,
                    queue_label: "segqueue".to_string(),
                    batch_size: None,
                },
                BenchRecord {
                    queue: "segqueue".to_string(),
                    mode: "throughput".to_string(),
                    batch_size: None,
                    items_per_producer: 1,
                    total_items: 1,
                    consumed_items: 1,
                    elapsed_ns: 10,
                    ops_per_sec: Some(10.0),
                    producer_ops_per_sec: None,
                    consumer_ops_per_sec: None,
                    written_bytes: None,
                    flush_count: None,
                    push_elapsed_ns: None,
                    pop_elapsed_ns: None,
                    fill_elapsed_ns: None,
                    drain_elapsed_ns: None,
                    avg_data_latency_ns: None,
                    producer_fairness_ratio: None,
                    consumer_fairness_ratio: None,
                    status: BenchRecordStatus::Completed,
                    failure_reason: None,
                    timeout_ns: None,
                },
            );
            index.records.insert(
                SampleKey {
                    scenario: scenario.name.clone(),
                    repeat_index,
                    mode: Mode::Throughput,
                    items_per_producer: 1,
                    queue_label: "concurrent-queue".to_string(),
                    batch_size: None,
                },
                BenchRecord {
                    queue: "concurrent-queue".to_string(),
                    mode: "throughput".to_string(),
                    batch_size: None,
                    items_per_producer: 1,
                    total_items: 1,
                    consumed_items: 1,
                    elapsed_ns: 11,
                    ops_per_sec: Some(9.0),
                    producer_ops_per_sec: None,
                    consumer_ops_per_sec: None,
                    written_bytes: None,
                    flush_count: None,
                    push_elapsed_ns: None,
                    pop_elapsed_ns: None,
                    fill_elapsed_ns: None,
                    drain_elapsed_ns: None,
                    avg_data_latency_ns: None,
                    producer_fairness_ratio: None,
                    consumer_fairness_ratio: None,
                    status: BenchRecordStatus::Completed,
                    failure_reason: None,
                    timeout_ns: None,
                },
            );
            index.records.insert(
                SampleKey {
                    scenario: scenario.name.clone(),
                    repeat_index,
                    mode: Mode::Throughput,
                    items_per_producer: 1,
                    queue_label: format!("ubq_{weaker_label}"),
                    batch_size: None,
                },
                BenchRecord {
                    queue: "ubq".to_string(),
                    mode: "throughput".to_string(),
                    batch_size: None,
                    items_per_producer: 1,
                    total_items: 1,
                    consumed_items: 1,
                    elapsed_ns: 20,
                    ops_per_sec: Some(20.0),
                    producer_ops_per_sec: None,
                    consumer_ops_per_sec: None,
                    written_bytes: None,
                    flush_count: None,
                    push_elapsed_ns: None,
                    pop_elapsed_ns: None,
                    fill_elapsed_ns: None,
                    drain_elapsed_ns: None,
                    avg_data_latency_ns: None,
                    producer_fairness_ratio: None,
                    consumer_fairness_ratio: None,
                    status: BenchRecordStatus::Completed,
                    failure_reason: None,
                    timeout_ns: None,
                },
            );
            index.records.insert(
                SampleKey {
                    scenario: scenario.name.clone(),
                    repeat_index,
                    mode: Mode::Throughput,
                    items_per_producer: 1,
                    queue_label: format!("ubq_{best_label}"),
                    batch_size: None,
                },
                BenchRecord {
                    queue: "ubq".to_string(),
                    mode: "throughput".to_string(),
                    batch_size: None,
                    items_per_producer: 1,
                    total_items: 1,
                    consumed_items: 1,
                    elapsed_ns: 15,
                    ops_per_sec: Some(25.0),
                    producer_ops_per_sec: None,
                    consumer_ops_per_sec: None,
                    written_bytes: None,
                    flush_count: None,
                    push_elapsed_ns: None,
                    pop_elapsed_ns: None,
                    fill_elapsed_ns: None,
                    drain_elapsed_ns: None,
                    avg_data_latency_ns: None,
                    producer_fairness_ratio: None,
                    consumer_fairness_ratio: None,
                    status: BenchRecordStatus::Completed,
                    failure_reason: None,
                    timeout_ns: None,
                },
            );
        }

        let config = FrontierConfig {
            machine_label: "local".to_string(),
            runs_dir: PathBuf::from(DEFAULT_RUNS_DIR),
            scenarios: vec![scenario.clone()],
            baseline_queues: vec![QueueKind::SegQueue, QueueKind::ConcurrentQueue],
            fastfifo_block_sizes: Vec::new(),
            lfqueue_segment_sizes: Vec::new(),
            wcq_capacities: Vec::new(),
            seed_labels: vec![weaker_label.to_string()],
            modes: vec![Mode::Throughput],
            items_per_producer_values: vec![1],
            repeats: 2,
            available_parallelism: 8,
        };
        let plan = compute_frontier_round_plan(&config, &index, &BTreeSet::new()).expect("plan");

        assert!(
            plan.bundles
                .iter()
                .any(|bundle| bundle.ubq_label.as_deref() == Some(best_only_neighbor))
        );
        assert!(
            !plan
                .bundles
                .iter()
                .any(|bundle| bundle.ubq_label.as_deref() == Some(weaker_only_neighbor))
        );
    }

    #[test]
    fn frontier_rejects_scenarios_without_valid_seed_labels() {
        let config = FrontierConfig {
            machine_label: "local".to_string(),
            runs_dir: PathBuf::from(DEFAULT_RUNS_DIR),
            scenarios: vec![ScenarioConfig::new(64, 1)],
            baseline_queues: vec![QueueKind::SegQueue, QueueKind::ConcurrentQueue],
            fastfifo_block_sizes: Vec::new(),
            lfqueue_segment_sizes: Vec::new(),
            wcq_capacities: Vec::new(),
            seed_labels: vec!["balanced,8,63,crossbeam".to_string()],
            modes: vec![Mode::Throughput],
            items_per_producer_values: vec![1],
            repeats: 1,
            available_parallelism: 128,
        };
        let err =
            compute_frontier_round_plan(&config, &ExistingRunsIndex::default(), &BTreeSet::new())
                .expect_err("expected validation error");
        assert!(err.contains("64p1c"));
        assert!(err.contains("no valid seed labels"));
    }

    #[test]
    fn frontier_runs_local_winner_across_all_scenarios() {
        let winner_scenario = ScenarioConfig::new(1, 1);
        let other_scenario = ScenarioConfig::new(1, 4);
        let winning_label = "balanced,8,127,crossbeam";
        let mut index = ExistingRunsIndex::default();

        for repeat_index in 1..=2 {
            index.records.insert(
                SampleKey {
                    scenario: winner_scenario.name.clone(),
                    repeat_index,
                    mode: Mode::Throughput,
                    items_per_producer: 1,
                    queue_label: "segqueue".to_string(),
                    batch_size: None,
                },
                BenchRecord {
                    queue: "segqueue".to_string(),
                    mode: "throughput".to_string(),
                    batch_size: None,
                    items_per_producer: 1,
                    total_items: 1,
                    consumed_items: 1,
                    elapsed_ns: 10,
                    ops_per_sec: Some(10.0),
                    producer_ops_per_sec: None,
                    consumer_ops_per_sec: None,
                    written_bytes: None,
                    flush_count: None,
                    push_elapsed_ns: None,
                    pop_elapsed_ns: None,
                    fill_elapsed_ns: None,
                    drain_elapsed_ns: None,
                    avg_data_latency_ns: None,
                    producer_fairness_ratio: None,
                    consumer_fairness_ratio: None,
                    status: BenchRecordStatus::Completed,
                    failure_reason: None,
                    timeout_ns: None,
                },
            );
            index.records.insert(
                SampleKey {
                    scenario: winner_scenario.name.clone(),
                    repeat_index,
                    mode: Mode::Throughput,
                    items_per_producer: 1,
                    queue_label: "concurrent-queue".to_string(),
                    batch_size: None,
                },
                BenchRecord {
                    queue: "concurrent-queue".to_string(),
                    mode: "throughput".to_string(),
                    batch_size: None,
                    items_per_producer: 1,
                    total_items: 1,
                    consumed_items: 1,
                    elapsed_ns: 11,
                    ops_per_sec: Some(11.0),
                    producer_ops_per_sec: None,
                    consumer_ops_per_sec: None,
                    written_bytes: None,
                    flush_count: None,
                    push_elapsed_ns: None,
                    pop_elapsed_ns: None,
                    fill_elapsed_ns: None,
                    drain_elapsed_ns: None,
                    avg_data_latency_ns: None,
                    producer_fairness_ratio: None,
                    consumer_fairness_ratio: None,
                    status: BenchRecordStatus::Completed,
                    failure_reason: None,
                    timeout_ns: None,
                },
            );
            index.records.insert(
                SampleKey {
                    scenario: winner_scenario.name.clone(),
                    repeat_index,
                    mode: Mode::Throughput,
                    items_per_producer: 1,
                    queue_label: format!("ubq_{winning_label}"),
                    batch_size: None,
                },
                BenchRecord {
                    queue: "ubq".to_string(),
                    mode: "throughput".to_string(),
                    batch_size: None,
                    items_per_producer: 1,
                    total_items: 1,
                    consumed_items: 1,
                    elapsed_ns: 20,
                    ops_per_sec: Some(20.0),
                    producer_ops_per_sec: None,
                    consumer_ops_per_sec: None,
                    written_bytes: None,
                    flush_count: None,
                    push_elapsed_ns: None,
                    pop_elapsed_ns: None,
                    fill_elapsed_ns: None,
                    drain_elapsed_ns: None,
                    avg_data_latency_ns: None,
                    producer_fairness_ratio: None,
                    consumer_fairness_ratio: None,
                    status: BenchRecordStatus::Completed,
                    failure_reason: None,
                    timeout_ns: None,
                },
            );
        }

        let config = FrontierConfig {
            machine_label: "local".to_string(),
            runs_dir: PathBuf::from(DEFAULT_RUNS_DIR),
            scenarios: vec![winner_scenario.clone(), other_scenario.clone()],
            baseline_queues: vec![QueueKind::SegQueue, QueueKind::ConcurrentQueue],
            fastfifo_block_sizes: Vec::new(),
            lfqueue_segment_sizes: Vec::new(),
            wcq_capacities: Vec::new(),
            seed_labels: vec!["balanced,1,31,crossbeam".to_string()],
            modes: vec![Mode::Throughput],
            items_per_producer_values: vec![1],
            repeats: 2,
            available_parallelism: 8,
        };
        let plan = compute_frontier_round_plan(&config, &index, &BTreeSet::new()).expect("plan");
        let winner_bundles: Vec<_> = plan
            .bundles
            .iter()
            .filter(|bundle| bundle.ubq_label.as_deref() == Some(winning_label))
            .collect();

        assert_eq!(winner_bundles.len(), 2);
        assert!(
            winner_bundles
                .iter()
                .all(|bundle| bundle.scenario.name == other_scenario.name)
        );
    }

    #[test]
    fn frontier_does_not_propagate_winner_invalid_for_scenario() {
        let winner_scenario = ScenarioConfig::new(1, 1);
        let constrained_scenario = ScenarioConfig::new(64, 1);
        let winning_label = "balanced,8,31,crossbeam";
        let mut index = ExistingRunsIndex::default();

        for repeat_index in 1..=2 {
            index.records.insert(
                SampleKey {
                    scenario: winner_scenario.name.clone(),
                    repeat_index,
                    mode: Mode::Throughput,
                    items_per_producer: 1,
                    queue_label: "segqueue".to_string(),
                    batch_size: None,
                },
                BenchRecord {
                    queue: "segqueue".to_string(),
                    mode: "throughput".to_string(),
                    batch_size: None,
                    items_per_producer: 1,
                    total_items: 1,
                    consumed_items: 1,
                    elapsed_ns: 10,
                    ops_per_sec: Some(10.0),
                    producer_ops_per_sec: None,
                    consumer_ops_per_sec: None,
                    written_bytes: None,
                    flush_count: None,
                    push_elapsed_ns: None,
                    pop_elapsed_ns: None,
                    fill_elapsed_ns: None,
                    drain_elapsed_ns: None,
                    avg_data_latency_ns: None,
                    producer_fairness_ratio: None,
                    consumer_fairness_ratio: None,
                    status: BenchRecordStatus::Completed,
                    failure_reason: None,
                    timeout_ns: None,
                },
            );
            index.records.insert(
                SampleKey {
                    scenario: winner_scenario.name.clone(),
                    repeat_index,
                    mode: Mode::Throughput,
                    items_per_producer: 1,
                    queue_label: "concurrent-queue".to_string(),
                    batch_size: None,
                },
                BenchRecord {
                    queue: "concurrent-queue".to_string(),
                    mode: "throughput".to_string(),
                    batch_size: None,
                    items_per_producer: 1,
                    total_items: 1,
                    consumed_items: 1,
                    elapsed_ns: 11,
                    ops_per_sec: Some(11.0),
                    producer_ops_per_sec: None,
                    consumer_ops_per_sec: None,
                    written_bytes: None,
                    flush_count: None,
                    push_elapsed_ns: None,
                    pop_elapsed_ns: None,
                    fill_elapsed_ns: None,
                    drain_elapsed_ns: None,
                    avg_data_latency_ns: None,
                    producer_fairness_ratio: None,
                    consumer_fairness_ratio: None,
                    status: BenchRecordStatus::Completed,
                    failure_reason: None,
                    timeout_ns: None,
                },
            );
            index.records.insert(
                SampleKey {
                    scenario: winner_scenario.name.clone(),
                    repeat_index,
                    mode: Mode::Throughput,
                    items_per_producer: 1,
                    queue_label: format!("ubq_{winning_label}"),
                    batch_size: None,
                },
                BenchRecord {
                    queue: "ubq".to_string(),
                    mode: "throughput".to_string(),
                    batch_size: None,
                    items_per_producer: 1,
                    total_items: 1,
                    consumed_items: 1,
                    elapsed_ns: 20,
                    ops_per_sec: Some(20.0),
                    producer_ops_per_sec: None,
                    consumer_ops_per_sec: None,
                    written_bytes: None,
                    flush_count: None,
                    push_elapsed_ns: None,
                    pop_elapsed_ns: None,
                    fill_elapsed_ns: None,
                    drain_elapsed_ns: None,
                    avg_data_latency_ns: None,
                    producer_fairness_ratio: None,
                    consumer_fairness_ratio: None,
                    status: BenchRecordStatus::Completed,
                    failure_reason: None,
                    timeout_ns: None,
                },
            );
        }

        let config = FrontierConfig {
            machine_label: "local".to_string(),
            runs_dir: PathBuf::from(DEFAULT_RUNS_DIR),
            scenarios: vec![winner_scenario, constrained_scenario.clone()],
            baseline_queues: vec![QueueKind::SegQueue, QueueKind::ConcurrentQueue],
            fastfifo_block_sizes: Vec::new(),
            lfqueue_segment_sizes: Vec::new(),
            wcq_capacities: Vec::new(),
            seed_labels: vec!["balanced,8,127,crossbeam".to_string()],
            modes: vec![Mode::Throughput],
            items_per_producer_values: vec![1],
            repeats: 2,
            available_parallelism: 128,
        };
        let plan = compute_frontier_round_plan(&config, &index, &BTreeSet::new()).expect("plan");
        assert!(!plan.bundles.iter().any(|bundle| {
            bundle.scenario.name == constrained_scenario.name
                && bundle.ubq_label.as_deref() == Some(winning_label)
        }));
    }

    #[test]
    fn scheduler_allows_jobs_until_thread_budget_is_exhausted() {
        let baseline = JobSpec {
            scenario: ScenarioConfig::new(1, 1),
            repeat_index: 1,
            mode: Mode::Throughput,
            items_per_producer: 1,
            queue: QueueKind::SegQueue,
            ubq_label: None,
            batch_size: None,
            fastfifo_block_size: None,
            lfqueue_segment_size: None,
            wcq_capacity: None,
        };
        let ubq = JobSpec {
            scenario: ScenarioConfig::new(1, 1),
            repeat_index: 1,
            mode: Mode::Throughput,
            items_per_producer: 1,
            queue: QueueKind::Ubq,
            ubq_label: Some("balanced,1,31,crossbeam".to_string()),
            batch_size: None,
            fastfifo_block_size: None,
            lfqueue_segment_size: None,
            wcq_capacity: None,
        };

        assert!(can_start_job(&baseline, 0, 8));
        assert!(can_start_job(&ubq, 0, 8));
        assert!(can_start_job(&baseline, 2, 8));
        assert!(can_start_job(&ubq, 2, 8));
        assert!(!can_start_job(&ubq, 7, 8));
    }
}
