#![allow(missing_docs)]

pub mod baselines;

use crate::{UBQ, backoff};
#[cfg(feature = "bench_moodycamel")]
use baselines::moodycamel_cq::MoodycamelQueue;
use baselines::{ms_queue::MsQueue, mutex_vecdeque::MutexQueue, naive_faa_queue::NaiveFaaQueue};
use concurrent_queue::{ConcurrentQueue, PopError};
use crossbeam_queue::{BatchQueue, SegQueue};
use crossbeam_utils::Backoff;
#[cfg(feature = "bench_lfqueue")]
use lfqueue::UnboundedQueue as LfUnboundedQueue;
#[cfg(feature = "bench_fastfifo")]
use rbbq::FastFifo;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::cell::UnsafeCell;
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
#[cfg(test)]
use std::ffi::OsStr;
use std::fmt;
use std::fs;
use std::io::{self, BufRead, BufReader, BufWriter, Write};
use std::num::NonZero;
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::{
    Arc, Barrier, Mutex, OnceLock,
    atomic::{AtomicBool, AtomicU64, Ordering as AtomicOrdering},
    mpsc,
};
use std::thread;
use std::thread::available_parallelism;

fn bench_core_ids() -> &'static [core_affinity::CoreId] {
    static IDS: OnceLock<Vec<core_affinity::CoreId>> = OnceLock::new();
    IDS.get_or_init(|| {
        let discovered = core_affinity::get_core_ids().unwrap_or_default();
        let Ok(raw) = std::env::var("UBQ_BENCH_CORE_IDS") else {
            return discovered;
        };
        let requested = parse_core_ids(&raw).unwrap_or_default();
        requested
            .into_iter()
            .filter_map(|id| discovered.iter().find(|core| core.id == id).copied())
            .collect()
    })
}

fn producer_core_slot(producers: usize, consumers: usize, producer_id: usize) -> usize {
    let paired = producers.min(consumers);
    if producer_id < paired {
        producer_id * 2
    } else {
        paired * 2 + producer_id - paired
    }
}

fn consumer_core_slot(producers: usize, consumers: usize, consumer_id: usize) -> usize {
    let paired = producers.min(consumers);
    if consumer_id < paired {
        consumer_id * 2 + 1
    } else {
        paired * 2 + consumer_id - paired
    }
}

fn producer_core_id(
    core_offset: usize,
    producers: usize,
    consumers: usize,
    producer_id: usize,
) -> Option<core_affinity::CoreId> {
    bench_core_ids()
        .get(core_offset + producer_core_slot(producers, consumers, producer_id))
        .copied()
}

fn consumer_core_id(
    core_offset: usize,
    producers: usize,
    consumers: usize,
    consumer_id: usize,
) -> Option<core_affinity::CoreId> {
    bench_core_ids()
        .get(core_offset + consumer_core_slot(producers, consumers, consumer_id))
        .copied()
}
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

pub const RUN_SCHEMA_VERSION: u32 = 7;
pub const PLAN_SCHEMA_VERSION: u32 = 6;
pub const DEFAULT_ITEMS_PER_PRODUCER: u64 = 1_000_000;
pub const DEFAULT_RUNS_DIR: &str = "bench_results/runs";
pub const DEFAULT_PLOTS_DIR: &str = "bench_results/plots";
pub const DEFAULT_SCENARIOS: &[&str] = &["pow2:machine"];
pub const BBQ_ATC22_X86_88T_SCENARIO_SUITE: &str = "bbq-atc22-x86-88t";
pub const BBQ_ATC22_OVERSUB_X86_12T_SCENARIO_SUITE: &str = "bbq-atc22-oversub-x86-12t";

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
const DEFAULT_BENCH_JOB_TIMEOUT_SECS: u64 = 30;
const BENCH_WORKER_ENV: &str = "UBQ_BENCH_INTERNAL_WORKER";
const BENCH_WORKER_PROTOCOL_VERSION: u32 = 1;
pub const DEFAULT_THROUGHPUT_WARMUP_MS: u64 = 250;
pub const DEFAULT_THROUGHPUT_PHASE_MS: u64 = 1_000;
pub const DEFAULT_THROUGHPUT_PILOT_MS: u64 = 100;
pub const DEFAULT_THROUGHPUT_MAX_ROUND_ITEMS: u64 = 8_388_608;
pub const DEFAULT_SCHEDULE_SEED: u64 = 0x5542_5106;
pub const DEFAULT_FASTFIFO_CAPACITY: usize = 1_048_576;
const INITIAL_THROUGHPUT_PILOT_ITEMS_PER_PRODUCER: u64 = 4_096;

fn default_schedule_seed() -> u64 {
    DEFAULT_SCHEDULE_SEED
}

fn default_fastfifo_capacities() -> Vec<usize> {
    vec![DEFAULT_FASTFIFO_CAPACITY]
}
const UBQ_BACKOFF_VALUES: [&str; 2] = ["crossbeam", "yield"];
const LEGACY_UBQ_BLOCK_VALUES: [u16; 12] = [
    31, 63, 127, 255, 511, 1023, 2047, 4095, 8191, 16383, 32767, 65535,
];
pub const DEFAULT_UBQ_BATCH_SIZES: [usize; 3] = [8, 32, 256];
const DEFAULT_LFQUEUE_SEGMENT_SIZES: [usize; 3] = [32, 256, 1024];
const DEFAULT_WCQ_CAPACITIES: [usize; 3] = [4096, 65536, 1048576];
const SUPPORTED_WCQ_CAPACITIES: [usize; 8] =
    [256, 1024, 4096, 16384, 65536, 262144, 1048576, 4194304];
const WCQ_MAX_THREADS: usize = 256;

fn bench_job_timeout(plan: &MatrixPlan) -> Duration {
    let configured_budget = plan
        .throughput_policy
        .warmup_duration()
        .checked_add(plan.throughput_policy.pilot_duration())
        .unwrap_or(Duration::MAX)
        .checked_add(plan.throughput_policy.phase_duration().saturating_mul(3))
        .unwrap_or(Duration::MAX)
        .saturating_mul(5)
        .max(Duration::from_secs(DEFAULT_BENCH_JOB_TIMEOUT_SECS));
    plan.job_timeout_secs
        .map(Duration::from_secs)
        .or_else(|| {
            std::env::var("UBQ_BENCH_JOB_TIMEOUT_SECS")
                .ok()
                .and_then(|raw| raw.parse::<u64>().ok())
                .filter(|secs| *secs > 0)
                .map(Duration::from_secs)
        })
        .unwrap_or(configured_budget)
}

fn spawn_bench_thread<F, T>(f: F) -> thread::JoinHandle<T>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    thread::spawn(f)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LogRecord {
    pub message: &'static str,
    pub meta: u64,
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
    fn try_recv_log(&self) -> Option<LogRecord>;
    fn recv_log(&self) -> LogRecord {
        let backoff = Backoff::new();
        loop {
            if let Some(record) = self.try_recv_log() {
                return record;
            }
            backoff.snooze();
        }
    }
}

pub trait LogQueueThreadOps: Send + 'static {
    fn send_log(&self, record: LogRecord);
    fn try_recv_log(&self) -> Option<LogRecord>;
    fn recv_log(&self) -> LogRecord {
        let backoff = Backoff::new();
        loop {
            if let Some(record) = self.try_recv_log() {
                return record;
            }
            backoff.snooze();
        }
    }
}

impl<Q: LogQueueOps> LogQueueThreadOps for Arc<Q> {
    fn send_log(&self, record: LogRecord) {
        (**self).send_log(record);
    }

    fn try_recv_log(&self) -> Option<LogRecord> {
        (**self).try_recv_log()
    }
}

pub trait LogQueueHandleFactory: Send + Sync + 'static {
    type ThreadHandle: LogQueueThreadOps;

    fn log_thread_handle(self: &Arc<Self>) -> Self::ThreadHandle;

    fn log_producer_thread_handle(self: &Arc<Self>) -> Self::ThreadHandle {
        self.log_thread_handle()
    }

    fn log_consumer_thread_handle(self: &Arc<Self>) -> Self::ThreadHandle {
        self.log_thread_handle()
    }
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
    fn try_send_value(&self, value: u64) -> bool;
    fn send_value(&self, value: u64) {
        let backoff = Backoff::new();
        while !self.try_send_value(value) {
            backoff.snooze();
        }
    }
    fn send_batch(&self, base: u64, offsets: std::ops::Range<usize>) {
        for offset in offsets {
            self.send_value(base + offset as u64);
        }
    }
    fn try_recv_value(&self) -> Option<u64>;
    fn try_recv_batch(&self, request_size: usize) -> usize {
        let mut received = 0;
        for _ in 0..request_size {
            if self.try_recv_value().is_none() {
                break;
            }

            received += 1;
        }
        received
    }
    fn bounded_capacity(&self) -> Option<usize> {
        None
    }
    fn recv_value(&self) -> u64 {
        let backoff = Backoff::new();
        loop {
            if let Some(value) = self.try_recv_value() {
                return value;
            }
            backoff.snooze();
        }
    }
}

pub trait BenchQueueThreadOps: Send + 'static {
    fn try_send_value(&self, value: u64) -> bool;
    fn send_value(&self, value: u64) {
        let backoff = Backoff::new();
        while !self.try_send_value(value) {
            backoff.snooze();
        }
    }
    fn send_batch(&self, base: u64, offsets: std::ops::Range<usize>);
    fn try_recv_value(&self) -> Option<u64>;
    fn try_recv_batch(&self, request_size: usize) -> usize {
        let mut received = 0;
        for _ in 0..request_size {
            if self.try_recv_value().is_none() {
                break;
            }

            received += 1;
        }
        received
    }
    fn recv_value(&self) -> u64 {
        let backoff = Backoff::new();
        loop {
            if let Some(value) = self.try_recv_value() {
                return value;
            }
            backoff.snooze();
        }
    }
}

impl<Q: BenchQueueOps> BenchQueueThreadOps for Arc<Q> {
    fn try_send_value(&self, value: u64) -> bool {
        (**self).try_send_value(value)
    }

    fn send_batch(&self, base: u64, offsets: std::ops::Range<usize>) {
        (**self).send_batch(base, offsets);
    }

    fn try_recv_value(&self) -> Option<u64> {
        (**self).try_recv_value()
    }

    fn try_recv_batch(&self, request_size: usize) -> usize {
        (**self).try_recv_batch(request_size)
    }
}

pub trait BenchQueueHandleFactory: Send + Sync + 'static {
    type ThreadHandle: BenchQueueThreadOps;

    fn thread_handle(self: &Arc<Self>) -> Self::ThreadHandle;

    fn producer_thread_handle(self: &Arc<Self>) -> Self::ThreadHandle {
        self.thread_handle()
    }

    fn consumer_thread_handle(self: &Arc<Self>) -> Self::ThreadHandle {
        self.thread_handle()
    }

    fn bounded_capacity(&self) -> Option<usize> {
        None
    }
}

impl<Q: BenchQueueOps> BenchQueueHandleFactory for Q {
    type ThreadHandle = Arc<Q>;

    fn thread_handle(self: &Arc<Self>) -> Self::ThreadHandle {
        self.clone()
    }

    fn bounded_capacity(&self) -> Option<usize> {
        BenchQueueOps::bounded_capacity(self)
    }
}

pub trait BenchQueue: BenchQueueOps {
    fn new_queue() -> Arc<Self>
    where
        Self: Sized;
}

impl<B> BenchQueueOps for UBQ<u64, B>
where
    B: backoff::BackoffPolicy + 'static,
{
    fn try_send_value(&self, value: u64) -> bool {
        self.push(value);
        true
    }

    fn send_batch(&self, base: u64, offsets: std::ops::Range<usize>) {
        self.push_batch(offsets.map(move |offset| base + offset as u64));
    }

    fn try_recv_value(&self) -> Option<u64> {
        self.pop()
    }

    fn try_recv_batch(&self, request_size: usize) -> usize {
        self.pop_batch(request_size).count()
    }
}

impl<B> BenchQueue for UBQ<u64, B>
where
    B: backoff::BackoffPolicy + 'static,
{
    fn new_queue() -> Arc<Self> {
        Self::new_arc()
    }
}

impl<B> LogQueueOps for UBQ<LogRecord, B>
where
    B: backoff::BackoffPolicy + 'static,
{
    fn send_log(&self, record: LogRecord) {
        self.push(record);
    }

    fn try_recv_log(&self) -> Option<LogRecord> {
        self.pop()
    }
}

struct LubqHandlePool<T> {
    senders: Mutex<Vec<crate::kfifo::Sender<T>>>,
    receivers: Mutex<Vec<crate::kfifo::Receiver<T>>>,
}

struct LubqBenchQueue<T> {
    pool: Arc<LubqHandlePool<T>>,
}

impl<T> LubqBenchQueue<T> {
    fn new(producers: usize, consumers: usize) -> Arc<Self> {
        assert!(producers > 0, "LUBQ requires at least one producer");
        assert!(consumers > 0, "LUBQ requires at least one consumer");

        let (first_sender, first_receiver) = crate::kfifo::channel();
        let mut senders = Vec::with_capacity(producers);
        senders.push(first_sender);
        while senders.len() < producers {
            senders.push(senders[0].clone());
        }
        let mut receivers = Vec::with_capacity(consumers);
        receivers.push(first_receiver);
        while receivers.len() < consumers {
            receivers.push(receivers[0].clone());
        }

        Arc::new(Self {
            pool: Arc::new(LubqHandlePool {
                senders: Mutex::new(senders),
                receivers: Mutex::new(receivers),
            }),
        })
    }
}

enum LubqThreadRole<T> {
    Producer(crate::kfifo::Sender<T>),
    Consumer {
        receiver: crate::kfifo::Receiver<T>,
        batch: Vec<T>,
    },
}

struct LubqThreadHandle<T> {
    role: UnsafeCell<Option<LubqThreadRole<T>>>,
    pool: Arc<LubqHandlePool<T>>,
}

impl<T> LubqThreadHandle<T> {
    fn producer(sender: crate::kfifo::Sender<T>, pool: Arc<LubqHandlePool<T>>) -> Self {
        Self {
            role: UnsafeCell::new(Some(LubqThreadRole::Producer(sender))),
            pool,
        }
    }

    fn consumer(receiver: crate::kfifo::Receiver<T>, pool: Arc<LubqHandlePool<T>>) -> Self {
        Self {
            role: UnsafeCell::new(Some(LubqThreadRole::Consumer {
                receiver,
                batch: Vec::new(),
            })),
            pool,
        }
    }

    fn role_mut(&self) -> &mut LubqThreadRole<T> {
        // SAFETY: a benchmark thread handle is Send but deliberately not Sync
        // (because it contains UnsafeCell). The harness moves each handle into
        // exactly one worker and never overlaps operations on that handle.
        unsafe { (&mut *self.role.get()).as_mut() }.expect("LUBQ thread handle is empty")
    }
}

impl<T> Drop for LubqThreadHandle<T> {
    fn drop(&mut self) {
        let Some(role) = self.role.get_mut().take() else {
            return;
        };
        match role {
            LubqThreadRole::Producer(sender) => self
                .pool
                .senders
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push(sender),
            LubqThreadRole::Consumer { receiver, .. } => self
                .pool
                .receivers
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push(receiver),
        }
    }
}

impl<T: Send + 'static> BenchQueueHandleFactory for LubqBenchQueue<T>
where
    LubqThreadHandle<T>: BenchQueueThreadOps,
{
    type ThreadHandle = LubqThreadHandle<T>;

    fn thread_handle(self: &Arc<Self>) -> Self::ThreadHandle {
        panic!("LUBQ benchmark handles must be requested for a producer or consumer role")
    }

    fn producer_thread_handle(self: &Arc<Self>) -> Self::ThreadHandle {
        let sender = self
            .pool
            .senders
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .pop()
            .expect("LUBQ producer handle pool exhausted");
        LubqThreadHandle::producer(sender, self.pool.clone())
    }

    fn consumer_thread_handle(self: &Arc<Self>) -> Self::ThreadHandle {
        let receiver = self
            .pool
            .receivers
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .pop()
            .expect("LUBQ consumer handle pool exhausted");
        LubqThreadHandle::consumer(receiver, self.pool.clone())
    }
}

impl BenchQueueThreadOps for LubqThreadHandle<u64> {
    fn try_send_value(&self, value: u64) -> bool {
        let LubqThreadRole::Producer(sender) = self.role_mut() else {
            panic!("attempted to send through a LUBQ consumer handle");
        };
        sender.send(value);
        true
    }

    fn send_batch(&self, base: u64, offsets: std::ops::Range<usize>) {
        let LubqThreadRole::Producer(sender) = self.role_mut() else {
            panic!("attempted to send a batch through a LUBQ consumer handle");
        };
        sender.send_batch(offsets.map(move |offset| base + offset as u64));
    }

    fn try_recv_value(&self) -> Option<u64> {
        let LubqThreadRole::Consumer { receiver, .. } = self.role_mut() else {
            panic!("attempted to receive through a LUBQ producer handle");
        };
        receiver.pop()
    }

    fn try_recv_batch(&self, request_size: usize) -> usize {
        let LubqThreadRole::Consumer { receiver, batch } = self.role_mut() else {
            panic!("attempted to receive a batch through a LUBQ producer handle");
        };
        batch.clear();
        receiver.pop_batch_into(batch, request_size)
    }
}

impl LogQueueHandleFactory for LubqBenchQueue<LogRecord> {
    type ThreadHandle = LubqThreadHandle<LogRecord>;

    fn log_thread_handle(self: &Arc<Self>) -> Self::ThreadHandle {
        panic!("LUBQ log handles must be requested for a producer or consumer role")
    }

    fn log_producer_thread_handle(self: &Arc<Self>) -> Self::ThreadHandle {
        let sender = self
            .pool
            .senders
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .pop()
            .expect("LUBQ log producer handle pool exhausted");
        LubqThreadHandle::producer(sender, self.pool.clone())
    }

    fn log_consumer_thread_handle(self: &Arc<Self>) -> Self::ThreadHandle {
        let receiver = self
            .pool
            .receivers
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .pop()
            .expect("LUBQ log consumer handle pool exhausted");
        LubqThreadHandle::consumer(receiver, self.pool.clone())
    }
}

impl LogQueueThreadOps for LubqThreadHandle<LogRecord> {
    fn send_log(&self, record: LogRecord) {
        let LubqThreadRole::Producer(sender) = self.role_mut() else {
            panic!("attempted to send a log record through a LUBQ consumer handle");
        };
        sender.send(record);
    }

    fn try_recv_log(&self) -> Option<LogRecord> {
        let LubqThreadRole::Consumer { receiver, .. } = self.role_mut() else {
            panic!("attempted to receive a log record through a LUBQ producer handle");
        };
        receiver.pop()
    }
}

impl<B> LogQueue for UBQ<LogRecord, B>
where
    B: backoff::BackoffPolicy + 'static,
{
    fn new_log_queue() -> Arc<Self> {
        Self::new_arc()
    }
}

impl BenchQueueOps for SegQueue<u64> {
    fn try_send_value(&self, value: u64) -> bool {
        self.push(value);
        true
    }

    fn try_recv_value(&self) -> Option<u64> {
        self.pop()
    }
}

impl BenchQueue for SegQueue<u64> {
    fn new_queue() -> Arc<Self> {
        Arc::new(Self::new())
    }
}

impl BenchQueueOps for BatchQueue<u64> {
    fn try_send_value(&self, value: u64) -> bool {
        self.push(core::iter::once(value));
        true
    }

    fn send_batch(&self, base: u64, offsets: std::ops::Range<usize>) {
        self.push(offsets.map(move |offset| base + offset as u64));
    }

    fn try_recv_value(&self) -> Option<u64> {
        self.pop(1).next()
    }

    fn try_recv_batch(&self, request_size: usize) -> usize {
        self.pop(request_size).count()
    }
}

impl BenchQueue for BatchQueue<u64> {
    fn new_queue() -> Arc<Self> {
        Arc::new(Self::new())
    }
}

impl LogQueueOps for SegQueue<LogRecord> {
    fn send_log(&self, record: LogRecord) {
        self.push(record);
    }

    fn try_recv_log(&self) -> Option<LogRecord> {
        self.pop()
    }
}

impl LogQueue for SegQueue<LogRecord> {
    fn new_log_queue() -> Arc<Self> {
        Arc::new(Self::new())
    }
}

impl BenchQueueOps for ConcurrentQueue<u64> {
    fn try_send_value(&self, value: u64) -> bool {
        self.push(value).expect("send failed");
        true
    }

    fn try_recv_value(&self) -> Option<u64> {
        match self.pop() {
            Ok(value) => Some(value),
            Err(PopError::Empty) => None,
            Err(PopError::Closed) => panic!("recv failed: queue closed"),
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
        self.push(record).expect("send failed");
    }

    fn try_recv_log(&self) -> Option<LogRecord> {
        match self.pop() {
            Ok(record) => Some(record),
            Err(PopError::Empty) => None,
            Err(PopError::Closed) => panic!("recv failed: queue closed"),
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
    fn try_send_value(&self, value: u64) -> bool {
        self.inner.enqueue(value);
        true
    }

    fn try_recv_value(&self) -> Option<u64> {
        self.inner.dequeue()
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
        self.inner.enqueue(record);
    }

    fn try_recv_log(&self) -> Option<LogRecord> {
        self.inner.dequeue()
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
        // this can overflow a normal benchmark-worker thread stack.
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

    fn bounded_capacity(&self) -> Option<usize> {
        Some(CAPACITY)
    }
}

#[cfg(feature = "bench_wcq")]
impl<const CAPACITY: usize> BenchQueueThreadOps for WcqThreadHandle<CAPACITY> {
    fn try_send_value(&self, value: u64) -> bool {
        self.queue.inner.enqueue(self.handle, value).is_ok()
    }

    fn send_batch(&self, base: u64, offsets: std::ops::Range<usize>) {
        for offset in offsets {
            self.send_value(base + offset as u64);
        }
    }

    fn try_recv_value(&self) -> Option<u64> {
        self.queue.inner.dequeue(self.handle)
    }
}

#[cfg(feature = "bench_fastfifo")]
struct RbbqBenchQueue {
    inner: FastFifo<u64>,
    capacity: usize,
}

#[cfg(feature = "bench_fastfifo")]
impl RbbqBenchQueue {
    fn new(block_size: usize, requested_capacity: usize) -> Arc<Self> {
        let data_blocks = requested_capacity.div_ceil(block_size).max(1);
        // FastFifo needs three control/transition blocks beyond the usable
        // data capacity (the prior workload-sized adapter used this same
        // safety margin implicitly).
        let num_blocks = data_blocks.checked_add(3).expect("FastFifo block overflow");
        Arc::new(Self {
            inner: FastFifo::new(num_blocks, block_size),
            capacity: data_blocks * block_size,
        })
    }
}

#[cfg(feature = "bench_fastfifo")]
impl BenchQueueOps for RbbqBenchQueue {
    fn try_send_value(&self, value: u64) -> bool {
        self.inner.push(value).is_ok()
    }

    fn try_recv_value(&self) -> Option<u64> {
        self.inner.pop().ok()
    }

    fn bounded_capacity(&self) -> Option<usize> {
        Some(self.capacity)
    }
}

#[cfg(feature = "bench_fastfifo")]
struct LogRbbqBenchQueue {
    inner: FastFifo<LogRecord>,
}

#[cfg(feature = "bench_fastfifo")]
impl LogRbbqBenchQueue {
    fn new(block_size: usize, requested_capacity: usize) -> Arc<Self> {
        let data_blocks = requested_capacity.div_ceil(block_size).max(1);
        let num_blocks = data_blocks.checked_add(3).expect("FastFifo block overflow");
        Arc::new(Self {
            inner: FastFifo::new(num_blocks, block_size),
        })
    }
}

#[cfg(feature = "bench_fastfifo")]
impl LogQueueOps for LogRbbqBenchQueue {
    fn send_log(&self, record: LogRecord) {
        let backoff = Backoff::new();
        loop {
            if self.inner.push(record).is_ok() {
                return;
            }
            backoff.snooze();
        }
    }

    fn try_recv_log(&self) -> Option<LogRecord> {
        self.inner.pop().ok()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Mode {
    Throughput,
    ComplexThroughput,
    DataLatency,
    Fairness,
    AppLogFanIn,
    AppPipeline,
    AppTaskRoundtrip,
    AppLogMpscFile,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ItemPolicy {
    #[default]
    Explicit,
    ScenarioScaledV1,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ThroughputPolicy {
    pub warmup_ms: u64,
    pub phase_ms: u64,
    pub pilot_ms: u64,
    pub max_round_items: u64,
}

impl Default for ThroughputPolicy {
    fn default() -> Self {
        Self {
            warmup_ms: DEFAULT_THROUGHPUT_WARMUP_MS,
            phase_ms: DEFAULT_THROUGHPUT_PHASE_MS,
            pilot_ms: DEFAULT_THROUGHPUT_PILOT_MS,
            max_round_items: DEFAULT_THROUGHPUT_MAX_ROUND_ITEMS,
        }
    }
}

impl ThroughputPolicy {
    pub fn warmup_duration(self) -> Duration {
        Duration::from_millis(self.warmup_ms)
    }

    pub fn phase_duration(self) -> Duration {
        Duration::from_millis(self.phase_ms)
    }

    pub fn pilot_duration(self) -> Duration {
        Duration::from_millis(self.pilot_ms)
    }

    pub fn validate(self) -> Result<(), String> {
        if self.warmup_ms == 0 || self.phase_ms == 0 || self.pilot_ms == 0 {
            return Err("throughput timing values must be greater than zero".to_string());
        }
        if self.max_round_items == 0 {
            return Err("throughput max round items must be greater than zero".to_string());
        }
        Ok(())
    }
}

impl ItemPolicy {
    pub fn name(self) -> &'static str {
        match self {
            Self::Explicit => "explicit",
            Self::ScenarioScaledV1 => "scenario_scaled_v1",
        }
    }
}

impl Mode {
    pub fn name(self) -> &'static str {
        match self {
            Mode::Throughput => "throughput",
            Mode::ComplexThroughput => "complex_throughput",
            Mode::DataLatency => "data_latency",
            Mode::Fairness => "fairness",
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
    Lubq,
    SegQueue,
    ConcurrentQueue,
    FastFifo,
    LfQueue,
    Wcq,
    MutexVecDeque,
    MsQueue,
    NaiveFaaQueue,
    MoodycamelConcurrentQueue,
}

impl QueueKind {
    pub fn name(self) -> &'static str {
        match self {
            QueueKind::Ubq => "ubq",
            QueueKind::Lubq => "lubq",
            QueueKind::SegQueue => "segqueue",
            QueueKind::ConcurrentQueue => "concurrent-queue",
            QueueKind::FastFifo => "fastfifo",
            QueueKind::LfQueue => "lfqueue",
            QueueKind::Wcq => "wcq",
            QueueKind::MutexVecDeque => "mutex-vecdeque",
            QueueKind::MsQueue => "ms-queue",
            QueueKind::NaiveFaaQueue => "naive-faa-queue",
            QueueKind::MoodycamelConcurrentQueue => "moodycamel-cq",
        }
    }

    pub fn parse(input: &str) -> Option<Self> {
        match input.trim().to_ascii_lowercase().as_str() {
            "ubq" => Some(Self::Ubq),
            "lubq" | "linked-ubq" | "kfifo" => Some(Self::Lubq),
            "segqueue" | "crossbeam" | "crossbeam-segqueue" => Some(Self::SegQueue),
            "concurrent-queue" | "concurrent" => Some(Self::ConcurrentQueue),
            "fastfifo" | "fast-fifo" | "rbbq" | "bbq" => Some(Self::FastFifo),
            "lfqueue" | "lf-queue" | "lscq" | "scq" => Some(Self::LfQueue),
            "wcq" | "w-cq" | "wait-free-cq" | "wait-free-queue" => Some(Self::Wcq),
            "mutex-vecdeque" | "mutex" | "vecdeque" | "mutex-queue" => Some(Self::MutexVecDeque),
            "ms-queue" | "michael-scott" | "msqueue" => Some(Self::MsQueue),
            "naive-faa-queue" | "infinite-array-queue" | "livelock-queue" | "pathological" => {
                Some(Self::NaiveFaaQueue)
            }
            "moodycamel-cq" | "moodycamel" | "mc-queue" => Some(Self::MoodycamelConcurrentQueue),
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
    pub backoff: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UbqGrid {
    Page,
    // Kept so schema-v7 plans and result metadata remain deserializable.
    Sparse,
    Dense,
}

#[derive(
    Clone, Copy, Debug, Default, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum CorePlacement {
    #[default]
    Interleaved,
}

impl CorePlacement {
    pub fn name(self) -> &'static str {
        match self {
            Self::Interleaved => "interleaved",
        }
    }
}

impl UbqGrid {
    pub fn name(self) -> &'static str {
        match self {
            Self::Page => "page",
            Self::Sparse => "sparse",
            Self::Dense => "dense",
        }
    }

    pub fn labels(self) -> Vec<String> {
        let _ = self;
        UBQ_BACKOFF_VALUES
            .iter()
            .map(|backoff| format!("balanced,1,page,{backoff}"))
            .collect()
    }
}

impl UbqLabel {
    pub fn text(&self) -> String {
        format!("{},1,page,{}", self.preset, self.backoff)
    }

    pub fn safe(&self) -> String {
        format!("{}_1_page_{}", self.preset, self.backoff)
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
    pub fastfifo_capacity: Option<usize>,
    #[serde(default)]
    pub lfqueue_segment_size: Option<usize>,
    #[serde(default)]
    pub wcq_capacity: Option<usize>,
}

#[derive(Debug, Serialize, Deserialize)]
struct WorkerRequest {
    protocol_version: u32,
    request_id: u64,
    command: WorkerCommand,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "command", rename_all = "snake_case")]
enum WorkerCommand {
    Run { spec: JobSpec },
    Shutdown,
}

#[derive(Debug, Serialize, Deserialize)]
struct WorkerResponse {
    protocol_version: u32,
    request_id: u64,
    result: WorkerResult,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "result", rename_all = "snake_case")]
enum WorkerResult {
    Completed { record: BenchRecord },
    Failed { reason: String },
    ShuttingDown,
    ProtocolError { reason: String },
}

impl JobSpec {
    pub fn queue_label(&self) -> String {
        match (
            &self.queue,
            &self.ubq_label,
            self.fastfifo_block_size,
            self.fastfifo_capacity,
            self.lfqueue_segment_size,
            self.wcq_capacity,
        ) {
            (QueueKind::Ubq, Some(label), _, _, _, _) => format!("ubq_{label}"),
            (QueueKind::FastFifo, _, Some(block_size), Some(capacity), _, _) => {
                fastfifo_queue_label(block_size, capacity)
            }
            (QueueKind::LfQueue, _, _, _, Some(segment_size), _) => {
                lfqueue_queue_label(segment_size)
            }
            (QueueKind::Wcq, _, _, _, _, Some(capacity)) => wcq_queue_label(capacity),
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
    pub core_placement: CorePlacement,
    #[serde(default)]
    pub item_policy: ItemPolicy,
    pub machine_label: String,
    pub runs_dir: PathBuf,
    pub available_parallelism: usize,
    #[serde(default)]
    pub core_ids: Vec<usize>,
    #[serde(default)]
    pub allow_unpinned: bool,
    #[serde(default = "default_schedule_seed")]
    pub schedule_seed: u64,
    #[serde(default)]
    pub throughput_policy: ThroughputPolicy,
    #[serde(default)]
    pub job_timeout_secs: Option<u64>,
    pub baseline_queues: Vec<QueueKind>,
    #[serde(default)]
    pub fastfifo_block_sizes: Vec<usize>,
    #[serde(default = "default_fastfifo_capacities")]
    pub fastfifo_capacities: Vec<usize>,
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

/// Outcome returned after a matrix scheduler finishes. Infrastructure errors
/// such as worker spawn or protocol failures still propagate as `Err`.
#[derive(Debug)]
pub struct BatchOutcome {
    /// `true` if the scheduler itself completed successfully.
    pub exit_success: bool,
    /// Legacy generated schedulers may identify an in-flight UBQ victim here.
    /// The persistent-worker scheduler checkpoints failures per sample instead.
    pub crashed_job: Option<(String, String)>,
}

/// Per-scenario file-level metadata. One `OutputMeta` describes the whole
/// coalesced `record.json` for a (machine_label, scenario) pair, not a
/// single sample — everything that varies per sample lives on `BenchRecord`
/// instead. `last_updated_unix_ms`, `host_uname`, `git_commit`, `git_dirty`,
/// `rustc_version`, and `package_version` are purely informational
/// provenance of the most recent write; nothing here gates reuse.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OutputMeta {
    pub machine_label: String,
    pub scenario: String,
    pub producers: usize,
    pub consumers: usize,
    pub last_updated_unix_ms: u128,
    #[serde(default)]
    pub host_uname: String,
    #[serde(default)]
    pub git_commit: String,
    #[serde(default)]
    pub git_dirty: bool,
    #[serde(default)]
    pub rustc_version: String,
    #[serde(default)]
    pub package_version: String,
    // Grid-coverage descriptor, merged (union/max) across every plan that has
    // ever written into this file — drives the "grid exhausted" badge.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ubq_grid: Option<UbqGrid>,
    #[serde(default)]
    pub expected_ubq_configurations: usize,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub ubq_batch_sizes: Vec<usize>,
    #[serde(default)]
    pub planned_repeats: usize,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub planned_items_per_producer: Vec<u64>,
}

/// The measurement conditions a sample was collected under. Stamped on every
/// `BenchRecord` (not just once per file) so reuse can gate per-sample: if a
/// scenario's `record.json` already has some records measured under one
/// protocol and a later run under the same machine-label changes e.g.
/// `--throughput-phase-ms`, only the affected samples recompute.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct MeasurementProtocol {
    pub available_parallelism: usize,
    pub core_placement: CorePlacement,
    pub affinity_authoritative: bool,
    pub selected_core_ids: Vec<usize>,
    pub item_policy: ItemPolicy,
    pub throughput_policy: ThroughputPolicy,
}

impl MeasurementProtocol {
    fn from_plan(plan: &MatrixPlan) -> Result<Self, String> {
        Ok(Self {
            available_parallelism: plan.available_parallelism,
            core_placement: plan.core_placement,
            affinity_authoritative: !plan.allow_unpinned,
            selected_core_ids: selected_plan_core_ids(plan)?,
            item_policy: plan.item_policy,
            throughput_policy: plan.throughput_policy,
        })
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BenchRecordStatus {
    #[default]
    Completed,
    Failed,
    TimedOut,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ThroughputMetrics {
    pub requested_items_per_producer: u64,
    pub pilot_items_per_producer: u64,
    pub calibration_elapsed_ns: u64,
    pub warmup_elapsed_ns: u64,
    pub warmup_rounds: usize,
    pub handoff_items: u64,
    pub handoff_elapsed_ns: u64,
    pub handoff_rounds: usize,
    pub enqueue_items: u64,
    pub enqueue_elapsed_ns: u64,
    pub enqueue_rounds: usize,
    pub dequeue_items: u64,
    pub dequeue_elapsed_ns: u64,
    pub dequeue_rounds: usize,
    pub enqueue_ops_per_sec: f64,
    pub dequeue_ops_per_sec: f64,
    pub affinity_authoritative: bool,
    pub schedule_seed: u64,
    pub execution_ordinal: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub requested_queue_capacity: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub effective_queue_capacity: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ceiling_warning: Option<String>,
}

impl BenchRecordStatus {
    fn completed() -> Self {
        Self::Completed
    }
}

fn is_completed_status(status: &BenchRecordStatus) -> bool {
    matches!(status, BenchRecordStatus::Completed)
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct BenchRecord {
    #[serde(default)]
    pub repeat_index: usize,
    pub queue: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ubq_label: Option<String>,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub throughput_metrics: Option<ThroughputMetrics>,
    #[serde(default)]
    pub protocol: MeasurementProtocol,
    // u64, not u128: this record crosses the worker subprocess IPC boundary
    // inside an internally-tagged `WorkerResult` enum, and serde's tagged-enum
    // deserialization does not support 128-bit integers.
    #[serde(default)]
    pub timestamp_unix_ms: u64,
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

/// All state for one coalesced `runs_dir/<machine_label>/<scenario>/record.json`.
/// A single scenario file aggregates every queue/config/repeat measured for
/// that scenario, so this groups by scenario rather than by `PlanBundle`.
#[derive(Clone)]
struct ScenarioOutputState {
    meta: OutputMeta,
    path: PathBuf,
    ordered_keys: Vec<SampleKey>,
    records: BTreeMap<SampleKey, BenchRecord>,
    dirty: bool,
}

impl ScenarioOutputState {
    fn new(plan: &MatrixPlan, scenario: &ScenarioConfig) -> Result<Self, String> {
        let path = output_path_for_scenario(plan, &scenario.name);
        let previous_meta = read_existing_output_meta(&path);
        Ok(Self {
            meta: scenario_output_meta(plan, scenario, previous_meta.as_ref()),
            path,
            ordered_keys: expected_keys_for_scenario(plan, &scenario.name),
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

        self.meta.last_updated_unix_ms = now_unix_ms();
        let output = OutputFile {
            schema_version: RUN_SCHEMA_VERSION,
            meta: self.meta.clone(),
            results: self
                .ordered_keys
                .iter()
                .filter_map(|key| self.records.get(key).cloned())
                .collect(),
        };
        let json = serde_json::to_string(&output)
            .map_err(|err| format!("failed to serialize output: {err}"))?;
        atomic_write_string(&self.path, &json)?;
        self.dirty = false;
        Ok(true)
    }
}

struct IncrementalOutputWriter {
    scenarios: Vec<ScenarioOutputState>,
    scenario_index_by_name: BTreeMap<String, usize>,
    write_count: usize,
}

impl IncrementalOutputWriter {
    fn new(plan: &MatrixPlan, cache: &ExistingRunsIndex) -> Result<Self, String> {
        let mut scenarios = Vec::new();
        let mut scenario_index_by_name: BTreeMap<String, usize> = BTreeMap::new();

        for bundle in &plan.bundles {
            if scenario_index_by_name.contains_key(&bundle.scenario.name) {
                continue;
            }
            let index = scenarios.len();
            let state = ScenarioOutputState::new(plan, &bundle.scenario)?;
            scenario_index_by_name.insert(bundle.scenario.name.clone(), index);
            scenarios.push(state);
        }

        let mut writer = Self {
            scenarios,
            scenario_index_by_name,
            write_count: 0,
        };
        for (key, record) in &cache.records {
            writer.seed_cached_record(key, record);
        }
        Ok(writer)
    }

    fn seed_cached_record(&mut self, key: &SampleKey, record: &BenchRecord) {
        let Some(&index) = self.scenario_index_by_name.get(&key.scenario) else {
            return;
        };
        self.scenarios[index]
            .records
            .insert(key.clone(), record.clone());
    }

    fn handle_completed_record(
        &mut self,
        key: SampleKey,
        record: BenchRecord,
    ) -> Result<(), String> {
        let Some(&index) = self.scenario_index_by_name.get(&key.scenario) else {
            return Err(format!(
                "missing output scenario mapping for {} scenario={} repeat={} mode={} items={}",
                key.queue_label,
                key.scenario,
                key.repeat_index,
                key.mode.name(),
                key.items_per_producer
            ));
        };

        let scenario = &mut self.scenarios[index];
        scenario.store_record(&key, &record);
        if scenario.flush()? {
            self.write_count += 1;
        }

        Ok(())
    }

    fn finish(mut self, expect_complete: bool) -> Result<usize, String> {
        if expect_complete {
            for scenario in &self.scenarios {
                if let Some(missing) = scenario.missing_keys().next() {
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

        for scenario in &mut self.scenarios {
            if scenario.flush()? {
                self.write_count += 1;
            }
        }

        progress_line(format!("wrote {} output snapshot(s)", self.write_count));
        Ok(self.write_count)
    }
}

enum OutputWriterMessage {
    Completed {
        key: SampleKey,
        record: BenchRecord,
        persisted: mpsc::Sender<Result<(), String>>,
    },
    Finish {
        expect_complete: bool,
    },
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
                    OutputWriterMessage::Completed {
                        key,
                        record,
                        persisted,
                    } => {
                        let result = writer.handle_completed_record(key, record);
                        let _ = persisted.send(result.clone());
                        result?;
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
        let (persisted_tx, persisted_rx) = mpsc::channel();
        self.tx
            .as_ref()
            .expect("output writer sender available")
            .send(OutputWriterMessage::Completed {
                key,
                record,
                persisted: persisted_tx,
            })
            .map_err(|_| format!("output writer stopped before persisting {label}"))?;
        persisted_rx
            .recv()
            .map_err(|_| format!("output writer stopped while persisting {label}"))?
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

/// Parse an ordered CPU selection such as `0-7,16-23`.
pub fn parse_core_ids(raw: &str) -> Result<Vec<usize>, String> {
    let mut ids = Vec::new();
    let mut seen = BTreeSet::new();
    for token in raw
        .split(',')
        .map(str::trim)
        .filter(|token| !token.is_empty())
    {
        if let Some((start, end)) = token.split_once('-') {
            let start = start
                .trim()
                .parse::<usize>()
                .map_err(|_| format!("invalid CPU range start `{start}`"))?;
            let end = end
                .trim()
                .parse::<usize>()
                .map_err(|_| format!("invalid CPU range end `{end}`"))?;
            if start > end {
                return Err(format!("CPU range `{token}` is descending"));
            }
            for id in start..=end {
                if !seen.insert(id) {
                    return Err(format!("CPU {id} is selected more than once"));
                }
                ids.push(id);
            }
        } else {
            let id = token
                .parse::<usize>()
                .map_err(|_| format!("invalid CPU id `{token}`"))?;
            if !seen.insert(id) {
                return Err(format!("CPU {id} is selected more than once"));
            }
            ids.push(id);
        }
    }
    if ids.is_empty() {
        return Err("at least one CPU id must be selected".to_string());
    }
    Ok(ids)
}

pub fn parse_schedule_seed(raw: &str) -> Result<u64, String> {
    let raw = raw.trim();
    if let Some(hex) = raw.strip_prefix("0x").or_else(|| raw.strip_prefix("0X")) {
        u64::from_str_radix(hex, 16)
            .map_err(|_| format!("invalid hexadecimal schedule seed `{raw}`"))
    } else {
        raw.parse::<u64>()
            .map_err(|_| format!("invalid schedule seed `{raw}`"))
    }
}

pub fn power_of_two_scenarios(available_parallelism: usize) -> Result<Vec<ScenarioConfig>, String> {
    if available_parallelism < 2 {
        return Err(
            "the power-of-two scenario grid requires available_parallelism >= 2".to_string(),
        );
    }

    let mut thread_counts = Vec::new();
    let mut count = 1usize;
    while count < available_parallelism {
        thread_counts.push(count);
        let Some(next) = count.checked_mul(2) else {
            break;
        };
        count = next;
    }

    let mut scenarios = Vec::new();
    for &producers in &thread_counts {
        for &consumers in &thread_counts {
            if producers
                .checked_add(consumers)
                .is_some_and(|total| total <= available_parallelism)
            {
                scenarios.push(ScenarioConfig::new(producers, consumers));
            }
        }
    }
    scenarios.sort_by_key(|scenario| {
        (
            scenario.total_threads(),
            scenario.producers,
            scenario.consumers,
        )
    });
    Ok(scenarios)
}

pub fn default_scenarios() -> Vec<ScenarioConfig> {
    detect_available_parallelism()
        .and_then(power_of_two_scenarios)
        .unwrap_or_else(|_| vec![ScenarioConfig::new(1, 1)])
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
    if matches!(normalized.as_str(), "pow2:machine" | "power-of-two:machine") {
        let parallelism = match machine_parallelism {
            Some(value) => value,
            None => detect_available_parallelism()?,
        };
        return power_of_two_scenarios(parallelism);
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

pub fn scenario_scaled_items_per_producer(producers: usize) -> u64 {
    match producers {
        0 => panic!("a benchmark scenario must have at least one producer"),
        1..=8 => 1_000_000,
        9..=16 => 250_000,
        17..=32 => 62_500,
        _ => 15_625,
    }
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

fn fastfifo_queue_label(block_size: usize, capacity: usize) -> String {
    format!("fastfifo_b{block_size}_c{capacity}")
}

pub fn parse_fastfifo_capacities(raw: Option<&str>) -> Result<Vec<usize>, String> {
    let source = raw
        .map(parse_csv_list)
        .unwrap_or_else(|| vec![DEFAULT_FASTFIFO_CAPACITY.to_string()]);
    let mut out = Vec::new();
    let mut seen = BTreeSet::new();
    for token in source {
        let parsed = token
            .parse::<usize>()
            .map_err(|_| format!("invalid FastFifo capacity `{token}`"))?;
        if parsed == 0 {
            return Err("FastFifo capacities must be > 0".to_string());
        }
        if seen.insert(parsed) {
            out.push(parsed);
        }
    }
    Ok(out)
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
    _mode: Mode,
    _capacity: usize,
    _scenario: &ScenarioConfig,
    _items_per_producer: u64,
) -> bool {
    // The current wCQ dependency remains excluded from comparative jobs due
    // to its known wrapped-ring emptiness and split-CAS correctness issues.
    false
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
    let legacy_pool = parts[1]
        .parse::<u8>()
        .map_err(|_| format!("invalid UBQ label '{token}'"))?;
    let block_is_valid = parts[2] == "page"
        || parts[2]
            .parse::<u16>()
            .is_ok_and(|block| LEGACY_UBQ_BLOCK_VALUES.contains(&block));
    if require_valid && (legacy_pool != 1 || !block_is_valid) {
        return Err(format!("invalid UBQ label '{token}'"));
    }
    let label = UbqLabel {
        preset: parts[0].to_string(),
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
    UBQ_BACKOFF_VALUES.contains(&label.backoff.as_str())
}

pub fn is_valid_ubq_label_for_scenario(label: &UbqLabel, _scenario: &ScenarioConfig) -> bool {
    is_valid_ubq_label(label)
}

fn validate_ubq_label_for_scenario(
    label: &UbqLabel,
    scenario: &ScenarioConfig,
) -> Result<(), String> {
    if is_valid_ubq_label_for_scenario(label, scenario) {
        return Ok(());
    }
    Err(format!(
        "invalid UBQ label '{}' for scenario {}",
        label.text(),
        scenario.name
    ))
}

pub fn normalize_ubq_label(token: &str, require_valid: bool) -> Option<String> {
    parse_ubq_label(token, require_valid)
        .ok()
        .map(|value| value.text())
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

fn immediate_neighbors(label: &UbqLabel, idx: usize) -> Vec<UbqLabel> {
    let mut out = Vec::new();
    if idx == 0 {
        for backoff in immediate_domain_neighbors_str(&label.backoff, &UBQ_BACKOFF_VALUES) {
            out.push(UbqLabel {
                preset: label.preset.clone(),
                backoff: backoff.to_string(),
            });
        }
    }
    out
}

fn required_ubq_labels_for_center(label: &UbqLabel) -> BTreeSet<UbqLabel> {
    let mut required = BTreeSet::new();
    required.insert(label.clone());

    for idx in 0..1 {
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
        let mut parsed = BTreeSet::new();
        for label in ubq_labels {
            parsed.insert(parse_ubq_label(label, true)?);
        }
        parsed.into_iter().collect::<Vec<_>>()
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
            }
            if !baseline_queues.is_empty() {
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
        core_placement: CorePlacement::Interleaved,
        item_policy: ItemPolicy::Explicit,
        machine_label: normalize_machine(machine_label),
        runs_dir,
        available_parallelism,
        core_ids: Vec::new(),
        allow_unpinned: false,
        schedule_seed: DEFAULT_SCHEDULE_SEED,
        throughput_policy: ThroughputPolicy::default(),
        job_timeout_secs: None,
        baseline_queues,
        fastfifo_block_sizes: normalized_fastfifo_block_sizes,
        fastfifo_capacities: default_fastfifo_capacities(),
        lfqueue_segment_sizes: normalized_lfqueue_segment_sizes,
        wcq_capacities: normalized_wcq_capacities,
        ubq_grid: None,
        ubq_batch_sizes: Vec::new(),
        planned_repeats: repeats,
        bundles,
        reuse_existing,
    })
}

fn normalize_ubq_batch_sizes(batch_sizes: &[usize]) -> Result<Vec<usize>, String> {
    let mut normalized = Vec::with_capacity(batch_sizes.len());
    let mut seen = BTreeSet::new();
    for &batch_size in batch_sizes {
        if batch_size < 2 {
            return Err(
                "queue batch sizes must be >= 2; scalar-compatible runs are measured separately"
                    .to_string(),
            );
        }
        if seen.insert(batch_size) {
            normalized.push(batch_size);
        }
    }
    Ok(normalized)
}

/// Like [`build_direct_matrix_plan`], but additionally sweeps `batch_sizes`
/// for every UBQ backoff policy in the plan (each is also measured scalar-only).
#[allow(clippy::too_many_arguments)]
pub fn build_direct_matrix_plan_with_batch_sizes(
    machine_label: &str,
    runs_dir: PathBuf,
    available_parallelism: usize,
    selected_queues: &[QueueKind],
    ubq_labels: &[String],
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
    let normalized_batch_sizes = normalize_ubq_batch_sizes(batch_sizes)?;
    let mut plan = build_direct_matrix_plan(
        machine_label,
        runs_dir,
        available_parallelism,
        selected_queues,
        ubq_labels,
        fastfifo_block_sizes,
        lfqueue_segment_sizes,
        wcq_capacities,
        scenarios,
        modes,
        items_per_producer_values,
        repeats,
        reuse_existing,
    )?;
    plan.ubq_batch_sizes = normalized_batch_sizes;
    Ok(plan)
}

#[allow(clippy::too_many_arguments)]
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
    explicit_items_per_producer_values: Option<&[u64]>,
    repeats: usize,
    reuse_existing: bool,
) -> Result<MatrixPlan, String> {
    build_grid_matrix_plan_impl(
        machine_label,
        runs_dir,
        available_parallelism,
        selected_queues,
        grid,
        &[],
        batch_sizes,
        fastfifo_block_sizes,
        lfqueue_segment_sizes,
        wcq_capacities,
        scenarios,
        modes,
        explicit_items_per_producer_values,
        repeats,
        reuse_existing,
    )
}

/// Like [`build_grid_matrix_plan`], but accepts additional UBQ labels for
/// compatibility with callers that explicitly select a backoff policy.
#[allow(clippy::too_many_arguments)]
pub fn build_grid_matrix_plan_with_extra_ubq_labels(
    machine_label: &str,
    runs_dir: PathBuf,
    available_parallelism: usize,
    selected_queues: &[QueueKind],
    grid: UbqGrid,
    extra_ubq_labels: &[String],
    batch_sizes: &[usize],
    fastfifo_block_sizes: &[usize],
    lfqueue_segment_sizes: &[usize],
    wcq_capacities: &[usize],
    scenarios: &[ScenarioConfig],
    modes: &[Mode],
    explicit_items_per_producer_values: Option<&[u64]>,
    repeats: usize,
    reuse_existing: bool,
) -> Result<MatrixPlan, String> {
    build_grid_matrix_plan_impl(
        machine_label,
        runs_dir,
        available_parallelism,
        selected_queues,
        grid,
        extra_ubq_labels,
        batch_sizes,
        fastfifo_block_sizes,
        lfqueue_segment_sizes,
        wcq_capacities,
        scenarios,
        modes,
        explicit_items_per_producer_values,
        repeats,
        reuse_existing,
    )
}

#[allow(clippy::too_many_arguments)]
fn build_grid_matrix_plan_impl(
    machine_label: &str,
    runs_dir: PathBuf,
    available_parallelism: usize,
    selected_queues: &[QueueKind],
    grid: UbqGrid,
    extra_ubq_labels: &[String],
    batch_sizes: &[usize],
    fastfifo_block_sizes: &[usize],
    lfqueue_segment_sizes: &[usize],
    wcq_capacities: &[usize],
    scenarios: &[ScenarioConfig],
    modes: &[Mode],
    explicit_items_per_producer_values: Option<&[u64]>,
    repeats: usize,
    reuse_existing: bool,
) -> Result<MatrixPlan, String> {
    let normalized_batch_sizes = normalize_ubq_batch_sizes(batch_sizes)?;
    let item_policy = if explicit_items_per_producer_values.is_some() {
        ItemPolicy::Explicit
    } else {
        ItemPolicy::ScenarioScaledV1
    };
    let explicit_items = explicit_items_per_producer_values.unwrap_or(&[]);
    if matches!(item_policy, ItemPolicy::Explicit) && explicit_items.is_empty() {
        return Err("at least one explicit items-per-producer value is required".to_string());
    }
    let direct_items = if matches!(item_policy, ItemPolicy::Explicit) {
        explicit_items
    } else {
        std::slice::from_ref(&DEFAULT_ITEMS_PER_PRODUCER)
    };
    let mut labels = grid.labels();
    labels.extend(extra_ubq_labels.iter().cloned());
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
        direct_items,
        repeats,
        reuse_existing,
    )?;
    plan.item_policy = item_policy;
    if matches!(item_policy, ItemPolicy::ScenarioScaledV1) {
        for bundle in &mut plan.bundles {
            bundle.items_per_producer_values = vec![scenario_scaled_items_per_producer(
                bundle.scenario.producers,
            )];
        }
    }
    plan.ubq_grid = selected_queues
        .iter()
        .any(|queue| *queue == QueueKind::Ubq)
        .then_some(grid);
    plan.ubq_batch_sizes = normalized_batch_sizes;
    plan.planned_repeats = repeats;
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

/// Loads every scenario's coalesced `record.json` a plan would touch and
/// keeps only samples that are reusable for it: schema-compatible,
/// completed, recorded under this exact `machine_label`, and measured under
/// the same [`MeasurementProtocol`] this plan would use. There's no
/// recursive directory scan — each scenario's path is a direct lookup, and
/// at most one record exists per `SampleKey` since a scenario's samples all
/// live in one file by construction.
pub fn load_existing_runs(plan: &MatrixPlan) -> Result<ExistingRunsIndex, String> {
    let protocol = MeasurementProtocol::from_plan(plan)?;
    let machine_label = normalize_machine(&plan.machine_label);
    let mut scenario_names: BTreeSet<&str> = BTreeSet::new();
    for bundle in &plan.bundles {
        scenario_names.insert(&bundle.scenario.name);
    }

    let mut index = ExistingRunsIndex::default();
    for scenario_name in scenario_names {
        let path = output_path_for_scenario(plan, scenario_name);
        let Ok(raw) = fs::read_to_string(&path) else {
            continue;
        };
        let Ok(parsed) = serde_json::from_str::<OutputFile>(&raw) else {
            continue;
        };
        if parsed.schema_version != RUN_SCHEMA_VERSION {
            continue;
        }
        if normalize_machine(&parsed.meta.machine_label) != machine_label {
            continue;
        }
        for record in parsed.results {
            if !record.completed() || record.protocol != protocol {
                continue;
            }
            let queue_label = match record.queue.as_str() {
                "ubq" => match record.ubq_label.as_deref() {
                    Some(label) => format!("ubq_{label}"),
                    None => continue,
                },
                _ => record.queue.clone(),
            };
            let requested_items_per_producer = record
                .throughput_metrics
                .as_ref()
                .map(|metrics| metrics.requested_items_per_producer)
                .unwrap_or(record.items_per_producer);
            let key = SampleKey {
                scenario: scenario_name.to_string(),
                repeat_index: record.repeat_index,
                mode: Mode::parse(&record.mode).unwrap_or(Mode::Throughput),
                items_per_producer: requested_items_per_producer,
                queue_label,
                batch_size: record.batch_size,
            };
            index.records.insert(key, record);
        }
    }

    Ok(index)
}

#[cfg(test)]
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
            let item_values = if *mode == Mode::Throughput {
                &bundle.items_per_producer_values[..bundle.items_per_producer_values.len().min(1)]
            } else {
                bundle.items_per_producer_values.as_slice()
            };
            for &items_per_producer in item_values {
                if bundle.ubq_label.is_none() {
                    for &baseline_queue in &plan.baseline_queues {
                        match baseline_queue {
                            QueueKind::SegQueue | QueueKind::Lubq => {
                                let spec = JobSpec {
                                    scenario: bundle.scenario.clone(),
                                    repeat_index: bundle.repeat_index,
                                    mode: *mode,
                                    items_per_producer,
                                    queue: baseline_queue,
                                    ubq_label: None,
                                    batch_size: None,
                                    fastfifo_block_size: None,
                                    fastfifo_capacity: None,
                                    lfqueue_segment_size: None,
                                    wcq_capacity: None,
                                };
                                out.insert(spec.clone());
                                if *mode == Mode::Throughput {
                                    for &batch_size in &plan.ubq_batch_sizes {
                                        out.insert(JobSpec {
                                            batch_size: Some(batch_size),
                                            ..spec.clone()
                                        });
                                    }
                                }
                            }
                            QueueKind::MutexVecDeque => {
                                let spec = JobSpec {
                                    scenario: bundle.scenario.clone(),
                                    repeat_index: bundle.repeat_index,
                                    mode: *mode,
                                    items_per_producer,
                                    queue: baseline_queue,
                                    ubq_label: None,
                                    batch_size: None,
                                    fastfifo_block_size: None,
                                    fastfifo_capacity: None,
                                    lfqueue_segment_size: None,
                                    wcq_capacity: None,
                                };
                                out.insert(spec.clone());
                                if *mode == Mode::Throughput {
                                    for &batch_size in &plan.ubq_batch_sizes {
                                        out.insert(JobSpec {
                                            batch_size: Some(batch_size),
                                            ..spec.clone()
                                        });
                                    }
                                }
                            }
                            QueueKind::FastFifo => {
                                for &block_size in &plan.fastfifo_block_sizes {
                                    for &capacity in &plan.fastfifo_capacities {
                                        out.insert(JobSpec {
                                            scenario: bundle.scenario.clone(),
                                            repeat_index: bundle.repeat_index,
                                            mode: *mode,
                                            items_per_producer,
                                            queue: baseline_queue,
                                            ubq_label: None,
                                            batch_size: None,
                                            fastfifo_block_size: Some(block_size),
                                            fastfifo_capacity: Some(capacity),
                                            lfqueue_segment_size: None,
                                            wcq_capacity: None,
                                        });
                                    }
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
                                        fastfifo_capacity: None,
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
                                            fastfifo_capacity: None,
                                            lfqueue_segment_size: None,
                                            wcq_capacity: Some(capacity),
                                        });
                                    }
                                }
                            }
                            QueueKind::MoodycamelConcurrentQueue => {
                                let spec = JobSpec {
                                    scenario: bundle.scenario.clone(),
                                    repeat_index: bundle.repeat_index,
                                    mode: *mode,
                                    items_per_producer,
                                    queue: baseline_queue,
                                    ubq_label: None,
                                    batch_size: None,
                                    fastfifo_block_size: None,
                                    fastfifo_capacity: None,
                                    lfqueue_segment_size: None,
                                    wcq_capacity: None,
                                };
                                out.insert(spec.clone());
                                if *mode == Mode::Throughput {
                                    for &batch_size in &plan.ubq_batch_sizes {
                                        out.insert(JobSpec {
                                            batch_size: Some(batch_size),
                                            ..spec.clone()
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
                                    fastfifo_capacity: None,
                                    lfqueue_segment_size: None,
                                    wcq_capacity: None,
                                });
                            }
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
                        fastfifo_capacity: None,
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
                                fastfifo_capacity: None,
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
        fastfifo_capacity: None,
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
    make_segqueue_job_factory_variant(scenario, repeat_index, mode, items_per_producer, None)
}

pub fn make_segqueue_job_factory_variant(
    scenario: ScenarioConfig,
    repeat_index: usize,
    mode: Mode,
    items_per_producer: u64,
    batch_size: Option<usize>,
) -> JobFactory {
    assert!(
        batch_size.is_none() || mode == Mode::Throughput,
        "batched SegQueue jobs are supported only in throughput mode"
    );
    let spec = JobSpec {
        scenario: scenario.clone(),
        repeat_index,
        mode,
        items_per_producer,
        queue: QueueKind::SegQueue,
        ubq_label: None,
        batch_size,
        fastfifo_block_size: None,
        fastfifo_capacity: None,
        lfqueue_segment_size: None,
        wcq_capacity: None,
    };
    let queue_name = QueueKind::SegQueue.name().to_string();
    let run_scenario = scenario.clone();
    let run = Arc::new(move |core_offset: usize| match mode {
        Mode::Throughput => match batch_size {
            Some(batch_size) => bench_throughput_batched_for::<BatchQueue<u64>>(
                &queue_name,
                &run_scenario,
                items_per_producer,
                batch_size,
                core_offset,
            ),
            None => bench_throughput_for::<SegQueue<u64>>(
                &queue_name,
                &run_scenario,
                items_per_producer,
                core_offset,
            ),
        },
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

pub fn make_lubq_job_factory(
    scenario: ScenarioConfig,
    repeat_index: usize,
    mode: Mode,
    items_per_producer: u64,
    batch_size: Option<usize>,
) -> JobFactory {
    assert!(
        batch_size.is_none() || mode == Mode::Throughput,
        "batched LUBQ jobs are supported only in throughput mode"
    );
    let spec = JobSpec {
        scenario: scenario.clone(),
        repeat_index,
        mode,
        items_per_producer,
        queue: QueueKind::Lubq,
        ubq_label: None,
        batch_size,
        fastfifo_block_size: None,
        fastfifo_capacity: None,
        lfqueue_segment_size: None,
        wcq_capacity: None,
    };
    let queue_name = QueueKind::Lubq.name().to_string();
    let run_scenario = scenario.clone();
    let run = Arc::new(move |core_offset: usize| match mode {
        Mode::Throughput => bench_throughput_with_queue_variant(
            LubqBenchQueue::new(run_scenario.producers, run_scenario.consumers),
            &queue_name,
            &run_scenario,
            items_per_producer,
            batch_size,
            core_offset,
        ),
        Mode::ComplexThroughput => bench_complex_throughput_with_queue(
            LubqBenchQueue::new(run_scenario.producers, run_scenario.consumers),
            &queue_name,
            &run_scenario,
            items_per_producer,
            core_offset,
        ),
        Mode::DataLatency => bench_data_latency_with_queue(
            LubqBenchQueue::new(run_scenario.producers, run_scenario.consumers),
            &queue_name,
            &run_scenario,
            items_per_producer,
            core_offset,
        ),
        Mode::Fairness => bench_fairness_with_queue(
            LubqBenchQueue::new(run_scenario.producers, run_scenario.consumers),
            &queue_name,
            &run_scenario,
            items_per_producer,
            core_offset,
        ),
        Mode::AppLogFanIn => bench_app_log_fan_in_with_queue(
            LubqBenchQueue::new(run_scenario.producers, run_scenario.consumers),
            &queue_name,
            &run_scenario,
            items_per_producer,
            core_offset,
        ),
        Mode::AppPipeline => bench_app_pipeline_with_queues(
            LubqBenchQueue::new(run_scenario.producers, run_scenario.consumers),
            LubqBenchQueue::new(run_scenario.consumers, 1),
            &queue_name,
            &run_scenario,
            items_per_producer,
            core_offset,
        ),
        Mode::AppTaskRoundtrip => bench_app_task_roundtrip_with_queues(
            LubqBenchQueue::new(run_scenario.producers, run_scenario.consumers),
            LubqBenchQueue::new(run_scenario.consumers, run_scenario.producers),
            &queue_name,
            &run_scenario,
            items_per_producer,
            core_offset,
        ),
        Mode::AppLogMpscFile => bench_app_log_mpsc_file_with_queue(
            LubqBenchQueue::new(run_scenario.producers, 1),
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
        fastfifo_capacity: None,
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

pub fn make_mutex_vecdeque_job_factory(
    scenario: ScenarioConfig,
    repeat_index: usize,
    mode: Mode,
    items_per_producer: u64,
    batch_size: Option<usize>,
) -> JobFactory {
    assert!(
        batch_size.is_none() || mode == Mode::Throughput,
        "batched mutex-vecdeque jobs are supported only in throughput mode"
    );
    let spec = JobSpec {
        scenario: scenario.clone(),
        repeat_index,
        mode,
        items_per_producer,
        queue: QueueKind::MutexVecDeque,
        ubq_label: None,
        batch_size,
        fastfifo_block_size: None,
        fastfifo_capacity: None,
        lfqueue_segment_size: None,
        wcq_capacity: None,
    };
    let queue_name = QueueKind::MutexVecDeque.name().to_string();
    let run_scenario = scenario.clone();
    let run = Arc::new(move |core_offset: usize| match mode {
        Mode::Throughput => match batch_size {
            Some(batch_size) => bench_throughput_batched_for::<MutexQueue<u64>>(
                &queue_name,
                &run_scenario,
                items_per_producer,
                batch_size,
                core_offset,
            ),
            None => bench_throughput_for::<MutexQueue<u64>>(
                &queue_name,
                &run_scenario,
                items_per_producer,
                core_offset,
            ),
        },
        Mode::ComplexThroughput => bench_complex_throughput_for::<MutexQueue<u64>>(
            &queue_name,
            &run_scenario,
            items_per_producer,
            core_offset,
        ),
        Mode::DataLatency => bench_data_latency_for::<MutexQueue<u64>>(
            &queue_name,
            &run_scenario,
            items_per_producer,
            core_offset,
        ),
        Mode::Fairness => bench_fairness_for::<MutexQueue<u64>>(
            &queue_name,
            &run_scenario,
            items_per_producer,
            core_offset,
        ),
        Mode::AppLogFanIn => bench_app_log_fan_in_for::<MutexQueue<u64>>(
            &queue_name,
            &run_scenario,
            items_per_producer,
            core_offset,
        ),
        Mode::AppPipeline => bench_app_pipeline_for::<MutexQueue<u64>>(
            &queue_name,
            &run_scenario,
            items_per_producer,
            core_offset,
        ),
        Mode::AppTaskRoundtrip => bench_app_task_roundtrip_for::<MutexQueue<u64>>(
            &queue_name,
            &run_scenario,
            items_per_producer,
            core_offset,
        ),
        Mode::AppLogMpscFile => bench_app_log_mpsc_file_for::<MutexQueue<LogRecord>>(
            &queue_name,
            &run_scenario,
            items_per_producer,
            core_offset,
        ),
    });
    JobFactory { spec, run }
}

#[cfg(feature = "bench_moodycamel")]
pub fn make_moodycamel_job_factory(
    scenario: ScenarioConfig,
    repeat_index: usize,
    mode: Mode,
    items_per_producer: u64,
    batch_size: Option<usize>,
) -> JobFactory {
    assert!(
        batch_size.is_none() || mode == Mode::Throughput,
        "batched moodycamel-cq jobs are supported only in throughput mode"
    );
    let spec = JobSpec {
        scenario: scenario.clone(),
        repeat_index,
        mode,
        items_per_producer,
        queue: QueueKind::MoodycamelConcurrentQueue,
        ubq_label: None,
        batch_size,
        fastfifo_block_size: None,
        fastfifo_capacity: None,
        lfqueue_segment_size: None,
        wcq_capacity: None,
    };
    let queue_name = QueueKind::MoodycamelConcurrentQueue.name().to_string();
    let run_scenario = scenario.clone();
    // MoodycamelQueue implements BenchQueueHandleFactory/LogQueueHandleFactory
    // rather than BenchQueueOps/LogQueueOps directly, so every mode here goes
    // through the `_with_queue*` entry points (which take an
    // already-constructed handle) instead of the `_for::<Q: BenchQueue>` ones
    // that build a queue internally from a `BenchQueueOps` type. See
    // baselines::moodycamel_cq for why: each producer/consumer thread needs
    // its own ProducerToken/ConsumerToken, created once up front, rather than
    // sharing a single token-free queue handle.
    let run = Arc::new(move |core_offset: usize| match mode {
        Mode::Throughput => match batch_size {
            Some(batch_size) => bench_throughput_with_queue_variant(
                MoodycamelQueue::new_handle(),
                &queue_name,
                &run_scenario,
                items_per_producer,
                Some(batch_size),
                core_offset,
            ),
            None => bench_throughput_with_queue(
                MoodycamelQueue::new_handle(),
                &queue_name,
                &run_scenario,
                items_per_producer,
                core_offset,
            ),
        },
        Mode::ComplexThroughput => bench_complex_throughput_with_queue(
            MoodycamelQueue::new_handle(),
            &queue_name,
            &run_scenario,
            items_per_producer,
            core_offset,
        ),
        Mode::DataLatency => bench_data_latency_with_queue(
            MoodycamelQueue::new_handle(),
            &queue_name,
            &run_scenario,
            items_per_producer,
            core_offset,
        ),
        Mode::Fairness => bench_fairness_with_queue(
            MoodycamelQueue::new_handle(),
            &queue_name,
            &run_scenario,
            items_per_producer,
            core_offset,
        ),
        Mode::AppLogFanIn => bench_app_log_fan_in_with_queue(
            MoodycamelQueue::new_handle(),
            &queue_name,
            &run_scenario,
            items_per_producer,
            core_offset,
        ),
        Mode::AppPipeline => bench_app_pipeline_with_queues(
            MoodycamelQueue::new_handle(),
            MoodycamelQueue::new_handle(),
            &queue_name,
            &run_scenario,
            items_per_producer,
            core_offset,
        ),
        Mode::AppTaskRoundtrip => bench_app_task_roundtrip_with_queues(
            MoodycamelQueue::new_handle(),
            MoodycamelQueue::new_handle(),
            &queue_name,
            &run_scenario,
            items_per_producer,
            core_offset,
        ),
        Mode::AppLogMpscFile => bench_app_log_mpsc_file_with_queue(
            MoodycamelQueue::new_handle(),
            &queue_name,
            &run_scenario,
            items_per_producer,
            core_offset,
        ),
    });
    JobFactory { spec, run }
}

pub fn make_ms_queue_job_factory(
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
        queue: QueueKind::MsQueue,
        ubq_label: None,
        batch_size: None,
        fastfifo_block_size: None,
        fastfifo_capacity: None,
        lfqueue_segment_size: None,
        wcq_capacity: None,
    };
    let queue_name = QueueKind::MsQueue.name().to_string();
    let run_scenario = scenario.clone();
    let run = Arc::new(move |core_offset: usize| match mode {
        Mode::Throughput => bench_throughput_for::<MsQueue<u64>>(
            &queue_name,
            &run_scenario,
            items_per_producer,
            core_offset,
        ),
        Mode::ComplexThroughput => bench_complex_throughput_for::<MsQueue<u64>>(
            &queue_name,
            &run_scenario,
            items_per_producer,
            core_offset,
        ),
        Mode::DataLatency => bench_data_latency_for::<MsQueue<u64>>(
            &queue_name,
            &run_scenario,
            items_per_producer,
            core_offset,
        ),
        Mode::Fairness => bench_fairness_for::<MsQueue<u64>>(
            &queue_name,
            &run_scenario,
            items_per_producer,
            core_offset,
        ),
        Mode::AppLogFanIn => bench_app_log_fan_in_for::<MsQueue<u64>>(
            &queue_name,
            &run_scenario,
            items_per_producer,
            core_offset,
        ),
        Mode::AppPipeline => bench_app_pipeline_for::<MsQueue<u64>>(
            &queue_name,
            &run_scenario,
            items_per_producer,
            core_offset,
        ),
        Mode::AppTaskRoundtrip => bench_app_task_roundtrip_for::<MsQueue<u64>>(
            &queue_name,
            &run_scenario,
            items_per_producer,
            core_offset,
        ),
        Mode::AppLogMpscFile => bench_app_log_mpsc_file_for::<MsQueue<LogRecord>>(
            &queue_name,
            &run_scenario,
            items_per_producer,
            core_offset,
        ),
    });
    JobFactory { spec, run }
}

pub fn make_naive_faa_queue_job_factory(
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
        queue: QueueKind::NaiveFaaQueue,
        ubq_label: None,
        batch_size: None,
        fastfifo_block_size: None,
        fastfifo_capacity: None,
        lfqueue_segment_size: None,
        wcq_capacity: None,
    };
    let queue_name = QueueKind::NaiveFaaQueue.name().to_string();
    let run_scenario = scenario.clone();
    let run = Arc::new(move |core_offset: usize| match mode {
        Mode::Throughput => bench_throughput_for::<NaiveFaaQueue<u64>>(
            &queue_name,
            &run_scenario,
            items_per_producer,
            core_offset,
        ),
        Mode::ComplexThroughput => bench_complex_throughput_for::<NaiveFaaQueue<u64>>(
            &queue_name,
            &run_scenario,
            items_per_producer,
            core_offset,
        ),
        Mode::DataLatency => bench_data_latency_for::<NaiveFaaQueue<u64>>(
            &queue_name,
            &run_scenario,
            items_per_producer,
            core_offset,
        ),
        Mode::Fairness => bench_fairness_for::<NaiveFaaQueue<u64>>(
            &queue_name,
            &run_scenario,
            items_per_producer,
            core_offset,
        ),
        Mode::AppLogFanIn => bench_app_log_fan_in_for::<NaiveFaaQueue<u64>>(
            &queue_name,
            &run_scenario,
            items_per_producer,
            core_offset,
        ),
        Mode::AppPipeline => bench_app_pipeline_for::<NaiveFaaQueue<u64>>(
            &queue_name,
            &run_scenario,
            items_per_producer,
            core_offset,
        ),
        Mode::AppTaskRoundtrip => bench_app_task_roundtrip_for::<NaiveFaaQueue<u64>>(
            &queue_name,
            &run_scenario,
            items_per_producer,
            core_offset,
        ),
        Mode::AppLogMpscFile => bench_app_log_mpsc_file_for::<NaiveFaaQueue<LogRecord>>(
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
        fastfifo_capacity: None,
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
        fastfifo_capacity: None,
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
                None,
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
    capacity: usize,
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
        fastfifo_capacity: Some(capacity),
        lfqueue_segment_size: None,
        wcq_capacity: None,
    };
    let queue_name = fastfifo_queue_label(block_size, capacity);
    let run_scenario = scenario.clone();
    let run = Arc::new(move |core_offset: usize| {
        let queue_handle = RbbqBenchQueue::new(block_size, capacity);
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
            Mode::AppLogFanIn => bench_app_log_fan_in_with_queue(
                queue_handle,
                &queue_name,
                &run_scenario,
                items_per_producer,
                core_offset,
            ),
            Mode::AppPipeline => bench_app_pipeline_with_queues(
                queue_handle,
                RbbqBenchQueue::new(block_size, capacity),
                &queue_name,
                &run_scenario,
                items_per_producer,
                core_offset,
            ),
            Mode::AppTaskRoundtrip => bench_app_task_roundtrip_with_queues(
                queue_handle,
                RbbqBenchQueue::new(block_size, capacity),
                &queue_name,
                &run_scenario,
                items_per_producer,
                core_offset,
            ),
            Mode::AppLogMpscFile => bench_app_log_mpsc_file_with_queue(
                LogRbbqBenchQueue::new(block_size, capacity),
                &queue_name,
                &run_scenario,
                items_per_producer,
                core_offset,
            ),
        }
    });
    JobFactory { spec, run }
}

fn selected_plan_core_ids(plan: &MatrixPlan) -> Result<Vec<usize>, String> {
    let discovered = core_affinity::get_core_ids()
        .unwrap_or_default()
        .into_iter()
        .map(|core| core.id)
        .collect::<BTreeSet<_>>();
    let selected = if plan.core_ids.is_empty() {
        discovered
            .iter()
            .copied()
            .take(plan.available_parallelism)
            .collect::<Vec<_>>()
    } else {
        plan.core_ids.clone()
    };
    if selected.is_empty() && !plan.allow_unpinned {
        return Err(
            "no CPUs were discovered; use --allow-unpinned only for non-authoritative diagnostics"
                .to_string(),
        );
    }
    if !plan.allow_unpinned {
        for id in &selected {
            if !discovered.contains(id) {
                return Err(format!(
                    "selected CPU {id} is not available to this process"
                ));
            }
        }
    }
    Ok(selected)
}

fn format_core_binding(core_ids: &[usize], role: char, role_id: usize, slot: usize) -> String {
    match core_ids.get(slot) {
        Some(core) => format!("{role}{role_id}->{core}"),
        None => format!("{role}{role_id}->unbound(slot={slot})"),
    }
}

fn print_core_placement(plan: &MatrixPlan) {
    let core_ids = selected_plan_core_ids(plan).unwrap_or_default();
    progress_line(format!("core placement: {}", plan.core_placement.name()));
    let scenarios = plan
        .bundles
        .iter()
        .map(|bundle| bundle.scenario.clone())
        .collect::<BTreeSet<_>>();
    for scenario in scenarios {
        let producers = (0..scenario.producers)
            .map(|producer_id| {
                format_core_binding(
                    &core_ids,
                    'p',
                    producer_id,
                    producer_core_slot(scenario.producers, scenario.consumers, producer_id),
                )
            })
            .collect::<Vec<_>>()
            .join(",");
        let consumers = (0..scenario.consumers)
            .map(|consumer_id| {
                format_core_binding(
                    &core_ids,
                    'c',
                    consumer_id,
                    consumer_core_slot(scenario.producers, scenario.consumers, consumer_id),
                )
            })
            .collect::<Vec<_>>()
            .join(",");
        progress_line(format!(
            "core map: {} | producers [{}] | consumers [{}]",
            scenario.name, producers, consumers
        ));
    }
}

/// Runs the private benchmark worker protocol when this process was launched by
/// the parent scheduler. Binaries must call this before parsing CLI arguments.
pub fn maybe_run_bench_worker() -> Option<Result<(), String>> {
    match std::env::var(BENCH_WORKER_ENV).as_deref() {
        Ok("1") => Some(run_bench_worker_loop()),
        _ => None,
    }
}

fn write_worker_message(writer: &mut impl Write, response: &WorkerResponse) -> Result<(), String> {
    serde_json::to_writer(&mut *writer, response)
        .map_err(|err| format!("failed to serialize worker response: {err}"))?;
    writer
        .write_all(b"\n")
        .and_then(|()| writer.flush())
        .map_err(|err| format!("failed to write worker response: {err}"))
}

fn run_bench_worker_loop() -> Result<(), String> {
    let stdin = io::stdin();
    let stdout = io::stdout();
    let mut writer = BufWriter::new(stdout.lock());
    for line in stdin.lock().lines() {
        let line = line.map_err(|err| format!("failed to read worker request: {err}"))?;
        let request: WorkerRequest = match serde_json::from_str(&line) {
            Ok(request) => request,
            Err(err) => {
                write_worker_message(
                    &mut writer,
                    &WorkerResponse {
                        protocol_version: BENCH_WORKER_PROTOCOL_VERSION,
                        request_id: 0,
                        result: WorkerResult::ProtocolError {
                            reason: format!("malformed worker request: {err}"),
                        },
                    },
                )?;
                continue;
            }
        };
        if request.protocol_version != BENCH_WORKER_PROTOCOL_VERSION {
            write_worker_message(
                &mut writer,
                &WorkerResponse {
                    protocol_version: BENCH_WORKER_PROTOCOL_VERSION,
                    request_id: request.request_id,
                    result: WorkerResult::ProtocolError {
                        reason: format!(
                            "worker protocol version mismatch: parent={}, worker={}",
                            request.protocol_version, BENCH_WORKER_PROTOCOL_VERSION
                        ),
                    },
                },
            )?;
            continue;
        }

        match request.command {
            WorkerCommand::Run { spec } => {
                if std::env::var("UBQ_BENCH_WORKER_TEST_CRASH_QUEUE")
                    .is_ok_and(|queue| queue == spec.queue_label())
                {
                    std::process::abort();
                }
                if std::env::var("UBQ_BENCH_WORKER_TEST_STALL_QUEUE")
                    .is_ok_and(|queue| queue == spec.queue_label())
                {
                    let stall_ms = std::env::var("UBQ_BENCH_WORKER_TEST_STALL_MS")
                        .ok()
                        .and_then(|raw| raw.parse::<u64>().ok())
                        .unwrap_or(2_000);
                    thread::sleep(Duration::from_millis(stall_ms));
                }
                let result = match job_factory_for_spec(&spec) {
                    Ok(factory) => {
                        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                            (factory.run)(0)
                        })) {
                            Ok(record) => WorkerResult::Completed { record },
                            Err(payload) => WorkerResult::Failed {
                                reason: panic_payload_message(payload),
                            },
                        }
                    }
                    Err(reason) => WorkerResult::Failed { reason },
                };
                write_worker_message(
                    &mut writer,
                    &WorkerResponse {
                        protocol_version: BENCH_WORKER_PROTOCOL_VERSION,
                        request_id: request.request_id,
                        result,
                    },
                )?;
            }
            WorkerCommand::Shutdown => {
                write_worker_message(
                    &mut writer,
                    &WorkerResponse {
                        protocol_version: BENCH_WORKER_PROTOCOL_VERSION,
                        request_id: request.request_id,
                        result: WorkerResult::ShuttingDown,
                    },
                )?;
                return Ok(());
            }
        }
    }
    Ok(())
}

enum WorkerRunOutcome {
    Completed(BenchRecord),
    Failed(String),
    TimedOut,
}

enum WorkerReceiveError {
    Exited(String),
    Protocol(String),
}

fn decode_worker_response(line: &str, request_id: u64) -> Result<WorkerResponse, String> {
    let response: WorkerResponse = serde_json::from_str(line)
        .map_err(|err| format!("malformed benchmark worker response: {err}"))?;
    if response.protocol_version != BENCH_WORKER_PROTOCOL_VERSION {
        return Err(format!(
            "worker protocol version mismatch: parent={}, worker={}",
            BENCH_WORKER_PROTOCOL_VERSION, response.protocol_version
        ));
    }
    if response.request_id != request_id {
        return Err(format!(
            "benchmark worker response id mismatch: expected {request_id}, got {}",
            response.request_id
        ));
    }
    Ok(response)
}

struct BenchWorker {
    child: Child,
    stdin: BufWriter<ChildStdin>,
    responses: mpsc::Receiver<Result<String, String>>,
    reader: Option<thread::JoinHandle<()>>,
    next_request_id: u64,
    reaped: bool,
}

impl BenchWorker {
    fn spawn_for_plan(plan: &MatrixPlan) -> Result<Self, String> {
        Self::spawn_with_plan(Some(plan))
    }

    fn spawn_with_plan(plan: Option<&MatrixPlan>) -> Result<Self, String> {
        let executable = std::env::current_exe()
            .map_err(|err| format!("failed to locate benchmark executable: {err}"))?;
        let mut command = Command::new(&executable);
        command.env(BENCH_WORKER_ENV, "1");
        if let Some(plan) = plan {
            let core_ids = if plan.core_ids.is_empty() {
                core_affinity::get_core_ids()
                    .unwrap_or_default()
                    .into_iter()
                    .map(|core| core.id)
                    .take(plan.available_parallelism)
                    .collect::<Vec<_>>()
            } else {
                plan.core_ids.clone()
            };
            command
                .env(
                    "UBQ_BENCH_CORE_IDS",
                    core_ids
                        .iter()
                        .map(usize::to_string)
                        .collect::<Vec<_>>()
                        .join(","),
                )
                .env(
                    "UBQ_BENCH_ALLOW_UNPINNED",
                    if plan.allow_unpinned { "1" } else { "0" },
                )
                .env(
                    "UBQ_BENCH_THROUGHPUT_WARMUP_MS",
                    plan.throughput_policy.warmup_ms.to_string(),
                )
                .env(
                    "UBQ_BENCH_THROUGHPUT_PHASE_MS",
                    plan.throughput_policy.phase_ms.to_string(),
                )
                .env(
                    "UBQ_BENCH_THROUGHPUT_PILOT_MS",
                    plan.throughput_policy.pilot_ms.to_string(),
                )
                .env(
                    "UBQ_BENCH_THROUGHPUT_MAX_ROUND_ITEMS",
                    plan.throughput_policy.max_round_items.to_string(),
                )
                .env("UBQ_BENCH_SCHEDULE_SEED", plan.schedule_seed.to_string());
        }
        let mut child = command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .map_err(|err| {
                format!(
                    "failed to launch benchmark worker {}: {err}",
                    executable.display()
                )
            })?;
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| "benchmark worker stdin was not piped".to_string())?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| "benchmark worker stdout was not piped".to_string())?;
        let (sender, responses) = mpsc::channel();
        let reader = thread::spawn(move || {
            let mut reader = BufReader::new(stdout);
            loop {
                let mut line = String::new();
                match reader.read_line(&mut line) {
                    Ok(0) => {
                        let _ = sender.send(Err("benchmark worker stdout closed".to_string()));
                        break;
                    }
                    Ok(_) => {
                        while matches!(line.as_bytes().last(), Some(b'\n' | b'\r')) {
                            line.pop();
                        }
                        if sender.send(Ok(line)).is_err() {
                            break;
                        }
                    }
                    Err(err) => {
                        let _ = sender.send(Err(format!(
                            "failed to read benchmark worker response: {err}"
                        )));
                        break;
                    }
                }
            }
        });
        Ok(Self {
            child,
            stdin: BufWriter::new(stdin),
            responses,
            reader: Some(reader),
            next_request_id: 1,
            reaped: false,
        })
    }

    fn send(&mut self, command: WorkerCommand) -> Result<u64, String> {
        let request_id = self.next_request_id;
        self.next_request_id = self
            .next_request_id
            .checked_add(1)
            .ok_or_else(|| "benchmark worker request id overflow".to_string())?;
        serde_json::to_writer(
            &mut self.stdin,
            &WorkerRequest {
                protocol_version: BENCH_WORKER_PROTOCOL_VERSION,
                request_id,
                command,
            },
        )
        .map_err(|err| format!("failed to serialize benchmark worker request: {err}"))?;
        self.stdin
            .write_all(b"\n")
            .and_then(|()| self.stdin.flush())
            .map_err(|err| format!("failed to send benchmark worker request: {err}"))?;
        Ok(request_id)
    }

    fn receive(
        &self,
        request_id: u64,
        timeout: Duration,
    ) -> Result<Option<WorkerResponse>, WorkerReceiveError> {
        let line = match self.responses.recv_timeout(timeout) {
            Ok(Ok(line)) => line,
            Ok(Err(reason)) => return Err(WorkerReceiveError::Exited(reason)),
            Err(mpsc::RecvTimeoutError::Timeout) => return Ok(None),
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                return Err(WorkerReceiveError::Exited(
                    "benchmark worker response channel disconnected".to_string(),
                ));
            }
        };
        let response =
            decode_worker_response(&line, request_id).map_err(WorkerReceiveError::Protocol)?;
        Ok(Some(response))
    }

    fn run_job(&mut self, spec: JobSpec, timeout: Duration) -> Result<WorkerRunOutcome, String> {
        let expected = spec.clone();
        let request_id = match self.send(WorkerCommand::Run { spec }) {
            Ok(request_id) => request_id,
            Err(reason) => return Ok(WorkerRunOutcome::Failed(reason)),
        };
        let response = match self.receive(request_id, timeout) {
            Ok(response) => response,
            Err(WorkerReceiveError::Exited(reason)) => {
                return Ok(WorkerRunOutcome::Failed(reason));
            }
            Err(WorkerReceiveError::Protocol(reason)) => return Err(reason),
        };
        let Some(response) = response else {
            return Ok(WorkerRunOutcome::TimedOut);
        };
        match response.result {
            WorkerResult::Completed { record } => {
                let expected_queue = if expected.queue == QueueKind::Ubq {
                    expected.queue.name().to_string()
                } else {
                    expected.queue_label()
                };
                let counts_match = if expected.mode == Mode::Throughput && record.completed() {
                    record.throughput_metrics.as_ref().is_some_and(|metrics| {
                        metrics.requested_items_per_producer == expected.items_per_producer
                            && metrics.handoff_items == record.total_items
                            && record.total_items == record.consumed_items
                    })
                } else {
                    record.items_per_producer == expected.items_per_producer
                        && record.total_items
                            == total_items(expected.items_per_producer, expected.scenario.producers)
                };
                if record.queue != expected_queue
                    || record.mode != expected.mode.name()
                    || !counts_match
                    || record.batch_size != expected.batch_size
                {
                    return Err(format!(
                        "benchmark worker returned a record that does not match request {}",
                        expected.queue_label()
                    ));
                }
                Ok(WorkerRunOutcome::Completed(record))
            }
            WorkerResult::Failed { reason } => Ok(WorkerRunOutcome::Failed(reason)),
            WorkerResult::ProtocolError { reason } => Err(reason),
            WorkerResult::ShuttingDown => {
                Err("benchmark worker shut down during a job".to_string())
            }
        }
    }

    fn terminate(&mut self) {
        if self.reaped {
            return;
        }
        let _ = self.child.kill();
        let _ = self.child.wait();
        self.reaped = true;
        if let Some(reader) = self.reader.take() {
            let _ = reader.join();
        }
    }

    fn shutdown(mut self) {
        let clean = match self.send(WorkerCommand::Shutdown) {
            Ok(request_id) => matches!(
                self.receive(request_id, Duration::from_secs(5)),
                Ok(Some(WorkerResponse {
                    result: WorkerResult::ShuttingDown,
                    ..
                }))
            ),
            Err(_) => false,
        };
        if clean {
            let _ = self.child.wait();
            self.reaped = true;
            if let Some(reader) = self.reader.take() {
                let _ = reader.join();
            }
        } else {
            self.terminate();
        }
    }
}

impl Drop for BenchWorker {
    fn drop(&mut self) {
        self.terminate();
    }
}

fn terminate_worker(worker: &mut Option<BenchWorker>) {
    if let Some(mut failed_worker) = worker.take() {
        failed_worker.terminate();
    }
}

fn stable_hash64(seed: u64, bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64 ^ seed;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

fn seeded_job_order_key(spec: &JobSpec, seed: u64) -> (usize, String, String, u64, u64) {
    let rotation_seed = seed.wrapping_add(spec.repeat_index as u64);
    let queue_order = stable_hash64(rotation_seed, spec.queue_label().as_bytes());
    (
        spec.repeat_index,
        spec.scenario.name.clone(),
        spec.mode.name().to_string(),
        queue_order,
        stable_hash64(rotation_seed, format!("{:?}", spec).as_bytes()),
    )
}

fn execute_job_specs_with_timeout(
    plan: &MatrixPlan,
    cache: &ExistingRunsIndex,
    mut pending: Vec<JobSpec>,
    available_parallelism: usize,
    timeout_ceiling: Duration,
    mut timing_estimator: TimingEstimator,
) -> Result<BTreeMap<SampleKey, BenchRecord>, String> {
    for spec in &pending {
        if spec.thread_budget() > available_parallelism {
            return Err(format!(
                "job {} requires {} threads but available_parallelism is {}",
                spec.queue_label(),
                spec.thread_budget(),
                available_parallelism
            ));
        }
        job_factory_for_spec(spec)?;
    }

    pending.sort_by_key(|spec| seeded_job_order_key(spec, plan.schedule_seed));
    let required_specs = required_job_specs(plan);
    let total_jobs = required_specs.len();
    let progress_layout = ProgressLayout::new(required_specs.iter());
    let initially_complete = total_jobs.saturating_sub(pending.len());
    if pending.is_empty() {
        let writer = OutputWriterHandle::start(plan, cache)?;
        writer.close(true)?;
        return Ok(BTreeMap::new());
    }

    let mut worker: Option<BenchWorker> = None;
    progress_line(format!(
        "starting {} pending benchmark job(s); {}/{} ({:.2}%) already complete; available parallelism {}; budget-derived hard timeout {}",
        pending.len(),
        initially_complete,
        total_jobs,
        completion_percent(initially_complete, total_jobs),
        available_parallelism,
        format_progress_duration(timeout_ceiling),
    ));
    let writer = match OutputWriterHandle::start(plan, cache) {
        Ok(writer) => writer,
        Err(reason) => {
            if let Some(healthy_worker) = worker.take() {
                healthy_worker.shutdown();
            }
            return Err(reason);
        }
    };

    let session_started_at = Instant::now();
    let mut results = BTreeMap::new();
    let mut completed = initially_complete;
    let pending_count = pending.len();
    let current_protocol = MeasurementProtocol::from_plan(plan)?;
    let execution_result = (|| -> Result<(), String> {
        for (index, spec) in pending.into_iter().enumerate() {
            if progress_header_due(index) {
                progress_layout.print_header();
            }
            let key = SampleKey::from_job(&spec);
            let budget = spec.thread_budget();
            let remaining = pending_count - index - 1;
            progress_layout.print_pending_row(&key, budget, remaining, completed + 1, total_jobs);
            let job_timeout = timeout_ceiling;

            let started_at = Instant::now();
            if worker.is_none() {
                worker = Some(BenchWorker::spawn_for_plan(plan)?);
            }
            let outcome = worker
                .as_mut()
                .expect("worker present")
                .run_job(spec.clone(), job_timeout)?;
            let mut record = match outcome {
                WorkerRunOutcome::Completed(record) => record,
                WorkerRunOutcome::Failed(reason) => failed_bench_record(
                    &spec,
                    BenchRecordStatus::Failed,
                    reason,
                    started_at.elapsed().as_nanos() as u64,
                    None,
                ),
                WorkerRunOutcome::TimedOut => failed_bench_record(
                    &spec,
                    BenchRecordStatus::TimedOut,
                    format!(
                        "benchmark job exceeded {} measurement-budget hard timeout",
                        format_progress_duration(job_timeout),
                    ),
                    started_at.elapsed().as_nanos() as u64,
                    Some(job_timeout.as_nanos().min(u64::MAX as u128) as u64),
                ),
            };
            if let Some(metrics) = record.throughput_metrics.as_mut() {
                metrics.schedule_seed = plan.schedule_seed;
                metrics.execution_ordinal = index + 1;
                metrics.requested_queue_capacity = match spec.queue {
                    QueueKind::FastFifo => spec.fastfifo_capacity,
                    QueueKind::Wcq => spec.wcq_capacity,
                    _ => None,
                };
            }
            record.repeat_index = spec.repeat_index;
            record.ubq_label = spec.ubq_label.clone();
            record.protocol = current_protocol.clone();
            record.timestamp_unix_ms = now_unix_ms() as u64;

            completed += 1;
            let status = match &record.status {
                BenchRecordStatus::Completed => "DONE",
                BenchRecordStatus::Failed => "FAILED",
                BenchRecordStatus::TimedOut => "TIMED OUT",
            };
            let trial_duration = started_at.elapsed();
            if record.completed() {
                timing_estimator.observe(trial_duration);
            }
            progress_layout.print_completed_suffix(
                trial_duration,
                session_started_at.elapsed(),
                timing_estimator.estimate_remaining(remaining),
                status,
            );
            if !record.completed() {
                if record.status != BenchRecordStatus::TimedOut {
                    progress_line(format!(
                        "reason | {}",
                        record.failure_reason.as_deref().unwrap_or("unknown")
                    ));
                }
                terminate_worker(&mut worker);
            }
            writer.submit(key.clone(), record.clone())?;
            results.insert(key, record);
        }
        Ok(())
    })();

    if let Some(healthy_worker) = worker.take() {
        healthy_worker.shutdown();
    }
    let writer_result = writer.close(execution_result.is_ok());
    match (execution_result, writer_result) {
        (Ok(()), Ok(_)) => Ok(results),
        (Err(exec_err), Ok(_)) => Err(exec_err),
        (Ok(()), Err(writer_err)) => Err(writer_err),
        (Err(exec_err), Err(writer_err)) => {
            Err(format!("{exec_err}; output writer error: {writer_err}"))
        }
    }
}

fn result_key_sort(lhs: &SampleKey, rhs: &SampleKey) -> Ordering {
    let queue_order = |label: &str| match label {
        value if value.starts_with("ubq_") => 0_u8,
        "segqueue" => 2,
        "concurrent-queue" => 3,
        value if value.starts_with("fastfifo_") => 4,
        value if value.starts_with("lfqueue_") => 5,
        value if value.starts_with("wcq_") => 6,
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
        let item_values = if *mode == Mode::Throughput {
            &bundle.items_per_producer_values[..bundle.items_per_producer_values.len().min(1)]
        } else {
            bundle.items_per_producer_values.as_slice()
        };
        for &items_per_producer in item_values {
            let baseline_queues = if bundle.ubq_label.is_some() {
                &[][..]
            } else {
                plan.baseline_queues.as_slice()
            };
            for baseline_queue in baseline_queues {
                match baseline_queue {
                    QueueKind::SegQueue | QueueKind::Lubq => {
                        let spec = JobSpec {
                            scenario: bundle.scenario.clone(),
                            repeat_index: bundle.repeat_index,
                            mode: *mode,
                            items_per_producer,
                            queue: *baseline_queue,
                            ubq_label: None,
                            batch_size: None,
                            fastfifo_block_size: None,
                            fastfifo_capacity: None,
                            lfqueue_segment_size: None,
                            wcq_capacity: None,
                        };
                        keys.push(SampleKey::from_job(&spec));
                        if *mode == Mode::Throughput {
                            for &batch_size in &plan.ubq_batch_sizes {
                                let spec = JobSpec {
                                    batch_size: Some(batch_size),
                                    ..spec.clone()
                                };
                                keys.push(SampleKey::from_job(&spec));
                            }
                        }
                    }
                    QueueKind::MutexVecDeque => {
                        let spec = JobSpec {
                            scenario: bundle.scenario.clone(),
                            repeat_index: bundle.repeat_index,
                            mode: *mode,
                            items_per_producer,
                            queue: *baseline_queue,
                            ubq_label: None,
                            batch_size: None,
                            fastfifo_block_size: None,
                            fastfifo_capacity: None,
                            lfqueue_segment_size: None,
                            wcq_capacity: None,
                        };
                        keys.push(SampleKey::from_job(&spec));
                        if *mode == Mode::Throughput {
                            for &batch_size in &plan.ubq_batch_sizes {
                                let spec = JobSpec {
                                    batch_size: Some(batch_size),
                                    ..spec.clone()
                                };
                                keys.push(SampleKey::from_job(&spec));
                            }
                        }
                    }
                    QueueKind::FastFifo => {
                        for &block_size in &plan.fastfifo_block_sizes {
                            for &capacity in &plan.fastfifo_capacities {
                                let spec = JobSpec {
                                    scenario: bundle.scenario.clone(),
                                    repeat_index: bundle.repeat_index,
                                    mode: *mode,
                                    items_per_producer,
                                    queue: *baseline_queue,
                                    ubq_label: None,
                                    batch_size: None,
                                    fastfifo_block_size: Some(block_size),
                                    fastfifo_capacity: Some(capacity),
                                    lfqueue_segment_size: None,
                                    wcq_capacity: None,
                                };
                                keys.push(SampleKey::from_job(&spec));
                            }
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
                                fastfifo_capacity: None,
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
                                    fastfifo_capacity: None,
                                    lfqueue_segment_size: None,
                                    wcq_capacity: Some(capacity),
                                };
                                keys.push(SampleKey::from_job(&spec));
                            }
                        }
                    }
                    QueueKind::MoodycamelConcurrentQueue => {
                        let spec = JobSpec {
                            scenario: bundle.scenario.clone(),
                            repeat_index: bundle.repeat_index,
                            mode: *mode,
                            items_per_producer,
                            queue: *baseline_queue,
                            ubq_label: None,
                            batch_size: None,
                            fastfifo_block_size: None,
                            fastfifo_capacity: None,
                            lfqueue_segment_size: None,
                            wcq_capacity: None,
                        };
                        keys.push(SampleKey::from_job(&spec));
                        if *mode == Mode::Throughput {
                            for &batch_size in &plan.ubq_batch_sizes {
                                let spec = JobSpec {
                                    batch_size: Some(batch_size),
                                    ..spec.clone()
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
                            fastfifo_capacity: None,
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
                    fastfifo_capacity: None,
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
                            fastfifo_capacity: None,
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

/// Union of `expected_keys_for_bundle` across every bundle for a scenario,
/// deduplicated. Every bundle used to duplicate its baseline-queue keys so
/// each per-label output file was self-contained; that duplication is no
/// longer needed once every bundle for a scenario shares one output file.
fn expected_keys_for_scenario(plan: &MatrixPlan, scenario_name: &str) -> Vec<SampleKey> {
    let mut keys: BTreeSet<SampleKey> = BTreeSet::new();
    for bundle in plan
        .bundles
        .iter()
        .filter(|bundle| bundle.scenario.name == scenario_name)
    {
        keys.extend(expected_keys_for_bundle(plan, bundle));
    }
    let mut keys: Vec<SampleKey> = keys.into_iter().collect();
    keys.sort_by(result_key_sort);
    keys
}

fn command_output(program: &str, args: &[&str]) -> String {
    Command::new(program)
        .args(args)
        .output()
        .ok()
        .filter(|output| output.status.success())
        .map(|output| String::from_utf8_lossy(&output.stdout).into_owned())
        .unwrap_or_else(|| "unavailable".to_string())
}

/// Cheap, purely informational provenance of the machine/build that produced
/// a scenario's data. Never gates reuse — reuse gates on machine-label plus
/// `MeasurementProtocol` (see [`MeasurementProtocol::from_plan`]) instead.
struct SystemProvenance {
    host_uname: String,
    git_commit: String,
    git_dirty: bool,
    rustc_version: String,
    package_version: String,
}

fn system_provenance() -> &'static SystemProvenance {
    static PROVENANCE: OnceLock<SystemProvenance> = OnceLock::new();
    PROVENANCE.get_or_init(|| SystemProvenance {
        host_uname: command_output("uname", &["-a"]).trim().to_string(),
        git_commit: command_output("git", &["rev-parse", "--short", "HEAD"])
            .trim()
            .to_string(),
        git_dirty: !command_output("git", &["status", "--porcelain=v1"])
            .trim()
            .is_empty(),
        rustc_version: command_output("rustc", &["--version"]).trim().to_string(),
        package_version: env!("CARGO_PKG_VERSION").to_string(),
    })
}

/// Builds this invocation's metadata for a scenario's coalesced record,
/// merging (union/max) grid-coverage fields with whatever was already on
/// disk so a narrower follow-up plan never shrinks the reported "expected"
/// grid. `previous` is the scenario's `record.json` meta from before this
/// write, if any.
fn scenario_output_meta(
    plan: &MatrixPlan,
    scenario: &ScenarioConfig,
    previous: Option<&OutputMeta>,
) -> OutputMeta {
    let expected_ubq_configurations = plan
        .bundles
        .iter()
        .filter(|candidate| candidate.scenario.name == scenario.name)
        .filter_map(|candidate| candidate.ubq_label.as_deref())
        .collect::<BTreeSet<_>>()
        .len();
    let planned_items_per_producer = plan
        .bundles
        .iter()
        .filter(|candidate| candidate.scenario.name == scenario.name)
        .flat_map(|candidate| candidate.items_per_producer_values.iter().copied())
        .collect::<BTreeSet<_>>();

    let provenance = system_provenance();
    let mut meta = OutputMeta {
        machine_label: plan.machine_label.clone(),
        scenario: scenario.name.clone(),
        producers: scenario.producers,
        consumers: scenario.consumers,
        last_updated_unix_ms: now_unix_ms(),
        host_uname: provenance.host_uname.clone(),
        git_commit: provenance.git_commit.clone(),
        git_dirty: provenance.git_dirty,
        rustc_version: provenance.rustc_version.clone(),
        package_version: provenance.package_version.clone(),
        ubq_grid: plan.ubq_grid,
        expected_ubq_configurations,
        ubq_batch_sizes: plan.ubq_batch_sizes.clone(),
        planned_repeats: plan.planned_repeats,
        planned_items_per_producer: planned_items_per_producer.into_iter().collect(),
    };

    if let Some(previous) = previous {
        meta.ubq_grid = meta.ubq_grid.or(previous.ubq_grid);
        meta.expected_ubq_configurations = meta
            .expected_ubq_configurations
            .max(previous.expected_ubq_configurations);
        meta.planned_repeats = meta.planned_repeats.max(previous.planned_repeats);
        let batch_sizes: BTreeSet<usize> = meta
            .ubq_batch_sizes
            .iter()
            .copied()
            .chain(previous.ubq_batch_sizes.iter().copied())
            .collect();
        meta.ubq_batch_sizes = batch_sizes.into_iter().collect();
        let items: BTreeSet<u64> = meta
            .planned_items_per_producer
            .iter()
            .copied()
            .chain(previous.planned_items_per_producer.iter().copied())
            .collect();
        meta.planned_items_per_producer = items.into_iter().collect();
    }

    meta
}

fn read_existing_output_meta(path: &Path) -> Option<OutputMeta> {
    let raw = fs::read_to_string(path).ok()?;
    let parsed: OutputFile = serde_json::from_str(&raw).ok()?;
    (parsed.schema_version == RUN_SCHEMA_VERSION).then_some(parsed.meta)
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

/// The single coalesced record for a (machine_label, scenario) pair. Every
/// queue/config/repeat measured for that scenario lives in this one file,
/// reopened and updated in place across invocations rather than a fresh file
/// per run.
fn output_path_for_scenario(plan: &MatrixPlan, scenario_name: &str) -> PathBuf {
    plan.runs_dir
        .join(sanitize_name(&plan.machine_label))
        .join(sanitize_name(scenario_name))
        .join("record.json")
}

fn progress_line(message: impl AsRef<str>) {
    println!("{}", message.as_ref());
    let _ = io::stdout().flush();
}

fn progress_row_start(message: impl AsRef<str>) {
    print!("{}", message.as_ref());
    let _ = io::stdout().flush();
}

fn progress_row_suffix(message: impl AsRef<str>) {
    println!("{}", message.as_ref());
    let _ = io::stdout().flush();
}

const PROGRESS_HEADER_INTERVAL: usize = 50;
const PROGRESS_TIME_WIDTH: usize = 9;
const PROGRESS_STATUS_WIDTH: usize = 9;

fn progress_header_due(row_index: usize) -> bool {
    row_index % PROGRESS_HEADER_INTERVAL == 0
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

fn format_progress_duration(duration: Duration) -> String {
    if duration.is_zero() {
        return "0s".to_string();
    }
    let seconds = duration.as_secs();
    if seconds >= 86_400 {
        format!("{}d{:02}h", seconds / 86_400, (seconds % 86_400) / 3_600)
    } else if seconds >= 3_600 {
        format!("{}h{:02}m", seconds / 3_600, (seconds % 3_600) / 60)
    } else if seconds >= 60 {
        format!("{}m{:02}s", seconds / 60, seconds % 60)
    } else if duration >= Duration::from_secs(10) {
        format!("{:.1}s", duration.as_secs_f64())
    } else if duration >= Duration::from_secs(1) {
        format!("{:.2}s", duration.as_secs_f64())
    } else if duration >= Duration::from_millis(1) {
        format!("{}ms", duration.as_millis())
    } else {
        format!("{}us", duration.as_micros())
    }
}

#[derive(Clone, Debug, Default)]
struct TimingEstimator {
    observed: Duration,
    samples: usize,
}

impl TimingEstimator {
    fn from_history(history: &ExistingRunsIndex) -> Self {
        let mut estimator = Self::default();
        for record in history.records.values() {
            if record.completed() && record.elapsed_ns > 0 {
                estimator.observe(Duration::from_nanos(record.elapsed_ns));
            }
        }
        estimator
    }

    fn observe(&mut self, duration: Duration) {
        self.observed += duration;
        self.samples += 1;
    }

    fn estimate_remaining(&self, remaining: usize) -> Option<Duration> {
        if remaining == 0 {
            return Some(Duration::ZERO);
        }
        if self.samples == 0 {
            return None;
        }
        Some(Duration::from_secs_f64(
            self.observed.as_secs_f64() / self.samples as f64 * remaining as f64,
        ))
    }
}

struct ProgressLayout {
    queue_width: usize,
    scenario_width: usize,
    repeat_width: usize,
    mode_width: usize,
    items_width: usize,
    batch_width: usize,
    thread_width: usize,
    count_width: usize,
}

impl ProgressLayout {
    fn new<'a>(specs: impl IntoIterator<Item = &'a JobSpec>) -> Self {
        let mut layout = Self {
            queue_width: "queue".len(),
            scenario_width: "scenario".len(),
            repeat_width: "repeat".len(),
            mode_width: "mode".len(),
            items_width: "items".len(),
            batch_width: "scalar".len(),
            thread_width: "threads".len(),
            count_width: 1,
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
        layout
    }

    fn print_header(&self) {
        let pending_width = self.count_width.max("pending".len());
        let on_done_width = (self.count_width * 2 + 1 + " (100.00%)".len()).max("on done".len());
        progress_line(format!(
            "{:<queue$} | {:<scenario$} | {:>repeat$} | {:<mode$} | {:>items$} | {:>batch$} | {:>threads$} | {:>pending_width$} | {:>on_done_width$} | {:>time_width$} | {:>time_width$} | {:>time_width$} | {:<status_width$}",
            "queue",
            "scenario",
            "repeat",
            "mode",
            "items",
            "batch",
            "threads",
            "pending",
            "on done",
            "δT",
            "ΔT",
            "ETA",
            "status",
            queue = self.queue_width,
            scenario = self.scenario_width,
            repeat = self.repeat_width,
            mode = self.mode_width,
            items = self.items_width,
            batch = self.batch_width,
            threads = self.thread_width,
            pending_width = pending_width,
            on_done_width = on_done_width,
            time_width = PROGRESS_TIME_WIDTH,
            status_width = PROGRESS_STATUS_WIDTH,
        ));
    }

    fn print_pending_row(
        &self,
        key: &SampleKey,
        threads: usize,
        pending: usize,
        completed_on_done: usize,
        total: usize,
    ) {
        let batch = key
            .batch_size
            .map(|value| value.to_string())
            .unwrap_or_else(|| "scalar".to_string());
        let pending_width = self.count_width.max("pending".len());
        let on_done_width = (self.count_width * 2 + 1 + " (100.00%)".len()).max("on done".len());
        let on_done = format!(
            "{:>width$}/{:<width$} ({:>6.2}%)",
            completed_on_done,
            total,
            completion_percent(completed_on_done, total),
            width = self.count_width
        );
        progress_row_start(format!(
            "{:<queue$} | {:<scenario$} | {:>repeat$} | {:<mode$} | {:>items$} | {:>batch$} | {:>threads$} | {:>pending_width$} | {:>on_done_width$} | ",
            key.queue_label,
            key.scenario,
            key.repeat_index,
            key.mode.name(),
            key.items_per_producer,
            batch,
            threads,
            pending,
            on_done,
            queue = self.queue_width,
            scenario = self.scenario_width,
            repeat = self.repeat_width,
            mode = self.mode_width,
            items = self.items_width,
            batch = self.batch_width,
            threads = self.thread_width,
            pending_width = pending_width,
            on_done_width = on_done_width,
        ));
    }

    fn print_completed_suffix(
        &self,
        trial_duration: Duration,
        total_duration: Duration,
        eta: Option<Duration>,
        status: &str,
    ) {
        let eta = eta
            .map(format_progress_duration)
            .unwrap_or_else(|| "—".to_string());
        progress_row_suffix(format!(
            "{:>time_width$} | {:>time_width$} | {:>time_width$} | {:<status_width$}",
            format_progress_duration(trial_duration),
            format_progress_duration(total_duration),
            eta,
            status,
            time_width = PROGRESS_TIME_WIDTH,
            status_width = PROGRESS_STATUS_WIDTH,
        ));
    }
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
    requested_items_per_producer: u64,
    batch_size: Option<usize>,
    core_offset: usize,
) -> BenchRecord {
    adaptive_throughput(
        queue_handle,
        queue_name,
        scenario,
        requested_items_per_producer,
        batch_size,
        core_offset,
    )
    .unwrap_or_else(|(reason, elapsed_ns)| {
        failed_runtime_bench_record(
            queue_name,
            Mode::Throughput,
            scenario,
            requested_items_per_producer,
            batch_size,
            reason,
            elapsed_ns,
        )
    })
}

#[derive(Clone, Copy, Debug)]
struct TimedRound {
    elapsed: Duration,
    producer_elapsed: Duration,
    items: u64,
    affinity_ok: bool,
}

#[derive(Clone, Debug)]
pub struct HandoffProfileConfig {
    pub queue: QueueKind,
    pub ubq_label: Option<String>,
    pub scenario: ScenarioConfig,
    pub batch_size: Option<usize>,
    pub warmup: Duration,
    pub duration: Duration,
    pub core_offset: usize,
    pub allow_unpinned: bool,
}

#[derive(Clone, Debug, Serialize)]
pub struct HandoffProfileResult {
    pub queue: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ubq_label: Option<String>,
    pub scenario: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub batch_size: Option<usize>,
    pub warmup_ms: u64,
    pub requested_duration_ms: u64,
    pub elapsed_ns: u64,
    pub items: u64,
    pub ops_per_sec: f64,
    pub affinity_ok: bool,
}

pub fn run_handoff_profile(config: &HandoffProfileConfig) -> Result<HandoffProfileResult, String> {
    if config.scenario.producers == 0 || config.scenario.consumers == 0 {
        return Err("profile scenario requires at least one producer and consumer".to_string());
    }
    if config.duration.is_zero() {
        return Err("profile duration must be greater than zero".to_string());
    }
    if config.batch_size == Some(0) {
        return Err("profile batch size must be greater than zero".to_string());
    }
    let required_cores = config
        .core_offset
        .checked_add(config.scenario.total_threads())
        .ok_or_else(|| "profile core count overflow".to_string())?;
    if required_cores > bench_core_ids().len() && !config.allow_unpinned {
        return Err(format!(
            "profile needs {required_cores} CPU slots but only {} are available; use a smaller \
             scenario or --allow-unpinned",
            bench_core_ids().len()
        ));
    }

    match config.queue {
        QueueKind::Ubq => {
            let label = config
                .ubq_label
                .as_deref()
                .ok_or_else(|| "--ubq-label is required when profiling UBQ".to_string())?;
            let normalized = parse_ubq_label(label, true)?.text();
            lookup_ubq_handoff_profile(&normalized, config).ok_or_else(|| {
                format!(
                    "no compiled UBQ configuration for label '{normalized}'; rebuild with \
                     --features bench_registry"
                )
            })?
        }
        QueueKind::Lubq => profile_handoff_with_queue(
            LubqBenchQueue::new(config.scenario.producers, config.scenario.consumers),
            QueueKind::Lubq.name(),
            None,
            config,
        ),
        QueueKind::SegQueue => match config.batch_size {
            Some(_) => {
                profile_handoff_for::<BatchQueue<u64>>(QueueKind::SegQueue.name(), None, config)
            }
            None => profile_handoff_for::<SegQueue<u64>>(QueueKind::SegQueue.name(), None, config),
        },
        _ => Err(format!(
            "queue '{}' is not supported by bench_profile; supported queues are ubq, lubq, and \
             segqueue",
            config.queue.name()
        )),
    }
}

fn profile_handoff_for<Q: BenchQueue>(
    queue_name: &str,
    ubq_label: Option<&str>,
    config: &HandoffProfileConfig,
) -> Result<HandoffProfileResult, String> {
    profile_handoff_with_queue(Q::new_queue(), queue_name, ubq_label, config)
}

fn profile_handoff_with_queue<Q: BenchQueueHandleFactory>(
    queue_handle: Arc<Q>,
    queue_name: &str,
    ubq_label: Option<&str>,
    config: &HandoffProfileConfig,
) -> Result<HandoffProfileResult, String> {
    let batch = config.batch_size.unwrap_or(1) as u64;
    let max_per_producer = u64::MAX / config.scenario.producers as u64;
    let normalize = |items: u64| {
        items
            .max(1)
            .div_ceil(batch)
            .saturating_mul(batch)
            .min((max_per_producer / batch) * batch)
    };

    // Calibrate with complete, drained handoff rounds so the measured round
    // lasts approximately the requested time without building an artificial
    // producer-side backlog that is drained after the sampling window.
    let mut pilot_items = normalize(INITIAL_THROUGHPUT_PILOT_ITEMS_PER_PRODUCER);
    let mut calibration_elapsed = Duration::ZERO;
    let mut calibration_items = 0_u64;
    let mut affinity_ok = true;
    loop {
        let round = run_handoff_round(
            &queue_handle,
            &config.scenario,
            pilot_items,
            config.batch_size,
            config.core_offset,
        )?;
        calibration_elapsed += round.elapsed.max(Duration::from_nanos(1));
        calibration_items = calibration_items
            .checked_add(round.items)
            .ok_or_else(|| "profile calibration item count overflow".to_string())?;
        affinity_ok &= round.affinity_ok;
        if round.elapsed >= Duration::from_millis(DEFAULT_THROUGHPUT_PILOT_MS) {
            break;
        }
        let next = normalize(pilot_items.saturating_mul(2));
        if next == pilot_items {
            break;
        }
        pilot_items = next;
    }

    while calibration_elapsed < config.warmup {
        let round = run_handoff_round(
            &queue_handle,
            &config.scenario,
            pilot_items,
            config.batch_size,
            config.core_offset,
        )?;
        calibration_elapsed += round.elapsed.max(Duration::from_nanos(1));
        calibration_items = calibration_items
            .checked_add(round.items)
            .ok_or_else(|| "profile warmup item count overflow".to_string())?;
        affinity_ok &= round.affinity_ok;
    }

    let calibrated_rate = calibration_items as f64 / calibration_elapsed.as_secs_f64();
    let target_total = (calibrated_rate * config.duration.as_secs_f64())
        .ceil()
        .clamp(1.0, u64::MAX as f64) as u64;
    let target_per_producer = normalize(target_total.div_ceil(config.scenario.producers as u64));
    let measured = run_handoff_round(
        &queue_handle,
        &config.scenario,
        target_per_producer,
        config.batch_size,
        config.core_offset,
    )?;
    affinity_ok &= measured.affinity_ok;
    if !affinity_ok && !config.allow_unpinned {
        return Err(
            "one or more profile workers could not be pinned to the requested CPU".to_string(),
        );
    }
    let elapsed_ns = measured.elapsed.as_nanos().min(u64::MAX as u128) as u64;
    let ops_per_sec = throughput_ops(measured.items, elapsed_ns).unwrap_or(0.0);
    Ok(HandoffProfileResult {
        queue: queue_name.to_string(),
        ubq_label: ubq_label.map(str::to_string),
        scenario: config.scenario.name.clone(),
        batch_size: config.batch_size,
        warmup_ms: config.warmup.as_millis().min(u64::MAX as u128) as u64,
        requested_duration_ms: config.duration.as_millis().min(u64::MAX as u128) as u64,
        elapsed_ns,
        items: measured.items,
        ops_per_sec,
        affinity_ok,
    })
}

fn runtime_throughput_policy() -> ThroughputPolicy {
    if cfg!(test) {
        return ThroughputPolicy {
            warmup_ms: 1,
            phase_ms: 1,
            pilot_ms: 1,
            max_round_items: 16_384,
        };
    }
    let parse = |name: &str, default: u64| {
        std::env::var(name)
            .ok()
            .and_then(|raw| raw.parse::<u64>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(default)
    };
    ThroughputPolicy {
        warmup_ms: parse(
            "UBQ_BENCH_THROUGHPUT_WARMUP_MS",
            DEFAULT_THROUGHPUT_WARMUP_MS,
        ),
        phase_ms: parse("UBQ_BENCH_THROUGHPUT_PHASE_MS", DEFAULT_THROUGHPUT_PHASE_MS),
        pilot_ms: parse("UBQ_BENCH_THROUGHPUT_PILOT_MS", DEFAULT_THROUGHPUT_PILOT_MS),
        max_round_items: parse(
            "UBQ_BENCH_THROUGHPUT_MAX_ROUND_ITEMS",
            DEFAULT_THROUGHPUT_MAX_ROUND_ITEMS,
        ),
    }
}

fn runtime_allow_unpinned() -> bool {
    cfg!(test)
        || std::env::var("UBQ_BENCH_ALLOW_UNPINNED")
            .is_ok_and(|value| value == "1" || value.eq_ignore_ascii_case("true"))
}

fn pin_current_bench_thread(core_id: Option<core_affinity::CoreId>) -> bool {
    core_id.is_some_and(core_affinity::set_for_current)
}

fn send_producer_items<T: BenchQueueThreadOps>(
    queue: &T,
    producer_id: usize,
    items_per_producer: u64,
    batch_size: Option<usize>,
) {
    let base = (producer_id as u64)
        .checked_mul(items_per_producer)
        .expect("item count overflow");
    if let Some(batch_size) = batch_size {
        let item_count =
            usize::try_from(items_per_producer).expect("batched item count must fit usize");
        let mut first = 0_usize;
        while first < item_count {
            let next = first.saturating_add(batch_size).min(item_count);
            queue.send_batch(base, first..next);
            first = next;
        }
    } else {
        let end = base
            .checked_add(items_per_producer)
            .expect("item count overflow");
        for value in base..end {
            queue.send_value(value);
        }
    }
}

fn receive_consumer_items<T: BenchQueueThreadOps>(
    queue: &T,
    batch_size: Option<usize>,
    producers_done: &AtomicBool,
    start: Instant,
) -> (Duration, u64) {
    let mut consumed = 0_u64;
    let mut last_data = Duration::ZERO;
    let backoff = Backoff::new();

    if let Some(batch_size) = batch_size {
        loop {
            let received = queue.try_recv_batch(batch_size);

            if received != 0 {
                consumed = consumed
                    .checked_add(u64::try_from(received).expect("batch size must fit u64"))
                    .expect("consumed count overflow");
                last_data = start.elapsed();
                continue;
            }

            // Once every producer has returned, an empty reservation means all
            // remaining values (if any) are already owned by other consumers.
            // No value-based sentinel is used: it would be unsound for any
            // baseline (e.g. moodycamel) that doesn't guarantee ordering
            // across different producer threads, and would also let one
            // consumer claim termination markers meant for several workers
            // if placed inside a batch.
            if producers_done.load(AtomicOrdering::Acquire) {
                break;
            }

            backoff.snooze();
        }
    } else {
        loop {
            match queue.try_recv_value() {
                Some(_) => {
                    consumed = consumed.checked_add(1).expect("consumed count overflow");
                    last_data = start.elapsed();
                }
                None => {
                    if producers_done.load(AtomicOrdering::Acquire) {
                        break;
                    }
                    backoff.snooze();
                }
            }
        }
    }

    (last_data, consumed)
}

fn run_handoff_round<Q: BenchQueueHandleFactory>(
    queue_handle: &Arc<Q>,
    scenario: &ScenarioConfig,
    items_per_producer: u64,
    batch_size: Option<usize>,
    core_offset: usize,
) -> Result<TimedRound, String> {
    let expected = total_items(items_per_producer, scenario.producers);
    let ready = Arc::new(Barrier::new(scenario.total_threads() + 1));
    let start_gate = Arc::new(Barrier::new(scenario.total_threads() + 1));
    let start = Arc::new(OnceLock::new());
    let producers_done = Arc::new(AtomicBool::new(false));
    let mut producers = Vec::with_capacity(scenario.producers);
    for producer_id in 0..scenario.producers {
        let queue = queue_handle.producer_thread_handle();
        let ready = ready.clone();
        let start_gate = start_gate.clone();
        let start = start.clone();
        let core_id = producer_core_id(
            core_offset,
            scenario.producers,
            scenario.consumers,
            producer_id,
        );
        producers.push(spawn_bench_thread(move || {
            let pinned = pin_current_bench_thread(core_id);
            ready.wait();
            start_gate.wait();
            let start: Instant = *start.get().expect("start set");
            send_producer_items(&queue, producer_id, items_per_producer, batch_size);
            (start.elapsed(), pinned)
        }));
    }

    let mut consumers = Vec::with_capacity(scenario.consumers);
    for consumer_id in 0..scenario.consumers {
        let queue = queue_handle.consumer_thread_handle();
        let ready = ready.clone();
        let start_gate = start_gate.clone();
        let start = start.clone();
        let producers_done = producers_done.clone();
        let core_id = consumer_core_id(
            core_offset,
            scenario.producers,
            scenario.consumers,
            consumer_id,
        );
        consumers.push(spawn_bench_thread(move || {
            let pinned = pin_current_bench_thread(core_id);
            ready.wait();
            start_gate.wait();
            let start: Instant = *start.get().expect("start set");
            let (last_data, consumed) =
                receive_consumer_items(&queue, batch_size, &producers_done, start);
            (last_data, consumed, pinned)
        }));
    }

    ready.wait();
    start.set(Instant::now()).ok();
    start_gate.wait();
    let producer_results = join_bench_threads(producers, "producer")?;
    producers_done.store(true, AtomicOrdering::Release);
    let consumer_results = join_bench_threads(consumers, "consumer")?;
    let consumed = consumer_results.iter().map(|(_, count, _)| *count).sum();
    if consumed != expected {
        return Err(format!(
            "handoff integrity mismatch: expected {expected} items, consumed {consumed}"
        ));
    }
    Ok(TimedRound {
        elapsed: consumer_results
            .iter()
            .map(|(elapsed, _, _)| *elapsed)
            .max()
            .unwrap_or_default(),
        producer_elapsed: producer_results
            .iter()
            .map(|(elapsed, _)| *elapsed)
            .max()
            .unwrap_or_default(),
        items: consumed,
        affinity_ok: producer_results.iter().all(|(_, pinned)| *pinned)
            && consumer_results.iter().all(|(_, _, pinned)| *pinned),
    })
}

fn run_enqueue_round<Q: BenchQueueHandleFactory>(
    queue_handle: &Arc<Q>,
    scenario: &ScenarioConfig,
    items_per_producer: u64,
    batch_size: Option<usize>,
    core_offset: usize,
) -> Result<TimedRound, String> {
    let ready = Arc::new(Barrier::new(scenario.producers + 1));
    let start_gate = Arc::new(Barrier::new(scenario.producers + 1));
    let start = Arc::new(OnceLock::new());
    let mut producers = Vec::with_capacity(scenario.producers);
    for producer_id in 0..scenario.producers {
        let queue = queue_handle.producer_thread_handle();
        let ready = ready.clone();
        let start_gate = start_gate.clone();
        let start = start.clone();
        let core_id = producer_core_id(
            core_offset,
            scenario.producers,
            scenario.consumers,
            producer_id,
        );
        producers.push(spawn_bench_thread(move || {
            let pinned = pin_current_bench_thread(core_id);
            ready.wait();
            start_gate.wait();
            let start: Instant = *start.get().expect("start set");
            send_producer_items(&queue, producer_id, items_per_producer, batch_size);
            (start.elapsed(), pinned)
        }));
    }
    ready.wait();
    start.set(Instant::now()).ok();
    start_gate.wait();
    let results = join_bench_threads(producers, "producer")?;
    let elapsed = results
        .iter()
        .map(|(elapsed, _)| *elapsed)
        .max()
        .unwrap_or_default();
    Ok(TimedRound {
        elapsed,
        producer_elapsed: elapsed,
        items: total_items(items_per_producer, scenario.producers),
        affinity_ok: results.iter().all(|(_, pinned)| *pinned),
    })
}

fn run_dequeue_round<Q: BenchQueueHandleFactory>(
    queue_handle: &Arc<Q>,
    scenario: &ScenarioConfig,
    expected: u64,
    batch_size: Option<usize>,
    core_offset: usize,
) -> Result<TimedRound, String> {
    let ready = Arc::new(Barrier::new(scenario.consumers + 1));
    let start_gate = Arc::new(Barrier::new(scenario.consumers + 1));
    let start = Arc::new(OnceLock::new());
    let producers_done = Arc::new(AtomicBool::new(true));
    let mut consumers = Vec::with_capacity(scenario.consumers);
    for consumer_id in 0..scenario.consumers {
        let queue = queue_handle.consumer_thread_handle();
        let ready = ready.clone();
        let start_gate = start_gate.clone();
        let start = start.clone();
        let producers_done = producers_done.clone();
        let core_id = consumer_core_id(
            core_offset,
            scenario.producers,
            scenario.consumers,
            consumer_id,
        );
        consumers.push(spawn_bench_thread(move || {
            let pinned = pin_current_bench_thread(core_id);
            ready.wait();
            start_gate.wait();
            let start: Instant = *start.get().expect("start set");
            let (last_data, consumed) =
                receive_consumer_items(&queue, batch_size, &producers_done, start);
            (last_data, consumed, pinned)
        }));
    }
    ready.wait();
    start.set(Instant::now()).ok();
    start_gate.wait();
    let results = join_bench_threads(consumers, "consumer")?;
    let consumed = results.iter().map(|(_, count, _)| *count).sum();
    if consumed != expected {
        return Err(format!(
            "dequeue integrity mismatch: expected {expected} items, consumed {consumed}"
        ));
    }
    let elapsed = results
        .iter()
        .map(|(elapsed, _, _)| *elapsed)
        .max()
        .unwrap_or_default();
    Ok(TimedRound {
        elapsed,
        producer_elapsed: Duration::ZERO,
        items: consumed,
        affinity_ok: results.iter().all(|(_, _, pinned)| *pinned),
    })
}

fn batch_aligned_round_items(items: u64, batch: u64, max_per_producer: u64) -> Option<u64> {
    let max_batch_aligned = (max_per_producer / batch) * batch;
    (max_batch_aligned > 0).then(|| {
        items
            .max(1)
            .div_ceil(batch)
            .saturating_mul(batch)
            .min(max_batch_aligned)
    })
}

fn adaptive_throughput<Q: BenchQueueHandleFactory>(
    queue_handle: Arc<Q>,
    queue_name: &str,
    scenario: &ScenarioConfig,
    requested_items_per_producer: u64,
    batch_size: Option<usize>,
    core_offset: usize,
) -> Result<BenchRecord, (String, u64)> {
    let policy = runtime_throughput_policy();
    let batch = batch_size.map(|value| value as u64).unwrap_or(1);
    let max_per_producer = (policy.max_round_items / scenario.producers as u64).max(1);
    if batch_aligned_round_items(1, batch, max_per_producer).is_none() {
        return Err((
            format!("batch size {batch} exceeds the per-producer round cap {max_per_producer}"),
            0,
        ));
    }
    let normalize = |items: u64| {
        batch_aligned_round_items(items, batch, max_per_producer)
            .expect("batch was validated against the round cap")
    };

    let mut pilot_items = normalize(INITIAL_THROUGHPUT_PILOT_ITEMS_PER_PRODUCER);
    let mut calibration_elapsed = Duration::ZERO;
    let mut affinity_ok = true;
    loop {
        let round = run_handoff_round(
            &queue_handle,
            scenario,
            pilot_items,
            batch_size,
            core_offset,
        )
        .map_err(|reason| (reason, calibration_elapsed.as_nanos() as u64))?;
        calibration_elapsed += round.elapsed;
        affinity_ok &= round.affinity_ok;
        let next = normalize(pilot_items.saturating_mul(2));
        if round.elapsed >= policy.pilot_duration() || next == pilot_items {
            break;
        }
        pilot_items = next;
    }

    let mut warmup_elapsed = Duration::ZERO;
    let mut warmup_rounds = 0_usize;
    while warmup_elapsed < policy.warmup_duration() {
        let round = run_handoff_round(
            &queue_handle,
            scenario,
            pilot_items,
            batch_size,
            core_offset,
        )
        .map_err(|reason| (reason, warmup_elapsed.as_nanos() as u64))?;
        warmup_elapsed += round.elapsed.max(Duration::from_nanos(1));
        warmup_rounds += 1;
        affinity_ok &= round.affinity_ok;
    }

    let mut handoff_elapsed = Duration::ZERO;
    let mut handoff_items = 0_u64;
    let mut handoff_rounds = 0_usize;
    let mut producer_elapsed_max = Duration::ZERO;
    while handoff_elapsed < policy.phase_duration() {
        let round = run_handoff_round(
            &queue_handle,
            scenario,
            pilot_items,
            batch_size,
            core_offset,
        )
        .map_err(|reason| (reason, handoff_elapsed.as_nanos() as u64))?;
        handoff_elapsed += round.elapsed.max(Duration::from_nanos(1));
        producer_elapsed_max = producer_elapsed_max.max(round.producer_elapsed);
        handoff_items = handoff_items
            .checked_add(round.items)
            .expect("handoff item count overflow");
        handoff_rounds += 1;
        affinity_ok &= round.affinity_ok;
    }

    let capacity_limit = queue_handle
        .bounded_capacity()
        .map(|capacity| capacity.saturating_sub(scenario.consumers) as u64)
        .unwrap_or(policy.max_round_items);
    let ceiling_max_per_producer = capacity_limit / scenario.producers as u64;
    let ceiling_items = if batch == 1 {
        pilot_items.min(ceiling_max_per_producer).max(1)
    } else {
        let candidate = pilot_items.min(ceiling_max_per_producer);
        if candidate < batch {
            0
        } else {
            (candidate / batch) * batch
        }
    };
    let ceiling_total = total_items(ceiling_items, scenario.producers);
    if ceiling_items == 0 || ceiling_total > capacity_limit {
        return Err((
            format!(
                "bounded capacity {} cannot hold one ceiling round plus {} consumer sentinels",
                queue_handle.bounded_capacity().unwrap_or(0),
                scenario.consumers
            ),
            0,
        ));
    }

    let mut enqueue_elapsed = Duration::ZERO;
    let mut dequeue_elapsed = Duration::ZERO;
    let mut enqueue_items = 0_u64;
    let mut dequeue_items = 0_u64;
    let mut ceiling_rounds = 0_usize;
    while enqueue_elapsed < policy.phase_duration() || dequeue_elapsed < policy.phase_duration() {
        let enqueue = run_enqueue_round(
            &queue_handle,
            scenario,
            ceiling_items,
            batch_size,
            core_offset,
        )
        .map_err(|reason| (reason, enqueue_elapsed.as_nanos() as u64))?;
        affinity_ok &= enqueue.affinity_ok;
        enqueue_elapsed += enqueue.elapsed.max(Duration::from_nanos(1));
        enqueue_items = enqueue_items
            .checked_add(enqueue.items)
            .expect("enqueue item count overflow");

        // No shutdown signal is needed before draining: run_dequeue_round's
        // producers_done flag starts true (this round's enqueue phase has
        // already fully completed and joined above), and its consumer loop
        // drains until the queue reports empty.
        let dequeue = run_dequeue_round(
            &queue_handle,
            scenario,
            ceiling_total,
            batch_size,
            core_offset,
        )
        .map_err(|reason| (reason, dequeue_elapsed.as_nanos() as u64))?;
        affinity_ok &= dequeue.affinity_ok;
        dequeue_elapsed += dequeue.elapsed.max(Duration::from_nanos(1));
        dequeue_items = dequeue_items
            .checked_add(dequeue.items)
            .expect("dequeue item count overflow");
        ceiling_rounds += 1;
    }

    if !affinity_ok && !runtime_allow_unpinned() {
        return Err((
            "one or more benchmark workers could not be pinned to the requested CPU".to_string(),
            handoff_elapsed.as_nanos() as u64,
        ));
    }

    let elapsed_ns = handoff_elapsed.as_nanos().min(u64::MAX as u128) as u64;
    let enqueue_elapsed_ns = enqueue_elapsed.as_nanos().min(u64::MAX as u128) as u64;
    let dequeue_elapsed_ns = dequeue_elapsed.as_nanos().min(u64::MAX as u128) as u64;
    let enqueue_ops_per_sec = throughput_ops(enqueue_items, enqueue_elapsed_ns).unwrap_or(0.0);
    let dequeue_ops_per_sec = throughput_ops(dequeue_items, dequeue_elapsed_ns).unwrap_or(0.0);
    let handoff_ops_per_sec = throughput_ops(handoff_items, elapsed_ns);
    let ceiling_warning = handoff_ops_per_sec.and_then(|handoff| {
        let ceiling = enqueue_ops_per_sec.min(dequeue_ops_per_sec);
        (ceiling > 0.0 && handoff > ceiling * 1.05).then(|| {
            format!(
                "handoff rate {:.3} exceeds the lower isolated ceiling {:.3} by more than 5%",
                handoff, ceiling
            )
        })
    });

    Ok(BenchRecord {
        repeat_index: 0,
        ubq_label: None,
        protocol: MeasurementProtocol::default(),
        timestamp_unix_ms: 0,
        queue: queue_name.to_string(),
        mode: Mode::Throughput.name().to_string(),
        batch_size,
        items_per_producer: handoff_items / scenario.producers as u64,
        total_items: handoff_items,
        consumed_items: handoff_items,
        elapsed_ns,
        ops_per_sec: handoff_ops_per_sec,
        producer_ops_per_sec: Some(enqueue_ops_per_sec),
        consumer_ops_per_sec: Some(dequeue_ops_per_sec),
        written_bytes: None,
        flush_count: None,
        push_elapsed_ns: Some(producer_elapsed_max.as_nanos() as u64),
        pop_elapsed_ns: Some(elapsed_ns),
        fill_elapsed_ns: Some(enqueue_elapsed_ns),
        drain_elapsed_ns: Some(dequeue_elapsed_ns),
        avg_data_latency_ns: None,
        producer_fairness_ratio: None,
        consumer_fairness_ratio: None,
        status: BenchRecordStatus::Completed,
        failure_reason: None,
        timeout_ns: None,
        throughput_metrics: Some(ThroughputMetrics {
            requested_items_per_producer,
            pilot_items_per_producer: pilot_items,
            calibration_elapsed_ns: calibration_elapsed.as_nanos() as u64,
            warmup_elapsed_ns: warmup_elapsed.as_nanos() as u64,
            warmup_rounds,
            handoff_items,
            handoff_elapsed_ns: elapsed_ns,
            handoff_rounds,
            enqueue_items,
            enqueue_elapsed_ns,
            enqueue_rounds: ceiling_rounds,
            dequeue_items,
            dequeue_elapsed_ns,
            dequeue_rounds: ceiling_rounds,
            enqueue_ops_per_sec,
            dequeue_ops_per_sec,
            affinity_authoritative: affinity_ok,
            schedule_seed: std::env::var("UBQ_BENCH_SCHEDULE_SEED")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(DEFAULT_SCHEDULE_SEED),
            execution_ordinal: 0,
            requested_queue_capacity: None,
            effective_queue_capacity: queue_handle.bounded_capacity(),
            ceiling_warning,
        }),
    })
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
            None,
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
    let producers_done = Arc::new(AtomicBool::new(false));

    let mut producer_handles = Vec::with_capacity(scenario.producers);
    for producer_id in 0..scenario.producers {
        let queue_thread = queue_handle.log_producer_thread_handle();
        let ready = ready.clone();
        let start_gate = start_gate.clone();
        let start = start.clone();
        let producer_max = producer_max.clone();
        let core_id = producer_core_id(
            core_offset,
            scenario.producers,
            scenario.consumers,
            producer_id,
        );
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

    let queue_thread = queue_handle.log_consumer_thread_handle();
    let ready_consumer = ready.clone();
    let start_gate_consumer = start_gate.clone();
    let start_consumer = start.clone();
    let queue_name_consumer = queue_name.to_string();
    let scenario_consumer = scenario.clone();
    let producers_done_consumer = producers_done.clone();
    let core_id = consumer_core_id(core_offset, scenario.producers, scenario.consumers, 0);
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
        let backoff = Backoff::new();
        loop {
            let Some(record) = queue_thread.try_recv_log() else {
                if producers_done_consumer.load(AtomicOrdering::Acquire) {
                    break;
                }
                backoff.snooze();
                continue;
            };
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
    producers_done.store(true, AtomicOrdering::Release);
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
            None,
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
        repeat_index: 0,
        ubq_label: None,
        protocol: MeasurementProtocol::default(),
        timestamp_unix_ms: 0,
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
        throughput_metrics: None,
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
    let producer_count = scenario.producers;
    let producers_done = Arc::new(AtomicBool::new(false));

    let mut producer_handles = Vec::with_capacity(scenario.producers);
    for producer_id in 0..scenario.producers {
        let queue_thread = queue_handle.producer_thread_handle();
        let ready = ready.clone();
        let start_gate = start_gate.clone();
        let start = start.clone();
        let producer_max = producer_max.clone();
        let core_id = producer_core_id(
            core_offset,
            scenario.producers,
            scenario.consumers,
            producer_id,
        );
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
        let queue_thread = queue_handle.consumer_thread_handle();
        let ready = ready.clone();
        let start_gate = start_gate.clone();
        let start = start.clone();
        let producers_done = producers_done.clone();
        let core_id = consumer_core_id(
            core_offset,
            scenario.producers,
            scenario.consumers,
            consumer_id,
        );
        consumer_handles.push(spawn_bench_thread(move || -> (u64, u64, u128) {
            if let Some(id) = core_id {
                core_affinity::set_for_current(id);
            }
            ready.wait();
            start_gate.wait();
            let start: Instant = *start.get().expect("start set");
            let mut consumed = 0_u64;
            let mut latency_total = 0_u128;
            let backoff = Backoff::new();
            loop {
                let Some(ptr) = queue_thread.try_recv_value() else {
                    if producers_done.load(AtomicOrdering::Acquire) {
                        break;
                    }
                    backoff.snooze();
                    continue;
                };
                let now_ns = start.elapsed().as_nanos() as u64;
                let record = unsafe { app_record_from_ptr(ptr) };
                let digest = app_work(producer_count + consumer_id, record.id ^ record.hash);
                std::hint::black_box(digest);
                latency_total = latency_total
                    .checked_add(now_ns.saturating_sub(record.created_ns) as u128)
                    .expect("latency total overflow");
                consumed = consumed.checked_add(1).expect("consumed count overflow");
            }
            (start.elapsed().as_nanos() as u64, consumed, latency_total)
        }));
    }

    ready.wait();
    start.set(Instant::now()).ok();
    start_gate.wait();

    let mut failure_reason = None;
    if let Err(err) = join_bench_threads(producer_handles, "producer") {
        failure_reason.get_or_insert(err);
    }
    producers_done.store(true, AtomicOrdering::Release);
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
            Mode::AppLogFanIn,
            scenario,
            items_per_producer,
            None,
            reason,
            elapsed_ns,
        );
    }

    let elapsed_ns = start.get().expect("start set").elapsed().as_nanos() as u64;
    let consumed = consumer_results.iter().map(|(_, count, _)| *count).sum();
    let latency_total: u128 = consumer_results
        .iter()
        .map(|(_, _, latency)| *latency)
        .sum();
    BenchRecord {
        repeat_index: 0,
        ubq_label: None,
        protocol: MeasurementProtocol::default(),
        timestamp_unix_ms: 0,
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
        pop_elapsed_ns: consumer_results.iter().map(|(end_ns, _, _)| *end_ns).max(),
        fill_elapsed_ns: None,
        drain_elapsed_ns: None,
        avg_data_latency_ns: average_latency_ns(latency_total, consumed),
        producer_fairness_ratio: None,
        consumer_fairness_ratio: None,
        status: BenchRecordStatus::Completed,
        failure_reason: None,
        timeout_ns: None,
        throughput_metrics: None,
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
    let producer_count = scenario.producers;
    let producers_done = Arc::new(AtomicBool::new(false));

    let mut producer_handles = Vec::with_capacity(scenario.producers);
    for producer_id in 0..scenario.producers {
        let queue_thread = stage1.producer_thread_handle();
        let ready = ready.clone();
        let start_gate = start_gate.clone();
        let start = start.clone();
        let producer_max = producer_max.clone();
        let core_id = producer_core_id(
            core_offset,
            scenario.producers,
            scenario.consumers,
            producer_id,
        );
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
        let input_thread = stage1.consumer_thread_handle();
        let output_thread = stage2.producer_thread_handle();
        let ready = ready.clone();
        let start_gate = start_gate.clone();
        let start = start.clone();
        let producers_done = producers_done.clone();
        let core_id = consumer_core_id(
            core_offset,
            scenario.producers,
            scenario.consumers,
            worker_id,
        );
        worker_handles.push(spawn_bench_thread(move || {
            if let Some(id) = core_id {
                core_affinity::set_for_current(id);
            }
            ready.wait();
            start_gate.wait();
            let _start: Instant = *start.get().expect("start set");
            let backoff = Backoff::new();
            loop {
                let Some(ptr) = input_thread.try_recv_value() else {
                    if producers_done.load(AtomicOrdering::Acquire) {
                        break;
                    }
                    backoff.snooze();
                    continue;
                };
                let mut record = unsafe { app_record_from_ptr(ptr) };
                record.hash ^= app_work(producer_count + worker_id, record.id);
                output_thread.send_value(Box::into_raw(record) as usize as u64);
            }
        }));
    }

    let collector = {
        let output_thread = stage2.consumer_thread_handle();
        let ready = ready.clone();
        let start_gate = start_gate.clone();
        let start = start.clone();
        let core_id = bench_core_ids()
            .get(core_offset + scenario.producers + scenario.consumers)
            .copied();
        spawn_bench_thread(move || -> (u64, u64, u128) {
            if let Some(id) = core_id {
                core_affinity::set_for_current(id);
            }
            ready.wait();
            start_gate.wait();
            let start: Instant = *start.get().expect("start set");
            let mut consumed = 0_u64;
            let mut latency_total = 0_u128;
            for _ in 0..total_items {
                let ptr = output_thread.recv_value();
                let now_ns = start.elapsed().as_nanos() as u64;
                let record = unsafe { app_record_from_ptr(ptr) };
                std::hint::black_box(record.hash);
                latency_total = latency_total
                    .checked_add(now_ns.saturating_sub(record.created_ns) as u128)
                    .expect("latency total overflow");
                consumed = consumed.checked_add(1).expect("consumed count overflow");
            }
            (start.elapsed().as_nanos() as u64, consumed, latency_total)
        })
    };

    ready.wait();
    start.set(Instant::now()).ok();
    start_gate.wait();

    let mut failure_reason = None;
    if let Err(err) = join_bench_threads(producer_handles, "producer") {
        failure_reason.get_or_insert(err);
    }
    producers_done.store(true, AtomicOrdering::Release);
    if let Err(err) = join_bench_threads(worker_handles, "worker") {
        failure_reason.get_or_insert(err);
    }
    let collector_result = match join_bench_thread(collector, "collector") {
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
            Mode::AppPipeline,
            scenario,
            items_per_producer,
            None,
            reason,
            elapsed_ns,
        );
    }

    let elapsed_ns = start.get().expect("start set").elapsed().as_nanos() as u64;
    let (collector_end, consumed, latency_total) = collector_result.expect("collector completed");
    BenchRecord {
        repeat_index: 0,
        ubq_label: None,
        protocol: MeasurementProtocol::default(),
        timestamp_unix_ms: 0,
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
        pop_elapsed_ns: Some(collector_end),
        fill_elapsed_ns: None,
        drain_elapsed_ns: None,
        avg_data_latency_ns: average_latency_ns(latency_total, consumed),
        producer_fairness_ratio: None,
        consumer_fairness_ratio: None,
        status: BenchRecordStatus::Completed,
        failure_reason: None,
        timeout_ns: None,
        throughput_metrics: None,
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
    let worker_max = Arc::new(AtomicU64::new(0));
    let producers_done = Arc::new(AtomicBool::new(false));

    let mut worker_handles = Vec::with_capacity(scenario.consumers);
    for worker_id in 0..scenario.consumers {
        let request_thread = request_queue.consumer_thread_handle();
        let response_thread = response_queue.producer_thread_handle();
        let ready = ready.clone();
        let start_gate = start_gate.clone();
        let start = start.clone();
        let worker_max = worker_max.clone();
        let producers_done = producers_done.clone();
        let core_id = consumer_core_id(
            core_offset,
            scenario.producers,
            scenario.consumers,
            worker_id,
        );
        worker_handles.push(spawn_bench_thread(move || {
            if let Some(id) = core_id {
                core_affinity::set_for_current(id);
            }
            ready.wait();
            start_gate.wait();
            let start: Instant = *start.get().expect("start set");
            let backoff = Backoff::new();
            loop {
                let Some(ptr) = request_thread.try_recv_value() else {
                    if producers_done.load(AtomicOrdering::Acquire) {
                        break;
                    }
                    backoff.snooze();
                    continue;
                };
                let mut record = unsafe { app_record_from_ptr(ptr) };
                record.hash ^= app_work(worker_id, record.id);
                response_thread.send_value(Box::into_raw(record) as usize as u64);
            }
            worker_max.fetch_max(start.elapsed().as_nanos() as u64, AtomicOrdering::Relaxed);
        }));
    }

    let mut client_handles = Vec::with_capacity(scenario.producers);
    for client_id in 0..scenario.producers {
        let request_thread = request_queue.producer_thread_handle();
        let response_thread = response_queue.consumer_thread_handle();
        let ready = ready.clone();
        let start_gate = start_gate.clone();
        let start = start.clone();
        let core_id = producer_core_id(
            core_offset,
            scenario.producers,
            scenario.consumers,
            client_id,
        );
        client_handles.push(spawn_bench_thread(move || -> (u64, u64, u128) {
            if let Some(id) = core_id {
                core_affinity::set_for_current(id);
            }
            ready.wait();
            start_gate.wait();
            let start: Instant = *start.get().expect("start set");
            let base = (client_id as u64)
                .checked_mul(items_per_producer)
                .expect("item count overflow");
            let mut consumed = 0_u64;
            let mut latency_total = 0_u128;
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
                latency_total = latency_total
                    .checked_add(now_ns.saturating_sub(record.created_ns) as u128)
                    .expect("latency total overflow");
                consumed = consumed.checked_add(1).expect("consumed count overflow");
            }
            (start.elapsed().as_nanos() as u64, consumed, latency_total)
        }));
    }

    ready.wait();
    start.set(Instant::now()).ok();
    start_gate.wait();

    let mut failure_reason = None;
    let client_results = match join_bench_threads(client_handles, "client") {
        Ok(values) => values,
        Err(err) => {
            failure_reason.get_or_insert(err);
            Vec::new()
        }
    };
    producers_done.store(true, AtomicOrdering::Release);
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
            None,
            reason,
            elapsed_ns,
        );
    }

    let elapsed_ns = start.get().expect("start set").elapsed().as_nanos() as u64;
    let consumed = client_results.iter().map(|(_, count, _)| *count).sum();
    let latency_total: u128 = client_results.iter().map(|(_, _, latency)| *latency).sum();
    BenchRecord {
        repeat_index: 0,
        ubq_label: None,
        protocol: MeasurementProtocol::default(),
        timestamp_unix_ms: 0,
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
        push_elapsed_ns: client_results.iter().map(|(end_ns, _, _)| *end_ns).max(),
        pop_elapsed_ns: Some(worker_max.load(AtomicOrdering::Relaxed)),
        fill_elapsed_ns: None,
        drain_elapsed_ns: None,
        avg_data_latency_ns: average_latency_ns(latency_total, consumed),
        producer_fairness_ratio: None,
        consumer_fairness_ratio: None,
        status: BenchRecordStatus::Completed,
        failure_reason: None,
        timeout_ns: None,
        throughput_metrics: None,
    }
}

fn average_latency_ns(latency_total: u128, consumed: u64) -> Option<f64> {
    if consumed == 0 {
        None
    } else {
        Some(latency_total as f64 / consumed as f64)
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
    let producer_count = scenario.producers;
    let producers_done = Arc::new(AtomicBool::new(false));

    let mut producer_handles = Vec::with_capacity(scenario.producers);
    for producer_id in 0..scenario.producers {
        let queue_thread = queue_handle.producer_thread_handle();
        let ready = ready.clone();
        let start_gate = start_gate.clone();
        let start = start.clone();
        let producer_max = producer_max.clone();
        let core_id = producer_core_id(
            core_offset,
            scenario.producers,
            scenario.consumers,
            producer_id,
        );
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
        let queue_thread = queue_handle.consumer_thread_handle();
        let ready = ready.clone();
        let start_gate = start_gate.clone();
        let start = start.clone();
        let producers_done = producers_done.clone();
        let core_id = consumer_core_id(
            core_offset,
            scenario.producers,
            scenario.consumers,
            consumer_id,
        );
        consumer_handles.push(spawn_bench_thread(move || -> (u64, u64) {
            if let Some(id) = core_id {
                core_affinity::set_for_current(id);
            }
            ready.wait();
            start_gate.wait();
            let start: Instant = *start.get().expect("start set");
            let mut consumed = 0_u64;
            let backoff = Backoff::new();
            loop {
                let Some(ptr) = queue_thread.try_recv_value() else {
                    if producers_done.load(AtomicOrdering::Acquire) {
                        break;
                    }
                    backoff.snooze();
                    continue;
                };
                deterministic_busy(producer_count + consumer_id, ptr);
                let boxed = unsafe { Box::from_raw(ptr as usize as *mut u64) };
                std::hint::black_box(*boxed);
                consumed = consumed.checked_add(1).expect("consumed count overflow");
            }
            let end_ns = start.elapsed().as_nanos() as u64;
            (end_ns, consumed)
        }));
    }

    ready.wait();
    start.set(Instant::now()).ok();
    start_gate.wait();

    let mut failure_reason = None;
    if let Err(err) = join_bench_threads(producer_handles, "producer") {
        failure_reason.get_or_insert(err);
    }
    producers_done.store(true, AtomicOrdering::Release);
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
            Mode::ComplexThroughput,
            scenario,
            items_per_producer,
            None,
            reason,
            elapsed_ns,
        );
    }

    let elapsed_ns = start.get().expect("start set").elapsed().as_nanos() as u64;
    let consumed = consumer_results.iter().map(|(_, count)| *count).sum();
    let ops_per_sec = throughput_ops(consumed, elapsed_ns);

    BenchRecord {
        repeat_index: 0,
        ubq_label: None,
        protocol: MeasurementProtocol::default(),
        timestamp_unix_ms: 0,
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
        pop_elapsed_ns: consumer_results.iter().map(|(end_ns, _)| *end_ns).max(),
        fill_elapsed_ns: None,
        drain_elapsed_ns: None,
        avg_data_latency_ns: None,
        producer_fairness_ratio: None,
        consumer_fairness_ratio: None,
        status: BenchRecordStatus::Completed,
        failure_reason: None,
        timeout_ns: None,
        throughput_metrics: None,
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
    let producer_count = scenario.producers;
    let producers_done = Arc::new(AtomicBool::new(false));

    let mut producer_handles = Vec::with_capacity(scenario.producers);
    for producer_id in 0..scenario.producers {
        let queue_thread = queue_handle.producer_thread_handle();
        let ready = ready.clone();
        let start_gate = start_gate.clone();
        let start = start.clone();
        let producer_max = producer_max.clone();
        let core_id = producer_core_id(
            core_offset,
            scenario.producers,
            scenario.consumers,
            producer_id,
        );
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
        let queue_thread = queue_handle.consumer_thread_handle();
        let ready = ready.clone();
        let start_gate = start_gate.clone();
        let start = start.clone();
        let producers_done = producers_done.clone();
        let core_id = consumer_core_id(
            core_offset,
            scenario.producers,
            scenario.consumers,
            consumer_id,
        );
        consumer_handles.push(spawn_bench_thread(move || -> (u64, u64, u128) {
            if let Some(id) = core_id {
                core_affinity::set_for_current(id);
            }
            ready.wait();
            start_gate.wait();
            let start: Instant = *start.get().expect("start set");
            let mut consumed = 0_u64;
            let mut latency_total = 0_u128;
            let backoff = Backoff::new();
            loop {
                let Some(ptr) = queue_thread.try_recv_value() else {
                    if producers_done.load(AtomicOrdering::Acquire) {
                        break;
                    }
                    backoff.snooze();
                    continue;
                };
                let now_ns = start.elapsed().as_nanos() as u64;
                deterministic_busy(producer_count + consumer_id, ptr);
                let enqueue_ns = unsafe { *Box::from_raw(ptr as usize as *mut u64) };
                latency_total = latency_total
                    .checked_add(now_ns.saturating_sub(enqueue_ns) as u128)
                    .expect("latency total overflow");
                consumed = consumed.checked_add(1).expect("consumed count overflow");
            }
            let end_ns = start.elapsed().as_nanos() as u64;
            (end_ns, consumed, latency_total)
        }));
    }

    ready.wait();
    start.set(Instant::now()).ok();
    start_gate.wait();

    let mut failure_reason = None;
    if let Err(err) = join_bench_threads(producer_handles, "producer") {
        failure_reason.get_or_insert(err);
    }
    // Once every producer has returned, an empty reservation from every
    // consumer means all remaining values (if any) are already owned by
    // other consumers — no shutdown sentinel value is needed. A value-based
    // sentinel would be unsound here for any baseline (e.g. moodycamel)
    // that doesn't guarantee ordering across different producer threads.
    producers_done.store(true, AtomicOrdering::Release);
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
            Mode::DataLatency,
            scenario,
            items_per_producer,
            None,
            reason,
            elapsed_ns,
        );
    }

    let elapsed_ns = start.get().expect("start set").elapsed().as_nanos() as u64;
    let consumed = consumer_results.iter().map(|(_, count, _)| *count).sum();
    let latency_total: u128 = consumer_results
        .iter()
        .map(|(_, _, latency)| *latency)
        .sum();
    let ops_per_sec = throughput_ops(consumed, elapsed_ns);
    let avg_data_latency_ns = if consumed == 0 {
        None
    } else {
        Some(latency_total as f64 / consumed as f64)
    };

    BenchRecord {
        repeat_index: 0,
        ubq_label: None,
        protocol: MeasurementProtocol::default(),
        timestamp_unix_ms: 0,
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
        pop_elapsed_ns: consumer_results.iter().map(|(end_ns, _, _)| *end_ns).max(),
        fill_elapsed_ns: None,
        drain_elapsed_ns: None,
        avg_data_latency_ns,
        producer_fairness_ratio: None,
        consumer_fairness_ratio: None,
        status: BenchRecordStatus::Completed,
        failure_reason: None,
        timeout_ns: None,
        throughput_metrics: None,
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
    let producers_done = Arc::new(AtomicBool::new(false));

    let mut producer_handles = Vec::with_capacity(scenario.producers);
    for producer_id in 0..scenario.producers {
        let queue_thread = queue_handle.producer_thread_handle();
        let ready = ready.clone();
        let start_gate = start_gate.clone();
        let start = start.clone();
        let core_id = producer_core_id(
            core_offset,
            scenario.producers,
            scenario.consumers,
            producer_id,
        );
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
        let queue_thread = queue_handle.consumer_thread_handle();
        let ready = ready.clone();
        let start_gate = start_gate.clone();
        let start = start.clone();
        let producers_done = producers_done.clone();
        let core_id = consumer_core_id(
            core_offset,
            scenario.producers,
            scenario.consumers,
            consumer_id,
        );
        consumer_handles.push(spawn_bench_thread(move || -> (u64, u64) {
            if let Some(id) = core_id {
                core_affinity::set_for_current(id);
            }
            ready.wait();
            start_gate.wait();
            let start: Instant = *start.get().expect("start set");
            let mut consumed = 0_u64;
            let backoff = Backoff::new();
            loop {
                match queue_thread.try_recv_value() {
                    Some(_) => consumed += 1,
                    None => {
                        if producers_done.load(AtomicOrdering::Acquire) {
                            break;
                        }
                        backoff.snooze();
                    }
                }
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
    producers_done.store(true, AtomicOrdering::Release);
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
            None,
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
        repeat_index: 0,
        ubq_label: None,
        protocol: MeasurementProtocol::default(),
        timestamp_unix_ms: 0,
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
        throughput_metrics: None,
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
        QueueKind::Ubq => spec.queue.name().to_string(),
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
        repeat_index: 0,
        ubq_label: None,
        protocol: MeasurementProtocol::default(),
        timestamp_unix_ms: 0,
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
        throughput_metrics: None,
    }
}

fn failed_runtime_bench_record(
    queue_name: &str,
    mode: Mode,
    scenario: &ScenarioConfig,
    items_per_producer: u64,
    batch_size: Option<usize>,
    reason: String,
    elapsed_ns: u64,
) -> BenchRecord {
    BenchRecord {
        repeat_index: 0,
        ubq_label: None,
        protocol: MeasurementProtocol::default(),
        timestamp_unix_ms: 0,
        queue: queue_name.to_string(),
        mode: mode.name().to_string(),
        batch_size,
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
        status: BenchRecordStatus::Failed,
        failure_reason: Some(reason),
        timeout_ns: None,
        throughput_metrics: None,
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

fn panic_payload_message(payload: Box<dyn std::any::Any + Send + 'static>) -> String {
    if let Some(message) = payload.downcast_ref::<&'static str>() {
        (*message).to_string()
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else {
        "benchmark job panicked".to_string()
    }
}

fn total_items(items_per_producer: u64, producers: usize) -> u64 {
    items_per_producer
        .checked_mul(producers as u64)
        .unwrap_or_else(|| panic!("total items overflow"))
}

fn now_unix_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_millis()
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

fn job_factory_for_spec(spec: &JobSpec) -> Result<JobFactory, String> {
    match spec.queue {
        QueueKind::Lubq => Ok(make_lubq_job_factory(
            spec.scenario.clone(),
            spec.repeat_index,
            spec.mode,
            spec.items_per_producer,
            spec.batch_size,
        )),
        QueueKind::SegQueue => Ok(make_segqueue_job_factory_variant(
            spec.scenario.clone(),
            spec.repeat_index,
            spec.mode,
            spec.items_per_producer,
            spec.batch_size,
        )),
        QueueKind::ConcurrentQueue => Ok(make_concurrent_queue_job_factory(
            spec.scenario.clone(),
            spec.repeat_index,
            spec.mode,
            spec.items_per_producer,
        )),
        QueueKind::MutexVecDeque => Ok(make_mutex_vecdeque_job_factory(
            spec.scenario.clone(),
            spec.repeat_index,
            spec.mode,
            spec.items_per_producer,
            spec.batch_size,
        )),
        QueueKind::MsQueue => Ok(make_ms_queue_job_factory(
            spec.scenario.clone(),
            spec.repeat_index,
            spec.mode,
            spec.items_per_producer,
        )),
        QueueKind::NaiveFaaQueue => Ok(make_naive_faa_queue_job_factory(
            spec.scenario.clone(),
            spec.repeat_index,
            spec.mode,
            spec.items_per_producer,
        )),
        QueueKind::MoodycamelConcurrentQueue => {
            #[cfg(feature = "bench_moodycamel")]
            {
                Ok(make_moodycamel_job_factory(
                    spec.scenario.clone(),
                    spec.repeat_index,
                    spec.mode,
                    spec.items_per_producer,
                    spec.batch_size,
                ))
            }
            #[cfg(not(feature = "bench_moodycamel"))]
            {
                Err(
                    "moodycamel-cq selected but the bench_moodycamel feature is not enabled; \
                     rebuild with --features bench_registry,bench_moodycamel"
                        .to_string(),
                )
            }
        }
        QueueKind::FastFifo => {
            let block_size = spec
                .fastfifo_block_size
                .ok_or_else(|| "RBBQ job spec is missing block size".to_string())?;
            let capacity = spec
                .fastfifo_capacity
                .ok_or_else(|| "FastFifo job spec is missing capacity".to_string())?;
            #[cfg(feature = "bench_fastfifo")]
            {
                Ok(make_fastfifo_job_factory(
                    block_size,
                    capacity,
                    spec.scenario.clone(),
                    spec.repeat_index,
                    spec.mode,
                    spec.items_per_producer,
                ))
            }
            #[cfg(not(feature = "bench_fastfifo"))]
            {
                let _ = (block_size, capacity);
                Err(
                    "RBBQ selected but the bench_fastfifo/bench_rbbq feature is not enabled; \
                     rebuild with --features bench_registry,bench_rbbq"
                        .to_string(),
                )
            }
        }
        QueueKind::LfQueue => {
            let segment_size = spec
                .lfqueue_segment_size
                .ok_or_else(|| "lfqueue job spec is missing segment size".to_string())?;
            #[cfg(feature = "bench_lfqueue")]
            {
                Ok(make_lfqueue_job_factory(
                    segment_size,
                    spec.scenario.clone(),
                    spec.repeat_index,
                    spec.mode,
                    spec.items_per_producer,
                ))
            }
            #[cfg(not(feature = "bench_lfqueue"))]
            {
                let _ = segment_size;
                Err(
                    "lfqueue selected but the bench_lfqueue feature is not enabled; \
                     rebuild with --features bench_registry,bench_lfqueue"
                        .to_string(),
                )
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
                })
            }
            #[cfg(not(feature = "bench_wcq"))]
            {
                let _ = capacity;
                Err("wCQ selected but the bench_wcq feature is not enabled; \
                     rebuild with --features bench_registry,bench_wcq"
                    .to_string())
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
            })
        }
    }
}

/// Run a [`MatrixPlan`] through the static queue registry. The parent keeps all
/// scheduling and checkpoint state while a reusable subprocess executes one job
/// at a time, providing a killable boundary for the hard per-job timeout.
pub fn run_matrix_plan_in_process(
    plan: &MatrixPlan,
    dry_run: bool,
) -> Result<BatchOutcome, String> {
    plan.throughput_policy.validate()?;
    let selected_core_ids = selected_plan_core_ids(plan)?;
    let required_specs = required_job_specs(plan);
    let required_cpu_count = required_specs
        .iter()
        .map(JobSpec::thread_budget)
        .max()
        .unwrap_or(0);
    if selected_core_ids.len() < required_cpu_count && !plan.allow_unpinned {
        return Err(format!(
            "authoritative run requires {required_cpu_count} selected CPUs, but only {} were selected",
            selected_core_ids.len()
        ));
    }
    let planned_item_handoffs: u128 = required_specs
        .iter()
        .map(|spec| total_items(spec.items_per_producer, spec.scenario.producers) as u128)
        .sum();
    progress_line(format!(
        "bench_matrix: {} bundle(s), {} unique job(s), {} planned item handoffs [persistent worker]",
        plan.bundles.len(),
        required_specs.len(),
        planned_item_handoffs,
    ));
    print_core_placement(plan);

    if dry_run {
        return Ok(BatchOutcome {
            exit_success: true,
            crashed_job: None,
        });
    }

    for spec in &required_specs {
        job_factory_for_spec(spec)?;
    }

    let existing_runs = load_existing_runs(plan)?;
    let timing_estimator = TimingEstimator::from_history(&existing_runs);
    let cache = if plan.reuse_existing {
        existing_runs
    } else {
        ExistingRunsIndex::default()
    };

    // Drop already-cached specs from the pending list.
    let pending: Vec<JobSpec> = required_specs
        .iter()
        .filter(|spec| !cache.records.contains_key(&SampleKey::from_job(spec)))
        .cloned()
        .collect();

    progress_line(format!(
        "{} bundle(s), {} required, {} cached, {} pending",
        plan.bundles.len(),
        required_specs.len(),
        required_specs.len().saturating_sub(pending.len()),
        pending.len(),
    ));

    execute_job_specs_with_timeout(
        plan,
        &cache,
        pending,
        plan.available_parallelism,
        bench_job_timeout(plan),
        timing_estimator,
    )?;

    Ok(BatchOutcome {
        exit_success: true,
        crashed_job: None,
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
            repeat_index: 1,
            queue: queue.to_string(),
            ubq_label: None,
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
            throughput_metrics: None,
            protocol: MeasurementProtocol::default(),
            timestamp_unix_ms: now_unix_ms() as u64,
        }
    }

    #[test]
    fn current_and_legacy_grid_markers_use_the_same_page_sized_variants() {
        let page = UbqGrid::Page.labels();
        let sparse = UbqGrid::Sparse.labels();
        let dense = UbqGrid::Dense.labels();

        assert_eq!(page.len(), 2);
        assert_eq!(sparse, page);
        assert_eq!(dense, sparse);
        assert!(sparse.contains(&"balanced,1,page,crossbeam".to_string()));
        assert!(sparse.contains(&"balanced,1,page,yield".to_string()));
    }

    #[test]
    fn default_scenarios_are_the_complete_feasible_power_of_two_grid() {
        let scenarios =
            parse_scenarios_with_parallelism(None, 16).expect("default machine scenario grid");
        let coordinates = scenarios
            .iter()
            .map(|scenario| (scenario.producers, scenario.consumers))
            .collect::<BTreeSet<_>>();
        let expected = [1, 2, 4, 8]
            .into_iter()
            .flat_map(|producers| {
                [1, 2, 4, 8]
                    .into_iter()
                    .filter(move |consumers| producers + consumers <= 16)
                    .map(move |consumers| (producers, consumers))
            })
            .collect::<BTreeSet<_>>();

        assert_eq!(coordinates, expected);
        assert_eq!(scenarios.len(), 16);
        assert!(
            scenarios
                .iter()
                .all(|scenario| scenario.producers.is_power_of_two()
                    && scenario.consumers.is_power_of_two()
                    && scenario.total_threads() <= 16)
        );

        let large =
            parse_scenarios_with_parallelism(None, 160).expect("large machine scenario grid");
        assert_eq!(large.len(), 61);
        assert!(large.iter().any(|scenario| scenario.name == "128p32c"));
        assert!(large.iter().any(|scenario| scenario.name == "32p128c"));
        assert!(!large.iter().any(|scenario| scenario.name == "128p64c"));
    }

    #[test]
    fn sparse_and_dense_grids_use_the_same_complete_scenario_set() {
        let scenarios =
            parse_scenarios_with_parallelism(None, 16).expect("default machine scenario grid");
        let build = |grid| {
            build_grid_matrix_plan(
                "local",
                PathBuf::from("runs"),
                16,
                &[QueueKind::Ubq, QueueKind::SegQueue],
                grid,
                &[],
                &[],
                &[],
                &[],
                &scenarios,
                &[Mode::Throughput],
                Some(&[10]),
                1,
                true,
            )
            .expect("grid plan")
        };
        let sparse = build(UbqGrid::Sparse);
        let dense = build(UbqGrid::Dense);
        let scenario_names = |plan: &MatrixPlan| {
            plan.bundles
                .iter()
                .map(|bundle| bundle.scenario.name.clone())
                .collect::<BTreeSet<_>>()
        };

        assert_eq!(scenario_names(&sparse), scenario_names(&dense));
        assert_eq!(scenario_names(&sparse).len(), 16);
        assert_eq!(sparse.bundles.len(), 16 * 3);
        assert_eq!(dense.bundles.len(), 16 * 3);

        for plan in [&sparse, &dense] {
            let specs = required_job_specs(plan);
            for queue in [QueueKind::Ubq, QueueKind::SegQueue] {
                let tested = specs
                    .iter()
                    .filter(|spec| spec.queue == queue)
                    .map(|spec| spec.scenario.name.clone())
                    .collect::<BTreeSet<_>>();
                assert_eq!(tested, scenario_names(plan));
            }
        }
    }

    #[test]
    fn scenario_scaled_item_policy_has_stable_band_boundaries() {
        let cases = [
            (1, 1_000_000),
            (8, 1_000_000),
            (9, 250_000),
            (16, 250_000),
            (17, 62_500),
            (32, 62_500),
            (33, 15_625),
            (64, 15_625),
            (159, 15_625),
        ];
        for (producers, expected) in cases {
            assert_eq!(scenario_scaled_items_per_producer(producers), expected);
        }
        assert_eq!(parse_items_per_producer(Some("10")).unwrap(), [10]);
        assert_eq!(
            parse_items_per_producer(Some("10,20,10")).unwrap(),
            [10, 20]
        );
    }

    #[test]
    fn grid_item_policy_applies_identical_work_to_baselines_and_ubq() {
        let scenarios = [ScenarioConfig::new(8, 1), ScenarioConfig::new(9, 1)];
        let scaled = build_grid_matrix_plan(
            "local",
            PathBuf::from("runs"),
            10,
            &[QueueKind::Ubq, QueueKind::SegQueue],
            UbqGrid::Sparse,
            &DEFAULT_UBQ_BATCH_SIZES,
            &[],
            &[],
            &[],
            &scenarios,
            &[Mode::Throughput],
            None,
            1,
            true,
        )
        .expect("scaled grid plan");
        assert_eq!(scaled.item_policy, ItemPolicy::ScenarioScaledV1);
        for bundle in &scaled.bundles {
            assert_eq!(
                bundle.items_per_producer_values,
                vec![scenario_scaled_items_per_producer(
                    bundle.scenario.producers
                )]
            );
            let meta = scenario_output_meta(&scaled, &bundle.scenario, None);
            assert_eq!(
                meta.planned_items_per_producer,
                bundle.items_per_producer_values
            );
        }

        let explicit = build_grid_matrix_plan(
            "local",
            PathBuf::from("runs"),
            10,
            &[QueueKind::Ubq, QueueKind::SegQueue],
            UbqGrid::Sparse,
            &DEFAULT_UBQ_BATCH_SIZES,
            &[],
            &[],
            &[],
            &scenarios,
            &[Mode::Throughput],
            Some(&[10, 20]),
            1,
            true,
        )
        .expect("explicit grid plan");
        assert_eq!(explicit.item_policy, ItemPolicy::Explicit);
        assert!(
            explicit
                .bundles
                .iter()
                .all(|bundle| bundle.items_per_producer_values == [10, 20])
        );
    }

    #[test]
    fn interleaved_core_slots_alternate_then_exhaust_the_remaining_role() {
        let slots = |producers, consumers| {
            let producer_slots = (0..producers)
                .map(|id| producer_core_slot(producers, consumers, id))
                .collect::<Vec<_>>();
            let consumer_slots = (0..consumers)
                .map(|id| consumer_core_slot(producers, consumers, id))
                .collect::<Vec<_>>();
            (producer_slots, consumer_slots)
        };

        assert_eq!(slots(4, 4), (vec![0, 2, 4, 6], vec![1, 3, 5, 7]));
        assert_eq!(slots(4, 1), (vec![0, 2, 3, 4], vec![1]));
        assert_eq!(slots(1, 4), (vec![0], vec![1, 2, 3, 4]));

        for (producers, consumers) in [(1, 1), (4, 1), (1, 4), (8, 8), (8, 3)] {
            let (producer_slots, consumer_slots) = slots(producers, consumers);
            let assigned = producer_slots
                .into_iter()
                .chain(consumer_slots)
                .collect::<BTreeSet<_>>();
            assert_eq!(assigned, (0..producers + consumers).collect());
        }
    }

    #[test]
    fn grid_plan_adds_every_batch_size_to_ubq_lubq_and_segqueue_throughput() {
        let plan = build_grid_matrix_plan(
            "local",
            PathBuf::from("runs"),
            2,
            &[
                QueueKind::Ubq,
                QueueKind::Lubq,
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
            Some(&[10]),
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
            2
        );
        assert_eq!(
            plan.bundles
                .iter()
                .filter(|bundle| bundle.ubq_label.is_none())
                .count(),
            1
        );
        assert_eq!(specs.len(), 17);
        assert_eq!(
            specs
                .iter()
                .filter(|spec| spec.queue == QueueKind::Ubq && spec.batch_size.is_none())
                .count(),
            2
        );
        assert_eq!(
            specs
                .iter()
                .filter(|spec| spec.queue == QueueKind::Lubq && spec.batch_size.is_none())
                .count(),
            1
        );
        assert_eq!(
            specs
                .iter()
                .filter(|spec| spec.queue == QueueKind::Lubq && spec.batch_size.is_some())
                .map(|spec| spec.batch_size.expect("batch size"))
                .collect::<BTreeSet<_>>(),
            BTreeSet::from(DEFAULT_UBQ_BATCH_SIZES)
        );
        assert_eq!(
            specs
                .iter()
                .filter(|spec| spec.queue == QueueKind::SegQueue && spec.batch_size.is_none())
                .count(),
            1
        );
        assert_eq!(
            specs
                .iter()
                .filter(|spec| spec.queue == QueueKind::SegQueue && spec.batch_size.is_some())
                .map(|spec| spec.batch_size.expect("batch size"))
                .collect::<BTreeSet<_>>(),
            BTreeSet::from(DEFAULT_UBQ_BATCH_SIZES)
        );
        assert_eq!(
            specs
                .iter()
                .filter(|spec| spec.queue == QueueKind::Ubq && spec.batch_size.is_some())
                .count(),
            6
        );
        assert!(
            specs
                .iter()
                .filter(|spec| {
                    spec.queue.is_baseline()
                        && !matches!(spec.queue, QueueKind::Lubq | QueueKind::SegQueue)
                })
                .all(|spec| spec.batch_size.is_none())
        );
        let baseline_bundle = plan
            .bundles
            .iter()
            .find(|bundle| bundle.ubq_label.is_none())
            .expect("baseline bundle");
        assert_eq!(expected_keys_for_bundle(&plan, baseline_bundle).len(), 9);
        let ubq_bundle = plan
            .bundles
            .iter()
            .find(|bundle| bundle.ubq_label.is_some())
            .expect("UBQ bundle");
        let ubq_keys = expected_keys_for_bundle(&plan, ubq_bundle);
        assert_eq!(ubq_keys.len(), 4);
        assert!(
            ubq_keys
                .iter()
                .all(|key| key.queue_label.starts_with("ubq_"))
        );
    }

    #[test]
    fn legacy_explicit_labels_normalize_to_the_page_variant_without_duplication() {
        let extra = vec!["balanced,1,65535,crossbeam".to_string()];
        let plan = build_grid_matrix_plan_with_extra_ubq_labels(
            "local",
            PathBuf::from("runs"),
            2,
            &[QueueKind::Ubq],
            UbqGrid::Sparse,
            &extra,
            &DEFAULT_UBQ_BATCH_SIZES,
            &[],
            &[],
            &[],
            &[ScenarioConfig::new(1, 1)],
            &[Mode::Throughput],
            Some(&[10]),
            1,
            true,
        )
        .expect("grid plan with extra ubq labels");

        let grid_only = build_grid_matrix_plan(
            "local",
            PathBuf::from("runs"),
            2,
            &[QueueKind::Ubq],
            UbqGrid::Sparse,
            &DEFAULT_UBQ_BATCH_SIZES,
            &[],
            &[],
            &[],
            &[ScenarioConfig::new(1, 1)],
            &[Mode::Throughput],
            Some(&[10]),
            1,
            true,
        )
        .expect("grid-only plan");

        assert_eq!(plan.bundles.len(), grid_only.bundles.len());
        let extra_bundle = plan
            .bundles
            .iter()
            .find(|bundle| bundle.ubq_label.as_deref() == Some("balanced,1,page,crossbeam"))
            .expect("normalized page bundle present");

        // The extra label gets the exact same batch-size treatment as every
        // grid label: one scalar spec plus one per DEFAULT_UBQ_BATCH_SIZES.
        let specs = required_job_specs(&plan);
        let extra_specs: Vec<_> = specs
            .iter()
            .filter(|spec| spec.ubq_label.as_deref() == Some("balanced,1,page,crossbeam"))
            .collect();
        assert_eq!(extra_specs.len(), 1 + DEFAULT_UBQ_BATCH_SIZES.len());
        assert!(extra_specs.iter().any(|spec| spec.batch_size.is_none()));
        assert_eq!(
            extra_specs
                .iter()
                .filter_map(|spec| spec.batch_size)
                .collect::<BTreeSet<_>>(),
            BTreeSet::from(DEFAULT_UBQ_BATCH_SIZES)
        );

        assert_eq!(plan.ubq_grid, Some(UbqGrid::Sparse));
        let _ = extra_bundle;
    }

    #[test]
    fn baseline_only_grid_schedules_segqueue_without_ubq_variants() {
        let plan = build_grid_matrix_plan(
            "local",
            PathBuf::from("runs"),
            2,
            &[QueueKind::SegQueue],
            UbqGrid::Sparse,
            &DEFAULT_UBQ_BATCH_SIZES,
            &[],
            &[],
            &[],
            &[ScenarioConfig::new(1, 1)],
            &[Mode::Throughput],
            Some(&[10]),
            1,
            false,
        )
        .expect("baseline-only plan");
        let specs = required_job_specs(&plan);

        assert_eq!(plan.ubq_grid, None);
        assert_eq!(plan.bundles.len(), 1);
        assert_eq!(specs.len(), 1 + DEFAULT_UBQ_BATCH_SIZES.len());
        assert!(specs.iter().all(|spec| spec.queue == QueueKind::SegQueue));
        assert_eq!(
            specs
                .iter()
                .filter_map(|spec| spec.batch_size)
                .collect::<BTreeSet<_>>(),
            BTreeSet::from(DEFAULT_UBQ_BATCH_SIZES)
        );
    }

    #[test]
    fn page_sized_variants_are_valid_at_high_producer_counts() {
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
            Some(&[10]),
            1,
            true,
        )
        .expect("grid plan");

        assert_eq!(plan.bundles.len(), 2);
        assert_eq!(required_job_specs(&plan).len(), 8);
    }

    #[test]
    fn batched_ubq_throughput_preserves_values_and_records_its_batch_size() {
        type Queue = UBQ<u64, backoff::Crossbeam>;
        let record =
            bench_throughput_batched_for::<Queue>("ubq", &ScenarioConfig::new(2, 2), 257, 16, 0);

        assert_eq!(record.status, BenchRecordStatus::Completed);
        assert_eq!(record.batch_size, Some(16));
        assert_eq!(record.consumed_items, record.total_items);
    }

    #[test]
    fn completion_percentage_includes_cached_jobs() {
        assert_eq!(completion_percent(0, 480), 0.0);
        assert_eq!(completion_percent(120, 480), 25.0);
        assert_eq!(completion_percent(480, 480), 100.0);
    }

    #[test]
    fn progress_header_repeats_every_fifty_trial_rows() {
        assert!(progress_header_due(0));
        assert!(!progress_header_due(1));
        assert!(!progress_header_due(49));
        assert!(progress_header_due(50));
        assert!(progress_header_due(100));
    }

    #[test]
    fn progress_timing_uses_a_running_average() {
        let mut timing = TimingEstimator::default();
        assert_eq!(timing.estimate_remaining(3), None);

        timing.observe(Duration::from_secs(2));
        timing.observe(Duration::from_secs(4));

        assert_eq!(timing.estimate_remaining(3), Some(Duration::from_secs(9)));
        assert_eq!(timing.estimate_remaining(0), Some(Duration::ZERO));
    }

    #[test]
    fn throughput_timeout_is_derived_only_from_declared_phase_budget() {
        let mut plan = build_direct_matrix_plan(
            "local",
            PathBuf::from(DEFAULT_RUNS_DIR),
            2,
            &[QueueKind::SegQueue],
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
        .expect("plan");
        plan.throughput_policy = ThroughputPolicy {
            warmup_ms: 250,
            phase_ms: 1_000,
            pilot_ms: 100,
            max_round_items: 1024,
        };
        assert_eq!(bench_job_timeout(&plan), Duration::from_secs(30));
        plan.throughput_policy.phase_ms = 10_000;
        assert_eq!(bench_job_timeout(&plan), Duration::from_millis(151_750));
    }

    #[test]
    fn explicit_cpu_lists_accept_ranges_and_reject_duplicates() {
        assert_eq!(
            parse_core_ids("0-2,8,10-11").unwrap(),
            vec![0, 1, 2, 8, 10, 11]
        );
        assert!(parse_core_ids("2-1").is_err());
        assert!(parse_core_ids("0,0").is_err());
        assert_eq!(
            parse_schedule_seed("0x55425106").unwrap(),
            DEFAULT_SCHEDULE_SEED
        );
    }

    #[test]
    fn pilot_scaling_rounds_up_to_batch_and_stops_at_cap() {
        assert_eq!(batch_aligned_round_items(4_097, 256, 10_000), Some(4_352));
        assert_eq!(batch_aligned_round_items(20_000, 256, 10_000), Some(9_984));
        assert_eq!(batch_aligned_round_items(1, 256, 128), None);
    }

    #[test]
    fn crossbeam_batch_queue_uses_native_batch_operations() {
        let queue = BatchQueue::new();

        queue.send_batch(10, 0..4);

        assert_eq!(queue.try_recv_batch(3), 3);
        assert_eq!(queue.try_recv_value(), Some(13));
        assert_eq!(queue.try_recv_value(), None);

        let record = bench_throughput_batched_for::<BatchQueue<u64>>(
            "segqueue",
            &ScenarioConfig::new(2, 2),
            257,
            16,
            0,
        );
        assert_eq!(record.status, BenchRecordStatus::Completed);
        assert_eq!(record.batch_size, Some(16));
        assert_eq!(record.consumed_items, record.total_items);
    }

    struct DropFirstQueue {
        inner: SegQueue<u64>,
        dropped: AtomicBool,
    }

    impl BenchQueueOps for DropFirstQueue {
        fn try_send_value(&self, value: u64) -> bool {
            // Silently swallow the very first send (pretend success without
            // actually storing it) to induce an intentional undercount, so
            // the harness's integrity check below has something real to catch.
            if !self.dropped.swap(true, AtomicOrdering::Relaxed) {
                return true;
            }
            self.inner.push(value);
            true
        }

        fn try_recv_value(&self) -> Option<u64> {
            self.inner.pop()
        }
    }

    #[test]
    fn exact_count_mismatch_fails_without_throughput() {
        let queue = Arc::new(DropFirstQueue {
            inner: SegQueue::new(),
            dropped: AtomicBool::new(false),
        });
        let error = run_handoff_round(&queue, &ScenarioConfig::new(1, 1), 16, None, 0)
            .expect_err("integrity failure");
        assert!(error.contains("integrity mismatch"));
    }

    struct RetryQueue {
        attempts: AtomicU64,
    }

    impl BenchQueueOps for RetryQueue {
        fn try_send_value(&self, _value: u64) -> bool {
            self.attempts.fetch_add(1, AtomicOrdering::Relaxed) >= 3
        }

        fn try_recv_value(&self) -> Option<u64> {
            None
        }
    }

    #[test]
    fn harness_retry_policy_retries_nonblocking_adapters_uniformly() {
        let queue = RetryQueue {
            attempts: AtomicU64::new(0),
        };
        queue.send_value(1);
        assert_eq!(queue.attempts.load(AtomicOrdering::Relaxed), 4);
    }

    struct BatchTrackingQueue {
        inner: SegQueue<u64>,
        scalar_sends: AtomicU64,
        scalar_receives: AtomicU64,
        batch_sends: AtomicU64,
        batch_receives: AtomicU64,
    }

    impl BenchQueueOps for BatchTrackingQueue {
        fn try_send_value(&self, value: u64) -> bool {
            self.scalar_sends.fetch_add(1, AtomicOrdering::Relaxed);
            self.inner.push(value);
            true
        }

        fn send_batch(&self, base: u64, offsets: std::ops::Range<usize>) {
            self.batch_sends.fetch_add(1, AtomicOrdering::Relaxed);
            for offset in offsets {
                self.inner.push(base + offset as u64);
            }
        }

        fn try_recv_value(&self) -> Option<u64> {
            self.scalar_receives.fetch_add(1, AtomicOrdering::Relaxed);
            self.inner.pop()
        }

        fn try_recv_batch(&self, request_size: usize) -> usize {
            self.batch_receives.fetch_add(1, AtomicOrdering::Relaxed);
            let mut received = 0;
            for _ in 0..request_size {
                if self.inner.pop().is_none() {
                    break;
                }
                received += 1;
            }
            received
        }
    }

    #[test]
    fn batched_throughput_uses_batch_operations_on_both_sides() {
        let queue = Arc::new(BatchTrackingQueue {
            inner: SegQueue::new(),
            scalar_sends: AtomicU64::new(0),
            scalar_receives: AtomicU64::new(0),
            batch_sends: AtomicU64::new(0),
            batch_receives: AtomicU64::new(0),
        });
        let record = bench_throughput_with_queue_variant(
            queue.clone(),
            "batch-tracking",
            &ScenarioConfig::new(2, 2),
            17,
            Some(8),
            0,
        );

        assert_eq!(record.status, BenchRecordStatus::Completed);
        assert_eq!(record.batch_size, Some(8));
        assert_eq!(record.consumed_items, record.total_items);
        assert!(queue.batch_sends.load(AtomicOrdering::Relaxed) > 0);
        assert!(queue.batch_receives.load(AtomicOrdering::Relaxed) > 0);
        assert_eq!(queue.scalar_sends.load(AtomicOrdering::Relaxed), 0);
        assert_eq!(queue.scalar_receives.load(AtomicOrdering::Relaxed), 0);
    }

    #[test]
    fn adaptive_rounds_exclude_warmup_and_reach_each_timing_target() {
        let record =
            bench_throughput_for::<SegQueue<u64>>("segqueue", &ScenarioConfig::new(1, 1), 7, 0);
        let metrics = record.throughput_metrics.expect("metrics");
        assert_eq!(metrics.requested_items_per_producer, 7);
        assert!(metrics.warmup_elapsed_ns >= 1_000_000);
        assert!(metrics.handoff_elapsed_ns >= 1_000_000);
        assert!(metrics.enqueue_elapsed_ns >= 1_000_000);
        assert!(metrics.dequeue_elapsed_ns >= 1_000_000);
        assert_eq!(
            record.ops_per_sec,
            throughput_ops(metrics.handoff_items, metrics.handoff_elapsed_ns)
        );
        assert_eq!(
            metrics.enqueue_ops_per_sec,
            throughput_ops(metrics.enqueue_items, metrics.enqueue_elapsed_ns).unwrap()
        );
        assert_eq!(
            metrics.dequeue_ops_per_sec,
            throughput_ops(metrics.dequeue_items, metrics.dequeue_elapsed_ns).unwrap()
        );
    }

    #[test]
    fn lubq_runs_scalar_and_native_batch_throughput() {
        for batch_size in [None, Some(8)] {
            let factory = make_lubq_job_factory(
                ScenarioConfig::new(2, 2),
                1,
                Mode::Throughput,
                64,
                batch_size,
            );
            let record = (factory.run)(0);
            assert_eq!(record.status, BenchRecordStatus::Completed);
            assert_eq!(record.queue, "lubq");
            assert_eq!(record.batch_size, batch_size);
        }
    }

    #[test]
    fn lubq_role_specific_handles_cover_application_modes() {
        for mode in [
            Mode::ComplexThroughput,
            Mode::DataLatency,
            Mode::Fairness,
            Mode::AppLogFanIn,
            Mode::AppPipeline,
            Mode::AppTaskRoundtrip,
        ] {
            let factory = make_lubq_job_factory(ScenarioConfig::new(2, 2), 1, mode, 64, None);
            let record = (factory.run)(0);
            assert_eq!(record.status, BenchRecordStatus::Completed, "mode={mode:?}");
            assert_eq!(record.queue, "lubq");
        }

        let factory =
            make_lubq_job_factory(ScenarioConfig::new(2, 1), 1, Mode::AppLogMpscFile, 64, None);
        assert_eq!((factory.run)(0).status, BenchRecordStatus::Completed);
    }

    #[cfg(feature = "bench_fastfifo")]
    #[test]
    fn bounded_fastfifo_ceiling_cycles_with_explicit_capacity() {
        let factory =
            make_fastfifo_job_factory(64, 1_024, ScenarioConfig::new(2, 2), 1, Mode::Throughput, 1);
        let record = (factory.run)(0);
        let metrics = record.throughput_metrics.expect("metrics");
        assert_eq!(metrics.requested_queue_capacity, None);
        assert_eq!(metrics.effective_queue_capacity, Some(1_024));
        assert_eq!(metrics.enqueue_items, metrics.enqueue_rounds as u64 * 1_022);
        assert_eq!(metrics.dequeue_items, metrics.enqueue_items);
    }

    #[test]
    fn progress_durations_are_compact_and_human_readable() {
        assert_eq!(format_progress_duration(Duration::ZERO), "0s");
        assert_eq!(format_progress_duration(Duration::from_micros(42)), "42us");
        assert_eq!(
            format_progress_duration(Duration::from_millis(250)),
            "250ms"
        );
        assert_eq!(
            format_progress_duration(Duration::from_millis(1_250)),
            "1.25s"
        );
        assert_eq!(format_progress_duration(Duration::from_secs(125)), "2m05s");
        assert_eq!(
            format_progress_duration(Duration::from_secs(7_500)),
            "2h05m"
        );
    }

    #[test]
    fn scalar_and_batched_samples_have_distinct_cache_keys() {
        let scalar = JobSpec {
            scenario: ScenarioConfig::new(1, 1),
            repeat_index: 1,
            mode: Mode::Throughput,
            items_per_producer: 10,
            queue: QueueKind::Ubq,
            ubq_label: Some("balanced,1,page,crossbeam".to_string()),
            batch_size: None,
            fastfifo_block_size: None,
            fastfifo_capacity: None,
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
    fn parses_current_and_legacy_ubq_labels_as_the_page_variant() {
        let parsed = parse_ubq_label("balanced,1,127,crossbeam", true).expect("label");
        assert_eq!(parsed.preset, "balanced");
        assert_eq!(parsed.backoff, "crossbeam");
        assert_eq!(parsed.text(), "balanced,1,page,crossbeam");
        assert_eq!(
            parse_ubq_label("balanced,1,page,yield", true)
                .expect("page label")
                .text(),
            "balanced,1,page,yield"
        );
        assert!(parse_ubq_label("balanced,8,127,crossbeam", true).is_err());
    }

    #[test]
    fn historical_block_labels_normalize_to_one_page_sized_configuration() {
        for &block in &LEGACY_UBQ_BLOCK_VALUES {
            let label_text = format!("balanced,1,{block},crossbeam");
            let parsed = parse_ubq_label(&label_text, true)
                .unwrap_or_else(|err| panic!("{label_text} should be valid: {err}"));
            assert_eq!(parsed.text(), "balanced,1,page,crossbeam");
        }
        assert!(parse_ubq_label("balanced,1,65534,crossbeam", true).is_err());
        assert_eq!(UbqGrid::Page.labels().len(), 2);
        assert_eq!(UbqGrid::Sparse.labels().len(), 2);
        assert_eq!(UbqGrid::Dense.labels().len(), 2);
    }

    #[test]
    fn mixed_grid_uses_disjoint_ubq_and_baseline_bundles() {
        let plan = build_grid_matrix_plan(
            "local",
            PathBuf::from("runs"),
            2,
            &[QueueKind::Ubq, QueueKind::SegQueue],
            UbqGrid::Sparse,
            &DEFAULT_UBQ_BATCH_SIZES,
            &[],
            &[],
            &[],
            &[ScenarioConfig::new(1, 1)],
            &[Mode::Throughput],
            Some(&[10]),
            1,
            false,
        )
        .expect("mixed grid");
        assert_eq!(plan.bundles.len(), 3);
        let ubq_bundles = plan
            .bundles
            .iter()
            .filter(|bundle| bundle.ubq_label.is_some())
            .count();
        let baseline_bundles = plan
            .bundles
            .iter()
            .filter(|bundle| bundle.ubq_label.is_none())
            .count();
        assert_eq!(ubq_bundles, 2);
        assert_eq!(baseline_bundles, 1);
        let specs = required_job_specs(&plan);
        assert_eq!(specs.len(), 12);
        assert_eq!(
            specs
                .iter()
                .filter(|spec| spec.queue == QueueKind::Ubq)
                .count(),
            8
        );
        assert_eq!(
            specs
                .iter()
                .filter(|spec| spec.queue == QueueKind::SegQueue)
                .count(),
            4
        );
    }

    #[test]
    fn scenario_search_uses_only_page_sized_backoff_variants() {
        let scenario = ScenarioConfig::new(64, 1);
        let labels = immediate_search_labels_for_scenario("balanced,1,127,crossbeam", &scenario)
            .expect("scenario labels");
        assert_eq!(
            labels,
            BTreeSet::from([
                "balanced,1,page,crossbeam".to_string(),
                "balanced,1,page,yield".to_string(),
            ])
        );
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
        assert_eq!(QueueKind::parse("lubq"), Some(QueueKind::Lubq));
        assert_eq!(QueueKind::parse("linked-ubq"), Some(QueueKind::Lubq));
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
    fn locally_aggregated_microbenchmarks_preserve_exact_counts() {
        let scenario = ScenarioConfig::new(2, 2);
        for mode in [Mode::Throughput, Mode::ComplexThroughput, Mode::DataLatency] {
            let record = match mode {
                Mode::Throughput => {
                    bench_throughput_for::<SegQueue<u64>>("segqueue", &scenario, 32, 0)
                }
                Mode::ComplexThroughput => {
                    bench_complex_throughput_for::<SegQueue<u64>>("segqueue", &scenario, 32, 0)
                }
                Mode::DataLatency => {
                    bench_data_latency_for::<SegQueue<u64>>("segqueue", &scenario, 32, 0)
                }
                _ => unreachable!(),
            };
            assert_eq!(record.status, BenchRecordStatus::Completed);
            if mode == Mode::Throughput {
                let metrics = record
                    .throughput_metrics
                    .as_ref()
                    .expect("adaptive metrics");
                assert_eq!(metrics.requested_items_per_producer, 32);
                assert_eq!(metrics.handoff_items, record.consumed_items);
                assert!(record.consumed_items >= 64);
            } else {
                assert_eq!(record.consumed_items, 64, "mode={}", mode.name());
            }
            assert_eq!(record.consumed_items, record.total_items);
            assert!(record.ops_per_sec.is_some());
            if mode == Mode::DataLatency {
                assert!(record.avg_data_latency_ns.is_some());
            }
        }
        assert_eq!(average_latency_ns(300, 3), Some(100.0));
        assert_eq!(average_latency_ns(0, 0), None);
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
            .collect::<BTreeSet<_>>();
        assert_eq!(
            labels,
            BTreeSet::from([
                "fastfifo_b64_c1048576".to_string(),
                "fastfifo_b256_c1048576".to_string(),
            ])
        );
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
            &[Mode::Throughput],
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
        assert_eq!(labels, vec!["lfqueue_32", "lfqueue_256"]);
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
    fn direct_plan_normalizes_legacy_block_labels_for_high_thread_counts() {
        let plan = build_direct_matrix_plan(
            "local",
            PathBuf::from(DEFAULT_RUNS_DIR),
            128,
            &[QueueKind::Ubq],
            &["balanced,1,63,crossbeam".to_string()],
            &[],
            &[],
            &[],
            &[ScenarioConfig::new(64, 1)],
            &[Mode::Throughput],
            &[1],
            1,
            false,
        )
        .expect("legacy block labels normalize to the page-sized queue");
        assert_eq!(plan.bundles.len(), 1);
        assert_eq!(
            plan.bundles[0].ubq_label.as_deref(),
            Some("balanced,1,page,crossbeam")
        );
    }

    #[test]
    fn reuse_ignores_scenario_coverage_but_honors_measurement_protocol() {
        let root =
            std::env::temp_dir().join(format!("ubq_scenario_subset_test_{}", now_unix_nanos()));
        let runs_dir = root.join("runs");
        let build = |scenarios: &[ScenarioConfig]| {
            build_direct_matrix_plan(
                "local",
                runs_dir.clone(),
                128,
                &[QueueKind::SegQueue],
                &[],
                &[],
                &[],
                &[],
                scenarios,
                &[Mode::Throughput],
                &[1_000_000],
                3,
                true,
            )
            .expect("direct plan")
        };
        let full = build(&[
            ScenarioConfig::new(16, 16),
            ScenarioConfig::new(32, 32),
            ScenarioConfig::new(64, 64),
        ]);
        let mut subset = build(&[ScenarioConfig::new(64, 64)]);

        let key = SampleKey {
            scenario: "64p64c".to_string(),
            repeat_index: 1,
            mode: Mode::Throughput,
            items_per_producer: 1_000_000,
            queue_label: "segqueue".to_string(),
            batch_size: None,
        };
        let mut record = test_record("segqueue", Mode::Throughput, 1_000_000);
        record.total_items = 64_000_000;
        record.consumed_items = 64_000_000;
        record.protocol = MeasurementProtocol::from_plan(&full).expect("protocol");
        let mut writer =
            IncrementalOutputWriter::new(&full, &ExistingRunsIndex::default()).expect("writer");
        writer
            .handle_completed_record(key.clone(), record)
            .expect("persist full-plan sample");
        writer.finish(false).expect("finish writer");

        let cached = load_existing_runs(&subset).expect("load subset cache");
        assert!(
            cached.records.contains_key(&key),
            "a narrower plan must reuse a sample the wider plan already collected"
        );

        subset.throughput_policy.phase_ms += 1;
        let after_protocol_change =
            load_existing_runs(&subset).expect("load subset cache after protocol change");
        assert!(
            !after_protocol_change.records.contains_key(&key),
            "a measurement protocol change under the same machine-label must not reuse the old sample"
        );

        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn serialization_omits_none_fields() {
        let output = OutputFile {
            schema_version: RUN_SCHEMA_VERSION,
            meta: OutputMeta {
                machine_label: "local".to_string(),
                scenario: "1p1c".to_string(),
                producers: 1,
                consumers: 1,
                last_updated_unix_ms: 1,
                host_uname: String::new(),
                git_commit: String::new(),
                git_dirty: false,
                rustc_version: String::new(),
                package_version: String::new(),
                ubq_grid: None,
                expected_ubq_configurations: 0,
                ubq_batch_sizes: Vec::new(),
                planned_repeats: 0,
                planned_items_per_producer: Vec::new(),
            },
            results: vec![BenchRecord {
                repeat_index: 1,
                queue: "segqueue".to_string(),
                ubq_label: None,
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
                throughput_metrics: None,
                protocol: MeasurementProtocol {
                    available_parallelism: 2,
                    core_placement: CorePlacement::Interleaved,
                    affinity_authoritative: true,
                    selected_core_ids: vec![0, 1],
                    item_policy: ItemPolicy::Explicit,
                    throughput_policy: ThroughputPolicy::default(),
                },
                timestamp_unix_ms: 1,
            }],
        };
        let json = serde_json::to_string(&output).expect("json");
        assert!(!json.contains("null"));
        assert!(!json.contains("\"ubq_label\""));
        assert!(!json.contains("fill_elapsed_ns"));
        assert!(!json.contains("status"));
        assert!(json.contains("\"core_placement\":\"interleaved\""));
    }

    #[test]
    fn incremental_writer_persists_partial_bundle_snapshots() {
        let root =
            std::env::temp_dir().join(format!("ubq_partial_snapshot_test_{}", now_unix_nanos()));
        let runs_dir = root.join("runs");
        fs::create_dir_all(&runs_dir).expect("mkdir");

        let plan = MatrixPlan {
            plan_schema_version: PLAN_SCHEMA_VERSION,
            core_placement: CorePlacement::Interleaved,
            item_policy: ItemPolicy::Explicit,
            machine_label: "local".to_string(),
            runs_dir: runs_dir.clone(),
            available_parallelism: 2,
            core_ids: Vec::new(),
            allow_unpinned: true,
            schedule_seed: DEFAULT_SCHEDULE_SEED,
            throughput_policy: ThroughputPolicy::default(),
            job_timeout_secs: None,
            baseline_queues: vec![QueueKind::SegQueue],
            fastfifo_block_sizes: Vec::new(),
            fastfifo_capacities: default_fastfifo_capacities(),
            lfqueue_segment_sizes: Vec::new(),
            wcq_capacities: Vec::new(),
            ubq_grid: None,
            ubq_batch_sizes: Vec::new(),
            planned_repeats: 1,
            bundles: vec![PlanBundle {
                scenario: ScenarioConfig::new(1, 1),
                repeat_index: 1,
                ubq_label: None,
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

        let mut record = test_record("segqueue", Mode::Throughput, 1);
        record.protocol = MeasurementProtocol::from_plan(&plan).expect("protocol");

        let writer =
            IncrementalOutputWriter::new(&plan, &ExistingRunsIndex::default()).expect("writer");
        let writer = {
            let mut writer = writer;
            writer
                .handle_completed_record(key.clone(), record)
                .expect("write partial snapshot");
            writer
        };

        let loaded = load_existing_runs(&plan).expect("load");
        assert_eq!(
            loaded.records.get(&key).expect("cached record").queue,
            "segqueue"
        );
        assert_eq!(loaded.records.len(), 1);

        let mut files = Vec::new();
        collect_run_jsons_recursive(&runs_dir, &mut files).expect("scan runs");
        assert_eq!(files.len(), 1);
        let snapshot = fs::read_to_string(&files[0]).expect("read snapshot");
        assert!(
            !snapshot.contains('\n'),
            "persisted snapshots should be compact"
        );
        let mut mismatched_schema: serde_json::Value =
            serde_json::from_str(&snapshot).expect("parse snapshot");
        mismatched_schema["schema_version"] = serde_json::Value::from(RUN_SCHEMA_VERSION - 1);
        fs::write(
            &files[0],
            serde_json::to_string_pretty(&mismatched_schema)
                .expect("serialize mismatched-schema snapshot"),
        )
        .expect("write mismatched-schema snapshot");
        let mismatched_loaded = load_existing_runs(&plan).expect("load mismatched schema");
        assert!(
            mismatched_loaded.records.is_empty(),
            "a schema version mismatch must not satisfy the cache"
        );

        writer.finish(false).expect("finish writer");
        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn worker_protocol_round_trips_and_rejects_version_mismatches() {
        let request = WorkerRequest {
            protocol_version: BENCH_WORKER_PROTOCOL_VERSION,
            request_id: 7,
            command: WorkerCommand::Run {
                spec: JobSpec {
                    scenario: ScenarioConfig::new(1, 1),
                    repeat_index: 1,
                    mode: Mode::Throughput,
                    items_per_producer: 10,
                    queue: QueueKind::SegQueue,
                    ubq_label: None,
                    batch_size: None,
                    fastfifo_block_size: None,
                    fastfifo_capacity: None,
                    lfqueue_segment_size: None,
                    wcq_capacity: None,
                },
            },
        };
        let json = serde_json::to_string(&request).expect("serialize request");
        let decoded: WorkerRequest = serde_json::from_str(&json).expect("decode request");
        assert_eq!(decoded.protocol_version, BENCH_WORKER_PROTOCOL_VERSION);
        assert_eq!(decoded.request_id, 7);

        let mismatch = WorkerResponse {
            protocol_version: BENCH_WORKER_PROTOCOL_VERSION,
            request_id: 7,
            result: WorkerResult::ProtocolError {
                reason: format!(
                    "worker protocol version mismatch: parent={}, worker={}",
                    BENCH_WORKER_PROTOCOL_VERSION + 1,
                    BENCH_WORKER_PROTOCOL_VERSION
                ),
            },
        };
        let mismatch_json = serde_json::to_string(&mismatch).expect("serialize mismatch");
        assert!(mismatch_json.contains("protocol_error"));
        assert!(mismatch_json.contains("version mismatch"));

        assert!(
            decode_worker_response("not-json", 7)
                .unwrap_err()
                .contains("malformed")
        );
        let wrong_version = WorkerResponse {
            protocol_version: BENCH_WORKER_PROTOCOL_VERSION + 1,
            request_id: 7,
            result: WorkerResult::ShuttingDown,
        };
        assert!(
            decode_worker_response(
                &serde_json::to_string(&wrong_version).expect("serialize wrong version"),
                7,
            )
            .unwrap_err()
            .contains("version mismatch")
        );
    }
}
