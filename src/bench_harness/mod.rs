#![allow(missing_docs)]

#[cfg(feature = "bench_registry")]
use crate::align;
use crate::{ConfiguredUBQ, backoff};
use concurrent_queue::{ConcurrentQueue, PopError};
use crossbeam_queue::SegQueue;
use crossbeam_utils::Backoff;
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

pub const RUN_SCHEMA_VERSION: u32 = 2;
pub const PLAN_SCHEMA_VERSION: u32 = 1;
pub const DEFAULT_ITEMS_PER_PRODUCER: u64 = 1_000_000;
pub const DEFAULT_RUNS_DIR: &str = "bench_results/runs";
pub const DEFAULT_PLOTS_DIR: &str = "bench_results/plots";
pub const DEFAULT_SCENARIOS: &[&str] = &[
    "1p1c", "4p1c", "1p4c", "4p4c", "8p1c", "8p4c", "8p8c", "1p8c", "4p8c", "16p1c", "1p16c",
    "8p16c", "16p8c", "16p16c", "32p1c", "1p32c", "16p32c", "32p16c", "32p32c", "64p1c", "1p64c",
    "32p64c", "64p32c", "64p64c",
];

const SENTINEL: u64 = u64::MAX;
const UBQ_POOL_VALUES: [u8; 8] = [0, 1, 2, 4, 8, 16, 32, 64];
const UBQ_BLOCK_VALUES: [u16; 8] = [31, 63, 127, 255, 511, 1023, 2047, 4095];
const UBQ_BACKOFF_VALUES: [&str; 2] = ["crossbeam", "yield"];

pub trait BenchQueue: Send + Sync + 'static {
    fn new_queue() -> Arc<Self>
    where
        Self: Sized;

    fn send_value(&self, value: u64);
    fn recv_value(&self) -> u64;
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

impl BenchQueue for SegQueue<u64> {
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

impl BenchQueue for ConcurrentQueue<u64> {
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

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Mode {
    Throughput,
    FillDrain,
}

impl Mode {
    pub fn name(self) -> &'static str {
        match self {
            Mode::Throughput => "throughput",
            Mode::FillDrain => "fill_drain",
        }
    }

    pub fn parse(input: &str) -> Option<Self> {
        match input.trim().to_ascii_lowercase().as_str() {
            "throughput" => Some(Self::Throughput),
            "fill_drain" | "fill-drain" => Some(Self::FillDrain),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum QueueKind {
    Ubq,
    SegQueue,
    ConcurrentQueue,
}

impl QueueKind {
    pub fn name(self) -> &'static str {
        match self {
            QueueKind::Ubq => "ubq",
            QueueKind::SegQueue => "segqueue",
            QueueKind::ConcurrentQueue => "concurrent-queue",
        }
    }

    pub fn parse(input: &str) -> Option<Self> {
        match input.trim().to_ascii_lowercase().as_str() {
            "ubq" => Some(Self::Ubq),
            "segqueue" | "crossbeam" | "crossbeam-segqueue" => Some(Self::SegQueue),
            "concurrent-queue" | "concurrent" => Some(Self::ConcurrentQueue),
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
}

impl JobSpec {
    pub fn queue_label(&self) -> String {
        match (&self.queue, &self.ubq_label) {
            (QueueKind::Ubq, Some(label)) => format!("ubq_{label}"),
            _ => self.queue.name().to_string(),
        }
    }

    pub fn thread_budget(&self) -> usize {
        self.scenario.total_threads()
    }

    pub fn sort_key(&self) -> (std::cmp::Reverse<usize>, String, usize, String, u64, String) {
        (
            std::cmp::Reverse(self.thread_budget()),
            self.scenario.name.clone(),
            self.repeat_index,
            self.mode.name().to_string(),
            self.items_per_producer,
            self.queue_label(),
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
}

impl SampleKey {
    pub fn from_job(job: &JobSpec) -> Self {
        Self {
            scenario: job.scenario.name.clone(),
            repeat_index: job.repeat_index,
            mode: job.mode,
            items_per_producer: job.items_per_producer,
            queue_label: job.queue_label(),
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
    pub bundles: Vec<PlanBundle>,
    pub reuse_existing: bool,
}

#[derive(Clone, Debug)]
pub struct FrontierConfig {
    pub machine_label: String,
    pub runs_dir: PathBuf,
    pub scenarios: Vec<ScenarioConfig>,
    pub baseline_queues: Vec<QueueKind>,
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
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BenchRecord {
    pub queue: String,
    pub mode: String,
    pub items_per_producer: u64,
    pub total_items: u64,
    pub consumed_items: u64,
    pub elapsed_ns: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ops_per_sec: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub push_elapsed_ns: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pop_elapsed_ns: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fill_elapsed_ns: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub drain_elapsed_ns: Option<u64>,
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
    wrote_once: bool,
}

impl BundleOutputState {
    fn new(plan: &MatrixPlan, bundle: &PlanBundle, run_id: &str) -> Result<Self, String> {
        Ok(Self {
            meta: bundle_output_meta(plan, bundle)?,
            path: output_path_for_bundle(plan, bundle, run_id),
            ordered_keys: expected_keys_for_bundle(plan, bundle),
            records: BTreeMap::new(),
            dirty: false,
            wrote_once: false,
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
        if !self.dirty && self.wrote_once {
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
        self.wrote_once = true;
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

pub fn parse_scenarios(raw: Option<&str>) -> Result<Vec<ScenarioConfig>, String> {
    let source = raw.map(parse_csv_list).unwrap_or_else(|| {
        DEFAULT_SCENARIOS
            .iter()
            .map(|value| value.to_string())
            .collect()
    });
    let mut out = Vec::new();
    let mut seen = BTreeSet::new();
    for token in source {
        let parsed = parse_scenario_token(&token)
            .ok_or_else(|| format!("invalid scenario token '{token}'"))?;
        if seen.insert(parsed.name.clone()) {
            out.push(parsed);
        }
    }
    out.sort_by_key(|scenario| (scenario.total_threads(), scenario.name.clone()));
    Ok(out)
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

fn immediate_neighbors(label: &UbqLabel, idx: usize) -> Vec<UbqLabel> {
    let mut out = Vec::new();
    match idx {
        0 => {
            for pool in immediate_domain_neighbors_u8(label.pool, &UBQ_POOL_VALUES) {
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
    if include_ubq && ubq_labels.is_empty() {
        return Err("at least one --ubq-label is required when queue set includes ubq".to_string());
    }
    let normalized_ubq_labels = if include_ubq {
        let mut parsed_labels = Vec::with_capacity(ubq_labels.len());
        for label in ubq_labels {
            parsed_labels.push(parse_ubq_label(label, true)?);
        }
        for scenario in scenarios {
            for label in &parsed_labels {
                validate_ubq_label_for_scenario(label, scenario)?;
            }
        }
        parsed_labels
            .into_iter()
            .map(|label| label.text())
            .collect::<Vec<_>>()
    } else {
        Vec::new()
    };
    for scenario in scenarios {
        if scenario.total_threads() > available_parallelism {
            return Err(format!(
                "scenario {} requires {} threads but available_parallelism is {}",
                scenario.name,
                scenario.total_threads(),
                available_parallelism
            ));
        }
    }
    if repeats == 0 {
        return Err("repeats must be > 0".to_string());
    }

    let mut bundles = Vec::new();
    for scenario in scenarios {
        for repeat_index in 1..=repeats {
            if include_ubq {
                for normalized in &normalized_ubq_labels {
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
        bundles,
        reuse_existing,
    })
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
                    out.insert(JobSpec {
                        scenario: bundle.scenario.clone(),
                        repeat_index: bundle.repeat_index,
                        mode: *mode,
                        items_per_producer,
                        queue: baseline_queue,
                        ubq_label: None,
                    });
                }
                if let Some(label) = bundle.ubq_label.as_ref() {
                    out.insert(JobSpec {
                        scenario: bundle.scenario.clone(),
                        repeat_index: bundle.repeat_index,
                        mode: *mode,
                        items_per_producer,
                        queue: QueueKind::Ubq,
                        ubq_label: Some(label.clone()),
                    });
                }
            }
        }
    }
    out
}

pub fn make_ubq_job_factory<Q: BenchQueue>(
    label: &str,
    scenario: ScenarioConfig,
    repeat_index: usize,
    mode: Mode,
    items_per_producer: u64,
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
    };
    let queue_name = "ubq".to_string();
    let run_scenario = scenario.clone();
    let run = Arc::new(move |core_offset: usize| match mode {
        Mode::Throughput => {
            bench_throughput_for::<Q>(&queue_name, &run_scenario, items_per_producer, core_offset)
        }
        Mode::FillDrain => {
            bench_fill_drain_for::<Q>(&queue_name, &run_scenario, items_per_producer, core_offset)
        }
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
        Mode::FillDrain => bench_fill_drain_for::<SegQueue<u64>>(
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
        Mode::FillDrain => bench_fill_drain_for::<ConcurrentQueue<u64>>(
            &queue_name,
            &run_scenario,
            items_per_producer,
            core_offset,
        ),
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
    mut pending: Vec<JobFactory>,
    available_parallelism: usize,
) -> Result<(BTreeMap<SampleKey, BenchRecord>, Option<(String, String)>), String> {
    let num_cores = bench_core_ids().len();
    if available_parallelism > num_cores {
        return Err(format!(
            "cannot pin bench threads: available_parallelism is {} but only {} CPU cores detected",
            available_parallelism, num_cores
        ));
    }
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
    let total_jobs = pending.len();
    progress_line(format!(
        "scheduler: starting {} benchmark job(s) with thread budget {}",
        total_jobs, available_parallelism
    ));
    let writer = OutputWriterHandle::start(plan, cache)?;
    if pending.is_empty() {
        writer.close(true)?;
        return Ok((BTreeMap::new(), None));
    }
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|err| format!("failed to build scheduler runtime: {err}"))?;

    // Each task returns (key, Option<record>, budget).
    // A None record means the spawn_blocking closure panicked.
    let execution_result: Result<(BTreeMap<SampleKey, BenchRecord>, Option<(String, String)>), String> =
        runtime.block_on(async {
            let mut results = BTreeMap::new();
            let mut running: JoinSet<Result<(SampleKey, Option<BenchRecord>, usize), String>> =
                JoinSet::new();
            let mut used_threads = 0_usize;
            let mut completed = 0_usize;
            let mut crashed_job: Option<(String, String)> = None;
            let mut stop_scheduling = false;

            while !pending.is_empty() || !running.is_empty() {
                let mut started = false;
                if !stop_scheduling {
                    loop {
                        let Some(index) = pending.iter().position(|job| {
                            can_start_job(&job.spec, used_threads, available_parallelism)
                        }) else {
                            break;
                        };
                        let job = pending.remove(index);
                        let key = SampleKey::from_job(&job.spec);
                        let budget = job.spec.thread_budget();
                        used_threads += budget;
                        started = true;
                        progress_line(format!(
                            "scheduler: start {} scenario={} repeat={} mode={} items={} threads={} active={}/{} pending={}",
                            job.spec.queue_label(),
                            job.spec.scenario.name.as_str(),
                            job.spec.repeat_index,
                            job.spec.mode.name(),
                            job.spec.items_per_producer,
                            budget,
                            used_threads,
                            available_parallelism,
                            pending.len()
                        ));
                        let core_offset = used_threads - budget;
                        running.spawn(async move {
                            let handle = tokio::task::spawn_blocking(move || (job.run)(core_offset));
                            match handle.await {
                                Ok(record) => Ok((key, Some(record), budget)),
                                Err(err) if err.is_panic() => {
                                    Ok((key, None, budget))
                                }
                                Err(err) => {
                                    Err(format!("benchmark task join failed: {err}"))
                                }
                            }
                        });
                    }
                }

                if running.is_empty() && !pending.is_empty() && !stop_scheduling {
                    return Err("scheduler stalled with pending work".to_string());
                }

                if !started || pending.is_empty() || stop_scheduling {
                    if let Some(joined) = running.join_next().await {
                        let (key, maybe_record, budget) =
                            joined.map_err(|err| format!("scheduler task failed: {err}"))??;
                        used_threads = used_threads.saturating_sub(budget);
                        if let Some(record) = maybe_record {
                            completed += 1;
                            progress_line(format!(
                                "scheduler: done {} scenario={} repeat={} mode={} items={} active={}/{} completed={}/{}",
                                key.queue_label.as_str(),
                                key.scenario.as_str(),
                                key.repeat_index,
                                key.mode.name(),
                                key.items_per_producer,
                                used_threads,
                                available_parallelism,
                                completed,
                                total_jobs
                            ));
                            writer.submit(key.clone(), record.clone())?;
                            results.insert(key, record);
                        } else {
                            // Panic caught: record crash, drain remaining running tasks.
                            progress_line(format!(
                                "scheduler: panic in {} scenario={} repeat={} — stopping new jobs",
                                key.queue_label.as_str(),
                                key.scenario.as_str(),
                                key.repeat_index,
                            ));
                            if crashed_job.is_none() && key.queue_label.starts_with("ubq_") {
                                crashed_job =
                                    Some((key.queue_label.clone(), key.scenario.clone()));
                            }
                            stop_scheduling = true;
                            pending.clear();
                        }
                    }
                }
            }

            Ok((results, crashed_job))
        });
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

fn can_start_job(
    spec: &JobSpec,
    used_threads: usize,
    available_parallelism: usize,
) -> bool {
    used_threads + spec.thread_budget() <= available_parallelism
}

fn result_key_sort(lhs: &SampleKey, rhs: &SampleKey) -> Ordering {
    let queue_order = |label: &str| match label {
        value if value.starts_with("ubq_") => 0_u8,
        "segqueue" => 1,
        "concurrent-queue" => 2,
        _ => 99,
    };
    (
        lhs.mode.name().to_string(),
        lhs.items_per_producer,
        queue_order(&lhs.queue_label),
        lhs.queue_label.clone(),
    )
        .cmp(&(
            rhs.mode.name().to_string(),
            rhs.items_per_producer,
            queue_order(&rhs.queue_label),
            rhs.queue_label.clone(),
        ))
}

fn expected_keys_for_bundle(plan: &MatrixPlan, bundle: &PlanBundle) -> Vec<SampleKey> {
    let mut keys = Vec::new();
    for mode in &bundle.modes {
        for &items_per_producer in &bundle.items_per_producer_values {
            for baseline_queue in &plan.baseline_queues {
                let spec = JobSpec {
                    scenario: bundle.scenario.clone(),
                    repeat_index: bundle.repeat_index,
                    mode: *mode,
                    items_per_producer,
                    queue: *baseline_queue,
                    ubq_label: None,
                };
                keys.push(SampleKey::from_job(&spec));
            }
            if let Some(label) = bundle.ubq_label.as_ref() {
                let spec = JobSpec {
                    scenario: bundle.scenario.clone(),
                    repeat_index: bundle.repeat_index,
                    mode: *mode,
                    items_per_producer,
                    queue: QueueKind::Ubq,
                    ubq_label: Some(label.clone()),
                };
                keys.push(SampleKey::from_job(&spec));
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
        "[package]\nname = \"ubq_generated_scheduler\"\nversion = \"0.0.0\"\nedition = \"2024\"\n\n[dependencies]\nubq = {{ path = {:?} }}\n",
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
            QueueKind::Ubq => {
                let label = parse_ubq_label(
                    spec.ubq_label
                        .as_deref()
                        .expect("ubq labels must be present"),
                    true,
                )
                .expect("valid label");
                out.push_str(&format!(
                    "    jobs.push(bench_harness::make_ubq_job_factory::<{}>(\"{}\", {scenario_expr}, {}, bench_harness::Mode::{:?}, {}));\n",
                    ubq_type_expr(&label),
                    label.text(),
                    spec.repeat_index,
                    spec.mode,
                    spec.items_per_producer
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

fn ubq_type_expr(label: &UbqLabel) -> String {
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
        "ConfiguredUBQ<u64, {backoff_ty}, {}, {}, {align_ty}>",
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
    modes: &[Mode],
    items_per_producer_values: &[u64],
    repeats: usize,
) -> BTreeSet<String> {
    let mut winners = BTreeSet::new();
    for scenario in scenarios {
        for mode in modes {
            for &items_per_producer in items_per_producer_values {
                let best_baseline = baseline_queues
                    .iter()
                    .filter_map(|queue| {
                        mean_ops(
                            index,
                            &scenario.name,
                            queue.name(),
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
    modes: &[Mode],
    items_per_producer_values: &[u64],
) -> bool {
    for mode in modes {
        for &items in items_per_producer_values {
            for baseline in baseline_queues {
                let key = SampleKey {
                    scenario: scenario.name.clone(),
                    repeat_index,
                    mode: *mode,
                    items_per_producer: items,
                    queue_label: baseline.name().to_string(),
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
    modes: &[Mode],
    items_per_producer_values: &[u64],
    repeats: usize,
) -> bool {
    (1..=repeats).all(|repeat_index| {
        bundle_complete(
            index,
            scenario,
            repeat_index,
            Some(label),
            baseline_queues,
            modes,
            items_per_producer_values,
        )
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
    let total_items = total_items(items_per_producer, scenario.producers);
    let queue_handle = Q::new_queue();
    let total_threads = scenario.total_threads();
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
        let core_id = bench_core_ids().get(core_offset + producer_id).copied();
        producer_handles.push(thread::spawn(move || {
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
                queue_handle.send_value(value);
            }
            let end_ns = start.elapsed().as_nanos() as u64;
            producer_max.fetch_max(end_ns, AtomicOrdering::Relaxed);
        }));
    }

    let mut consumer_handles = Vec::with_capacity(scenario.consumers);
    for consumer_id in 0..scenario.consumers {
        let queue_handle = queue_handle.clone();
        let ready = ready.clone();
        let start_gate = start_gate.clone();
        let start = start.clone();
        let consumer_max = consumer_max.clone();
        let consumed_total = consumed_total.clone();
        let core_id = bench_core_ids()
            .get(core_offset + scenario.producers + consumer_id)
            .copied();
        consumer_handles.push(thread::spawn(move || {
            if let Some(id) = core_id {
                core_affinity::set_for_current(id);
            }
            ready.wait();
            start_gate.wait();
            let start: Instant = *start.get().expect("start set");
            loop {
                let value = queue_handle.recv_value();
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
    let consumed = consumed_total.load(AtomicOrdering::Relaxed);
    let ops_per_sec = throughput_ops(consumed, elapsed_ns);

    BenchRecord {
        queue: queue_name.to_string(),
        mode: Mode::Throughput.name().to_string(),
        items_per_producer,
        total_items,
        consumed_items: consumed,
        elapsed_ns,
        ops_per_sec,
        push_elapsed_ns: Some(producer_max.load(AtomicOrdering::Relaxed)),
        pop_elapsed_ns: Some(consumer_max.load(AtomicOrdering::Relaxed)),
        fill_elapsed_ns: None,
        drain_elapsed_ns: None,
    }
}

fn bench_fill_drain_for<Q: BenchQueue>(
    queue_name: &str,
    scenario: &ScenarioConfig,
    items_per_producer: u64,
    core_offset: usize,
) -> BenchRecord {
    let total_items = total_items(items_per_producer, scenario.producers);
    let queue_handle = Q::new_queue();
    let fill_elapsed = run_producers_only_for(
        &queue_handle,
        scenario.producers,
        items_per_producer,
        core_offset,
    );
    for _ in 0..scenario.consumers {
        queue_handle.send_value(SENTINEL);
    }
    let (drain_elapsed, consumed) =
        run_consumers_only_for(&queue_handle, scenario.consumers, core_offset);
    let elapsed_ns = (fill_elapsed + drain_elapsed).as_nanos() as u64;
    let ops_per_sec = throughput_ops(consumed, elapsed_ns);
    BenchRecord {
        queue: queue_name.to_string(),
        mode: Mode::FillDrain.name().to_string(),
        items_per_producer,
        total_items,
        consumed_items: consumed,
        elapsed_ns,
        ops_per_sec,
        push_elapsed_ns: None,
        pop_elapsed_ns: None,
        fill_elapsed_ns: Some(fill_elapsed.as_nanos() as u64),
        drain_elapsed_ns: Some(drain_elapsed.as_nanos() as u64),
    }
}

fn throughput_ops(consumed: u64, elapsed_ns: u64) -> Option<f64> {
    if elapsed_ns == 0 || consumed == 0 {
        None
    } else {
        Some(consumed as f64 / (elapsed_ns as f64 / 1_000_000_000.0))
    }
}

fn run_producers_only_for<Q: BenchQueue>(
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
        let queue_handle = queue_handle.clone();
        let ready = ready.clone();
        let start_gate = start_gate.clone();
        let start = start.clone();
        let max_end = max_end.clone();
        let core_id = bench_core_ids().get(core_offset + producer_id).copied();
        handles.push(thread::spawn(move || {
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
                queue_handle.send_value(value);
            }
            let end_ns = start.elapsed().as_nanos() as u64;
            max_end.fetch_max(end_ns, AtomicOrdering::Relaxed);
        }));
    }

    ready.wait();
    start.set(Instant::now()).ok();
    start_gate.wait();
    for handle in handles {
        handle.join().expect("producer join failed");
    }
    Duration::from_nanos(max_end.load(AtomicOrdering::Relaxed))
}

fn run_consumers_only_for<Q: BenchQueue>(
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
        let queue_handle = queue_handle.clone();
        let ready = ready.clone();
        let start_gate = start_gate.clone();
        let start = start.clone();
        let max_end = max_end.clone();
        let consumed_total = consumed_total.clone();
        let core_id = bench_core_ids().get(core_offset + consumer_id).copied();
        handles.push(thread::spawn(move || {
            if let Some(id) = core_id {
                core_affinity::set_for_current(id);
            }
            ready.wait();
            start_gate.wait();
            let start: Instant = *start.get().expect("start set");
            loop {
                let value = queue_handle.recv_value();
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
    for handle in handles {
        handle.join().expect("consumer join failed");
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
    if total >= SENTINEL {
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
/// configurations are now monomorphised once at build time; each frontier-search round
/// dispatches directly into the pre-compiled functions with no subprocess overhead.
///
/// Panics inside individual benchmark jobs are caught via
/// `tokio::task::spawn_blocking` join errors and reported as a [`BatchOutcome`]
/// with `exit_success = false` and the crashing job identified, matching the
/// contract expected by [`frontier_search`].
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
// Defines: fn lookup_ubq_job_factory(label, scenario, repeat_index, mode, items_per_producer)
//          -> Option<JobFactory>
include!(concat!(env!("OUT_DIR"), "/bench_registry.rs"));

#[cfg(test)]
mod tests {
    use super::*;

    fn test_record(queue: &str, mode: Mode, items_per_producer: u64) -> BenchRecord {
        BenchRecord {
            queue: queue.to_string(),
            mode: mode.name().to_string(),
            items_per_producer,
            total_items: items_per_producer,
            consumed_items: items_per_producer,
            elapsed_ns: 1,
            ops_per_sec: Some(items_per_producer as f64),
            push_elapsed_ns: None,
            pop_elapsed_ns: None,
            fill_elapsed_ns: None,
            drain_elapsed_ns: None,
        }
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
    fn direct_plan_requires_ubq_labels_if_ubq_selected() {
        let err = build_direct_matrix_plan(
            "local",
            PathBuf::from(DEFAULT_RUNS_DIR),
            16,
            &[QueueKind::Ubq, QueueKind::SegQueue],
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
    fn direct_plan_rejects_blocks_below_producer_count() {
        let err = build_direct_matrix_plan(
            "local",
            PathBuf::from(DEFAULT_RUNS_DIR),
            128,
            &[QueueKind::Ubq],
            &["balanced,8,63,crossbeam".to_string()],
            &[ScenarioConfig::new(64, 1)],
            &[Mode::Throughput],
            &[1],
            1,
            false,
        )
        .expect_err("expected validation error");
        assert!(err.contains("64p1c"));
        assert!(err.contains("block size 63"));
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
            },
            results: vec![BenchRecord {
                queue: "segqueue".to_string(),
                mode: "throughput".to_string(),
                items_per_producer: 1,
                total_items: 1,
                consumed_items: 1,
                elapsed_ns: 1,
                ops_per_sec: Some(1.0),
                push_elapsed_ns: None,
                pop_elapsed_ns: None,
                fill_elapsed_ns: None,
                drain_elapsed_ns: None,
            }],
        };
        let json = serde_json::to_string(&output).expect("json");
        assert!(!json.contains("null"));
        assert!(!json.contains("ubq_label"));
        assert!(!json.contains("fill_elapsed_ns"));
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
    fn execute_job_factories_writes_cached_bundles_without_pending_jobs() {
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
        };
        let concurrent_key = SampleKey {
            scenario: scenario.name.clone(),
            repeat_index: 1,
            mode: Mode::Throughput,
            items_per_producer: 1,
            queue_label: "concurrent-queue".to_string(),
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

        let loaded = load_existing_runs(&runs_dir, "local").expect("load");
        assert!(loaded.records.contains_key(&segqueue_key));
        assert!(loaded.records.contains_key(&concurrent_key));

        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn execute_job_factories_can_run_multiple_ubq_jobs_concurrently() {
        let root =
            std::env::temp_dir().join(format!("ubq_parallel_ubq_test_{}", now_unix_nanos()));
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
                },
                run: std::sync::Arc::new(move |_| {
                    start_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    let now_active =
                        active.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
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
    fn frontier_bootstraps_seed_across_all_scenarios() {
        let config = FrontierConfig {
            machine_label: "local".to_string(),
            runs_dir: PathBuf::from(DEFAULT_RUNS_DIR),
            scenarios: vec![ScenarioConfig::new(1, 1), ScenarioConfig::new(1, 4)],
            baseline_queues: vec![QueueKind::SegQueue, QueueKind::ConcurrentQueue],
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
                },
                BenchRecord {
                    queue: "segqueue".to_string(),
                    mode: "throughput".to_string(),
                    items_per_producer: 1,
                    total_items: 1,
                    consumed_items: 1,
                    elapsed_ns: 10,
                    ops_per_sec: Some(10.0),
                    push_elapsed_ns: None,
                    pop_elapsed_ns: None,
                    fill_elapsed_ns: None,
                    drain_elapsed_ns: None,
                },
            );
            index.records.insert(
                SampleKey {
                    scenario: scenario.name.clone(),
                    repeat_index,
                    mode: Mode::Throughput,
                    items_per_producer: 1,
                    queue_label: "concurrent-queue".to_string(),
                },
                BenchRecord {
                    queue: "concurrent-queue".to_string(),
                    mode: "throughput".to_string(),
                    items_per_producer: 1,
                    total_items: 1,
                    consumed_items: 1,
                    elapsed_ns: 9,
                    ops_per_sec: Some(9.0),
                    push_elapsed_ns: None,
                    pop_elapsed_ns: None,
                    fill_elapsed_ns: None,
                    drain_elapsed_ns: None,
                },
            );
            index.records.insert(
                SampleKey {
                    scenario: scenario.name.clone(),
                    repeat_index,
                    mode: Mode::Throughput,
                    items_per_producer: 1,
                    queue_label: "ubq,8,127,crossbeam".to_string(),
                },
                BenchRecord {
                    queue: "ubq".to_string(),
                    mode: "throughput".to_string(),
                    items_per_producer: 1,
                    total_items: 1,
                    consumed_items: 1,
                    elapsed_ns: 20,
                    ops_per_sec: Some(20.0),
                    push_elapsed_ns: None,
                    pop_elapsed_ns: None,
                    fill_elapsed_ns: None,
                    drain_elapsed_ns: None,
                },
            );
        }

        let config = FrontierConfig {
            machine_label: "local".to_string(),
            runs_dir: PathBuf::from(DEFAULT_RUNS_DIR),
            scenarios: vec![scenario.clone()],
            baseline_queues: vec![QueueKind::SegQueue, QueueKind::ConcurrentQueue],
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
                },
                BenchRecord {
                    queue: "segqueue".to_string(),
                    mode: "throughput".to_string(),
                    items_per_producer: 1,
                    total_items: 1,
                    consumed_items: 1,
                    elapsed_ns: 10,
                    ops_per_sec: Some(30.0),
                    push_elapsed_ns: None,
                    pop_elapsed_ns: None,
                    fill_elapsed_ns: None,
                    drain_elapsed_ns: None,
                },
            );
            index.records.insert(
                SampleKey {
                    scenario: scenario.name.clone(),
                    repeat_index,
                    mode: Mode::Throughput,
                    items_per_producer: 1,
                    queue_label: "concurrent-queue".to_string(),
                },
                BenchRecord {
                    queue: "concurrent-queue".to_string(),
                    mode: "throughput".to_string(),
                    items_per_producer: 1,
                    total_items: 1,
                    consumed_items: 1,
                    elapsed_ns: 11,
                    ops_per_sec: Some(29.0),
                    push_elapsed_ns: None,
                    pop_elapsed_ns: None,
                    fill_elapsed_ns: None,
                    drain_elapsed_ns: None,
                },
            );
            index.records.insert(
                SampleKey {
                    scenario: scenario.name.clone(),
                    repeat_index,
                    mode: Mode::Throughput,
                    items_per_producer: 1,
                    queue_label: "ubq,8,127,crossbeam".to_string(),
                },
                BenchRecord {
                    queue: "ubq".to_string(),
                    mode: "throughput".to_string(),
                    items_per_producer: 1,
                    total_items: 1,
                    consumed_items: 1,
                    elapsed_ns: 20,
                    ops_per_sec: Some(20.0),
                    push_elapsed_ns: None,
                    pop_elapsed_ns: None,
                    fill_elapsed_ns: None,
                    drain_elapsed_ns: None,
                },
            );
        }

        let config = FrontierConfig {
            machine_label: "local".to_string(),
            runs_dir: PathBuf::from(DEFAULT_RUNS_DIR),
            scenarios: vec![scenario.clone()],
            baseline_queues: vec![QueueKind::SegQueue, QueueKind::ConcurrentQueue],
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
                },
                BenchRecord {
                    queue: "segqueue".to_string(),
                    mode: "throughput".to_string(),
                    items_per_producer: 1,
                    total_items: 1,
                    consumed_items: 1,
                    elapsed_ns: 10,
                    ops_per_sec: Some(10.0),
                    push_elapsed_ns: None,
                    pop_elapsed_ns: None,
                    fill_elapsed_ns: None,
                    drain_elapsed_ns: None,
                },
            );
            index.records.insert(
                SampleKey {
                    scenario: scenario.name.clone(),
                    repeat_index,
                    mode: Mode::Throughput,
                    items_per_producer: 1,
                    queue_label: "concurrent-queue".to_string(),
                },
                BenchRecord {
                    queue: "concurrent-queue".to_string(),
                    mode: "throughput".to_string(),
                    items_per_producer: 1,
                    total_items: 1,
                    consumed_items: 1,
                    elapsed_ns: 11,
                    ops_per_sec: Some(9.0),
                    push_elapsed_ns: None,
                    pop_elapsed_ns: None,
                    fill_elapsed_ns: None,
                    drain_elapsed_ns: None,
                },
            );
            index.records.insert(
                SampleKey {
                    scenario: scenario.name.clone(),
                    repeat_index,
                    mode: Mode::Throughput,
                    items_per_producer: 1,
                    queue_label: format!("ubq_{weaker_label}"),
                },
                BenchRecord {
                    queue: "ubq".to_string(),
                    mode: "throughput".to_string(),
                    items_per_producer: 1,
                    total_items: 1,
                    consumed_items: 1,
                    elapsed_ns: 20,
                    ops_per_sec: Some(20.0),
                    push_elapsed_ns: None,
                    pop_elapsed_ns: None,
                    fill_elapsed_ns: None,
                    drain_elapsed_ns: None,
                },
            );
            index.records.insert(
                SampleKey {
                    scenario: scenario.name.clone(),
                    repeat_index,
                    mode: Mode::Throughput,
                    items_per_producer: 1,
                    queue_label: format!("ubq_{best_label}"),
                },
                BenchRecord {
                    queue: "ubq".to_string(),
                    mode: "throughput".to_string(),
                    items_per_producer: 1,
                    total_items: 1,
                    consumed_items: 1,
                    elapsed_ns: 15,
                    ops_per_sec: Some(25.0),
                    push_elapsed_ns: None,
                    pop_elapsed_ns: None,
                    fill_elapsed_ns: None,
                    drain_elapsed_ns: None,
                },
            );
        }

        let config = FrontierConfig {
            machine_label: "local".to_string(),
            runs_dir: PathBuf::from(DEFAULT_RUNS_DIR),
            scenarios: vec![scenario.clone()],
            baseline_queues: vec![QueueKind::SegQueue, QueueKind::ConcurrentQueue],
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
                },
                BenchRecord {
                    queue: "segqueue".to_string(),
                    mode: "throughput".to_string(),
                    items_per_producer: 1,
                    total_items: 1,
                    consumed_items: 1,
                    elapsed_ns: 10,
                    ops_per_sec: Some(10.0),
                    push_elapsed_ns: None,
                    pop_elapsed_ns: None,
                    fill_elapsed_ns: None,
                    drain_elapsed_ns: None,
                },
            );
            index.records.insert(
                SampleKey {
                    scenario: winner_scenario.name.clone(),
                    repeat_index,
                    mode: Mode::Throughput,
                    items_per_producer: 1,
                    queue_label: "concurrent-queue".to_string(),
                },
                BenchRecord {
                    queue: "concurrent-queue".to_string(),
                    mode: "throughput".to_string(),
                    items_per_producer: 1,
                    total_items: 1,
                    consumed_items: 1,
                    elapsed_ns: 11,
                    ops_per_sec: Some(11.0),
                    push_elapsed_ns: None,
                    pop_elapsed_ns: None,
                    fill_elapsed_ns: None,
                    drain_elapsed_ns: None,
                },
            );
            index.records.insert(
                SampleKey {
                    scenario: winner_scenario.name.clone(),
                    repeat_index,
                    mode: Mode::Throughput,
                    items_per_producer: 1,
                    queue_label: format!("ubq_{winning_label}"),
                },
                BenchRecord {
                    queue: "ubq".to_string(),
                    mode: "throughput".to_string(),
                    items_per_producer: 1,
                    total_items: 1,
                    consumed_items: 1,
                    elapsed_ns: 20,
                    ops_per_sec: Some(20.0),
                    push_elapsed_ns: None,
                    pop_elapsed_ns: None,
                    fill_elapsed_ns: None,
                    drain_elapsed_ns: None,
                },
            );
        }

        let config = FrontierConfig {
            machine_label: "local".to_string(),
            runs_dir: PathBuf::from(DEFAULT_RUNS_DIR),
            scenarios: vec![winner_scenario.clone(), other_scenario.clone()],
            baseline_queues: vec![QueueKind::SegQueue, QueueKind::ConcurrentQueue],
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
                },
                BenchRecord {
                    queue: "segqueue".to_string(),
                    mode: "throughput".to_string(),
                    items_per_producer: 1,
                    total_items: 1,
                    consumed_items: 1,
                    elapsed_ns: 10,
                    ops_per_sec: Some(10.0),
                    push_elapsed_ns: None,
                    pop_elapsed_ns: None,
                    fill_elapsed_ns: None,
                    drain_elapsed_ns: None,
                },
            );
            index.records.insert(
                SampleKey {
                    scenario: winner_scenario.name.clone(),
                    repeat_index,
                    mode: Mode::Throughput,
                    items_per_producer: 1,
                    queue_label: "concurrent-queue".to_string(),
                },
                BenchRecord {
                    queue: "concurrent-queue".to_string(),
                    mode: "throughput".to_string(),
                    items_per_producer: 1,
                    total_items: 1,
                    consumed_items: 1,
                    elapsed_ns: 11,
                    ops_per_sec: Some(11.0),
                    push_elapsed_ns: None,
                    pop_elapsed_ns: None,
                    fill_elapsed_ns: None,
                    drain_elapsed_ns: None,
                },
            );
            index.records.insert(
                SampleKey {
                    scenario: winner_scenario.name.clone(),
                    repeat_index,
                    mode: Mode::Throughput,
                    items_per_producer: 1,
                    queue_label: format!("ubq_{winning_label}"),
                },
                BenchRecord {
                    queue: "ubq".to_string(),
                    mode: "throughput".to_string(),
                    items_per_producer: 1,
                    total_items: 1,
                    consumed_items: 1,
                    elapsed_ns: 20,
                    ops_per_sec: Some(20.0),
                    push_elapsed_ns: None,
                    pop_elapsed_ns: None,
                    fill_elapsed_ns: None,
                    drain_elapsed_ns: None,
                },
            );
        }

        let config = FrontierConfig {
            machine_label: "local".to_string(),
            runs_dir: PathBuf::from(DEFAULT_RUNS_DIR),
            scenarios: vec![winner_scenario, constrained_scenario.clone()],
            baseline_queues: vec![QueueKind::SegQueue, QueueKind::ConcurrentQueue],
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
        };
        let ubq = JobSpec {
            scenario: ScenarioConfig::new(1, 1),
            repeat_index: 1,
            mode: Mode::Throughput,
            items_per_producer: 1,
            queue: QueueKind::Ubq,
            ubq_label: Some("balanced,1,31,crossbeam".to_string()),
        };

        assert!(can_start_job(&baseline, 0, 8));
        assert!(can_start_job(&ubq, 0, 8));
        assert!(can_start_job(&baseline, 2, 8));
        assert!(can_start_job(&ubq, 2, 8));
        assert!(!can_start_job(&ubq, 7, 8));
    }
}
