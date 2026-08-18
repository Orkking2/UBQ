use clap::Parser;
use std::fs;
use std::path::PathBuf;

use ubq::bench_harness::{
    DEFAULT_RUNS_DIR, DEFAULT_SCHEDULE_SEED, DEFAULT_THROUGHPUT_MAX_ROUND_ITEMS,
    DEFAULT_THROUGHPUT_PHASE_MS, DEFAULT_THROUGHPUT_PILOT_MS, DEFAULT_THROUGHPUT_WARMUP_MS,
    MatrixPlan, ThroughputPolicy, build_direct_matrix_plan, detect_available_parallelism,
    maybe_run_bench_worker, parse_core_ids, parse_fastfifo_block_sizes, parse_fastfifo_capacities,
    parse_items_per_producer, parse_lfqueue_segment_sizes, parse_modes, parse_queue_kinds,
    parse_scenarios_with_parallelism, parse_schedule_seed, parse_wcq_capacities,
    run_matrix_plan_in_process,
};

#[derive(Parser, Debug)]
#[command(name = "bench_matrix")]
struct Args {
    #[arg(long)]
    plan: Option<PathBuf>,

    #[arg(long)]
    machine_label: Option<String>,

    #[arg(long, default_value = DEFAULT_RUNS_DIR)]
    runs_dir: PathBuf,

    #[arg(long)]
    queues: Option<String>,

    /// Explicit selectors; omitted runs every feasible power-of-two producer/consumer pair.
    #[arg(long)]
    scenarios: Option<String>,

    #[arg(long)]
    modes: Option<String>,

    #[arg(long)]
    items_per_producer: Option<String>,

    #[arg(long, default_value_t = 3)]
    repeats: usize,

    #[arg(long)]
    parallelism: Option<usize>,

    #[arg(long = "ubq-label")]
    ubq_labels: Vec<String>,

    #[arg(long, visible_alias = "rbbq-block-sizes")]
    fastfifo_block_sizes: Option<String>,

    #[arg(long)]
    fastfifo_capacities: Option<String>,

    /// Ordered CPU list/ranges, for example 0-7,16-23.
    #[arg(long)]
    core_ids: Option<String>,

    #[arg(long)]
    allow_unpinned: bool,

    #[arg(long, default_value_t = DEFAULT_SCHEDULE_SEED, value_parser = parse_schedule_seed)]
    schedule_seed: u64,

    #[arg(long, default_value_t = DEFAULT_THROUGHPUT_WARMUP_MS)]
    throughput_warmup_ms: u64,

    #[arg(long, default_value_t = DEFAULT_THROUGHPUT_PHASE_MS)]
    throughput_phase_ms: u64,

    #[arg(long, default_value_t = DEFAULT_THROUGHPUT_PILOT_MS)]
    throughput_pilot_ms: u64,

    #[arg(long, default_value_t = DEFAULT_THROUGHPUT_MAX_ROUND_ITEMS)]
    throughput_max_round_items: u64,

    #[arg(long)]
    job_timeout_secs: Option<u64>,

    #[arg(long)]
    lfqueue_segment_sizes: Option<String>,

    #[arg(long)]
    wcq_capacities: Option<String>,

    #[arg(long)]
    reuse_existing: bool,

    #[arg(long)]
    dry_run: bool,
}

fn load_plan(path: &PathBuf) -> Result<MatrixPlan, String> {
    let raw = fs::read_to_string(path)
        .map_err(|err| format!("failed to read plan {}: {err}", path.display()))?;
    serde_json::from_str(&raw).map_err(|err| format!("invalid plan {}: {err}", path.display()))
}

fn main() {
    if let Some(result) = maybe_run_bench_worker() {
        if let Err(err) = result {
            eprintln!("{err}");
            std::process::exit(1);
        }
        return;
    }
    let args = Args::parse();

    let plan = match args.plan.as_ref() {
        Some(path) => load_plan(path),
        None => args
            .machine_label
            .as_deref()
            .ok_or_else(|| "--machine-label is required in direct mode".to_string())
            .and_then(|machine_label| {
                let selected_queues = parse_queue_kinds(
                    args.queues
                        .as_deref()
                        .unwrap_or("ubq,segqueue,concurrent-queue"),
                )?;
                let requested_core_ids =
                    args.core_ids.as_deref().map(parse_core_ids).transpose()?;
                let available_parallelism = match args.parallelism {
                    Some(value) => value,
                    None => requested_core_ids
                        .as_ref()
                        .map(Vec::len)
                        .unwrap_or(detect_available_parallelism()?),
                };
                let all_scenarios = parse_scenarios_with_parallelism(
                    args.scenarios.as_deref(),
                    available_parallelism,
                )?;
                let modes = parse_modes(args.modes.as_deref())?;
                let items = parse_items_per_producer(args.items_per_producer.as_deref())?;
                let fastfifo_block_sizes =
                    parse_fastfifo_block_sizes(args.fastfifo_block_sizes.as_deref())?;
                let lfqueue_segment_sizes =
                    parse_lfqueue_segment_sizes(args.lfqueue_segment_sizes.as_deref())?;
                let wcq_capacities = parse_wcq_capacities(args.wcq_capacities.as_deref())?;
                let scenarios: Vec<_> = all_scenarios
                    .into_iter()
                    .filter(|s| {
                        let ok = s.total_threads() <= available_parallelism;
                        if !ok {
                            eprintln!(
                                "scenario {} requires {} threads but available_parallelism is {}",
                                s.name,
                                s.total_threads(),
                                available_parallelism
                            );
                        }
                        ok
                    })
                    .collect();
                let mut plan = build_direct_matrix_plan(
                    machine_label,
                    args.runs_dir.clone(),
                    available_parallelism,
                    &selected_queues,
                    &args.ubq_labels,
                    &fastfifo_block_sizes,
                    &lfqueue_segment_sizes,
                    &wcq_capacities,
                    &scenarios,
                    &modes,
                    &items,
                    args.repeats,
                    args.reuse_existing,
                )?;
                plan.core_ids = requested_core_ids.unwrap_or_default();
                plan.allow_unpinned = args.allow_unpinned;
                plan.schedule_seed = args.schedule_seed;
                plan.throughput_policy = ThroughputPolicy {
                    warmup_ms: args.throughput_warmup_ms,
                    phase_ms: args.throughput_phase_ms,
                    pilot_ms: args.throughput_pilot_ms,
                    max_round_items: args.throughput_max_round_items,
                };
                plan.job_timeout_secs = args.job_timeout_secs;
                plan.fastfifo_capacities =
                    parse_fastfifo_capacities(args.fastfifo_capacities.as_deref())?;
                Ok(plan)
            }),
    };

    match plan.and_then(|plan| run_matrix_plan_in_process(&plan, args.dry_run)) {
        Ok(outcome) => {
            if let Some((queue_label, scenario)) = outcome.crashed_job {
                eprintln!(
                    "bench_matrix: scheduler crashed while running \
                     ({queue_label}, scenario={scenario})"
                );
                std::process::exit(1);
            } else if !outcome.exit_success {
                eprintln!("bench_matrix: scheduler crashed; check stderr for details");
                std::process::exit(1);
            }
        }
        Err(err) => {
            eprintln!("{err}");
            std::process::exit(1);
        }
    }
}
