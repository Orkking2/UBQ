use clap::Parser;
use std::fs;
use std::path::PathBuf;

use ubq::bench_harness::{
    DEFAULT_RUNS_DIR, MatrixPlan, build_direct_matrix_plan, detect_available_parallelism,
    parse_fastfifo_block_sizes, parse_items_per_producer, parse_lfqueue_segment_sizes, parse_modes,
    parse_queue_kinds, parse_scenarios_with_parallelism, parse_wcq_capacities,
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

    #[arg(long)]
    scenarios: Option<String>,

    #[arg(long)]
    modes: Option<String>,

    #[arg(long)]
    items_per_producer: Option<String>,

    #[arg(long, default_value_t = 1)]
    repeats: usize,

    #[arg(long)]
    parallelism: Option<usize>,

    #[arg(long = "ubq-label")]
    ubq_labels: Vec<String>,

    #[arg(long, visible_alias = "rbbq-block-sizes")]
    fastfifo_block_sizes: Option<String>,

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
                let available_parallelism = match args.parallelism {
                    Some(value) => value,
                    None => detect_available_parallelism()?,
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
                build_direct_matrix_plan(
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
                )
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
