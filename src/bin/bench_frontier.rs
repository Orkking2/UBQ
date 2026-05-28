use clap::Parser;
use std::path::PathBuf;

use ubq::bench_harness::{
    DEFAULT_RUNS_DIR, FrontierConfig, QueueKind, build_direct_matrix_plan,
    detect_available_parallelism, frontier_search, parse_fastfifo_block_sizes,
    parse_items_per_producer, parse_lfqueue_segment_sizes, parse_modes, parse_queue_kinds,
    parse_scenarios_with_parallelism, parse_wcq_capacities,
};

#[derive(Parser, Debug)]
#[command(name = "bench_frontier")]
struct Args {
    #[arg(long)]
    machine_label: Option<String>,

    #[arg(long, default_value = DEFAULT_RUNS_DIR)]
    runs_dir: PathBuf,

    #[arg(long, default_value = "ubq,segqueue,concurrent-queue")]
    queues: String,

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

    #[arg(long = "seed-label")]
    seed_labels: Vec<String>,

    #[arg(long, visible_alias = "rbbq-block-sizes")]
    fastfifo_block_sizes: Option<String>,

    #[arg(long)]
    lfqueue_segment_sizes: Option<String>,

    #[arg(long)]
    wcq_capacities: Option<String>,

    #[arg(long)]
    dry_run: bool,
}

fn main() {
    let args = Args::parse();

    let result = (|| -> Result<(), String> {
        let machine_label = args
            .machine_label
            .as_deref()
            .ok_or_else(|| "--machine-label is required".to_string())?;
        if args.seed_labels.is_empty() {
            return Err("at least one --seed-label is required".to_string());
        }
        let queues = parse_queue_kinds(&args.queues)?;
        if !queues.iter().any(|queue| *queue == QueueKind::Ubq) {
            return Err("frontier search requires ubq in --queues".to_string());
        }
        let baseline_queues: Vec<QueueKind> = queues
            .iter()
            .copied()
            .filter(|queue| queue.is_baseline())
            .collect();
        if baseline_queues.is_empty() {
            return Err("frontier search requires at least one baseline queue".to_string());
        }
        let available_parallelism = match args.parallelism {
            Some(value) => value,
            None => detect_available_parallelism()?,
        };
        let scenarios =
            parse_scenarios_with_parallelism(args.scenarios.as_deref(), available_parallelism)?;
        let modes = parse_modes(args.modes.as_deref())?;
        let items = parse_items_per_producer(args.items_per_producer.as_deref())?;
        let fastfifo_block_sizes =
            parse_fastfifo_block_sizes(args.fastfifo_block_sizes.as_deref())?;
        let lfqueue_segment_sizes =
            parse_lfqueue_segment_sizes(args.lfqueue_segment_sizes.as_deref())?;
        let wcq_capacities = parse_wcq_capacities(args.wcq_capacities.as_deref())?;
        let mut runnable_scenarios = Vec::new();
        let mut skipped_scenarios = Vec::new();
        for scenario in scenarios {
            if scenario.total_threads() <= available_parallelism {
                runnable_scenarios.push(scenario);
            } else {
                skipped_scenarios.push(scenario.name);
            }
        }
        println!("machine: {}", machine_label);
        println!("runs dir: {}", args.runs_dir.display());
        println!("available parallelism: {}", available_parallelism);
        if !skipped_scenarios.is_empty() {
            println!(
                "skipping scenarios above available_parallelism: {}",
                skipped_scenarios.join(", ")
            );
        }
        if runnable_scenarios.is_empty() {
            return Err("no runnable scenarios remain for this machine".to_string());
        }

        let _ = build_direct_matrix_plan(
            machine_label,
            args.runs_dir.clone(),
            available_parallelism,
            &queues,
            &args.seed_labels,
            &fastfifo_block_sizes,
            &lfqueue_segment_sizes,
            &wcq_capacities,
            &runnable_scenarios,
            &modes,
            &items,
            args.repeats,
            true,
        )?;

        let config = FrontierConfig {
            machine_label: machine_label.to_string(),
            runs_dir: args.runs_dir.clone(),
            scenarios: runnable_scenarios,
            baseline_queues,
            fastfifo_block_sizes,
            lfqueue_segment_sizes,
            wcq_capacities,
            seed_labels: args.seed_labels.clone(),
            modes,
            items_per_producer_values: items,
            repeats: args.repeats,
            available_parallelism,
        };
        frontier_search(&config, args.dry_run)
    })();

    if let Err(err) = result {
        eprintln!("{err}");
        std::process::exit(1);
    }
}
