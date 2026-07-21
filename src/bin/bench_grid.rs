use clap::Parser;
use std::path::PathBuf;

use ubq::bench_harness::{
    DEFAULT_RUNS_DIR, DEFAULT_UBQ_BATCH_SIZES, QueueKind, UbqGrid, build_grid_matrix_plan,
    detect_available_parallelism, parse_fastfifo_block_sizes, parse_items_per_producer,
    parse_lfqueue_segment_sizes, parse_modes, parse_queue_kinds, parse_scenarios_with_parallelism,
    parse_wcq_capacities, run_matrix_plan_in_process,
};

#[derive(Parser, Debug)]
#[command(name = "bench_grid")]
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

    /// Exhaust the 128-configuration grid instead of the default sparse grid.
    #[arg(short = 'd', long)]
    dense: bool,

    /// Ignore compatible schema-v3 results already present under --runs-dir.
    #[arg(long)]
    rerun: bool,

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
        let queues = parse_queue_kinds(&args.queues)?;
        if !queues.contains(&QueueKind::Ubq) {
            return Err("bench_grid requires ubq in --queues".to_string());
        }
        let available_parallelism = match args.parallelism {
            Some(value) => value,
            None => detect_available_parallelism()?,
        };
        let all_scenarios =
            parse_scenarios_with_parallelism(args.scenarios.as_deref(), available_parallelism)?;
        let mut scenarios = Vec::new();
        let mut skipped = Vec::new();
        for scenario in all_scenarios {
            if scenario.total_threads() <= available_parallelism {
                scenarios.push(scenario);
            } else {
                skipped.push(scenario.name);
            }
        }
        if scenarios.is_empty() {
            return Err("no runnable scenarios remain for this machine".to_string());
        }
        let modes = parse_modes(args.modes.as_deref())?;
        let items = parse_items_per_producer(args.items_per_producer.as_deref())?;
        let fastfifo_block_sizes =
            parse_fastfifo_block_sizes(args.fastfifo_block_sizes.as_deref())?;
        let lfqueue_segment_sizes =
            parse_lfqueue_segment_sizes(args.lfqueue_segment_sizes.as_deref())?;
        let wcq_capacities = parse_wcq_capacities(args.wcq_capacities.as_deref())?;
        let grid = if args.dense {
            UbqGrid::Dense
        } else {
            UbqGrid::Sparse
        };
        let plan = build_grid_matrix_plan(
            machine_label,
            args.runs_dir.clone(),
            available_parallelism,
            &queues,
            grid,
            &DEFAULT_UBQ_BATCH_SIZES,
            &fastfifo_block_sizes,
            &lfqueue_segment_sizes,
            &wcq_capacities,
            &scenarios,
            &modes,
            &items,
            args.repeats,
            !args.rerun,
        )?;

        println!("machine: {machine_label}");
        println!("runs dir: {}", args.runs_dir.display());
        println!("available parallelism: {available_parallelism}");
        println!(
            "UBQ grid: {} ({} configurations before scenario constraints)",
            grid.name(),
            grid.labels().len()
        );
        println!(
            "throughput variants per UBQ configuration: {} (scalar + {} batch sizes)",
            1 + DEFAULT_UBQ_BATCH_SIZES.len(),
            DEFAULT_UBQ_BATCH_SIZES.len()
        );
        println!(
            "existing data: {}",
            if args.rerun {
                "ignored (--rerun)"
            } else {
                "reused"
            }
        );
        if !skipped.is_empty() {
            println!(
                "skipping scenarios above available_parallelism: {}",
                skipped.join(", ")
            );
        }

        let outcome = run_matrix_plan_in_process(&plan, args.dry_run)?;
        if let Some((queue_label, scenario)) = outcome.crashed_job {
            return Err(format!(
                "scheduler crashed while running ({queue_label}, scenario={scenario})"
            ));
        }
        if !outcome.exit_success {
            return Err("scheduler failed; check stderr for details".to_string());
        }
        Ok(())
    })();

    if let Err(err) = result {
        eprintln!("{err}");
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sparse_is_the_default_grid() {
        let args =
            Args::try_parse_from(["bench_grid", "--machine-label", "local"]).expect("arguments");
        assert!(!args.dense);
        assert!(!args.rerun);
    }

    #[test]
    fn short_dense_flag_and_rerun_are_supported() {
        let args =
            Args::try_parse_from(["bench_grid", "--machine-label", "local", "-d", "--rerun"])
                .expect("arguments");
        assert!(args.dense);
        assert!(args.rerun);
    }
}
