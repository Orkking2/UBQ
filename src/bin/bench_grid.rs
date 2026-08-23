use clap::Parser;
use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

use ubq::bench_harness::{
    DEFAULT_RUNS_DIR, DEFAULT_SCHEDULE_SEED, DEFAULT_THROUGHPUT_MAX_ROUND_ITEMS,
    DEFAULT_THROUGHPUT_PHASE_MS, DEFAULT_THROUGHPUT_PILOT_MS, DEFAULT_THROUGHPUT_WARMUP_MS,
    DEFAULT_UBQ_BATCH_SIZES, QueueKind, ThroughputPolicy, UbqGrid, build_grid_matrix_plan,
    detect_available_parallelism, maybe_run_bench_worker, parse_core_ids,
    parse_fastfifo_block_sizes, parse_fastfifo_capacities, parse_items_per_producer,
    parse_lfqueue_segment_sizes, parse_modes, parse_queue_kinds, parse_scenarios_with_parallelism,
    parse_schedule_seed, parse_wcq_capacities, run_matrix_plan_in_process,
};

#[derive(Parser, Debug)]
#[command(name = "bench_grid")]
struct Args {
    #[arg(long)]
    machine_label: Option<String>,

    #[arg(long, default_value = DEFAULT_RUNS_DIR)]
    runs_dir: PathBuf,

    #[arg(long, default_value = "ubq,lubq,segqueue,concurrent-queue")]
    queues: String,

    /// Explicit selectors; omitted runs every feasible power-of-two producer/consumer pair.
    #[arg(long)]
    scenarios: Option<String>,

    #[arg(long)]
    modes: Option<String>,

    /// Comma-separated explicit counts; omitted selects scenario_scaled_v1.
    #[arg(long)]
    items_per_producer: Option<String>,

    #[arg(long, default_value_t = 3)]
    repeats: usize,

    #[arg(long)]
    parallelism: Option<usize>,

    /// Comma-separated UBQ/LUBQ/SegQueue batch sizes; scalar runs are always included.
    #[arg(
        long,
        value_delimiter = ',',
        num_args = 1..,
        default_values_t = DEFAULT_UBQ_BATCH_SIZES
    )]
    batch_sizes: Vec<usize>,

    /// Ignore existing data for this machine-label and recompute everything,
    /// instead of greedily reusing samples already recorded under it.
    #[arg(long)]
    rerun: bool,

    #[arg(long, visible_alias = "rbbq-block-sizes")]
    fastfifo_block_sizes: Option<String>,

    #[arg(long)]
    fastfifo_capacities: Option<String>,

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
    dry_run: bool,
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
    let result = (|| -> Result<(), String> {
        let machine_label = args
            .machine_label
            .as_deref()
            .ok_or_else(|| "--machine-label is required".to_string())?;
        let queues = parse_queue_kinds(&args.queues)?;
        let includes_ubq = queues.contains(&QueueKind::Ubq);
        let requested_core_ids = args.core_ids.as_deref().map(parse_core_ids).transpose()?;
        let available_parallelism = match args.parallelism {
            Some(value) => value,
            None => requested_core_ids
                .as_ref()
                .map(Vec::len)
                .unwrap_or(detect_available_parallelism()?),
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
        let explicit_items = args
            .items_per_producer
            .as_deref()
            .map(|raw| parse_items_per_producer(Some(raw)))
            .transpose()?;
        let fastfifo_block_sizes =
            parse_fastfifo_block_sizes(args.fastfifo_block_sizes.as_deref())?;
        let lfqueue_segment_sizes =
            parse_lfqueue_segment_sizes(args.lfqueue_segment_sizes.as_deref())?;
        let wcq_capacities = parse_wcq_capacities(args.wcq_capacities.as_deref())?;
        let mut plan = build_grid_matrix_plan(
            machine_label,
            args.runs_dir.clone(),
            available_parallelism,
            &queues,
            UbqGrid::Page,
            &args.batch_sizes,
            &fastfifo_block_sizes,
            &lfqueue_segment_sizes,
            &wcq_capacities,
            &scenarios,
            &modes,
            explicit_items.as_deref(),
            args.repeats,
            !args.rerun,
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
        plan.fastfifo_capacities = parse_fastfifo_capacities(args.fastfifo_capacities.as_deref())?;

        println!("machine: {machine_label}");
        println!("runs dir: {}", args.runs_dir.display());
        println!("available parallelism: {available_parallelism}");
        println!("core placement: {}", plan.core_placement.name());
        println!("item policy: {}", plan.item_policy.name());
        let mut resolved_items: BTreeMap<&str, BTreeSet<u64>> = BTreeMap::new();
        for bundle in &plan.bundles {
            resolved_items
                .entry(&bundle.scenario.name)
                .or_default()
                .extend(bundle.items_per_producer_values.iter().copied());
        }
        println!(
            "items per producer by scenario: {}",
            resolved_items
                .into_iter()
                .map(|(scenario, items)| format!(
                    "{scenario}={}",
                    items
                        .into_iter()
                        .map(|value| value.to_string())
                        .collect::<Vec<_>>()
                        .join("/")
                ))
                .collect::<Vec<_>>()
                .join(", ")
        );
        if includes_ubq {
            println!(
                "static UBQ variants: page-derived ({} backoff configurations before constraints)",
                UbqGrid::Page.labels().len()
            );
            println!(
                "throughput variants per UBQ configuration: {} (scalar-compatible + {} batch sizes)",
                1 + plan.ubq_batch_sizes.len(),
                plan.ubq_batch_sizes.len()
            );
        }
        println!(
            "queue batch sizes (UBQ/LUBQ/SegQueue): {}",
            plan.ubq_batch_sizes
                .iter()
                .map(|size| size.to_string())
                .collect::<Vec<_>>()
                .join(",")
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
    fn page_sized_backoff_variants_are_the_default() {
        let args =
            Args::try_parse_from(["bench_grid", "--machine-label", "local"]).expect("arguments");
        assert!(!args.rerun);
        assert_eq!(args.repeats, 3);
        assert_eq!(args.schedule_seed, DEFAULT_SCHEDULE_SEED);
        assert_eq!(args.throughput_warmup_ms, DEFAULT_THROUGHPUT_WARMUP_MS);
        assert_eq!(args.throughput_phase_ms, DEFAULT_THROUGHPUT_PHASE_MS);
        assert_eq!(args.batch_sizes, DEFAULT_UBQ_BATCH_SIZES);
    }

    #[test]
    fn removed_dense_flag_is_rejected() {
        assert!(Args::try_parse_from(["bench_grid", "--machine-label", "local", "-d"]).is_err());
    }

    #[test]
    fn custom_batch_sizes_are_typed_and_comma_delimited() {
        let args = Args::try_parse_from([
            "bench_grid",
            "--machine-label",
            "local",
            "--batch-sizes",
            "8,32,128,512",
        ])
        .expect("arguments");
        assert_eq!(args.batch_sizes, vec![8, 32, 128, 512]);

        assert!(
            Args::try_parse_from([
                "bench_grid",
                "--machine-label",
                "local",
                "--batch-sizes",
                "eight",
            ])
            .is_err()
        );
    }
}
