use clap::Parser;
use std::time::Duration;
use ubq::bench_harness::{
    HandoffProfileConfig, QueueKind, parse_scenarios_with_parallelism, run_handoff_profile,
};

#[derive(Parser, Debug)]
#[command(name = "bench_profile")]
#[command(about = "Run one foreground queue handoff workload for a sampling profiler")]
struct Args {
    /// Queue to profile: ubq, lubq, or segqueue.
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

    /// Measured handoff duration.
    #[arg(long, default_value_t = 30)]
    duration_secs: u64,

    /// In-process warmup before the measured handoff.
    #[arg(long, default_value_t = 2)]
    warmup_secs: u64,

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

fn parse_queue(raw: &str) -> Result<QueueKind, String> {
    let queue = QueueKind::parse(raw).ok_or_else(|| format!("unknown queue: {raw}"))?;
    match queue {
        QueueKind::Ubq | QueueKind::Lubq | QueueKind::SegQueue => Ok(queue),
        _ => Err("bench_profile supports ubq, lubq, and segqueue".to_string()),
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
    if args.queue != QueueKind::Ubq && args.ubq_label.is_some() {
        return Err("--ubq-label is valid only with --queue ubq".to_string());
    }

    let mut scenarios = parse_scenarios_with_parallelism(Some(&args.scenario), usize::MAX)?;
    if scenarios.len() != 1 {
        return Err("--scenario must select exactly one producer/consumer pair".to_string());
    }
    let scenario = scenarios.pop().expect("one scenario was validated");
    let config = HandoffProfileConfig {
        queue: args.queue,
        ubq_label: args.ubq_label,
        scenario,
        batch_size: args.batch_size,
        warmup: Duration::from_secs(args.warmup_secs),
        duration: Duration::from_secs(args.duration_secs),
        core_offset: args.core_offset,
        allow_unpinned: args.allow_unpinned,
    };

    if !args.json {
        eprintln!(
            "profiling queue={} scenario={} batch={} warmup={}s duration={}s",
            config.queue.name(),
            config.scenario.name,
            config
                .batch_size
                .map(|size| size.to_string())
                .unwrap_or_else(|| "scalar".to_string()),
            args.warmup_secs,
            args.duration_secs,
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
