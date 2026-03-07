mod bench_tooling;

use bench_tooling::{
    Stats, bench_label_sort_key, format_cmd, format_missing_key,
    has_complete_immediate_winner_variants, load_grouped_runs, normalize_machine,
    normalize_ubq_label, parse_scenario_threads, parse_scenarios,
    strict_immediate_winner_ubq_labels, total_valid_ubq_label_count,
};
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::thread;
use std::thread::available_parallelism;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[derive(Clone, Debug)]
struct CompleteArgs {
    machine_label: String,
    runs_dir: PathBuf,
    scenarios: Vec<String>,
    mode: String,
    seed_label: Option<String>,
    allow_incomplete: bool,
    bench_script: PathBuf,
    bench_args: Vec<String>,
    dry_run: bool,
}

#[derive(Clone, Debug)]
struct ScenarioState {
    completed: bool,
    rounds_run: usize,
    previous_missing_key: Option<String>,
    reason: Option<String>,
}

#[derive(Clone, Debug)]
struct ScenarioRoundPlan {
    complete: bool,
    plan_labels: Vec<String>,
    unresolved_no_seed: bool,
    missing_key: Option<String>,
}

#[derive(Clone, Debug)]
struct BenchRunContext<'a> {
    scenario: &'a str,
    round_idx: usize,
    label_count: usize,
}

fn has_no_progress(previous_missing_key: Option<&str>, current_missing_key: Option<&str>) -> bool {
    match (previous_missing_key, current_missing_key) {
        (Some(prev), Some(curr)) => prev == curr,
        _ => false,
    }
}

fn filter_scenarios_for_host(
    scenarios: &[String],
    available_parallelism: usize,
) -> (Vec<String>, Vec<String>) {
    let mut runnable = Vec::new();
    let mut skipped = Vec::new();

    for scenario in scenarios {
        let Some((producers, consumers)) = parse_scenario_threads(scenario) else {
            runnable.push(scenario.clone());
            continue;
        };
        if producers + consumers <= available_parallelism {
            runnable.push(scenario.clone());
        } else {
            skipped.push(scenario.clone());
        }
    }

    (runnable, skipped)
}

fn print_usage_and_exit(code: i32) -> ! {
    eprintln!(
        "Usage: cargo run --bin complete_benches -- [options]\n\
         \n\
         Required:\n\
           --machine-label LABEL\n\
         \n\
         Options:\n\
           --runs-dir DIR            (default: bench_results/runs)\n\
           --scenarios CSV           (default: complete scenario list)\n\
           --mode MODE               (default: throughput)\n\
           --seed-label LABEL        (default: v4,8,127)\n\
           --allow-incomplete        (default: false)\n\
           --bench-script PATH       (default: ./scripts/bench_dual_host.sh)\n\
           --bench-arg ARG           (repeatable)\n\
           --dry-run\n\
           -h, --help"
    );
    std::process::exit(code);
}

fn parse_args() -> Result<CompleteArgs, String> {
    let mut machine_label: Option<String> = None;
    let mut runs_dir = PathBuf::from("bench_results/runs");
    let mut scenarios_override: Option<String> = None;
    let mut mode = "throughput".to_string();
    let mut seed_label = Some("v4,8,127".to_string());
    let mut allow_incomplete = false;
    let mut bench_script = PathBuf::from("./scripts/bench_dual_host.sh");
    let mut bench_args: Vec<String> = Vec::new();
    let mut dry_run = false;

    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        if arg == "--help" || arg == "-h" {
            print_usage_and_exit(0);
        }
        if arg == "--allow-incomplete" {
            allow_incomplete = true;
            continue;
        }
        if arg == "--dry-run" {
            dry_run = true;
            continue;
        }

        if arg == "--machine-label" {
            let value = args
                .next()
                .ok_or_else(|| "--machine-label requires a value".to_string())?;
            machine_label = Some(value.trim().to_string());
            continue;
        }
        if let Some(value) = arg.strip_prefix("--machine-label=") {
            machine_label = Some(value.trim().to_string());
            continue;
        }
        if arg == "--runs-dir" {
            let value = args
                .next()
                .ok_or_else(|| "--runs-dir requires a value".to_string())?;
            runs_dir = PathBuf::from(value);
            continue;
        }
        if let Some(value) = arg.strip_prefix("--runs-dir=") {
            runs_dir = PathBuf::from(value);
            continue;
        }
        if arg == "--scenarios" {
            let value = args
                .next()
                .ok_or_else(|| "--scenarios requires a value".to_string())?;
            scenarios_override = Some(value);
            continue;
        }
        if let Some(value) = arg.strip_prefix("--scenarios=") {
            scenarios_override = Some(value.to_string());
            continue;
        }
        if arg == "--mode" {
            let value = args
                .next()
                .ok_or_else(|| "--mode requires a value".to_string())?;
            mode = value.trim().to_string();
            continue;
        }
        if let Some(value) = arg.strip_prefix("--mode=") {
            mode = value.trim().to_string();
            continue;
        }
        if arg == "--seed-label" {
            let value = args
                .next()
                .ok_or_else(|| "--seed-label requires a value".to_string())?;
            let trimmed = value.trim().to_string();
            seed_label = if trimmed.is_empty() {
                None
            } else {
                Some(trimmed)
            };
            continue;
        }
        if let Some(value) = arg.strip_prefix("--seed-label=") {
            let trimmed = value.trim().to_string();
            seed_label = if trimmed.is_empty() {
                None
            } else {
                Some(trimmed)
            };
            continue;
        }
        if arg == "--max-rounds" || arg.starts_with("--max-rounds=") {
            return Err(
                "--max-rounds was removed; the search now stops on completion, missing seed, or no-progress within a finite UBQ label space".to_string(),
            );
        }
        if arg == "--bench-script" {
            let value = args
                .next()
                .ok_or_else(|| "--bench-script requires a value".to_string())?;
            bench_script = PathBuf::from(value);
            continue;
        }
        if let Some(value) = arg.strip_prefix("--bench-script=") {
            bench_script = PathBuf::from(value);
            continue;
        }
        if arg == "--bench-arg" {
            let value = args
                .next()
                .ok_or_else(|| "--bench-arg requires a value".to_string())?;
            bench_args.push(value);
            continue;
        }
        if let Some(value) = arg.strip_prefix("--bench-arg=") {
            bench_args.push(value.to_string());
            continue;
        }

        return Err(format!("unknown argument: {arg}"));
    }

    let machine_label = machine_label
        .map(|v| normalize_machine(&v))
        .filter(|v| !v.is_empty())
        .ok_or_else(|| "--machine-label is required".to_string())?;
    if !bench_script.exists() {
        return Err(format!(
            "bench script not found: {}",
            bench_script.display()
        ));
    }

    let scenarios = parse_scenarios(scenarios_override.as_deref());
    if scenarios.is_empty() {
        return Err("no scenarios configured".to_string());
    }

    let seed_label = match seed_label {
        Some(seed) => normalize_ubq_label(&seed, true)
            .ok_or_else(|| format!("invalid --seed-label '{}'", seed))?
            .into(),
        None => None,
    };

    Ok(CompleteArgs {
        machine_label,
        runs_dir,
        scenarios,
        mode,
        seed_label,
        allow_incomplete,
        bench_script,
        bench_args,
        dry_run,
    })
}

fn evaluate_scenario_round(
    machine_label: &str,
    scenario: &str,
    mode: &str,
    grouped: &BTreeMap<String, BTreeMap<String, BTreeMap<String, BTreeMap<String, Stats>>>>,
    seed_label: Option<&str>,
) -> ScenarioRoundPlan {
    let entries = grouped
        .get(machine_label)
        .and_then(|m| m.get(mode))
        .and_then(|m| m.get(scenario))
        .cloned()
        .unwrap_or_default();

    if has_complete_immediate_winner_variants(&entries) {
        return ScenarioRoundPlan {
            complete: true,
            plan_labels: Vec::new(),
            unresolved_no_seed: false,
            missing_key: None,
        };
    }

    let Some((winner, required)) = strict_immediate_winner_ubq_labels(&entries) else {
        if let Some(seed) = seed_label {
            return ScenarioRoundPlan {
                complete: false,
                plan_labels: vec![seed.to_string()],
                unresolved_no_seed: false,
                missing_key: None,
            };
        }
        return ScenarioRoundPlan {
            complete: false,
            plan_labels: Vec::new(),
            unresolved_no_seed: true,
            missing_key: None,
        };
    };

    let mut missing_labels: Vec<String> = required
        .into_iter()
        .filter_map(|label| label.strip_prefix("ubq_").map(|v| v.to_string()))
        .filter(|label| !entries.contains_key(&format!("ubq_{label}")))
        .collect();
    missing_labels.sort_by_key(|label| bench_label_sort_key(label));

    ScenarioRoundPlan {
        complete: false,
        plan_labels: missing_labels.clone(),
        unresolved_no_seed: false,
        missing_key: Some(format_missing_key(&winner, &missing_labels)),
    }
}

fn build_bench_cmd(args: &CompleteArgs, scenario: &str, labels: &[String]) -> Vec<String> {
    let out_root = args
        .runs_dir
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
        .display()
        .to_string();
    let mut cmd = vec![
        args.bench_script.display().to_string(),
        "--ubq-labels".to_string(),
        labels.join(";"),
        "--out-root".to_string(),
        out_root,
        "--scenarios".to_string(),
        scenario.to_string(),
        "--throughput-only".to_string(),
        "--n=1".to_string(),
        "--skip-plot".to_string(),
        "--skip-remote".to_string(),
        "--local-machine-label".to_string(),
        args.machine_label.clone(),
    ];
    cmd.extend(args.bench_args.clone());
    cmd
}

fn sanitize_name(raw: &str) -> String {
    let mut out = String::new();
    for ch in raw.chars() {
        if ch.is_ascii_alphanumeric() || matches!(ch, '.' | '_' | '-') {
            out.push(ch.to_ascii_lowercase());
        } else {
            out.push('_');
        }
    }
    let trimmed = out.trim_matches('_');
    if trimmed.is_empty() {
        "name".to_string()
    } else {
        trimmed.to_string()
    }
}

fn bench_log_path(machine_label: &str, scenario: &str, round_idx: usize) -> PathBuf {
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_secs();
    PathBuf::from("target")
        .join("complete_benches_logs")
        .join(sanitize_name(machine_label))
        .join(format!(
            "{}_round{}_{}.log",
            sanitize_name(scenario),
            round_idx,
            stamp
        ))
}

fn print_log_tail(path: &Path, max_lines: usize) {
    let file = match File::open(path) {
        Ok(file) => file,
        Err(_) => return,
    };
    let reader = BufReader::new(file);
    let mut lines: Vec<String> = reader.lines().map_while(Result::ok).collect();
    if lines.len() > max_lines {
        lines = lines.split_off(lines.len() - max_lines);
    }
    if lines.is_empty() {
        return;
    }
    println!("    log tail:");
    for line in lines {
        println!("      {line}");
    }
}

fn run_bench_cmd(
    cmd: &[String],
    dry_run: bool,
    machine_label: &str,
    ctx: &BenchRunContext<'_>,
) -> Result<(), String> {
    println!(
        "  bench driver: scenario={} round={} labels={}",
        ctx.scenario, ctx.round_idx, ctx.label_count
    );
    if dry_run {
        println!("    command: {}", format_cmd(cmd));
        return Ok(());
    }

    let log_path = bench_log_path(machine_label, ctx.scenario, ctx.round_idx);
    if let Some(parent) = log_path.parent() {
        fs::create_dir_all(parent)
            .map_err(|err| format!("failed to create log dir {}: {err}", parent.display()))?;
    }
    let stdout_log = File::create(&log_path)
        .map_err(|err| format!("failed to create log file {}: {err}", log_path.display()))?;
    let stderr_log = stdout_log
        .try_clone()
        .map_err(|err| format!("failed to clone log handle {}: {err}", log_path.display()))?;

    println!("    log: {}", log_path.display());
    let mut command = Command::new(&cmd[0]);
    command
        .args(&cmd[1..])
        .stdout(Stdio::from(stdout_log))
        .stderr(Stdio::from(stderr_log));
    let mut child = command
        .spawn()
        .map_err(|err| format!("failed to run bench command: {err}"))?;

    let started = Instant::now();
    let mut next_progress = Duration::from_secs(30);
    let status = loop {
        if let Some(status) = child
            .try_wait()
            .map_err(|err| format!("failed waiting on bench command: {err}"))?
        {
            break status;
        }

        let elapsed = started.elapsed();
        if elapsed >= next_progress {
            println!(
                "    still running: scenario={} round={} elapsed={}s",
                ctx.scenario,
                ctx.round_idx,
                elapsed.as_secs()
            );
            next_progress += Duration::from_secs(30);
        }
        thread::sleep(Duration::from_secs(1));
    };
    if !status.success() {
        println!(
            "    failed: scenario={} round={} exit={}",
            ctx.scenario,
            ctx.round_idx,
            status.code().unwrap_or(1)
        );
        print_log_tail(&log_path, 20);
        return Err(format!(
            "bench command failed with exit code {} (log: {})",
            status.code().unwrap_or(1),
            log_path.display()
        ));
    }
    println!(
        "    finished: scenario={} round={} elapsed={}s",
        ctx.scenario,
        ctx.round_idx,
        started.elapsed().as_secs()
    );
    Ok(())
}

fn run(args: &CompleteArgs) -> Result<(), String> {
    let available_parallelism = available_parallelism()
        .ok()
        .map(|value| value.get())
        .ok_or_else(|| "unable to determine available parallelism".to_string())?;
    let (runnable_scenarios, skipped_scenarios) =
        filter_scenarios_for_host(&args.scenarios, available_parallelism);

    println!("machine: {}", args.machine_label);
    println!("runs dir: {}", args.runs_dir.display());
    println!("mode: {}", args.mode);
    println!("available parallelism: {}", available_parallelism);
    println!("requested scenarios: {}", args.scenarios.join(","));
    if !skipped_scenarios.is_empty() {
        println!(
            "skipping scenarios above available parallelism: {}",
            skipped_scenarios.join(", ")
        );
    }
    println!("runnable scenarios: {}", runnable_scenarios.join(", "));
    println!(
        "search space: {} valid UBQ labels per scenario",
        total_valid_ubq_label_count()
    );

    if runnable_scenarios.is_empty() {
        println!("\nNo runnable scenarios for this host.");
        return Ok(());
    }

    let mut state: BTreeMap<String, ScenarioState> = runnable_scenarios
        .iter()
        .map(|scenario| {
            (
                scenario.clone(),
                ScenarioState {
                    completed: false,
                    rounds_run: 0,
                    previous_missing_key: None,
                    reason: None,
                },
            )
        })
        .collect();

    let mut active = runnable_scenarios.clone();
    let mut global_round = 0_usize;

    while !active.is_empty() {
        global_round += 1;
        println!("\n==== Round {global_round} ====");
        println!("active scenarios: {}", active.join(", "));
        let grouped = load_grouped_runs(&args.runs_dir)?;
        let mut planned: Vec<(String, Vec<String>)> = Vec::new();
        let active_now = active.clone();

        for scenario in active_now {
            println!("\n== Scenario {scenario} ==");
            let item = state
                .get_mut(&scenario)
                .ok_or_else(|| format!("internal missing state for {scenario}"))?;

            let plan = evaluate_scenario_round(
                &args.machine_label,
                &scenario,
                &args.mode,
                &grouped,
                args.seed_label.as_deref(),
            );
            if plan.complete {
                println!("  complete");
                item.completed = true;
                item.reason = Some("complete".to_string());
                active.retain(|v| v != &scenario);
                continue;
            }

            if has_no_progress(
                item.previous_missing_key.as_deref(),
                plan.missing_key.as_deref(),
            ) {
                println!("  no progress from previous round; stopping this scenario");
                item.reason = Some("no-progress".to_string());
                active.retain(|v| v != &scenario);
                continue;
            }
            item.previous_missing_key = plan.missing_key.clone();

            if plan.plan_labels.is_empty() {
                if plan.unresolved_no_seed {
                    println!("  no winner and no seed available");
                    item.reason = Some("missing-seed".to_string());
                } else {
                    println!("  no labels to bench");
                    item.reason = Some("no-labels".to_string());
                }
                active.retain(|v| v != &scenario);
                continue;
            }

            // Termination is guaranteed here: each runnable round benches at least one
            // previously unseen UBQ label, and the valid UBQ label domain is finite.
            item.rounds_run += 1;
            println!(
                "  round {}: benching {} labels",
                item.rounds_run,
                plan.plan_labels.len()
            );
            planned.push((scenario, plan.plan_labels));
        }

        if planned.is_empty() {
            if !active.is_empty() {
                println!("\nNo runnable scenarios remain.");
                for scenario in active.drain(..) {
                    if let Some(item) = state.get_mut(&scenario) {
                        if item.reason.is_none() {
                            item.reason = Some("no-runnable-work".to_string());
                        }
                    }
                }
            }
            break;
        }

        let scheduled_summary = planned
            .iter()
            .map(|(scenario, labels)| format!("{scenario}({})", labels.len()))
            .collect::<Vec<_>>()
            .join(", ");
        println!("scheduled bench work: {scheduled_summary}");

        for (scenario, labels) in planned {
            let cmd = build_bench_cmd(args, &scenario, &labels);
            let round_idx = state
                .get(&scenario)
                .map(|item| item.rounds_run)
                .ok_or_else(|| format!("internal missing state for {scenario}"))?;
            let context = BenchRunContext {
                scenario: &scenario,
                round_idx,
                label_count: labels.len(),
            };
            run_bench_cmd(&cmd, args.dry_run, &args.machine_label, &context)?;
        }
    }

    let incomplete: Vec<String> = runnable_scenarios
        .iter()
        .filter(|scenario| !state.get(*scenario).map(|v| v.completed).unwrap_or(false))
        .cloned()
        .collect();

    if !incomplete.is_empty() {
        println!("\nIncomplete scenarios: {}", incomplete.join(", "));
        for scenario in &incomplete {
            if let Some(item) = state.get(scenario) {
                println!(
                    "  {scenario}: {}",
                    item.reason.clone().unwrap_or_else(|| "unknown".to_string())
                );
            }
        }
        if !args.allow_incomplete {
            return Err("incomplete scenarios remained".to_string());
        }
        println!("Allowing incomplete scenarios (--allow-incomplete).");
    } else {
        println!("\nAll scenarios complete.");
    }

    Ok(())
}

fn main() {
    let args = match parse_args() {
        Ok(v) => v,
        Err(err) => {
            eprintln!("{err}");
            std::process::exit(2);
        }
    };
    if let Err(err) = run(&args) {
        eprintln!("{err}");
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn grouped_with_entries(
        machine: &str,
        scenario: &str,
        entries: BTreeMap<String, Stats>,
    ) -> BTreeMap<String, BTreeMap<String, BTreeMap<String, BTreeMap<String, Stats>>>> {
        let mut grouped = BTreeMap::new();
        grouped
            .entry(machine.to_string())
            .or_insert_with(BTreeMap::new)
            .entry("throughput".to_string())
            .or_insert_with(BTreeMap::new)
            .insert(scenario.to_string(), entries);
        grouped
    }

    #[test]
    fn scenario_round_complete() {
        let mut entries = BTreeMap::new();
        entries.insert(
            "ubq_v4,1,1023".to_string(),
            Stats {
                mean_ops_per_sec: 120.0,
            },
        );
        entries.insert(
            "ubq_v3,1,1023".to_string(),
            Stats {
                mean_ops_per_sec: 110.0,
            },
        );
        entries.insert(
            "ubq_v5,1,1023".to_string(),
            Stats {
                mean_ops_per_sec: 109.0,
            },
        );
        entries.insert(
            "ubq_v7,1,1023".to_string(),
            Stats {
                mean_ops_per_sec: 108.0,
            },
        );
        entries.insert(
            "ubq_v6,0,1023".to_string(),
            Stats {
                mean_ops_per_sec: 107.0,
            },
        );
        entries.insert(
            "ubq_v4,2,1023".to_string(),
            Stats {
                mean_ops_per_sec: 106.0,
            },
        );
        entries.insert(
            "ubq_v4,1,511".to_string(),
            Stats {
                mean_ops_per_sec: 105.0,
            },
        );
        entries.insert(
            "ubq_v4,1,2047".to_string(),
            Stats {
                mean_ops_per_sec: 104.0,
            },
        );
        entries.insert(
            "ubq_v4,1,1023,b".to_string(),
            Stats {
                mean_ops_per_sec: 103.0,
            },
        );
        let grouped = grouped_with_entries("local", "1p1c", entries);
        let plan =
            evaluate_scenario_round("local", "1p1c", "throughput", &grouped, Some("v4,8,127"));
        assert!(plan.complete);
        assert!(plan.plan_labels.is_empty());
    }

    #[test]
    fn scenario_round_missing_seed() {
        let grouped: BTreeMap<String, BTreeMap<String, BTreeMap<String, BTreeMap<String, Stats>>>> =
            BTreeMap::new();
        let plan = evaluate_scenario_round("local", "1p1c", "throughput", &grouped, None);
        assert!(!plan.complete);
        assert!(plan.plan_labels.is_empty());
        assert!(plan.unresolved_no_seed);
    }

    #[test]
    fn scenario_round_uses_seed_when_no_winner() {
        let grouped: BTreeMap<String, BTreeMap<String, BTreeMap<String, BTreeMap<String, Stats>>>> =
            BTreeMap::new();
        let plan =
            evaluate_scenario_round("local", "1p1c", "throughput", &grouped, Some("v4,8,127"));
        assert!(!plan.complete);
        assert_eq!(plan.plan_labels, vec!["v4,8,127".to_string()]);
    }

    #[test]
    fn build_cmd_contains_local_only_flags() {
        let args = CompleteArgs {
            machine_label: "lab".to_string(),
            runs_dir: PathBuf::from("bench_results/runs"),
            scenarios: vec!["1p1c".to_string()],
            mode: "throughput".to_string(),
            seed_label: Some("v4,8,127".to_string()),
            allow_incomplete: true,
            bench_script: PathBuf::from("./scripts/bench_dual_host.sh"),
            bench_args: vec!["--items-per-producer=1000".to_string()],
            dry_run: true,
        };
        let cmd = build_bench_cmd(&args, "1p1c", &["v4,8,127".to_string()]);
        assert!(cmd.contains(&"--skip-remote".to_string()));
        assert!(cmd.contains(&"--local-machine-label".to_string()));
        assert!(cmd.contains(&"lab".to_string()));
        assert!(cmd.contains(&"--out-root".to_string()));
        assert!(cmd.contains(&"bench_results".to_string()));
    }

    #[test]
    fn detects_no_progress_from_missing_key() {
        assert!(has_no_progress(
            Some("ubq_v4,8,127|v4,4,127"),
            Some("ubq_v4,8,127|v4,4,127")
        ));
        assert!(!has_no_progress(
            Some("ubq_v4,8,127|v4,4,127"),
            Some("ubq_v4,8,127|v4,16,127")
        ));
        assert!(!has_no_progress(None, Some("x")));
        assert!(!has_no_progress(Some("x"), None));
    }

    #[test]
    fn filters_scenarios_above_available_parallelism() {
        let scenarios = vec![
            "1p1c".to_string(),
            "8p8c".to_string(),
            "16p16c".to_string(),
            "64p64c".to_string(),
        ];
        let (runnable, skipped) = filter_scenarios_for_host(&scenarios, 16);
        assert_eq!(runnable, vec!["1p1c".to_string(), "8p8c".to_string()]);
        assert_eq!(skipped, vec!["16p16c".to_string(), "64p64c".to_string()]);
    }
}
