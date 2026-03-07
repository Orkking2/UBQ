mod bench_tooling;

use bench_tooling::{
    collect_machine_labels, find_missing_machine_labels, format_cmd, join_remote_path,
    normalize_machine, normalize_machine_list, remote_cd_expr, validate_forwarded_args,
};
use serde::Deserialize;
use std::collections::BTreeMap;
use std::fs;
use std::io::{BufRead, BufReader, Read};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::thread;

const REPO_SYNC_INCLUDE_PATTERNS: &[&str] = &[
    "/Cargo.toml",
    "/Cargo.lock",
    "/README.md",
    "/LICENSE",
    "/src/",
    "/src/**",
    "/benches/",
    "/benches/**",
    "/tests/",
    "/tests/**",
    "/scripts/",
    "/scripts/**",
];

const FORBIDDEN_COMPLETE_ARGS: &[&str] = &["--machine-label", "--runs-dir", "--dry-run"];
const FORBIDDEN_BENCH_ARGS: &[&str] = &[
    "--ubq-label",
    "--ubq-labels",
    "--remote-host",
    "--local-machine-label",
    "--remote-dir",
    "--out-root",
    "--skip-remote",
    "--skip-local",
    "--skip-plot",
];
const REMOVED_COMPLETE_ARGS: &[&str] = &["--max-rounds"];

#[derive(Clone, Debug, Deserialize)]
struct FleetConfig {
    defaults: FleetDefaults,
    machines: BTreeMap<String, MachineConfig>,
}

#[derive(Clone, Debug, Deserialize, Default)]
struct FleetDefaults {
    runs_dir: Option<String>,
    plot_out_dir: Option<String>,
    remote_repo_dir: Option<String>,
    remote_runs_dir: Option<String>,
    scenarios: Option<Vec<String>>,
    seed_label: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Default)]
struct MachineConfig {
    local: Option<bool>,
    host: Option<String>,
    machine_label: Option<String>,
    remote_repo_dir: Option<String>,
    remote_runs_dir: Option<String>,
}

#[derive(Clone, Debug)]
struct FleetArgs {
    machines: Vec<String>,
    config_path: PathBuf,
    no_sync_repo: bool,
    strict_complete: bool,
    skip_local_plot: bool,
    plot_partial: bool,
    dry_run: bool,
    mode: FleetMode,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum FleetMode {
    Search {
        complete_args: Vec<String>,
    },
    FixedLabels {
        ubq_labels: Vec<String>,
        bench_args: Vec<String>,
    },
}

#[derive(Clone, Debug)]
struct ResolvedMachine {
    name: String,
    is_local: bool,
    host: String,
    machine_label: String,
    remote_repo_dir: String,
    remote_runs_dir: String,
}

#[derive(Clone, Debug)]
struct FleetRuntime {
    repo_root: PathBuf,
    runs_dir: PathBuf,
    plot_out_dir: PathBuf,
    scenarios: Vec<String>,
    seed_label: Option<String>,
    sync_repo: bool,
    strict_complete: bool,
    dry_run: bool,
    mode: FleetMode,
}

#[derive(Clone, Debug)]
struct MachineRunResult {
    machine_name: String,
    machine_label: String,
    ok: bool,
    error: Option<String>,
}

fn print_usage_and_exit(code: i32) -> ! {
    eprintln!(
        "Usage: cargo run --bin full_bench_fleet -- [options]\n\
         \n\
         Required:\n\
           --machines CSV\n\
         \n\
         Options:\n\
           --config PATH             (default: bench_fleet.toml)\n\
           --no-sync-repo\n\
           --strict-complete\n\
           --skip-local-plot\n\
           --plot-partial\n\
           --dry-run\n\
         \n\
         Search Mode:\n\
           --complete-arg ARG        (repeatable)\n\
         \n\
         Fixed-Label Mode:\n\
           --ubq-label LABEL         (repeatable)\n\
           --ubq-labels LIST         (semicolon-separated labels)\n\
           --bench-arg ARG           (repeatable; forwarded to bench_dual_host.sh)\n\
           -h, --help"
    );
    std::process::exit(code);
}

fn parse_args() -> Result<FleetArgs, String> {
    parse_args_from(std::env::args().skip(1))
}

fn parse_args_from<I, S>(raw_args: I) -> Result<FleetArgs, String>
where
    I: IntoIterator<Item = S>,
    S: Into<String>,
{
    let mut machines_raw: Option<String> = None;
    let mut config_path = PathBuf::from("bench_fleet.toml");
    let mut no_sync_repo = false;
    let mut strict_complete = false;
    let mut skip_local_plot = false;
    let mut plot_partial = false;
    let mut dry_run = false;
    let mut complete_args: Vec<String> = Vec::new();
    let mut bench_args: Vec<String> = Vec::new();
    let mut ubq_labels: Vec<String> = Vec::new();

    let mut args = raw_args.into_iter().map(Into::into);
    while let Some(arg) = args.next() {
        if arg == "-h" || arg == "--help" {
            print_usage_and_exit(0);
        }
        if arg == "--no-sync-repo" {
            no_sync_repo = true;
            continue;
        }
        if arg == "--strict-complete" {
            strict_complete = true;
            continue;
        }
        if arg == "--skip-local-plot" {
            skip_local_plot = true;
            continue;
        }
        if arg == "--plot-partial" {
            plot_partial = true;
            continue;
        }
        if arg == "--dry-run" {
            dry_run = true;
            continue;
        }
        if arg == "--machines" {
            let value = args
                .next()
                .ok_or_else(|| "--machines requires a value".to_string())?;
            machines_raw = Some(value);
            continue;
        }
        if let Some(value) = arg.strip_prefix("--machines=") {
            machines_raw = Some(value.to_string());
            continue;
        }
        if arg == "--config" {
            let value = args
                .next()
                .ok_or_else(|| "--config requires a value".to_string())?;
            config_path = PathBuf::from(value);
            continue;
        }
        if let Some(value) = arg.strip_prefix("--config=") {
            config_path = PathBuf::from(value);
            continue;
        }
        if arg == "--complete-arg" {
            let value = args
                .next()
                .ok_or_else(|| "--complete-arg requires a value".to_string())?;
            complete_args.push(value);
            continue;
        }
        if let Some(value) = arg.strip_prefix("--complete-arg=") {
            complete_args.push(value.to_string());
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
        if arg == "--ubq-label" {
            let value = args
                .next()
                .ok_or_else(|| "--ubq-label requires a value".to_string())?;
            ubq_labels.push(value);
            continue;
        }
        if let Some(value) = arg.strip_prefix("--ubq-label=") {
            ubq_labels.push(value.to_string());
            continue;
        }
        if arg == "--ubq-labels" {
            let value = args
                .next()
                .ok_or_else(|| "--ubq-labels requires a value".to_string())?;
            ubq_labels.extend(split_label_list(&value));
            continue;
        }
        if let Some(value) = arg.strip_prefix("--ubq-labels=") {
            ubq_labels.extend(split_label_list(value));
            continue;
        }
        return Err(format!("unknown argument: {arg}"));
    }

    let machines = normalize_machine_list(
        machines_raw
            .as_deref()
            .ok_or_else(|| "--machines is required".to_string())?,
    );
    if machines.is_empty() {
        return Err("--machines produced no valid machine names".to_string());
    }

    let ubq_labels = normalize_labels(ubq_labels);
    let mode = if ubq_labels.is_empty() {
        if !bench_args.is_empty() {
            return Err(
                "--bench-arg requires fixed-label mode; add --ubq-label or --ubq-labels"
                    .to_string(),
            );
        }
        validate_forwarded_args(&complete_args, FORBIDDEN_COMPLETE_ARGS)?;
        validate_removed_complete_args(&complete_args)?;
        FleetMode::Search { complete_args }
    } else {
        if !complete_args.is_empty() {
            return Err(
                "cannot combine fixed-label mode (--ubq-label/--ubq-labels) with --complete-arg"
                    .to_string(),
            );
        }
        validate_forwarded_args(&bench_args, FORBIDDEN_BENCH_ARGS)?;
        FleetMode::FixedLabels {
            ubq_labels,
            bench_args,
        }
    };

    Ok(FleetArgs {
        machines,
        config_path,
        no_sync_repo,
        strict_complete,
        skip_local_plot,
        plot_partial,
        dry_run,
        mode,
    })
}

fn split_label_list(raw: &str) -> Vec<String> {
    raw.split(';').map(|value| value.to_string()).collect()
}

fn normalize_labels(labels: Vec<String>) -> Vec<String> {
    labels
        .into_iter()
        .map(|label| label.trim().to_string())
        .filter(|label| !label.is_empty())
        .collect()
}

fn validate_removed_complete_args(args: &[String]) -> Result<(), String> {
    for arg in args {
        for key in REMOVED_COMPLETE_ARGS {
            if arg == key || arg.starts_with(&format!("{key}=")) {
                return Err(format!(
                    "{key} was removed; the search now stops on completion, missing seed, or no-progress within a finite UBQ label space"
                ));
            }
        }
    }
    Ok(())
}

fn forwarded_args_contain_key(args: &[String], key: &str) -> bool {
    args.iter()
        .any(|arg| arg == key || arg.starts_with(&format!("{key}=")))
}

fn derive_out_root_from_runs_dir(runs_dir: &Path) -> Result<String, String> {
    let path_text = runs_dir.display().to_string();
    let file_name = runs_dir.file_name().and_then(|value| value.to_str());
    if file_name != Some("runs") {
        return Err(format!(
            "fixed-label mode requires runs dir ending with 'runs' so it can derive --out-root (got: {path_text})"
        ));
    }
    let parent = runs_dir.parent().unwrap_or_else(|| Path::new(""));
    if parent.as_os_str().is_empty() {
        Ok(".".to_string())
    } else {
        Ok(parent.display().to_string())
    }
}

fn load_config(path: &Path) -> Result<FleetConfig, String> {
    let raw = fs::read_to_string(path)
        .map_err(|err| format!("failed to read config {}: {err}", path.display()))?;
    toml::from_str::<FleetConfig>(&raw)
        .map_err(|err| format!("invalid config {}: {err}", path.display()))
}

fn resolve_machine(
    name: &str,
    defaults: &FleetDefaults,
    machine_cfg: &MachineConfig,
) -> Result<ResolvedMachine, String> {
    let is_local = machine_cfg.local.unwrap_or(false);
    let host = machine_cfg
        .host
        .clone()
        .unwrap_or_else(|| name.to_string())
        .trim()
        .to_string();
    let machine_label = machine_cfg
        .machine_label
        .clone()
        .unwrap_or_else(|| name.to_string())
        .trim()
        .to_string();
    if machine_label.is_empty() {
        return Err(format!("machine {name}: machine_label cannot be empty"));
    }
    let remote_repo_dir = machine_cfg
        .remote_repo_dir
        .clone()
        .or_else(|| defaults.remote_repo_dir.clone())
        .unwrap_or_else(|| "~/UBQ".to_string());
    let remote_runs_dir = machine_cfg
        .remote_runs_dir
        .clone()
        .or_else(|| defaults.remote_runs_dir.clone())
        .unwrap_or_else(|| "bench_results/runs".to_string());

    Ok(ResolvedMachine {
        name: name.to_string(),
        is_local,
        host,
        machine_label: normalize_machine(&machine_label),
        remote_repo_dir,
        remote_runs_dir,
    })
}

fn run_cmd(args: &[String], cwd: &Path, dry_run: bool) -> Result<i32, String> {
    if dry_run {
        println!("    command: {}", format_cmd(args));
    }
    if dry_run {
        return Ok(0);
    }
    let mut command = Command::new(&args[0]);
    command.args(&args[1..]).current_dir(cwd);
    let status = command
        .status()
        .map_err(|err| format!("failed to run command '{}': {err}", args[0]))?;
    Ok(status.code().unwrap_or(1))
}

fn command_exists(cmd: &str) -> bool {
    Command::new(cmd)
        .arg("--version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|status| status.success())
        .unwrap_or(false)
}

fn resolve_python_bin_with_override(py_override: Option<&str>) -> Result<String, String> {
    if let Some(value) = py_override {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            return Err("PYTHON is set but empty".to_string());
        }
        if command_exists(trimmed) {
            return Ok(trimmed.to_string());
        }
        return Err(format!("configured python command not found: {trimmed}"));
    }

    for candidate in ["python3", "python"] {
        if command_exists(candidate) {
            return Ok(candidate.to_string());
        }
    }
    Err("missing required command: python3 or python".to_string())
}

fn resolve_python_bin() -> Result<String, String> {
    resolve_python_bin_with_override(std::env::var("PYTHON").ok().as_deref())
}

fn spawn_prefixed_reader<R>(reader: R, prefix: String, to_stderr: bool) -> thread::JoinHandle<()>
where
    R: Read + Send + 'static,
{
    thread::spawn(move || {
        let reader = BufReader::new(reader);
        for line in reader.lines().map_while(Result::ok) {
            if line.trim().is_empty() {
                continue;
            }
            if to_stderr {
                eprintln!("[{prefix}] {line}");
            } else {
                println!("[{prefix}] {line}");
            }
        }
    })
}

fn run_streaming_cmd(
    args: &[String],
    cwd: &Path,
    dry_run: bool,
    prefix: &str,
) -> Result<i32, String> {
    if dry_run {
        println!("    command: {}", format_cmd(args));
        return Ok(0);
    }

    let mut command = Command::new(&args[0]);
    command
        .args(&args[1..])
        .current_dir(cwd)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = command
        .spawn()
        .map_err(|err| format!("failed to run command '{}': {err}", args[0]))?;

    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| format!("failed to capture stdout for '{}'", args[0]))?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| format!("failed to capture stderr for '{}'", args[0]))?;

    let stdout_thread = spawn_prefixed_reader(stdout, prefix.to_string(), false);
    let stderr_thread = spawn_prefixed_reader(stderr, prefix.to_string(), true);

    let status = child
        .wait()
        .map_err(|err| format!("failed to wait for command '{}': {err}", args[0]))?;
    let _ = stdout_thread.join();
    let _ = stderr_thread.join();
    Ok(status.code().unwrap_or(1))
}

fn make_complete_base_args(
    runtime: &FleetRuntime,
    machine_label: &str,
    runs_dir: &str,
) -> Vec<String> {
    let mut complete_args = vec![
        "--machine-label".to_string(),
        machine_label.to_string(),
        "--runs-dir".to_string(),
        runs_dir.to_string(),
    ];
    if !runtime.scenarios.is_empty() {
        complete_args.push("--scenarios".to_string());
        complete_args.push(runtime.scenarios.join(","));
    }
    if let Some(seed_label) = &runtime.seed_label {
        complete_args.push("--seed-label".to_string());
        complete_args.push(seed_label.clone());
    }
    if !runtime.strict_complete {
        complete_args.push("--allow-incomplete".to_string());
    }
    let forwarded = match &runtime.mode {
        FleetMode::Search { complete_args } => complete_args,
        FleetMode::FixedLabels { .. } => {
            panic!("complete args requested while running fixed-label mode")
        }
    };
    complete_args.extend(forwarded.clone());
    if runtime.dry_run {
        complete_args.push("--dry-run".to_string());
    }
    complete_args
}

fn build_local_complete_cmd(runtime: &FleetRuntime, machine: &ResolvedMachine) -> Vec<String> {
    let runs_dir = runtime.runs_dir.display().to_string();
    let mut cmd = vec![
        "cargo".to_string(),
        "run".to_string(),
        "--quiet".to_string(),
        "--release".to_string(),
        "--bin".to_string(),
        "complete_benches".to_string(),
        "--".to_string(),
    ];
    cmd.extend(make_complete_base_args(
        runtime,
        &machine.machine_label,
        &runs_dir,
    ));
    cmd
}

fn build_remote_complete_cmd(runtime: &FleetRuntime, machine: &ResolvedMachine) -> Vec<String> {
    let mut inner = vec![
        "cargo".to_string(),
        "run".to_string(),
        "--quiet".to_string(),
        "--release".to_string(),
        "--bin".to_string(),
        "complete_benches".to_string(),
        "--".to_string(),
    ];
    inner.extend(make_complete_base_args(
        runtime,
        &machine.machine_label,
        &machine.remote_runs_dir,
    ));
    let inner_quoted = inner
        .iter()
        .map(|s| bench_tooling::shell_quote(s))
        .collect::<Vec<_>>()
        .join(" ");

    let payload = format!(
        "if [ -f \"$HOME/.cargo/env\" ]; then . \"$HOME/.cargo/env\"; fi; \
         export PATH=\"$HOME/.cargo/bin:$PATH\"; \
         cd {} && {}",
        remote_cd_expr(&machine.remote_repo_dir),
        inner_quoted
    );
    vec!["ssh".to_string(), machine.host.clone(), payload]
}

fn make_fixed_label_base_args(
    runtime: &FleetRuntime,
    machine_label: &str,
    out_root: &str,
) -> Vec<String> {
    let (ubq_labels, bench_args) = match &runtime.mode {
        FleetMode::FixedLabels {
            ubq_labels,
            bench_args,
        } => (ubq_labels, bench_args),
        FleetMode::Search { .. } => {
            panic!("fixed-label args requested while running search mode")
        }
    };

    let mut args = Vec::new();
    for label in ubq_labels {
        args.push("--ubq-label".to_string());
        args.push(label.clone());
    }
    args.push("--skip-remote".to_string());
    args.push("--skip-plot".to_string());
    args.push("--local-machine-label".to_string());
    args.push(machine_label.to_string());
    args.push("--out-root".to_string());
    args.push(out_root.to_string());
    if !runtime.scenarios.is_empty() && !forwarded_args_contain_key(bench_args, "--scenarios") {
        args.push("--scenarios".to_string());
        args.push(runtime.scenarios.join(","));
    }
    args.extend(bench_args.clone());
    args
}

fn build_local_fixed_label_cmd(
    runtime: &FleetRuntime,
    machine: &ResolvedMachine,
) -> Result<Vec<String>, String> {
    let out_root = derive_out_root_from_runs_dir(&runtime.runs_dir)?;
    let mut cmd = vec!["bash".to_string(), "scripts/bench_dual_host.sh".to_string()];
    cmd.extend(make_fixed_label_base_args(
        runtime,
        &machine.machine_label,
        &out_root,
    ));
    Ok(cmd)
}

fn build_remote_fixed_label_cmd(
    runtime: &FleetRuntime,
    machine: &ResolvedMachine,
) -> Result<Vec<String>, String> {
    let out_root = derive_out_root_from_runs_dir(Path::new(&machine.remote_runs_dir))?;
    let mut inner = vec!["bash".to_string(), "scripts/bench_dual_host.sh".to_string()];
    inner.extend(make_fixed_label_base_args(
        runtime,
        &machine.machine_label,
        &out_root,
    ));
    let inner_quoted = inner
        .iter()
        .map(|s| bench_tooling::shell_quote(s))
        .collect::<Vec<_>>()
        .join(" ");

    let payload = format!(
        "if [ -f \"$HOME/.cargo/env\" ]; then . \"$HOME/.cargo/env\"; fi; \
         export PATH=\"$HOME/.cargo/bin:$PATH\"; \
         cd {} && {}",
        remote_cd_expr(&machine.remote_repo_dir),
        inner_quoted
    );
    Ok(vec!["ssh".to_string(), machine.host.clone(), payload])
}

fn build_sync_cmd(machine: &ResolvedMachine) -> Vec<String> {
    let mut cmd = vec![
        "rsync".to_string(),
        "-avz".to_string(),
        "--delete".to_string(),
        "--prune-empty-dirs".to_string(),
    ];
    for pattern in REPO_SYNC_INCLUDE_PATTERNS {
        cmd.push(format!("--include={pattern}"));
    }
    cmd.push("--exclude=*".to_string());
    cmd.push("./".to_string());
    cmd.push(format!(
        "{}:{}/",
        machine.host,
        machine.remote_repo_dir.trim_end_matches('/')
    ));
    cmd
}

fn build_remote_dir_exists_cmd(machine: &ResolvedMachine, remote_runs_root: &str) -> Vec<String> {
    vec![
        "ssh".to_string(),
        machine.host.clone(),
        format!("test -d {}", remote_cd_expr(remote_runs_root)),
    ]
}

fn build_pull_runs_cmd(
    machine: &ResolvedMachine,
    remote_runs_root: &str,
    local_runs_dir: &Path,
) -> Vec<String> {
    vec![
        "rsync".to_string(),
        "-avz".to_string(),
        format!(
            "{}:{}/",
            machine.host,
            remote_runs_root.trim_end_matches('/')
        ),
        format!("{}/", local_runs_dir.display()),
    ]
}

fn run_machine(runtime: Arc<FleetRuntime>, machine: ResolvedMachine) -> MachineRunResult {
    println!("\n=== Machine: {} ===", machine.name);
    let result = if machine.is_local {
        match &runtime.mode {
            FleetMode::Search { .. } => {
                println!("  starting local search");
                let cmd = build_local_complete_cmd(&runtime, &machine);
                run_streaming_cmd(&cmd, &runtime.repo_root, runtime.dry_run, &machine.name)
                    .and_then(|code| {
                        if code == 0 {
                            Ok(())
                        } else {
                            Err(format!(
                                "local complete_benches failed with exit code {code}"
                            ))
                        }
                    })
            }
            FleetMode::FixedLabels { ubq_labels, .. } => {
                println!(
                    "  starting local fixed-label bench ({} labels)",
                    ubq_labels.len()
                );
                build_local_fixed_label_cmd(&runtime, &machine).and_then(|cmd| {
                    run_streaming_cmd(&cmd, &runtime.repo_root, runtime.dry_run, &machine.name)
                        .and_then(|code| {
                            if code == 0 {
                                Ok(())
                            } else {
                                Err(format!(
                                    "local fixed-label bench failed with exit code {code}"
                                ))
                            }
                        })
                })
            }
        }
    } else {
        let outcome = if runtime.sync_repo {
            println!(
                "  syncing repo to {}:{}",
                machine.host, machine.remote_repo_dir
            );
            let cmd = build_sync_cmd(&machine);
            run_cmd(&cmd, &runtime.repo_root, runtime.dry_run).and_then(|code| {
                if code == 0 {
                    Ok(())
                } else {
                    Err(format!("repo sync failed with exit code {code}"))
                }
            })
        } else {
            Ok(())
        };

        let outcome = outcome.and_then(|_| match &runtime.mode {
            FleetMode::Search { .. } => {
                println!("  starting remote search on {}", machine.host);
                let cmd = build_remote_complete_cmd(&runtime, &machine);
                run_streaming_cmd(&cmd, &runtime.repo_root, runtime.dry_run, &machine.name)
                    .and_then(|code| {
                        if code == 0 {
                            Ok(())
                        } else {
                            Err(format!(
                                "remote complete_benches failed with exit code {code}"
                            ))
                        }
                    })
            }
            FleetMode::FixedLabels { ubq_labels, .. } => {
                println!(
                    "  starting remote fixed-label bench on {} ({} labels)",
                    machine.host,
                    ubq_labels.len()
                );
                build_remote_fixed_label_cmd(&runtime, &machine).and_then(|cmd| {
                    run_streaming_cmd(&cmd, &runtime.repo_root, runtime.dry_run, &machine.name)
                        .and_then(|code| {
                            if code == 0 {
                                Ok(())
                            } else {
                                Err(format!(
                                    "remote fixed-label bench failed with exit code {code}"
                                ))
                            }
                        })
                })
            }
        });

        outcome.and_then(|_| {
            let remote_runs_root =
                join_remote_path(&machine.remote_repo_dir, &machine.remote_runs_dir);
            println!("  pulling runs from {}:{}", machine.host, remote_runs_root);
            let exists_cmd = build_remote_dir_exists_cmd(&machine, &remote_runs_root);
            let exists_code = run_cmd(&exists_cmd, &runtime.repo_root, runtime.dry_run)?;
            if exists_code == 1 {
                println!(
                    "WARNING: remote runs dir missing for {}: {}",
                    machine.name, remote_runs_root
                );
                return Ok(());
            }
            if exists_code != 0 {
                return Err(format!(
                    "failed to probe remote runs dir (exit {exists_code}) for {}",
                    machine.name
                ));
            }

            let pull_cmd = build_pull_runs_cmd(&machine, &remote_runs_root, &runtime.runs_dir);
            run_cmd(&pull_cmd, &runtime.repo_root, runtime.dry_run).and_then(|code| {
                if code == 0 {
                    Ok(())
                } else {
                    Err(format!("failed to pull remote runs with exit code {code}"))
                }
            })
        })
    };

    let result = match result {
        Ok(()) => MachineRunResult {
            machine_name: machine.name,
            machine_label: machine.machine_label,
            ok: true,
            error: None,
        },
        Err(err) => MachineRunResult {
            machine_name: machine.name,
            machine_label: machine.machine_label,
            ok: false,
            error: Some(err),
        },
    };

    if result.ok {
        println!("  machine complete: {}", result.machine_name);
    }

    result
}

fn render_plots(runtime: &FleetRuntime, no_clean: bool) -> Result<(), String> {
    let python_bin = resolve_python_bin()?;
    let mut cmd = vec![
        python_bin,
        "scripts/plot_runs_folder.py".to_string(),
        "--runs-dir".to_string(),
        runtime.runs_dir.display().to_string(),
        "--out-dir".to_string(),
        runtime.plot_out_dir.display().to_string(),
    ];
    if no_clean {
        cmd.push("--no-clean".to_string());
    }
    if runtime.dry_run {
        cmd.push("--dry-run".to_string());
    }
    let code = run_cmd(&cmd, &runtime.repo_root, runtime.dry_run)?;
    if code != 0 {
        return Err(format!("plot command failed with exit code {code}"));
    }
    Ok(())
}

fn run(args: FleetArgs) -> Result<i32, String> {
    let config = load_config(&args.config_path)?;
    let defaults = config.defaults.clone();

    let runs_dir = PathBuf::from(
        defaults
            .runs_dir
            .clone()
            .unwrap_or_else(|| "bench_results/runs".to_string()),
    );
    let plot_out_dir = PathBuf::from(
        defaults
            .plot_out_dir
            .clone()
            .unwrap_or_else(|| "bench_results/plots".to_string()),
    );
    let scenarios = defaults.scenarios.clone().unwrap_or_default();
    let seed_label = defaults.seed_label.clone().map(|v| v.trim().to_string());

    let repo_root =
        std::env::current_dir().map_err(|err| format!("failed to read current dir: {err}"))?;

    let runtime = Arc::new(FleetRuntime {
        repo_root,
        runs_dir,
        plot_out_dir,
        scenarios,
        seed_label,
        sync_repo: !args.no_sync_repo,
        strict_complete: args.strict_complete,
        dry_run: args.dry_run,
        mode: args.mode.clone(),
    });

    let mut resolved = Vec::new();
    for machine_name in &args.machines {
        let machine_cfg = config
            .machines
            .get(machine_name)
            .ok_or_else(|| format!("machine '{}' not found in config", machine_name))?;
        resolved.push(resolve_machine(machine_name, &defaults, machine_cfg)?);
    }

    println!(
        "Machines: {}",
        resolved
            .iter()
            .map(|m| m.name.as_str())
            .collect::<Vec<_>>()
            .join(", ")
    );
    println!("Local runs dir: {}", runtime.runs_dir.display());
    println!("Plot out dir: {}", runtime.plot_out_dir.display());
    match &runtime.mode {
        FleetMode::Search { complete_args } => {
            println!("Mode: complete search");
            if !complete_args.is_empty() {
                println!("Forwarded complete args: {}", complete_args.join(" "));
            }
        }
        FleetMode::FixedLabels {
            ubq_labels,
            bench_args,
        } => {
            println!("Mode: fixed-label bench");
            println!("UBQ labels: {}", ubq_labels.join("; "));
            if !bench_args.is_empty() {
                println!("Forwarded bench args: {}", bench_args.join(" "));
            }
        }
    }

    let mut joins = Vec::new();
    for machine in resolved.clone() {
        let runtime = Arc::clone(&runtime);
        joins.push(thread::spawn(move || run_machine(runtime, machine)));
    }

    let mut failures: Vec<MachineRunResult> = Vec::new();
    let mut all_results: Vec<MachineRunResult> = Vec::new();
    for handle in joins {
        let result = handle
            .join()
            .map_err(|_| "machine thread panicked".to_string())?;
        if !result.ok {
            failures.push(result.clone());
        }
        all_results.push(result);
    }

    if !failures.is_empty() {
        println!("\nFailed machines:");
        for failure in &failures {
            println!(
                "  {}: {}",
                failure.machine_name,
                failure.error.as_deref().unwrap_or("unknown error")
            );
        }
    }

    if args.skip_local_plot {
        println!("\nSkipping local plot generation (--skip-local-plot).");
        return Ok(if failures.is_empty() { 0 } else { 1 });
    }

    let seen_labels = collect_machine_labels(&runtime.runs_dir)?;
    if seen_labels.is_empty() {
        println!(
            "\nNo benchmark JSON files found in aggregated runs directory: {}",
            runtime.runs_dir.display()
        );
        println!("Nothing to plot.");
        return Ok(1);
    }
    let requested_labels = all_results
        .iter()
        .map(|r| r.machine_label.clone())
        .collect::<Vec<_>>();
    let missing_runs = find_missing_machine_labels(&requested_labels, &seen_labels);
    if !missing_runs.is_empty() {
        println!(
            "WARNING: no run JSONs found for requested machine labels: {}",
            missing_runs.join(", ")
        );
    }

    let partial_missing = !failures.is_empty() || !missing_runs.is_empty();
    if partial_missing && !args.plot_partial {
        println!(
            "\nSkipping local plot generation because requested machine coverage is incomplete. \
             Re-run with --plot-partial to force partial plot refresh."
        );
        return Ok(1);
    }

    if !failures.is_empty() {
        println!("\nRendering plots from available runs despite machine failures...");
    } else {
        println!("\nRendering local plots from aggregated runs...");
    }
    render_plots(&runtime, partial_missing)?;
    Ok(if failures.is_empty() { 0 } else { 1 })
}

fn main() {
    let args = match parse_args() {
        Ok(v) => v,
        Err(err) => {
            eprintln!("{err}");
            std::process::exit(2);
        }
    };
    match run(args) {
        Ok(code) => std::process::exit(code),
        Err(err) => {
            eprintln!("{err}");
            std::process::exit(1);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn runtime() -> FleetRuntime {
        FleetRuntime {
            repo_root: PathBuf::from("."),
            runs_dir: PathBuf::from("bench_results/runs"),
            plot_out_dir: PathBuf::from("bench_results/plots"),
            scenarios: vec!["1p1c".to_string(), "8p8c".to_string()],
            seed_label: Some("v4,8,127".to_string()),
            sync_repo: true,
            strict_complete: false,
            dry_run: true,
            mode: FleetMode::Search {
                complete_args: vec!["--bench-arg=--items-per-producer=1000".to_string()],
            },
        }
    }

    fn fixed_runtime() -> FleetRuntime {
        FleetRuntime {
            repo_root: PathBuf::from("."),
            runs_dir: PathBuf::from("bench_results/runs"),
            plot_out_dir: PathBuf::from("bench_results/plots"),
            scenarios: vec!["1p1c".to_string(), "8p8c".to_string()],
            seed_label: Some("v4,8,127".to_string()),
            sync_repo: true,
            strict_complete: false,
            dry_run: true,
            mode: FleetMode::FixedLabels {
                ubq_labels: vec!["v7,16,511".to_string(), "v6,0,511".to_string()],
                bench_args: vec!["--throughput-only".to_string()],
            },
        }
    }

    fn machine_local() -> ResolvedMachine {
        ResolvedMachine {
            name: "local".to_string(),
            is_local: true,
            host: "local".to_string(),
            machine_label: "local".to_string(),
            remote_repo_dir: "~/UBQ".to_string(),
            remote_runs_dir: "bench_results/runs".to_string(),
        }
    }

    fn machine_remote() -> ResolvedMachine {
        ResolvedMachine {
            name: "lab".to_string(),
            is_local: false,
            host: "lab".to_string(),
            machine_label: "lab".to_string(),
            remote_repo_dir: "~/UBQ".to_string(),
            remote_runs_dir: "bench_results/runs".to_string(),
        }
    }

    #[test]
    fn local_complete_command_contains_expected_args() {
        let runtime = runtime();
        let machine = machine_local();
        let cmd = build_local_complete_cmd(&runtime, &machine);
        assert!(cmd.starts_with(&[
            "cargo".to_string(),
            "run".to_string(),
            "--quiet".to_string(),
            "--release".to_string(),
            "--bin".to_string(),
            "complete_benches".to_string(),
            "--".to_string(),
        ]));
        assert!(cmd.contains(&"--machine-label".to_string()));
        assert!(cmd.contains(&"local".to_string()));
        assert!(cmd.contains(&"--allow-incomplete".to_string()));
    }

    #[test]
    fn local_fixed_label_command_contains_expected_args() {
        let runtime = fixed_runtime();
        let machine = machine_local();
        let cmd = build_local_fixed_label_cmd(&runtime, &machine).expect("fixed-label command");
        assert_eq!(cmd[0], "bash");
        assert_eq!(cmd[1], "scripts/bench_dual_host.sh");
        assert!(cmd.contains(&"--ubq-label".to_string()));
        assert!(cmd.contains(&"v7,16,511".to_string()));
        assert!(cmd.contains(&"v6,0,511".to_string()));
        assert!(cmd.contains(&"--skip-remote".to_string()));
        assert!(cmd.contains(&"--skip-plot".to_string()));
        assert!(cmd.contains(&"--local-machine-label".to_string()));
        assert!(cmd.contains(&"--out-root".to_string()));
        assert!(cmd.contains(&"bench_results".to_string()));
        assert!(cmd.contains(&"--scenarios".to_string()));
        assert!(cmd.contains(&"1p1c,8p8c".to_string()));
        assert!(cmd.contains(&"--throughput-only".to_string()));
    }

    #[test]
    fn remote_complete_command_uses_ssh_and_cargo() {
        let runtime = runtime();
        let machine = machine_remote();
        let cmd = build_remote_complete_cmd(&runtime, &machine);
        assert_eq!(cmd[0], "ssh");
        assert_eq!(cmd[1], "lab");
        assert!(cmd[2].contains("cargo"));
        assert!(cmd[2].contains("--quiet"));
        assert!(cmd[2].contains("complete_benches"));
        assert!(cmd[2].contains("cd \"$HOME/UBQ\""));
    }

    #[test]
    fn remote_fixed_label_command_uses_ssh_and_bench_script() {
        let runtime = fixed_runtime();
        let machine = machine_remote();
        let cmd = build_remote_fixed_label_cmd(&runtime, &machine).expect("remote fixed command");
        assert_eq!(cmd[0], "ssh");
        assert_eq!(cmd[1], "lab");
        assert!(cmd[2].contains("bash"));
        assert!(cmd[2].contains("scripts/bench_dual_host.sh"));
        assert!(cmd[2].contains("--skip-remote"));
        assert!(cmd[2].contains("--skip-plot"));
        assert!(cmd[2].contains("cd \"$HOME/UBQ\""));
    }

    #[test]
    fn sync_and_pull_commands_are_formed() {
        let machine = machine_remote();
        let sync = build_sync_cmd(&machine);
        assert_eq!(sync[0], "rsync");
        assert!(sync.iter().any(|arg| arg == "--delete"));

        let remote_runs_root = join_remote_path(&machine.remote_repo_dir, &machine.remote_runs_dir);
        let pull =
            build_pull_runs_cmd(&machine, &remote_runs_root, Path::new("bench_results/runs"));
        assert_eq!(pull[0], "rsync");
        assert!(pull[2].starts_with("lab:"));
    }

    #[test]
    fn forwarded_arg_validation_blocks_protected_keys() {
        let bad = vec!["--machine-label=foo".to_string()];
        assert!(validate_forwarded_args(&bad, FORBIDDEN_COMPLETE_ARGS).is_err());
        let ok = vec![
            "--mode=throughput".to_string(),
            "--bench-arg=--n=1".to_string(),
        ];
        assert!(validate_forwarded_args(&ok, FORBIDDEN_COMPLETE_ARGS).is_ok());
    }

    #[test]
    fn removed_complete_args_are_rejected() {
        let bad = vec!["--max-rounds=12".to_string()];
        assert!(validate_removed_complete_args(&bad).is_err());
        let ok = vec!["--seed-label=v4,8,127".to_string()];
        assert!(validate_removed_complete_args(&ok).is_ok());
    }

    #[test]
    fn forwarded_bench_args_block_protected_keys() {
        let bad = vec!["--out-root=/tmp/bench_results".to_string()];
        assert!(validate_forwarded_args(&bad, FORBIDDEN_BENCH_ARGS).is_err());
        let ok = vec!["--throughput-only".to_string(), "--runs=3".to_string()];
        assert!(validate_forwarded_args(&ok, FORBIDDEN_BENCH_ARGS).is_ok());
    }

    #[test]
    fn fixed_label_mode_is_selected_when_labels_are_present() {
        let args = parse_args_from([
            "--machines",
            "local,lab",
            "--ubq-label",
            "v7,16,511",
            "--ubq-labels",
            "v6,0,511;v5,16,511",
            "--bench-arg",
            "--throughput-only",
        ])
        .expect("parse fixed-label args");
        assert_eq!(args.machines, vec!["local".to_string(), "lab".to_string()]);
        match args.mode {
            FleetMode::FixedLabels {
                ubq_labels,
                bench_args,
            } => {
                assert_eq!(
                    ubq_labels,
                    vec![
                        "v7,16,511".to_string(),
                        "v6,0,511".to_string(),
                        "v5,16,511".to_string()
                    ]
                );
                assert_eq!(bench_args, vec!["--throughput-only".to_string()]);
            }
            FleetMode::Search { .. } => panic!("expected fixed-label mode"),
        }
    }

    #[test]
    fn fixed_label_and_search_modes_cannot_be_combined() {
        let err = parse_args_from([
            "--machines",
            "local",
            "--ubq-label",
            "v7,16,511",
            "--complete-arg",
            "--mode=throughput",
        ])
        .expect_err("expected conflict");
        assert!(err.contains("cannot combine fixed-label mode"));
    }

    #[test]
    fn configured_python_is_used_when_present() {
        let resolved = resolve_python_bin_with_override(Some("python3")).expect("resolve python");
        assert_eq!(resolved, "python3");
    }
}
