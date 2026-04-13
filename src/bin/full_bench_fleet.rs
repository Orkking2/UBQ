#[path = "bench_tooling/fleet.rs"]
mod bench_tooling;

use bench_tooling::{
    collect_machine_labels, find_missing_machine_labels, format_cmd, join_remote_path,
    normalize_machine, normalize_machine_list, remote_cd_expr, validate_forwarded_args,
};
use clap::Parser;
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
    "/build.rs",
    "/src/",
    "/src/**",
    "/benches/",
    "/benches/**",
];

const FORBIDDEN_FRONTIER_ARGS: &[&str] =
    &["--machine-label", "--runs-dir", "--repeats", "--dry-run"];

#[derive(Parser, Debug)]
#[command(name = "full_bench_fleet")]
struct Args {
    /// Comma-separated list of machine names (from config)
    #[arg(long, required = true, value_delimiter = ',')]
    machines: Vec<String>,

    #[arg(long, default_value = "bench_fleet.toml")]
    config: PathBuf,

    #[arg(long)]
    repeats: Option<usize>,

    #[arg(long)]
    no_sync_repo: bool,

    #[arg(long)]
    skip_local_plot: bool,

    #[arg(long)]
    plot_partial: bool,

    #[arg(long)]
    dry_run: bool,

    #[arg(long = "frontier-arg", alias = "complete-arg")]
    frontier_args: Vec<String>,
}

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
    queues: Option<Vec<String>>,
    modes: Option<Vec<String>>,
    items_per_producer: Option<Vec<u64>>,
    repeats: Option<usize>,
    ubq_labels: Option<Vec<String>>,
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
    queues: Vec<String>,
    modes: Vec<String>,
    items_per_producer: Vec<u64>,
    repeats: usize,
    ubq_labels: Vec<String>,
    sync_repo: bool,
    dry_run: bool,
    frontier_args: Vec<String>,
}

#[derive(Clone, Debug)]
struct MachineRunResult {
    machine_name: String,
    machine_label: String,
    ok: bool,
    error: Option<String>,
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

fn make_frontier_base_args(
    runtime: &FleetRuntime,
    machine_label: &str,
    runs_dir: &str,
) -> Vec<String> {
    let mut frontier_args = vec![
        "--machine-label".to_string(),
        machine_label.to_string(),
        "--runs-dir".to_string(),
        runs_dir.to_string(),
    ];
    if !runtime.scenarios.is_empty() {
        frontier_args.push("--scenarios".to_string());
        frontier_args.push(runtime.scenarios.join(","));
    }
    if !runtime.queues.is_empty() {
        frontier_args.push("--queues".to_string());
        frontier_args.push(runtime.queues.join(","));
    }
    if !runtime.modes.is_empty() {
        frontier_args.push("--modes".to_string());
        frontier_args.push(runtime.modes.join(","));
    }
    if !runtime.items_per_producer.is_empty() {
        frontier_args.push("--items-per-producer".to_string());
        frontier_args.push(
            runtime
                .items_per_producer
                .iter()
                .map(|value| value.to_string())
                .collect::<Vec<_>>()
                .join(","),
        );
    }
    frontier_args.push("--repeats".to_string());
    frontier_args.push(runtime.repeats.to_string());
    for seed_label in &runtime.ubq_labels {
        frontier_args.push("--seed-label".to_string());
        frontier_args.push(seed_label.clone());
    }
    frontier_args.extend(runtime.frontier_args.clone());
    if runtime.dry_run {
        frontier_args.push("--dry-run".to_string());
    }
    frontier_args
}

fn build_local_complete_cmd(runtime: &FleetRuntime, machine: &ResolvedMachine) -> Vec<String> {
    let runs_dir = runtime.runs_dir.display().to_string();
    let mut cmd = vec![
        "cargo".to_string(),
        "run".to_string(),
        "--quiet".to_string(),
        "--release".to_string(),
        "--features".to_string(),
        "bench_registry".to_string(),
        "--bin".to_string(),
        "bench_frontier".to_string(),
        "--".to_string(),
    ];
    cmd.extend(make_frontier_base_args(
        runtime,
        &machine.machine_label,
        &runs_dir,
    ));
    cmd
}

/// Build the shell payload (no SSH wrapper) for running bench_frontier on a remote machine.
fn build_remote_bench_payload(runtime: &FleetRuntime, machine: &ResolvedMachine) -> String {
    let mut inner = vec![
        "cargo".to_string(),
        "run".to_string(),
        "--quiet".to_string(),
        "--release".to_string(),
        "--features".to_string(),
        "bench_registry".to_string(),
        "--bin".to_string(),
        "bench_frontier".to_string(),
        "--".to_string(),
    ];
    inner.extend(make_frontier_base_args(
        runtime,
        &machine.machine_label,
        &machine.remote_runs_dir,
    ));
    let inner_quoted = inner
        .iter()
        .map(|s| bench_tooling::shell_quote(s))
        .collect::<Vec<_>>()
        .join(" ");
    format!(
        "if [ -f \"$HOME/.cargo/env\" ]; then . \"$HOME/.cargo/env\"; fi; \
         export PATH=\"$HOME/.cargo/bin:$PATH\"; \
         cd {} && {}",
        remote_cd_expr(&machine.remote_repo_dir),
        inner_quoted
    )
}

#[cfg(test)]
fn build_remote_complete_cmd(runtime: &FleetRuntime, machine: &ResolvedMachine) -> Vec<String> {
    let payload = build_remote_bench_payload(runtime, machine);
    vec!["ssh".to_string(), machine.host.clone(), payload]
}

// ── tmux helpers ─────────────────────────────────────────────────────────────

fn tmux_session_name(machine_label: &str) -> String {
    let safe: String = machine_label
        .chars()
        .map(|c| if c.is_alphanumeric() { c } else { '_' })
        .collect();
    format!("ubq_bench_{safe}")
}

/// Shell expression (double-quoted, `$HOME`-safe) for the remote bench log file.
fn remote_bench_log_expr(machine: &ResolvedMachine) -> String {
    let safe_label: String = machine
        .machine_label
        .chars()
        .map(|c| if c.is_alphanumeric() { c } else { '_' })
        .collect();
    let log_path = format!(
        "{}/target/ubq_bench_{safe_label}.log",
        machine.remote_repo_dir.trim_end_matches('/')
    );
    remote_cd_expr(&log_path)
}

/// Check whether a tmux session is alive on the remote host.
/// Returns exit code 0 if the session exists, non-zero otherwise.
fn build_remote_check_tmux_cmd(machine: &ResolvedMachine, session: &str) -> Vec<String> {
    vec![
        "ssh".to_string(),
        machine.host.clone(),
        format!(
            "tmux has-session -t {} 2>/dev/null",
            bench_tooling::shell_quote(session)
        ),
    ]
}

/// Count JSON result files already collected in the remote runs directory.
fn build_remote_count_json_cmd(machine: &ResolvedMachine, remote_runs_root: &str) -> Vec<String> {
    vec![
        "ssh".to_string(),
        machine.host.clone(),
        format!(
            "find {} -name '*.json' -type f 2>/dev/null | wc -l",
            remote_cd_expr(remote_runs_root)
        ),
    ]
}

/// Launch a new detached tmux session that runs the bench payload and tees
/// stdout+stderr to `log_expr`, appending `__BENCH_DONE__` when finished.
fn build_remote_tmux_launch_cmd(
    machine: &ResolvedMachine,
    session: &str,
    bench_payload: &str,
    log_expr: &str,
) -> Vec<String> {
    let full_payload = format!(
        "mkdir -p $(dirname {log_expr}) && {{ {bench_payload}; }} 2>&1 | tee {log_expr}; \
         echo __BENCH_DONE__ >> {log_expr}"
    );
    let tmux_cmd = format!(
        "tmux new-session -d -s {} {}",
        bench_tooling::shell_quote(session),
        bench_tooling::shell_quote(&full_payload),
    );
    vec!["ssh".to_string(), machine.host.clone(), tmux_cmd]
}

/// Stream the remote log file, replaying all existing content then following
/// new writes.  Terminates when the `__BENCH_DONE__` sentinel appears.
///
/// The log file is created by `tee` when the tmux session's shell pipeline
/// starts, but cargo may take several minutes to compile before any output
/// appears.  We poll until the file exists before starting `tail -f` so that
/// we don't race compilation startup.
///
/// `awk` is used with an explicit `fflush()` after every print to defeat the
/// full-buffering that awk applies when its stdout is a pipe (non-TTY).
/// Without `fflush()` output would only appear once awk's 4-8 KB internal
/// buffer filled, giving the appearance of no output at all.
fn build_remote_stream_log_cmd(
    machine: &ResolvedMachine,
    session: &str,
    log_expr: &str,
) -> Vec<String> {
    let session_quoted = bench_tooling::shell_quote(session);
    let stream_cmd = format!(
        // Wait for the log file with periodic feedback (cargo compile can take
        // several minutes on the first bench_registry build).
        "attempt=0; \
         while [ ! -f {log_expr} ]; do \
             if ! tmux has-session -t {session_quoted} 2>/dev/null; then \
                 echo \"bench: tmux session exited before log was created\" >&2; \
                 exit 2; \
             fi; \
             sleep 2; attempt=$((attempt+1)); \
             [ $((attempt % 15)) -eq 0 ] && \
                 echo \"bench: still waiting for log ($((attempt*2))s elapsed)\"; \
         done; \
         (tail -n +1 -f {log_expr} | \
             awk '/^__BENCH_DONE__$/ {{exit}} {{print; fflush()}}') & \
         stream_pid=$!; \
         while kill -0 \"$stream_pid\" 2>/dev/null; do \
             if grep -q '^__BENCH_DONE__$' {log_expr}; then \
                 wait \"$stream_pid\" 2>/dev/null || true; \
                 exit 0; \
             fi; \
             if ! tmux has-session -t {session_quoted} 2>/dev/null; then \
                 echo \"bench: tmux session exited before completion sentinel\" >&2; \
                 kill \"$stream_pid\" 2>/dev/null || true; \
                 wait \"$stream_pid\" 2>/dev/null || true; \
                 exit 3; \
             fi; \
             sleep 2; \
         done; \
         wait \"$stream_pid\""
    );
    vec!["ssh".to_string(), machine.host.clone(), stream_cmd]
}

fn describe_remote_stream_failure(machine: &ResolvedMachine, code: i32) -> String {
    match code {
        2 => format!(
            "remote tmux session on {} exited before its log file was created",
            machine.host
        ),
        3 => format!(
            "remote tmux session on {} exited before writing the completion sentinel",
            machine.host
        ),
        _ => format!(
            "remote log stream failed for {} with exit code {code}",
            machine.name
        ),
    }
}

/// Run a remote command capturing its stdout as a trimmed string.
fn run_capturing_stdout(args: &[String], cwd: &Path) -> Result<String, String> {
    let output = std::process::Command::new(&args[0])
        .args(&args[1..])
        .current_dir(cwd)
        .output()
        .map_err(|err| format!("failed to run '{}': {err}", args[0]))?;
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

/// Run a remote machine via a tmux session so that bench progress survives
/// SSH disconnections.  On reconnect the existing session is detected and
/// its log is replayed from the beginning, giving a full progress report.
fn run_remote_machine_via_tmux(
    runtime: &FleetRuntime,
    machine: &ResolvedMachine,
) -> Result<(), String> {
    // 1. Optionally sync the repo.
    if runtime.sync_repo {
        println!(
            "  syncing repo to {}:{}",
            machine.host, machine.remote_repo_dir
        );
        let cmd = build_sync_cmd(machine);
        let code = run_cmd(&cmd, &runtime.repo_root, runtime.dry_run)?;
        if code != 0 {
            return Err(format!("repo sync failed with exit code {code}"));
        }
    }

    let session = tmux_session_name(&machine.machine_label);
    let log_expr = remote_bench_log_expr(machine);
    let remote_runs_root = join_remote_path(&machine.remote_repo_dir, &machine.remote_runs_dir);

    // 2. Check for a running session.
    let check_cmd = build_remote_check_tmux_cmd(machine, &session);
    let session_code = run_cmd(&check_cmd, &runtime.repo_root, runtime.dry_run)?;
    let session_exists = session_code == 0;

    if session_exists {
        println!(
            "  reconnecting to existing tmux session '{}' on {}",
            session, machine.host
        );
        // Show how many result files are already on disk as a quick progress hint.
        let count_cmd = build_remote_count_json_cmd(machine, &remote_runs_root);
        if let Ok(count) = run_capturing_stdout(&count_cmd, &runtime.repo_root) {
            println!(
                "  progress: {} JSON result file(s) collected so far",
                count.trim()
            );
        }
        println!("  replaying log from start — all previous output follows:");
    } else {
        println!(
            "  launching bench_frontier on {} via tmux session '{}'",
            machine.host, session
        );
        let bench_payload = build_remote_bench_payload(runtime, machine);
        let launch_cmd = build_remote_tmux_launch_cmd(machine, &session, &bench_payload, &log_expr);
        let code = run_cmd(&launch_cmd, &runtime.repo_root, runtime.dry_run)?;
        if code != 0 {
            return Err(format!(
                "failed to launch tmux session '{session}' on {}",
                machine.host
            ));
        }
    }

    // 3. Stream the log until the bench completes (sentinel appears).
    let stream_cmd = build_remote_stream_log_cmd(machine, &session, &log_expr);
    let stream_code = run_streaming_cmd(
        &stream_cmd,
        &runtime.repo_root,
        runtime.dry_run,
        &machine.name,
    )?;
    if stream_code != 0 {
        return Err(describe_remote_stream_failure(machine, stream_code));
    }

    // 4. Pull results back to the local runs directory.
    println!("  pulling runs from {}:{}", machine.host, remote_runs_root);
    let exists_cmd = build_remote_dir_exists_cmd(machine, &remote_runs_root);
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

    let pull_cmd = build_pull_runs_cmd(machine, &remote_runs_root, &runtime.runs_dir);
    let code = run_cmd(&pull_cmd, &runtime.repo_root, runtime.dry_run)?;
    if code != 0 {
        return Err(format!("failed to pull remote runs with exit code {code}"));
    }
    Ok(())
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
        println!("  starting local search");
        let cmd = build_local_complete_cmd(&runtime, &machine);
        run_streaming_cmd(&cmd, &runtime.repo_root, runtime.dry_run, &machine.name).and_then(
            |code| {
                if code == 0 {
                    Ok(())
                } else {
                    Err(format!("local bench_frontier failed with exit code {code}"))
                }
            },
        )
    } else {
        run_remote_machine_via_tmux(&runtime, &machine)
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
        println!("  machine frontier-complete: {}", result.machine_name);
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

fn run(args: Args) -> Result<i32, String> {
    validate_forwarded_args(&args.frontier_args, FORBIDDEN_FRONTIER_ARGS)?;

    let machines = normalize_machine_list(&args.machines.join(","));
    if machines.is_empty() {
        return Err("--machines produced no valid machine names".to_string());
    }

    let config = load_config(&args.config)?;
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
    let queues = defaults.queues.clone().unwrap_or_else(|| {
        vec![
            "ubq".to_string(),
            "segqueue".to_string(),
            "concurrent-queue".to_string(),
        ]
    });
    let modes = defaults
        .modes
        .clone()
        .unwrap_or_else(|| vec!["throughput".to_string()]);
    let items_per_producer = defaults
        .items_per_producer
        .clone()
        .unwrap_or_else(|| vec![1_000_000]);
    let repeats = args
        .repeats
        .unwrap_or_else(|| defaults.repeats.unwrap_or(1));
    let ubq_labels = defaults.ubq_labels.clone().unwrap_or_default();

    let repo_root =
        std::env::current_dir().map_err(|err| format!("failed to read current dir: {err}"))?;

    let runtime = Arc::new(FleetRuntime {
        repo_root,
        runs_dir,
        plot_out_dir,
        scenarios,
        queues,
        modes,
        items_per_producer,
        repeats,
        ubq_labels,
        sync_repo: !args.no_sync_repo,
        dry_run: args.dry_run,
        frontier_args: args.frontier_args.clone(),
    });

    let mut resolved = Vec::new();
    for machine_name in &machines {
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
    println!("Mode: frontier search");
    println!("Repeats: {}", runtime.repeats);
    if !runtime.frontier_args.is_empty() {
        println!(
            "Forwarded frontier args: {}",
            runtime.frontier_args.join(" ")
        );
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
    let args = Args::parse();
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
            queues: vec![
                "ubq".to_string(),
                "segqueue".to_string(),
                "concurrent-queue".to_string(),
            ],
            modes: vec!["throughput".to_string()],
            items_per_producer: vec![1_000],
            repeats: 2,
            ubq_labels: vec!["balanced,8,127,crossbeam,cas".to_string()],
            sync_repo: true,
            dry_run: true,
            frontier_args: vec!["--parallelism=16".to_string()],
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
    fn local_frontier_command_contains_expected_args() {
        let runtime = runtime();
        let machine = machine_local();
        let cmd = build_local_complete_cmd(&runtime, &machine);
        assert!(cmd.starts_with(&[
            "cargo".to_string(),
            "run".to_string(),
            "--quiet".to_string(),
            "--release".to_string(),
            "--features".to_string(),
            "bench_registry".to_string(),
            "--bin".to_string(),
            "bench_frontier".to_string(),
            "--".to_string(),
        ]));
        assert!(cmd.contains(&"--machine-label".to_string()));
        assert!(cmd.contains(&"local".to_string()));
        assert!(cmd.contains(&"--seed-label".to_string()));
    }

    #[test]
    fn remote_frontier_command_uses_ssh_and_cargo() {
        let runtime = runtime();
        let machine = machine_remote();
        let cmd = build_remote_complete_cmd(&runtime, &machine);
        assert_eq!(cmd[0], "ssh");
        assert_eq!(cmd[1], "lab");
        assert!(cmd[2].contains("cargo"));
        assert!(cmd[2].contains("--quiet"));
        assert!(cmd[2].contains("bench_frontier"));
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
        assert!(validate_forwarded_args(&bad, FORBIDDEN_FRONTIER_ARGS).is_err());
        let ok = vec![
            "--parallelism=16".to_string(),
            "--seed-label=balanced,8,127,crossbeam,cas".to_string(),
        ];
        assert!(validate_forwarded_args(&ok, FORBIDDEN_FRONTIER_ARGS).is_ok());
    }

    #[test]
    fn parse_args_accepts_top_level_repeats_override() {
        let args = Args::try_parse_from(["prog", "--machines", "local,lab", "--repeats", "3"])
            .expect("parse args");
        let machines = normalize_machine_list(&args.machines.join(","));
        assert_eq!(machines, vec!["local".to_string(), "lab".to_string()]);
        assert_eq!(args.repeats, Some(3));
    }

    #[test]
    fn forwarded_arg_validation_blocks_repeats() {
        let bad = vec!["--repeats=4".to_string()];
        assert!(validate_forwarded_args(&bad, FORBIDDEN_FRONTIER_ARGS).is_err());
    }

    #[test]
    fn configured_python_is_used_when_present() {
        let resolved = resolve_python_bin_with_override(Some("python3")).expect("resolve python");
        assert_eq!(resolved, "python3");
    }

    #[test]
    fn tmux_session_name_is_deterministic_and_safe() {
        assert_eq!(tmux_session_name("lab"), "ubq_bench_lab");
        assert_eq!(tmux_session_name("my-server"), "ubq_bench_my_server");
        assert_eq!(tmux_session_name("box.local"), "ubq_bench_box_local");
    }

    #[test]
    fn remote_bench_log_expr_expands_home() {
        let machine = machine_remote();
        let expr = remote_bench_log_expr(&machine);
        assert!(expr.contains("$HOME"), "log expr should reference $HOME");
        assert!(
            expr.contains("ubq_bench_lab"),
            "log expr should include machine label"
        );
        assert!(expr.ends_with(".log\""), "log expr should end with .log");
    }

    #[test]
    fn tmux_launch_cmd_wraps_payload_and_sentinel() {
        let machine = machine_remote();
        let launch = build_remote_tmux_launch_cmd(
            &machine,
            "ubq_bench_lab",
            "cd /repo && cargo run",
            "\"$HOME/UBQ/target/ubq_bench_lab.log\"",
        );
        assert_eq!(launch[0], "ssh");
        assert_eq!(launch[1], "lab");
        assert!(launch[2].contains("tmux new-session"));
        assert!(
            launch[2].contains("__BENCH_DONE__"),
            "sentinel must be written on completion"
        );
    }

    #[test]
    fn tmux_stream_cmd_tails_log_with_sentinel_exit() {
        let machine = machine_remote();
        let stream = build_remote_stream_log_cmd(
            &machine,
            "ubq_bench_lab",
            "\"$HOME/UBQ/target/ubq_bench_lab.log\"",
        );
        assert_eq!(stream[0], "ssh");
        assert!(
            stream[2].contains("tail -n +1 -f"),
            "must replay from beginning"
        );
        assert!(
            stream[2].contains("__BENCH_DONE__"),
            "must stop at sentinel"
        );
        assert!(
            stream[2].contains("tmux has-session"),
            "must detect dead tmux sessions"
        );
    }

    #[test]
    fn remote_stream_failures_are_descriptive() {
        let machine = machine_remote();
        assert!(
            describe_remote_stream_failure(&machine, 2).contains("log file was created"),
            "exit code 2 should explain the missing log case"
        );
        assert!(
            describe_remote_stream_failure(&machine, 3).contains("completion sentinel"),
            "exit code 3 should explain the missing sentinel case"
        );
    }

    #[test]
    fn remote_bench_payload_includes_features_flag() {
        let runtime = runtime();
        let machine = machine_remote();
        let payload = build_remote_bench_payload(&runtime, &machine);
        assert!(
            payload.contains("--features"),
            "payload must pass --features"
        );
        assert!(
            payload.contains("bench_registry"),
            "payload must enable bench_registry"
        );
    }
}
