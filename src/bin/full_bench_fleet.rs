#[path = "bench_tooling/fleet.rs"]
mod bench_tooling;

use bench_tooling::{
    collect_machine_labels, find_missing_machine_labels, format_cmd, join_remote_path,
    normalize_machine, normalize_machine_list, remote_cd_expr, validate_forwarded_args,
};
use clap::Parser;
use serde::Deserialize;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fs;
use std::io::{BufRead, BufReader, Read};
use std::path::{Component, Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::thread;

const REPO_SYNC_INCLUDE_PATTERNS: &[&str] = &[
    "/.gitmodules",
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
    fastfifo_block_sizes: Option<Vec<usize>>,
    lfqueue_segment_sizes: Option<Vec<usize>>,
    wcq_capacities: Option<Vec<usize>>,
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
    fastfifo_block_sizes: Vec<usize>,
    lfqueue_segment_sizes: Vec<usize>,
    wcq_capacities: Vec<usize>,
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

#[derive(Clone, Debug, PartialEq, Eq)]
struct PathDependencySync {
    local_dir: PathBuf,
    remote_dir: String,
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

fn read_manifest_path_dependencies(manifest_dir: &Path) -> Result<Vec<String>, String> {
    let manifest_path = manifest_dir.join("Cargo.toml");
    let raw = fs::read_to_string(&manifest_path)
        .map_err(|err| format!("failed to read manifest {}: {err}", manifest_path.display()))?;
    let parsed: toml::Value = toml::from_str(&raw)
        .map_err(|err| format!("invalid manifest {}: {err}", manifest_path.display()))?;
    let mut deps = BTreeSet::new();
    collect_manifest_path_dependencies(&parsed, &mut deps);
    Ok(deps.into_iter().collect())
}

fn collect_manifest_path_dependencies(value: &toml::Value, out: &mut BTreeSet<String>) {
    let Some(table) = value.as_table() else {
        if let Some(array) = value.as_array() {
            for item in array {
                collect_manifest_path_dependencies(item, out);
            }
        }
        return;
    };

    for key in ["dependencies", "dev-dependencies", "build-dependencies"] {
        if let Some(dep_table) = table.get(key) {
            collect_dep_table_paths(dep_table, out);
        }
    }

    for child in table.values() {
        collect_manifest_path_dependencies(child, out);
    }
}

fn collect_dep_table_paths(value: &toml::Value, out: &mut BTreeSet<String>) {
    let Some(table) = value.as_table() else {
        return;
    };
    for dep_spec in table.values() {
        let Some(dep_table) = dep_spec.as_table() else {
            continue;
        };
        let Some(path) = dep_table.get("path").and_then(|value| value.as_str()) else {
            continue;
        };
        let trimmed = path.trim();
        if !trimmed.is_empty() {
            out.insert(trimmed.to_string());
        }
    }
}

fn resolve_local_dependency_dir(manifest_dir: &Path, dep_path: &str) -> Result<PathBuf, String> {
    let resolved = manifest_dir.join(dep_path).canonicalize().map_err(|err| {
        format!(
            "failed to resolve local cargo path dependency '{}' from {}: {err}",
            dep_path,
            manifest_dir.display()
        )
    })?;
    let dep_dir = if resolved.is_file() {
        resolved
            .parent()
            .ok_or_else(|| {
                format!(
                    "local cargo path dependency '{}' resolved to file without parent: {}",
                    dep_path,
                    resolved.display()
                )
            })?
            .to_path_buf()
    } else {
        resolved
    };
    let dep_manifest = dep_dir.join("Cargo.toml");
    if !dep_manifest.is_file() {
        return Err(format!(
            "local cargo path dependency '{}' resolved to {} but {} is missing",
            dep_path,
            dep_dir.display(),
            dep_manifest.display()
        ));
    }
    Ok(dep_dir)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RemotePathBase {
    Home,
    Absolute,
    Relative,
}

fn resolve_remote_dependency_dir(base: &str, dep_path: &str) -> Result<String, String> {
    let dep_path = dep_path.trim();
    if dep_path.is_empty() {
        return Err("cargo path dependency cannot be empty".to_string());
    }

    let base = base.trim();
    let (base_kind, mut segments) = if base == "~" {
        (RemotePathBase::Home, Vec::new())
    } else if let Some(tail) = base.strip_prefix("~/") {
        (
            RemotePathBase::Home,
            tail.split('/')
                .filter(|part| !part.is_empty())
                .map(str::to_string)
                .collect::<Vec<_>>(),
        )
    } else if let Some(tail) = base.strip_prefix('/') {
        (
            RemotePathBase::Absolute,
            tail.split('/')
                .filter(|part| !part.is_empty())
                .map(str::to_string)
                .collect::<Vec<_>>(),
        )
    } else {
        (
            RemotePathBase::Relative,
            base.split('/')
                .filter(|part| !part.is_empty() && *part != ".")
                .map(str::to_string)
                .collect::<Vec<_>>(),
        )
    };

    let mut leading_parents = 0usize;
    for component in Path::new(dep_path).components() {
        match component {
            Component::CurDir => {}
            Component::Normal(part) => segments.push(part.to_string_lossy().into_owned()),
            Component::ParentDir => {
                if segments.pop().is_none() && base_kind != RemotePathBase::Absolute {
                    leading_parents += 1;
                }
            }
            Component::RootDir | Component::Prefix(_) => {
                return Err(format!(
                    "absolute cargo path dependency '{}' is unsupported for remote sync",
                    dep_path
                ));
            }
        }
    }

    let mut rendered_components = Vec::with_capacity(leading_parents + segments.len());
    for _ in 0..leading_parents {
        rendered_components.push("..".to_string());
    }
    rendered_components.extend(segments);

    let rendered = match base_kind {
        RemotePathBase::Home => {
            if rendered_components.is_empty() {
                "~".to_string()
            } else {
                format!("~/{}", rendered_components.join("/"))
            }
        }
        RemotePathBase::Absolute => {
            if rendered_components.is_empty() {
                "/".to_string()
            } else {
                format!("/{}", rendered_components.join("/"))
            }
        }
        RemotePathBase::Relative => {
            if rendered_components.is_empty() {
                ".".to_string()
            } else {
                rendered_components.join("/")
            }
        }
    };

    Ok(rendered)
}

fn record_path_dependency_sync(
    syncs: &mut Vec<PathDependencySync>,
    candidate: PathDependencySync,
) -> Result<(), String> {
    if let Some(existing) = syncs
        .iter()
        .find(|existing| existing.local_dir == candidate.local_dir)
    {
        if existing.remote_dir != candidate.remote_dir {
            return Err(format!(
                "local cargo path dependency {} mapped to conflicting remote paths '{}' and '{}'",
                candidate.local_dir.display(),
                existing.remote_dir,
                candidate.remote_dir
            ));
        }
        return Ok(());
    }

    if syncs
        .iter()
        .any(|existing| candidate.local_dir.starts_with(&existing.local_dir))
    {
        return Ok(());
    }

    syncs.retain(|existing| !existing.local_dir.starts_with(&candidate.local_dir));
    syncs.push(candidate);
    Ok(())
}

fn discover_path_dependency_syncs(
    repo_root: &Path,
    remote_repo_dir: &str,
) -> Result<Vec<PathDependencySync>, String> {
    let repo_root = repo_root.canonicalize().map_err(|err| {
        format!(
            "failed to canonicalize repo root {}: {err}",
            repo_root.display()
        )
    })?;
    let mut pending = VecDeque::from([(repo_root.clone(), remote_repo_dir.to_string())]);
    let mut visited = BTreeSet::new();
    let mut syncs = Vec::new();

    while let Some((manifest_dir, remote_manifest_dir)) = pending.pop_front() {
        if !visited.insert(manifest_dir.clone()) {
            continue;
        }

        for dep_path in read_manifest_path_dependencies(&manifest_dir)? {
            let local_dir = resolve_local_dependency_dir(&manifest_dir, &dep_path)?;
            let remote_dir = resolve_remote_dependency_dir(&remote_manifest_dir, &dep_path)?;
            record_path_dependency_sync(
                &mut syncs,
                PathDependencySync {
                    local_dir: local_dir.clone(),
                    remote_dir: remote_dir.clone(),
                },
            )?;
            pending.push_back((local_dir, remote_dir));
        }
    }

    syncs.sort_by(|a, b| {
        a.remote_dir
            .cmp(&b.remote_dir)
            .then_with(|| a.local_dir.cmp(&b.local_dir))
    });
    Ok(syncs)
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
    if !runtime.fastfifo_block_sizes.is_empty() {
        frontier_args.push("--fastfifo-block-sizes".to_string());
        frontier_args.push(
            runtime
                .fastfifo_block_sizes
                .iter()
                .map(|value| value.to_string())
                .collect::<Vec<_>>()
                .join(","),
        );
    }
    if !runtime.lfqueue_segment_sizes.is_empty() {
        frontier_args.push("--lfqueue-segment-sizes".to_string());
        frontier_args.push(
            runtime
                .lfqueue_segment_sizes
                .iter()
                .map(|value| value.to_string())
                .collect::<Vec<_>>()
                .join(","),
        );
    }
    if !runtime.wcq_capacities.is_empty() {
        frontier_args.push("--wcq-capacities".to_string());
        frontier_args.push(
            runtime
                .wcq_capacities
                .iter()
                .map(|value| value.to_string())
                .collect::<Vec<_>>()
                .join(","),
        );
    }
    frontier_args.extend(runtime.frontier_args.clone());
    if runtime.dry_run {
        frontier_args.push("--dry-run".to_string());
    }
    frontier_args
}

fn queue_list_includes_fastfifo(queues: &[String]) -> bool {
    queues.iter().any(|queue| {
        matches!(
            queue.trim().to_ascii_lowercase().as_str(),
            "fastfifo" | "fast-fifo" | "bbq"
        )
    })
}

fn queue_list_includes_lfqueue(queues: &[String]) -> bool {
    queues.iter().any(|queue| {
        matches!(
            queue.trim().to_ascii_lowercase().as_str(),
            "lfqueue" | "lf-queue" | "lscq" | "scq"
        )
    })
}

fn queue_list_includes_wcq(queues: &[String]) -> bool {
    queues.iter().any(|queue| {
        matches!(
            queue.trim().to_ascii_lowercase().as_str(),
            "wcq" | "w-cq" | "wait-free-cq" | "wait-free-queue"
        )
    })
}

fn cargo_feature_arg(runtime: &FleetRuntime) -> String {
    let mut features = vec!["bench_registry"];
    if queue_list_includes_fastfifo(&runtime.queues) {
        features.push("bench_fastfifo");
    }
    if queue_list_includes_lfqueue(&runtime.queues) {
        features.push("bench_lfqueue");
    }
    if queue_list_includes_wcq(&runtime.queues) {
        features.push("bench_wcq");
    }
    features.join(",")
}

fn build_local_complete_cmd(runtime: &FleetRuntime, machine: &ResolvedMachine) -> Vec<String> {
    let runs_dir = runtime.runs_dir.display().to_string();
    let features = cargo_feature_arg(runtime);
    let mut cmd = vec![
        "cargo".to_string(),
        "run".to_string(),
        "--quiet".to_string(),
        "--release".to_string(),
        "--features".to_string(),
        features,
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
    let features = cargo_feature_arg(runtime);
    let mut inner = vec![
        "cargo".to_string(),
        "run".to_string(),
        "--quiet".to_string(),
        "--release".to_string(),
        "--features".to_string(),
        features,
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
        let path_dep_syncs =
            discover_path_dependency_syncs(&runtime.repo_root, &machine.remote_repo_dir)?;
        println!(
            "  syncing repo to {}:{}",
            machine.host, machine.remote_repo_dir
        );
        let cmd = build_sync_cmd(machine);
        let code = run_cmd(&cmd, &runtime.repo_root, runtime.dry_run)?;
        if code != 0 {
            return Err(format!("repo sync failed with exit code {code}"));
        }
        for sync in &path_dep_syncs {
            let label = sync
                .local_dir
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("<unknown>");
            println!(
                "  syncing cargo path dependency {} to {}:{}",
                label, machine.host, sync.remote_dir
            );
            let cmd = build_path_dep_sync_cmd(machine, sync);
            let code = run_cmd(&cmd, &runtime.repo_root, runtime.dry_run)?;
            if code != 0 {
                return Err(format!(
                    "cargo path dependency sync failed for {} with exit code {code}",
                    sync.local_dir.display()
                ));
            }
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

fn build_path_dep_sync_cmd(machine: &ResolvedMachine, sync: &PathDependencySync) -> Vec<String> {
    let remote_parent = remote_parent_dir(&sync.remote_dir);
    vec![
        "rsync".to_string(),
        "-avz".to_string(),
        "--delete".to_string(),
        "--exclude=.git/".to_string(),
        "--exclude=target/".to_string(),
        "--rsync-path".to_string(),
        format!("mkdir -p {} && rsync", remote_cd_expr(&remote_parent)),
        format!("{}/", sync.local_dir.display()),
        format!(
            "{}:{}/",
            machine.host,
            sync.remote_dir.trim_end_matches('/')
        ),
    ]
}

fn remote_parent_dir(path: &str) -> String {
    let trimmed = path.trim().trim_end_matches('/');
    if trimmed.is_empty() || trimmed == "~" || trimmed == "/" {
        return trimmed.to_string();
    }
    match trimmed.rfind('/') {
        Some(0) => "/".to_string(),
        Some(idx) => trimmed[..idx].to_string(),
        None => ".".to_string(),
    }
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
    let fastfifo_block_sizes = defaults
        .fastfifo_block_sizes
        .clone()
        .unwrap_or_else(|| vec![64, 256, 1024, 4096]);
    let lfqueue_segment_sizes = defaults
        .lfqueue_segment_sizes
        .clone()
        .unwrap_or_else(|| vec![32, 256, 1024]);
    let wcq_capacities = defaults
        .wcq_capacities
        .clone()
        .unwrap_or_else(|| vec![4096, 65536, 1048576]);

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
        fastfifo_block_sizes,
        lfqueue_segment_sizes,
        wcq_capacities,
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
    use std::time::{SystemTime, UNIX_EPOCH};

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
            fastfifo_block_sizes: vec![64, 256, 1024, 4096],
            lfqueue_segment_sizes: vec![32, 256, 1024],
            wcq_capacities: vec![4096, 65536, 1048576],
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

    fn temp_root(name: &str) -> PathBuf {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        std::env::temp_dir().join(format!("ubq_full_bench_fleet_{name}_{stamp}"))
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
    fn local_frontier_command_enables_fastfifo_feature_when_selected() {
        let mut runtime = runtime();
        runtime.queues.push("fastfifo".to_string());
        let machine = machine_local();
        let cmd = build_local_complete_cmd(&runtime, &machine);
        assert!(cmd.contains(&"bench_registry,bench_fastfifo".to_string()));
        assert!(cmd.contains(&"--fastfifo-block-sizes".to_string()));
        assert!(cmd.contains(&"64,256,1024,4096".to_string()));
    }

    #[test]
    fn local_frontier_command_enables_publication_queue_features_when_selected() {
        let mut runtime = runtime();
        runtime.queues.push("lfqueue".to_string());
        runtime.queues.push("wcq".to_string());
        let machine = machine_local();
        let cmd = build_local_complete_cmd(&runtime, &machine);
        assert!(cmd.contains(&"bench_registry,bench_lfqueue,bench_wcq".to_string()));
        assert!(cmd.contains(&"--lfqueue-segment-sizes".to_string()));
        assert!(cmd.contains(&"32,256,1024".to_string()));
        assert!(cmd.contains(&"--wcq-capacities".to_string()));
        assert!(cmd.contains(&"4096,65536,1048576".to_string()));
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
    fn path_dependency_sync_creates_remote_parent() {
        let machine = machine_remote();
        let cmd = build_path_dep_sync_cmd(
            &machine,
            &PathDependencySync {
                local_dir: PathBuf::from("/tmp/FastFifo"),
                remote_dir: "~/UBQ/vendor/FastFifo".to_string(),
            },
        );
        assert!(cmd.contains(&"--rsync-path".to_string()));
        assert!(cmd.contains(&"mkdir -p \"$HOME/UBQ/vendor\" && rsync".to_string()));
    }

    #[test]
    fn remote_parent_dir_handles_common_forms() {
        assert_eq!(remote_parent_dir("~/UBQ/vendor/FastFifo"), "~/UBQ/vendor");
        assert_eq!(
            remote_parent_dir("/srv/bench/UBQ/vendor/FastFifo"),
            "/srv/bench/UBQ/vendor"
        );
        assert_eq!(remote_parent_dir("FastFifo"), ".");
    }

    #[test]
    fn remote_dependency_paths_preserve_relative_layout() {
        assert_eq!(
            resolve_remote_dependency_dir("~/UBQ", "../FastFifo").expect("resolve"),
            "~/FastFifo"
        );
        assert_eq!(
            resolve_remote_dependency_dir("/srv/bench/UBQ", "../FastFifo").expect("resolve"),
            "/srv/bench/FastFifo"
        );
        assert_eq!(
            resolve_remote_dependency_dir("UBQ", "../FastFifo").expect("resolve"),
            "FastFifo"
        );
    }

    #[test]
    fn discover_path_dependency_syncs_follow_recursive_manifests() {
        let root = temp_root("path_syncs");
        let repo = root.join("UBQ");
        let dep_a = repo.join("vendor").join("FastFifo");
        let dep_b = dep_a.join("fastfifoprocmacro");
        let dep_c = repo.join("vendor").join("AtomicList");

        fs::create_dir_all(repo.join("src")).expect("mkdir repo");
        fs::create_dir_all(dep_a.join("src")).expect("mkdir dep_a");
        fs::create_dir_all(dep_b.join("src")).expect("mkdir dep_b");
        fs::create_dir_all(dep_c.join("src")).expect("mkdir dep_c");

        fs::write(
            repo.join("Cargo.toml"),
            r#"[package]
name = "ubq"
version = "0.1.0"
edition = "2024"

[dependencies]
fastfifo = { path = "vendor/FastFifo", optional = true }
"#,
        )
        .expect("write repo manifest");
        fs::write(repo.join("src/lib.rs"), "").expect("write repo lib");

        fs::write(
            dep_a.join("Cargo.toml"),
            r#"[package]
name = "fastfifo"
version = "0.1.0"
edition = "2024"

[dependencies]
fastfifoprocmacro = { path = "./fastfifoprocmacro" }
atomic_list = { path = "../AtomicList" }
"#,
        )
        .expect("write dep_a manifest");
        fs::write(dep_a.join("src/lib.rs"), "").expect("write dep_a lib");

        fs::write(
            dep_b.join("Cargo.toml"),
            r#"[package]
name = "fastfifoprocmacro"
version = "0.1.0"
edition = "2024"
"#,
        )
        .expect("write dep_b manifest");
        fs::write(dep_b.join("src/lib.rs"), "").expect("write dep_b lib");

        fs::write(
            dep_c.join("Cargo.toml"),
            r#"[package]
name = "atomic_list"
version = "0.1.0"
edition = "2024"
"#,
        )
        .expect("write dep_c manifest");
        fs::write(dep_c.join("src/lib.rs"), "").expect("write dep_c lib");

        let syncs = discover_path_dependency_syncs(&repo, "~/UBQ").expect("discover syncs");
        assert_eq!(
            syncs.len(),
            2,
            "nested deps inside FastFifo should be covered"
        );
        assert!(syncs.iter().any(|sync| {
            sync.local_dir == dep_a.canonicalize().expect("canon dep_a")
                && sync.remote_dir == "~/UBQ/vendor/FastFifo"
        }));
        assert!(syncs.iter().any(|sync| {
            sync.local_dir == dep_c.canonicalize().expect("canon dep_c")
                && sync.remote_dir == "~/UBQ/vendor/AtomicList"
        }));
        assert!(
            !syncs
                .iter()
                .any(|sync| sync.local_dir == dep_b.canonicalize().expect("canon dep_b")),
            "FastFifo sync should already include fastfifoprocmacro"
        );

        let _ = fs::remove_dir_all(&root);
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
