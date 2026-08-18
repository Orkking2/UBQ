#![cfg(feature = "bench_registry")]

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

fn collect_json(path: &Path, files: &mut Vec<PathBuf>) {
    let Ok(entries) = fs::read_dir(path) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect_json(&path, files);
        } else if path
            .extension()
            .is_some_and(|extension| extension == "json")
        {
            files.push(path);
        }
    }
}

#[test]
fn hard_timeout_kills_worker_and_next_job_completes() {
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    let root = std::env::temp_dir().join(format!("ubq_worker_timeout_{stamp}"));
    let runs = root.join("runs");
    fs::create_dir_all(&runs).expect("create runs directory");

    let started = Instant::now();
    let output = Command::new(env!("CARGO_BIN_EXE_bench_grid"))
        .env("UBQ_BENCH_JOB_TIMEOUT_SECS", "1")
        .env("UBQ_BENCH_WORKER_TEST_STALL_QUEUE", "concurrent-queue")
        .env("UBQ_BENCH_WORKER_TEST_STALL_MS", "10000")
        .args([
            "--machine-label",
            "worker-timeout-test",
            "--runs-dir",
            runs.to_str().expect("utf8 runs path"),
            "--parallelism",
            "2",
            "--allow-unpinned",
            "--job-timeout-secs",
            "1",
            "--queues",
            "segqueue,concurrent-queue",
            "--scenarios",
            "1p1c",
            "--items-per-producer",
            "1",
            "--repeats",
            "1",
            "--throughput-warmup-ms",
            "1",
            "--throughput-phase-ms",
            "1",
            "--throughput-pilot-ms",
            "1",
            "--throughput-max-round-items",
            "4096",
        ])
        .output()
        .expect("run bench_grid");

    assert!(
        output.status.success(),
        "bench_grid failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        started.elapsed().as_secs() < 8,
        "the stalled worker was not killed promptly"
    );

    let mut files = Vec::new();
    collect_json(&runs, &mut files);
    assert!(!files.is_empty(), "benchmark produced no snapshots");
    let mut concurrent_status = None;
    let mut segqueue_completed = false;
    for path in files {
        let value: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(path).expect("read snapshot"))
                .expect("parse snapshot");
        assert_eq!(value["schema_version"], ubq::bench_harness::RUN_SCHEMA_VERSION);
        for record in value["results"].as_array().expect("results") {
            match record["queue"].as_str() {
                Some("segqueue") => {
                    segqueue_completed = record
                        .get("status")
                        .is_none_or(|status| status == "completed");
                }
                Some("concurrent-queue") => {
                    concurrent_status = record["status"].as_str().map(str::to_string);
                    assert_eq!(record["consumed_items"], 0);
                }
                _ => {}
            }
        }
    }
    assert_eq!(concurrent_status.as_deref(), Some("timed_out"));
    assert!(segqueue_completed);

    let retry = Command::new(env!("CARGO_BIN_EXE_bench_grid"))
        .args([
            "--machine-label",
            "worker-timeout-test",
            "--runs-dir",
            runs.to_str().expect("utf8 runs path"),
            "--parallelism",
            "2",
            "--allow-unpinned",
            "--job-timeout-secs",
            "1",
            "--queues",
            "segqueue,concurrent-queue",
            "--scenarios",
            "1p1c",
            "--items-per-producer",
            "1",
            "--repeats",
            "1",
            "--throughput-warmup-ms",
            "1",
            "--throughput-phase-ms",
            "1",
            "--throughput-pilot-ms",
            "1",
            "--throughput-max-round-items",
            "4096",
        ])
        .output()
        .expect("retry bench_grid");
    assert!(retry.status.success());
    assert!(
        String::from_utf8_lossy(&retry.stdout).contains("4 cached, 1 pending"),
        "completed samples were not reused or the timeout was not retried"
    );

    let _ = fs::remove_dir_all(root);
}

/// Deterministic proof that the deliberately pathological `naive-faa-queue`
/// baseline is correctly wired into the timeout/DNF path, independent of
/// whether its real algorithm happens to livelock on any given run: this
/// uses the same stall-injection hook the `concurrent-queue` case above
/// uses, rather than relying on real contention timing.
#[test]
fn naive_faa_queue_stall_times_out() {
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    let root = std::env::temp_dir().join(format!("ubq_naive_faa_timeout_{stamp}"));
    let runs = root.join("runs");
    fs::create_dir_all(&runs).expect("create runs directory");

    let output = Command::new(env!("CARGO_BIN_EXE_bench_grid"))
        .env("UBQ_BENCH_JOB_TIMEOUT_SECS", "1")
        .env("UBQ_BENCH_WORKER_TEST_STALL_QUEUE", "naive-faa-queue")
        .env("UBQ_BENCH_WORKER_TEST_STALL_MS", "10000")
        .args([
            "--machine-label",
            "naive-faa-timeout-test",
            "--runs-dir",
            runs.to_str().expect("utf8 runs path"),
            "--parallelism",
            "2",
            "--allow-unpinned",
            "--job-timeout-secs",
            "1",
            "--queues",
            "segqueue,naive-faa-queue",
            "--scenarios",
            "1p1c",
            "--items-per-producer",
            "1",
            "--repeats",
            "1",
            "--throughput-warmup-ms",
            "1",
            "--throughput-phase-ms",
            "1",
            "--throughput-pilot-ms",
            "1",
            "--throughput-max-round-items",
            "4096",
        ])
        .output()
        .expect("run bench_grid");

    assert!(
        output.status.success(),
        "bench_grid failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let mut files = Vec::new();
    collect_json(&runs, &mut files);
    assert!(!files.is_empty(), "benchmark produced no snapshots");
    let mut naive_faa_status = None;
    let mut segqueue_completed = false;
    for path in files {
        let value: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(path).expect("read snapshot"))
                .expect("parse snapshot");
        for record in value["results"].as_array().expect("results") {
            match record["queue"].as_str() {
                Some("segqueue") => {
                    segqueue_completed = record
                        .get("status")
                        .is_none_or(|status| status == "completed");
                }
                Some("naive-faa-queue") => {
                    naive_faa_status = record["status"].as_str().map(str::to_string);
                    assert_eq!(record["consumed_items"], 0);
                }
                _ => {}
            }
        }
    }
    assert_eq!(naive_faa_status.as_deref(), Some("timed_out"));
    assert!(segqueue_completed);

    let _ = fs::remove_dir_all(root);
}

#[test]
fn crashed_worker_records_failure_and_restarts() {
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    let root = std::env::temp_dir().join(format!("ubq_worker_crash_{stamp}"));
    let runs = root.join("runs");
    fs::create_dir_all(&runs).expect("create runs directory");

    let output = Command::new(env!("CARGO_BIN_EXE_bench_grid"))
        .env("UBQ_BENCH_WORKER_TEST_CRASH_QUEUE", "concurrent-queue")
        .args([
            "--machine-label",
            "worker-crash-test",
            "--runs-dir",
            runs.to_str().expect("utf8 runs path"),
            "--parallelism",
            "2",
            "--allow-unpinned",
            "--queues",
            "segqueue,concurrent-queue",
            "--scenarios",
            "1p1c",
            "--items-per-producer",
            "1",
            "--repeats",
            "1",
            "--throughput-warmup-ms",
            "1",
            "--throughput-phase-ms",
            "1",
            "--throughput-pilot-ms",
            "1",
            "--throughput-max-round-items",
            "4096",
        ])
        .output()
        .expect("run bench_grid");
    assert!(
        output.status.success(),
        "bench_grid failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let mut files = Vec::new();
    collect_json(&runs, &mut files);
    let mut concurrent_failed = false;
    let mut segqueue_completed = false;
    for path in files {
        let value: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(path).expect("read snapshot"))
                .expect("parse snapshot");
        for record in value["results"].as_array().expect("results") {
            match record["queue"].as_str() {
                Some("concurrent-queue") => {
                    concurrent_failed = record["status"] == "failed";
                }
                Some("segqueue") => {
                    segqueue_completed = record
                        .get("status")
                        .is_none_or(|status| status == "completed");
                }
                _ => {}
            }
        }
    }
    assert!(concurrent_failed);
    assert!(segqueue_completed, "fresh worker did not run the next job");

    let _ = fs::remove_dir_all(root);
}
