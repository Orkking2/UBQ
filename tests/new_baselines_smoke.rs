//! End-to-end wiring check for the three Phase 1 benchmark baselines
//! (`mutex-vecdeque`, `ms-queue`, `naive-faa-queue`): runs each through the
//! real `bench_grid` binary on a tiny scenario and confirms every job
//! completes and records a plausible (non-zero) throughput, proving the
//! `QueueKind`/`job_factory_for_spec`/plan-expansion wiring is correct end
//! to end, not just that the underlying queue types compile and pass their
//! own unit-level smoke tests.

#![cfg(feature = "bench_registry")]

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

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
fn new_baselines_complete_a_small_grid() {
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    let root = std::env::temp_dir().join(format!("ubq_new_baselines_{stamp}"));
    let runs = root.join("runs");
    fs::create_dir_all(&runs).expect("create runs directory");

    let output = Command::new(env!("CARGO_BIN_EXE_bench_grid"))
        .args([
            "--machine-label",
            "new-baselines-test",
            "--runs-dir",
            runs.to_str().expect("utf8 runs path"),
            "--parallelism",
            "4",
            "--allow-unpinned",
            "--job-timeout-secs",
            "30",
            "--queues",
            "mutex-vecdeque,ms-queue,naive-faa-queue",
            "--scenarios",
            "1p1c,4p4c",
            "--items-per-producer",
            "2000",
            "--repeats",
            "1",
            "--throughput-warmup-ms",
            "20",
            "--throughput-phase-ms",
            "20",
            "--throughput-pilot-ms",
            "20",
            "--throughput-max-round-items",
            "65536",
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

    let expected_queues = ["mutex-vecdeque", "ms-queue", "naive-faa-queue"];
    let mut seen_completed = std::collections::BTreeSet::new();
    for path in files {
        let value: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(path).expect("read snapshot"))
                .expect("parse snapshot");
        for record in value["results"].as_array().expect("results") {
            let Some(queue) = record["queue"].as_str() else {
                continue;
            };
            if !expected_queues.contains(&queue) {
                continue;
            }
            let completed = record
                .get("status")
                .is_none_or(|status| status == "completed");
            assert!(completed, "job for {queue} did not complete: {record}");
            if record["mode"] == "throughput" {
                let ops_per_sec = record
                    .get("ops_per_sec")
                    .and_then(|value| value.as_f64())
                    .unwrap_or(0.0);
                assert!(
                    ops_per_sec > 0.0,
                    "job for {queue} completed but reported non-positive throughput: {record}"
                );
            }
            seen_completed.insert(queue.to_string());
        }
    }

    for queue in expected_queues {
        assert!(
            seen_completed.contains(queue),
            "no completed record found for {queue}"
        );
    }

    let _ = fs::remove_dir_all(root);
}
