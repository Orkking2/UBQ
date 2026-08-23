//! Worst-case public-API stress test for recycled-block ABA failures.
//!
//! Run with:
//!
//! ```text
//! cargo test --test recycling_aba -- --ignored --nocapture
//! ```
//!
//! Optional tuning:
//!
//! ```text
//! UBQ_ABA_TRIALS=1000 UBQ_ABA_ITEMS=8192 UBQ_ABA_CONSUMERS=64 \
//! UBQ_ABA_IN_FLIGHT=2 UBQ_ABA_TIMEOUT_MS=3000 UBQ_ABA_STOP_AFTER_OBSERVED=100 \
//! cargo test --test recycling_aba -- --ignored --nocapture
//! ```

use std::{
    env,
    process::{Command, Stdio},
    sync::{
        Arc, Barrier,
        atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

use ubq::{UBQ, backoff};

const CHILD_ENV: &str = "UBQ_ABA_CHILD";
const DEFAULT_TRIALS: usize = 50;
const DEFAULT_ITEMS: usize = 16_384;
const DEFAULT_IN_FLIGHT: usize = 2;
const DEFAULT_TIMEOUT_MS: u64 = 1000;
const DEFAULT_STOP_AFTER_OBSERVED: usize = 10;

#[test]
#[ignore = "statistical stress test; run explicitly with --ignored --nocapture"]
fn recycled_block_aba_worst_case_frequency() {
    let trials = env_usize("UBQ_ABA_TRIALS", DEFAULT_TRIALS);
    let items = env_usize("UBQ_ABA_ITEMS", DEFAULT_ITEMS);
    let consumers = env_usize("UBQ_ABA_CONSUMERS", default_consumers());
    let in_flight = env_usize("UBQ_ABA_IN_FLIGHT", DEFAULT_IN_FLIGHT).max(1);
    let timeout = Duration::from_millis(env_u64("UBQ_ABA_TIMEOUT_MS", DEFAULT_TIMEOUT_MS));
    let stop_after_observed = env_usize("UBQ_ABA_STOP_AFTER_OBSERVED", DEFAULT_STOP_AFTER_OBSERVED);
    let progress_every = (trials / 10).max(1);

    let mut clean = 0;
    let mut corrupt = 0;
    let mut timed_out = 0;
    let mut completed = 0;
    let mut first_failure = None;

    let started = Instant::now();

    for trial in 0..trials {
        match run_child_trial(trial, items, consumers, in_flight, timeout) {
            TrialOutcome::Clean => clean += 1,
            TrialOutcome::Corrupt(output) => {
                corrupt += 1;
                first_failure.get_or_insert_with(|| format!("trial {trial} failed:\n{output}"));
            }
            TrialOutcome::TimedOut(output) => {
                timed_out += 1;
                first_failure.get_or_insert_with(|| format!("trial {trial} timed out:\n{output}"));
            }
        }

        completed = trial + 1;
        let observed = corrupt + timed_out;

        if completed % progress_every == 0 || observed == stop_after_observed {
            println!(
                "recycled-block ABA stress progress: completed={completed}/{trials}, clean={clean}, corrupt={corrupt}, timed_out={timed_out}",
            );
        }

        if observed >= stop_after_observed {
            break;
        }
    }

    let observed = corrupt + timed_out;
    let attempted_items = completed * items;
    let elapsed = started.elapsed();

    println!(
        "recycled-block ABA stress: completed_trials={completed}, configured_trials={trials}, clean={clean}, corrupt={corrupt}, timed_out={timed_out}, observed_rate={observed}/{completed}, items_per_trial={items}, attempted_items={attempted_items}, consumers={consumers}, in_flight={in_flight}, timeout_ms={}, elapsed={elapsed:?}",
        timeout.as_millis(),
    );

    if let Some(failure) = first_failure {
        panic!("{failure}");
    }
}

#[test]
#[ignore = "child process for recycled_block_aba_worst_case_frequency"]
fn recycled_block_aba_child_process() {
    if env::var_os(CHILD_ENV).is_none() {
        return;
    }

    let items = env_usize("UBQ_ABA_ITEMS", DEFAULT_ITEMS);
    let consumers = env_usize("UBQ_ABA_CONSUMERS", default_consumers());
    let in_flight = env_usize("UBQ_ABA_IN_FLIGHT", DEFAULT_IN_FLIGHT).max(1);

    run_public_api_worst_case(items, consumers, in_flight);
}

fn run_public_api_worst_case(items: usize, consumers: usize, in_flight: usize) {
    type Q = UBQ<u64, backoff::Yield>;

    let q = Arc::new(Q::new());
    let seen = Arc::new((0..items).map(|_| AtomicU8::new(0)).collect::<Vec<_>>());
    let consumed = Arc::new(AtomicUsize::new(0));
    let duplicates = Arc::new(AtomicUsize::new(0));
    let out_of_range = Arc::new(AtomicUsize::new(0));
    let done = Arc::new(AtomicBool::new(false));
    let start = Arc::new(Barrier::new(consumers + 2));

    let producer = {
        let q = Arc::clone(&q);
        let consumed = Arc::clone(&consumed);
        let done = Arc::clone(&done);
        let start = Arc::clone(&start);

        thread::spawn(move || {
            start.wait();

            for value in 0..items {
                while value.saturating_sub(consumed.load(Ordering::Acquire)) >= in_flight {
                    thread::yield_now();
                }

                q.push(value as u64);
                thread::yield_now();
            }

            while consumed.load(Ordering::Acquire) < items {
                thread::yield_now();
            }
            done.store(true, Ordering::Release);
        })
    };

    let mut consumer_threads = Vec::with_capacity(consumers);
    for _ in 0..consumers {
        let q = Arc::clone(&q);
        let seen = Arc::clone(&seen);
        let consumed = Arc::clone(&consumed);
        let duplicates = Arc::clone(&duplicates);
        let out_of_range = Arc::clone(&out_of_range);
        let done = Arc::clone(&done);
        let start = Arc::clone(&start);

        consumer_threads.push(thread::spawn(move || {
            start.wait();

            loop {
                match q.pop() {
                    Some(value) if (value as usize) < seen.len() => {
                        let previous = seen[value as usize].fetch_add(1, Ordering::AcqRel);
                        if previous != 0 {
                            duplicates.fetch_add(1, Ordering::Relaxed);
                        }
                        consumed.fetch_add(1, Ordering::Release);
                    }
                    Some(_) => {
                        out_of_range.fetch_add(1, Ordering::Relaxed);
                        consumed.fetch_add(1, Ordering::Release);
                    }
                    None if done.load(Ordering::Acquire) => break,
                    None => thread::yield_now(),
                }
            }
        }));
    }

    start.wait();

    producer.join().expect("producer panicked");

    for thread in consumer_threads {
        thread.join().expect("consumer panicked");
    }

    let duplicate_count = duplicates.load(Ordering::Relaxed);
    let out_of_range_count = out_of_range.load(Ordering::Relaxed);
    let consumed_count = consumed.load(Ordering::Acquire);
    let missing = seen
        .iter()
        .enumerate()
        .filter(|(_, seen)| seen.load(Ordering::Acquire) == 0)
        .map(|(index, _)| index)
        .take(10)
        .collect::<Vec<_>>();

    assert_eq!(duplicate_count, 0, "duplicate item count");
    assert_eq!(out_of_range_count, 0, "out-of-range item count");
    assert_eq!(consumed_count, items, "consumed item count");
    assert!(missing.is_empty(), "missing items: first 10 = {missing:?}");
}

enum TrialOutcome {
    Clean,
    Corrupt(String),
    TimedOut(String),
}

fn run_child_trial(
    trial: usize,
    items: usize,
    consumers: usize,
    in_flight: usize,
    timeout: Duration,
) -> TrialOutcome {
    let mut child = Command::new(env::current_exe().expect("current test binary path"))
        .arg("--exact")
        .arg("recycled_block_aba_child_process")
        .arg("--ignored")
        .arg("--nocapture")
        .env(CHILD_ENV, "1")
        .env("UBQ_ABA_TRIAL_INDEX", trial.to_string())
        .env("UBQ_ABA_ITEMS", items.to_string())
        .env("UBQ_ABA_CONSUMERS", consumers.to_string())
        .env("UBQ_ABA_IN_FLIGHT", in_flight.to_string())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn child stress trial");

    let started = Instant::now();

    loop {
        if child.try_wait().expect("poll child stress trial").is_some() {
            let output = child.wait_with_output().expect("collect child output");
            let text = child_output(output.stdout, output.stderr);
            return if output.status.success() {
                TrialOutcome::Clean
            } else {
                TrialOutcome::Corrupt(text)
            };
        }

        if started.elapsed() >= timeout {
            let _ = child.kill();
            let output = child.wait_with_output().expect("collect timed-out child");
            return TrialOutcome::TimedOut(child_output(output.stdout, output.stderr));
        }

        thread::sleep(Duration::from_millis(5));
    }
}

fn child_output(stdout: Vec<u8>, stderr: Vec<u8>) -> String {
    let mut out = String::new();
    out.push_str(&String::from_utf8_lossy(&stdout));
    out.push_str(&String::from_utf8_lossy(&stderr));
    out
}

fn default_consumers() -> usize {
    thread::available_parallelism()
        .map(|threads| threads.get() * 4)
        .unwrap_or(32)
        .clamp(16, 64)
}

fn env_usize(name: &str, default: usize) -> usize {
    env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn env_u64(name: &str, default: u64) -> u64 {
    env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}
