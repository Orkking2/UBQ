//! Keep a stuck or crashing queue from taking down the Cargo test harness.
use std::{
    env,
    io::Read,
    process::{Child, Command, ExitStatus, Stdio},
    thread,
    time::{Duration, Instant},
};

pub const WORKER_ENV: &str = "UBQ_V8_TEST_ISOLATED";

struct Worker(Child);

impl Drop for Worker {
    fn drop(&mut self) {
        // Also clean up when a polling/read assertion unwinds.
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

struct Outcome {
    status: ExitStatus,
    timed_out: bool,
    output: String,
}

fn execute(name: &str, timeout: Duration) -> Outcome {
    let mut worker = Worker(
        Command::new(env::current_exe().unwrap())
            .args(["--exact", name, "--nocapture", "--test-threads=1"])
            .env(WORKER_ENV, name)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn isolated test"),
    );
    let mut stdout = worker.0.stdout.take().unwrap();
    let mut stderr = worker.0.stderr.take().unwrap();
    // Drain both pipes while the worker runs, so panic output cannot block it.
    let stdout = thread::spawn(move || {
        let mut bytes = Vec::new();
        stdout.read_to_end(&mut bytes).unwrap();
        bytes
    });
    let stderr = thread::spawn(move || {
        let mut bytes = Vec::new();
        stderr.read_to_end(&mut bytes).unwrap();
        bytes
    });
    let started = Instant::now();
    let (status, timed_out) = loop {
        if let Some(status) = worker.0.try_wait().expect("poll isolated test") {
            break (status, false);
        }
        if started.elapsed() >= timeout {
            let _ = worker.0.kill();
            break (worker.0.wait().expect("reap timed-out test"), true);
        }
        thread::sleep(Duration::from_millis(10));
    };
    let output = String::from_utf8_lossy(&stdout.join().unwrap()).into_owned()
        + &String::from_utf8_lossy(&stderr.join().unwrap());
    Outcome {
        status,
        timed_out,
        output,
    }
}

/// Returns true in the parent after checking the child; false in the worker.
pub fn run(name: &str) -> bool {
    if env::var(WORKER_ENV).as_deref() == Ok(name) {
        return false;
    }
    let outcome = execute(name, Duration::from_secs(20));
    assert!(
        !outcome.timed_out,
        "{name} timed out after 20s\n{}",
        outcome.output
    );
    assert!(
        outcome.status.success(),
        "{name} exited with {}\n{}",
        outcome.status,
        outcome.output
    );
    assert!(
        outcome
            .output
            .contains("test result: ok. 1 passed; 0 failed; 0 ignored;"),
        "{name} did not execute exactly one test\n{}",
        outcome.output
    );
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    fn name(function: &str) -> String {
        format!("{}::{function}", module_path!().split_once("::").unwrap().1)
    }

    #[test]
    fn a_stuck_worker_is_killed_and_reaped() {
        let name = name("a_stuck_worker_is_killed_and_reaped");
        if env::var(WORKER_ENV).as_deref() == Ok(&name) {
            thread::sleep(Duration::from_secs(60));
            return;
        }
        let result = execute(&name, Duration::from_millis(200));
        assert!(result.timed_out);
        assert!(!result.status.success());
    }

    #[test]
    fn a_panicking_worker_reports_its_assertion() {
        let name = name("a_panicking_worker_reports_its_assertion");
        if env::var(WORKER_ENV).as_deref() == Ok(&name) {
            panic!("deliberate worker assertion");
        }
        let result = execute(&name, Duration::from_secs(5));
        assert!(!result.timed_out);
        assert!(!result.status.success());
        assert!(result.output.contains("deliberate worker assertion"));
    }

    #[test]
    fn a_passing_worker_is_accepted() {
        run(&name("a_passing_worker_is_accepted"));
    }
}
