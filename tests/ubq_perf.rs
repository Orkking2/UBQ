mod logger;

use logger::{init_trace_to_file, spawn_ubq_tracer};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::Duration,
};

#[path = "../benches/ubq_perf.rs"]
mod perf;
use perf::*;
use std::time::Instant;
use crossbeam_channel as cb;

macro_rules! perf_tests {
    ($($bench_method:ident),+ $(,)?) => {
        paste::paste! {
            $(
                #[test]
                fn [<test_ $bench_method>]() {
                    let test_name = stringify!([<test_ $bench_method>]);
                    let log_path = init_trace_to_file(test_name)
                        .unwrap_or_else(|err| panic!("failed to initialise trace file for {test_name}: {err}"));
                    eprintln!("logging traces for {test_name} to {}", log_path.display());
                    let ubq = new_ubq();
                    let kill_switch = spawn_ubq_tracer(ubq.clone());
                    $bench_method(ubq);
                    drop(kill_switch);
                }
            )+
        }
    }
}

perf_tests! {
    bench_ubq_spsc_fill_and_empty,
    bench_ubq_spsc_fill_and_empty_simultaneous,
    bench_ubq_mpmc_fill_and_empty_prod_eq_cons_from_new,
}

fn bench_ubq_mpmc_fill_and_empty_prod_eq_cons_from_new(ubq: ubq::UBQ<usize>) {
    bench_ubq_mpmc_fill_and_empty_prod_eq_cons(new_pc(ubq));
}

#[test]
#[ignore]
fn stress_bench_ubq_spsc_fill_and_empty_simultaneous() {
    let iters: usize = std::env::var("UBQ_STRESS_ITERS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(500);
    let timeout_ms: u64 = std::env::var("UBQ_STRESS_TIMEOUT_MS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1000);
    let timeout = Duration::from_millis(timeout_ms);

    for i in 0..iters {
        let ubq = new_ubq();
        let (done_tx, done_rx) = cb::bounded::<()>(1);

        let handle = thread::spawn(move || {
            bench_ubq_spsc_fill_and_empty_simultaneous(ubq);
            let _ = done_tx.send(());
        });

        let start = Instant::now();
        match done_rx.recv_timeout(timeout) {
            Ok(()) => {
                handle.join().unwrap();
            }
            Err(_) => {
                panic!(
                    "iteration {i} exceeded {:?} (elapsed {:?})",
                    timeout,
                    start.elapsed()
                );
            }
        }
    }
}
