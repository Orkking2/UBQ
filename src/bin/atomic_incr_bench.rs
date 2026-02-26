use std::{
    env,
    fmt::Write as _,
    fs,
    path::PathBuf,
    process,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

use crossbeam_utils::Backoff;

#[derive(Clone, Copy)]
struct AtomicIncrMethod {
    name: &'static str,
    f: fn(&AtomicUsize),
}

#[derive(Clone, Copy)]
struct AtomicIncrMeasurement {
    method: &'static str,
    threads: usize,
    increments_per_thread: usize,
    elapsed: Duration,
}

impl AtomicIncrMeasurement {
    fn total_increments(self) -> usize {
        self.threads
            .checked_mul(self.increments_per_thread)
            .expect("total increments overflowed usize")
    }
}

#[derive(Clone)]
struct OnceConfig {
    threads: usize,
    increments: usize,
}

#[derive(Clone)]
struct SweepConfig {
    thread_counts: Vec<usize>,
    increments: Vec<usize>,
    repeats: usize,
    out_path: PathBuf,
}

fn fmt_num(num: usize) -> String {
    let s = num.to_string();
    let len = s.len();

    if len <= 3 {
        return s;
    }

    let mut out = String::with_capacity(len + ((len - 1) / 3));
    let mut i = len % 3;

    if i != 0 {
        out.push_str(&s[..i]);
        out.push(',');
    }

    while i < len {
        out.push_str(&s[i..i + 3]);
        i += 3;

        if i < len {
            out.push(',');
        }
    }

    out
}

fn atomic_incr_cas(a: &AtomicUsize) {
    let mut old = a.load(Ordering::Relaxed);

    loop {
        let new = old + 1;

        match a.compare_exchange(old, new, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => break,
            Err(real) => old = real,
        }
    }
}

fn atomic_incr_casb(a: &AtomicUsize) {
    let backoff = Backoff::new();

    let mut old = a.load(Ordering::Acquire);
    loop {
        let new = old + 1;

        match a.compare_exchange(old, new, Ordering::SeqCst, Ordering::Acquire) {
            Ok(_) => break,
            Err(real) => {
                old = real;
            }
        }

        backoff.spin();
    }
}

fn atomic_incr_faa(a: &AtomicUsize) {
    a.fetch_add(1, Ordering::Relaxed);
}

fn atomic_incr_max(a: &AtomicUsize) {
    let mut old = a.load(Ordering::Relaxed);

    loop {
        let new = old + 1;

        let real = a.fetch_max(new, Ordering::Relaxed);

        if real == old {
            break;
        } else {
            old = real;
        }
    }
}

fn atomic_incr_maxb(a: &AtomicUsize) {
    let backoff = Backoff::new();
    let mut old = a.load(Ordering::Relaxed);

    loop {
        let new = old + 1;

        let real = a.fetch_max(new, Ordering::Relaxed);

        if real == old {
            break;
        } else {
            old = real;
        }

        backoff.spin();
    }
}

fn atomic_incr_methods() -> [AtomicIncrMethod; 5] {
    [
        AtomicIncrMethod {
            name: "CAS",
            f: atomic_incr_cas,
        },
        AtomicIncrMethod {
            name: "CASB",
            f: atomic_incr_casb,
        },
        AtomicIncrMethod {
            name: "FAA",
            f: atomic_incr_faa,
        },
        AtomicIncrMethod {
            name: "MAX",
            f: atomic_incr_max,
        },
        AtomicIncrMethod {
            name: "MAXB",
            f: atomic_incr_maxb,
        },
    ]
}

fn measure_atomic_incr(
    threads: usize,
    increments_per_thread: usize,
    method: AtomicIncrMethod,
) -> AtomicIncrMeasurement {
    let total = threads
        .checked_mul(increments_per_thread)
        .expect("total increments overflowed usize");
    let atm = Arc::new(AtomicUsize::new(0));
    let epoch = Instant::now();

    (0..threads)
        .map(|_| {
            let atm = Arc::clone(&atm);
            let f = method.f;

            thread::spawn(move || {
                for _ in 0..increments_per_thread {
                    f(&atm);
                }
            })
        })
        .collect::<Vec<_>>()
        .into_iter()
        .for_each(|h| h.join().unwrap());

    let final_count = atm.load(Ordering::SeqCst);
    assert_eq!(final_count, total);

    AtomicIncrMeasurement {
        method: method.name,
        threads,
        increments_per_thread,
        elapsed: epoch.elapsed(),
    }
}

fn run_atomic_incr_suite(
    threads: usize,
    increments_per_thread: usize,
) -> Vec<AtomicIncrMeasurement> {
    atomic_incr_methods()
        .into_iter()
        .map(|method| measure_atomic_incr(threads, increments_per_thread, method))
        .collect()
}

fn parse_usize_token(token: &str) -> Option<usize> {
    let trimmed = token.trim();
    if trimmed.is_empty() {
        return None;
    }

    let mut cleaned = String::with_capacity(trimmed.len());
    for c in trimmed.chars() {
        if c != '_' && c != ',' {
            cleaned.push(c);
        }
    }

    if cleaned.is_empty() {
        return None;
    }

    cleaned.parse::<usize>().ok()
}

fn parse_usize_list(raw: &str, field_name: &str) -> Result<Vec<usize>, String> {
    let mut out = Vec::new();

    for token in raw.split(',') {
        let value = parse_usize_token(token)
            .ok_or_else(|| format!("invalid usize value `{token}` in `{field_name}`"))?;
        out.push(value);
    }

    if out.is_empty() {
        return Err(format!("`{field_name}` cannot be empty"));
    }

    Ok(out)
}

fn parse_env_usize_list(name: &str) -> Result<Option<Vec<usize>>, String> {
    let raw = match env::var(name) {
        Ok(v) => v,
        Err(_) => return Ok(None),
    };
    Ok(Some(parse_usize_list(&raw, name)?))
}

fn parse_env_usize(name: &str) -> Result<Option<usize>, String> {
    let raw = match env::var(name) {
        Ok(v) => v,
        Err(_) => return Ok(None),
    };
    let value = parse_usize_token(&raw)
        .ok_or_else(|| format!("invalid usize value in `{name}`: `{raw}`"))?;
    Ok(Some(value))
}

fn parse_required_usize(value: &str, flag: &str) -> Result<usize, String> {
    parse_usize_token(value).ok_or_else(|| format!("invalid usize for `{flag}`: `{value}`"))
}

fn print_usage() {
    eprintln!(
        "Atomic increment benchmark\n\
         \n\
         Usage:\n\
           cargo run --bin atomic_incr_bench -- [once] [--threads N] [--increments N]\n\
           cargo run --bin atomic_incr_bench -- sweep [--threads LIST] [--counts LIST] [--repeats N] [--out PATH]\n\
         \n\
         Commands:\n\
           once   Run a single benchmark suite (default command)\n\
           sweep  Run a sweep and write CSV for plotting (`--repeats` = runs per point)\n\
         \n\
         Examples:\n\
           cargo run --bin atomic_incr_bench -- once --threads 8 --increments 1000000\n\
           cargo run --bin atomic_incr_bench -- sweep --threads 1,2,4,8,16 --counts 1000,10000,100000,1000000 --repeats 3\n\
         \n\
         Sweep environment fallbacks (used if flags are omitted):\n\
           UBQ_ATOMIC_INCR_THREADS, UBQ_ATOMIC_INCR_COUNTS, UBQ_ATOMIC_INCR_REPEATS, UBQ_ATOMIC_INCR_CSV\n"
    );
}

fn next_value(args: &[String], i: &mut usize, flag: &str) -> Result<String, String> {
    *i += 1;
    args.get(*i)
        .cloned()
        .ok_or_else(|| format!("missing value for `{flag}`"))
}

fn parse_once_args(args: &[String]) -> Result<OnceConfig, String> {
    let mut cfg = OnceConfig {
        threads: 8,
        increments: 1_000_000,
    };

    let mut i = 0;
    while i < args.len() {
        let arg = &args[i];
        if arg == "-h" || arg == "--help" {
            print_usage();
            process::exit(0);
        } else if let Some(v) = arg.strip_prefix("--threads=") {
            cfg.threads = parse_required_usize(v, "--threads")?;
        } else if arg == "--threads" {
            let v = next_value(args, &mut i, "--threads")?;
            cfg.threads = parse_required_usize(&v, "--threads")?;
        } else if let Some(v) = arg.strip_prefix("--increments=") {
            cfg.increments = parse_required_usize(v, "--increments")?;
        } else if arg == "--increments" || arg == "--count" {
            let v = next_value(args, &mut i, arg)?;
            cfg.increments = parse_required_usize(&v, arg)?;
        } else if let Some(v) = arg.strip_prefix("--count=") {
            cfg.increments = parse_required_usize(v, "--count")?;
        } else {
            return Err(format!("unknown argument for `once`: `{arg}`"));
        }
        i += 1;
    }

    if cfg.threads == 0 {
        return Err("`--threads` must be > 0".to_string());
    }
    if cfg.increments == 0 {
        return Err("`--increments` must be > 0".to_string());
    }

    Ok(cfg)
}

fn parse_sweep_args(args: &[String]) -> Result<SweepConfig, String> {
    let mut cfg = SweepConfig {
        thread_counts: parse_env_usize_list("UBQ_ATOMIC_INCR_THREADS")?
            .unwrap_or_else(|| vec![1, 2, 4, 8, 16]),
        increments: parse_env_usize_list("UBQ_ATOMIC_INCR_COUNTS")?
            .unwrap_or_else(|| vec![1_000, 10_000, 100_000, 1_000_000]),
        repeats: parse_env_usize("UBQ_ATOMIC_INCR_REPEATS")?.unwrap_or(1),
        out_path: env::var("UBQ_ATOMIC_INCR_CSV")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from("bench_results/plots/atomic_incr.csv")),
    };

    let mut i = 0;
    while i < args.len() {
        let arg = &args[i];
        if arg == "-h" || arg == "--help" {
            print_usage();
            process::exit(0);
        } else if let Some(v) = arg.strip_prefix("--threads=") {
            cfg.thread_counts = parse_usize_list(v, "--threads")?;
        } else if arg == "--threads" {
            let v = next_value(args, &mut i, "--threads")?;
            cfg.thread_counts = parse_usize_list(&v, "--threads")?;
        } else if let Some(v) = arg.strip_prefix("--counts=") {
            cfg.increments = parse_usize_list(v, "--counts")?;
        } else if arg == "--counts" {
            let v = next_value(args, &mut i, "--counts")?;
            cfg.increments = parse_usize_list(&v, "--counts")?;
        } else if let Some(v) = arg.strip_prefix("--repeats=") {
            cfg.repeats = parse_required_usize(v, "--repeats")?;
        } else if arg == "--repeats" {
            let v = next_value(args, &mut i, "--repeats")?;
            cfg.repeats = parse_required_usize(&v, "--repeats")?;
        } else if let Some(v) = arg.strip_prefix("--out=") {
            cfg.out_path = PathBuf::from(v);
        } else if arg == "--out" {
            let v = next_value(args, &mut i, "--out")?;
            cfg.out_path = PathBuf::from(v);
        } else {
            return Err(format!("unknown argument for `sweep`: `{arg}`"));
        }
        i += 1;
    }

    if cfg.repeats == 0 {
        return Err("`--repeats` must be > 0".to_string());
    }
    if cfg.thread_counts.iter().any(|&n| n == 0) {
        return Err("thread counts must all be > 0".to_string());
    }
    if cfg.increments.iter().any(|&n| n == 0) {
        return Err("counts must all be > 0".to_string());
    }

    Ok(cfg)
}

fn run_once(cfg: OnceConfig) {
    let total = cfg
        .threads
        .checked_mul(cfg.increments)
        .expect("total increments overflowed usize");
    println!(
        "How fast can {} threads each increment the same counter {} times, for a total of {}?",
        cfg.threads,
        fmt_num(cfg.increments),
        fmt_num(total),
    );

    let methods = atomic_incr_methods();
    let slen = methods
        .iter()
        .map(|method| method.name.len())
        .max()
        .unwrap_or(0);

    for result in run_atomic_incr_suite(cfg.threads, cfg.increments) {
        println!(
            "For {name:>slen$}: {:?}",
            result.elapsed,
            name = result.method,
            slen = slen
        );
    }
}

fn run_sweep(cfg: SweepConfig) {
    let mut csv = String::from(
        "method,threads,increments_per_thread,total_increments,repeat,elapsed_ns,elapsed_ms\n",
    );

    let total_cases = cfg.thread_counts.len() * cfg.increments.len() * cfg.repeats;
    let mut case_idx = 0usize;

    for &threads in &cfg.thread_counts {
        for &count in &cfg.increments {
            for repeat in 0..cfg.repeats {
                case_idx += 1;
                let total = threads
                    .checked_mul(count)
                    .expect("total increments overflowed usize");
                println!(
                    "[{case_idx}/{total_cases}] threads={threads}, increments_per_thread={}, total={}",
                    fmt_num(count),
                    fmt_num(total),
                );

                for result in run_atomic_incr_suite(threads, count) {
                    let elapsed_ns = result.elapsed.as_nanos();
                    let elapsed_ms = result.elapsed.as_secs_f64() * 1_000.0;

                    writeln!(
                        csv,
                        "{},{},{},{},{},{},{}",
                        result.method,
                        result.threads,
                        result.increments_per_thread,
                        result.total_increments(),
                        repeat + 1,
                        elapsed_ns,
                        elapsed_ms
                    )
                    .unwrap();
                }
            }
        }
    }

    if let Some(parent) = cfg.out_path.parent() {
        fs::create_dir_all(parent).unwrap();
    }
    fs::write(&cfg.out_path, csv).unwrap();

    println!("Wrote CSV: {}", cfg.out_path.display());
    println!(
        "Plot with: python scripts/plot_atomic_incr.py {} --log-x",
        cfg.out_path.display()
    );
}

fn real_main() -> Result<(), String> {
    let args: Vec<String> = env::args().skip(1).collect();

    if args.is_empty() {
        run_once(parse_once_args(&[])?);
        return Ok(());
    }

    if args[0] == "-h" || args[0] == "--help" || args[0] == "help" {
        print_usage();
        return Ok(());
    }

    match args[0].as_str() {
        "once" => {
            run_once(parse_once_args(&args[1..])?);
            Ok(())
        }
        "sweep" => {
            run_sweep(parse_sweep_args(&args[1..])?);
            Ok(())
        }
        _ => {
            // Treat bare flags as `once` for convenience.
            run_once(parse_once_args(&args)?);
            Ok(())
        }
    }
}

/*
cargo run --bin atomic_incr_bench -- sweep \
  --threads 1,2,4,8,16 \
  --counts 1000,2500,5000,7500,10000,25000,50000,75000,100000,250000,500000,750000,1000000 \
  --repeats 10

python scripts/plot_atomic_incr.py bench_results/plots/atomic_incr.csv --log-x
python scripts/plot_atomic_incr.py bench_results/plots/atomic_incr.csv --error-bars sem --log-x --log-y
python scripts/plot_atomic_incr.py bench_results/plots/atomic_incr.csv --error-bars none --log-x
*/

fn main() {
    if let Err(err) = real_main() {
        eprintln!("error: {err}");
        eprintln!();
        print_usage();
        process::exit(2);
    }
}
