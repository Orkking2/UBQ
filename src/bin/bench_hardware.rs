use clap::Parser;
use std::path::PathBuf;
use ubq::bench_harness::hardware::{Config, run};

#[derive(Parser)]
#[command(about = "Bounded persistent-worker hardware and memory probe")]
struct Args {
    /// JSON workload configuration, as written by the lab (lab/ablab/task.py).
    #[arg(long)]
    config: PathBuf,
}

fn main() {
    let result = (|| -> Result<_, String> {
        let args = Args::parse();
        let raw = std::fs::read(args.config).map_err(|e| e.to_string())?;
        let config: Config = serde_json::from_slice(&raw).map_err(|e| e.to_string())?;
        run(&config)
    })();
    match result {
        Ok(result) => println!("{}", serde_json::to_string(&result).unwrap()),
        Err(error) => {
            eprintln!("bench_hardware: {error}");
            // A failed run writes no result document, so surface the batch
            // repair diagnostics here or they are lost with the exit code.
            #[cfg(feature = "probe_batch_repair")]
            {
                let (reverts, false_fills, false_fill_slots) = ubq::batch_repair_probe::counts();
                eprintln!(
                    "bench_hardware: batch_repair_probe reverts={reverts} \
false_fills={false_fills} false_fill_slots={false_fill_slots}"
                );
            }
            std::process::exit(1);
        }
    }
}
