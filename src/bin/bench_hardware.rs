use clap::Parser;
use std::path::PathBuf;
use ubq::bench_harness::hardware::{Config, run};

#[derive(Parser)]
#[command(about = "Bounded persistent-worker hardware and memory probe")]
struct Args {
    /// Frozen JSON workload configuration produced by hardware_campaign.py.
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
            std::process::exit(1);
        }
    }
}
