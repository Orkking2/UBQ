# BSC-CNS Presentation Runbook

This runbook is for previewing UBQ results locally or on `hebrides`, then
running the publication-quality benchmark suite on the BSC-CNS machine.

## Current Preview Results

I ran a bounded local preview on this machine with:

```bash
cargo run --release --features bench_registry,bench_rbbq,bench_lfqueue --bin bench_matrix -- \
  --machine-label local-preview \
  --runs-dir bench_results/presentation_runs \
  --queues ubq,segqueue,concurrent-queue,rbbq,lfqueue \
  --ubq-label balanced,8,127,crossbeam \
  --fastfifo-block-sizes 64,256,1024,4096 \
  --lfqueue-segment-sizes 1024 \
  --scenarios 1p4c,4p4c,16p16c,64p64c \
  --modes throughput \
  --items-per-producer 100000 \
  --repeats 1 \
  --parallelism 128 \
  --reuse-existing
```

The runner only reported one hardware thread through
`std::thread::available_parallelism()`, so this is a sanity check rather than a
paper result. The command uses `--parallelism 128` to allow the selected
multi-threaded scenarios to run.

| scenario | UBQ ops/sec | best non-UBQ | best non-UBQ ops/sec | UBQ speedup |
| --- | ---: | --- | ---: | ---: |
| 1p4c | 5,905,583 | concurrent-queue | 5,179,422 | 1.14x |
| 4p4c | 11,475,306 | concurrent-queue | 14,997,747 | 0.77x |
| 16p16c | 4,298,067 | crossbeam SegQueue | 6,819,665 | 0.63x |
| 64p64c | 15,700,386 | crossbeam SegQueue | 25,958,356 | 0.60x |

Interpretation: the fixed preview label `balanced,8,127,crossbeam` is enough to
show a fan-out win, but it is not the tuned high-contention label. Use the BSC
frontier run below for real claims.

I also attempted:

```bash
ssh hebrides hostname
```

from this environment, but DNS resolution failed:

```text
ssh: Could not resolve hostname hebrides: Temporary failure in name resolution
```

Run the hebrides preview from a networked shell where that host resolves.

## Preview Run On Local Or Hebrides

Use the small preview config when you need a fast presentation dry run:

```bash
cargo run --release --bin full_bench_fleet -- \
  --config bench_fleet_preview.toml \
  --machines local \
  --plot-partial \
  --frontier-arg=--parallelism=$(nproc)
```

For hebrides:

```bash
cargo run --release --bin full_bench_fleet -- \
  --config bench_fleet_preview.toml \
  --machines hebrides \
  --plot-partial
```

If hebrides reports too little available parallelism, add the same forwarded
argument:

```bash
--frontier-arg=--parallelism=<hardware-thread-count>
```

Preview output locations:

- `bench_results/presentation_runs`
- `bench_results/presentation_plots`

## Real BSC-CNS Run

On the BSC-CNS machine, clone or update this repository, then run:

```bash
cargo run --release --bin full_bench_fleet -- \
  --config bench_fleet_bsc_cns.toml \
  --machines bsc-cns \
  --repeats 3 \
  --plot-partial \
  --frontier-arg=--parallelism=$(nproc)
```

This uses:

- all 49 producer/consumer grid points from 1 to 64 threads,
- `throughput`, `complex_throughput`, `data_latency`, and `fairness`,
- UBQ frontier expansion seeded at `balanced,8,127,crossbeam`,
- SegQueue, concurrent-queue, RBBQ/BBQ, LSCQ, and wCQ baselines,
- three repeats for the paper run.

If the full run is too long, first run the preview config on BSC-CNS:

```bash
cargo run --release --bin full_bench_fleet -- \
  --config bench_fleet_preview.toml \
  --machines bsc-cns \
  --plot-partial \
  --frontier-arg=--parallelism=$(nproc)
```

## Paper Figures From BSC-CNS Results

After `full_bench_fleet` renders standard plots, generate paper-oriented
advantage plots from the BSC-CNS CSVs:

```bash
python3 scripts/plot_paper_advantages.py \
  --results-dir bench_results/plots/bsc-cns/csv \
  --out-dir bench_results/plots/bsc-cns/paper
```

Recommended figures for a colleague presentation:

1. `throughput_speedup_heatmap.svg`: best UBQ variant versus best non-UBQ queue.
2. `throughput_scaling_lines.svg`: per-family throughput scaling.
3. `complex_high_contention_speedup_heatmap.svg`: high-contention robustness.
4. `data_latency_elapsed_speedup_distribution.svg`: operation-cost advantage.
5. `throughput_workload_class_summary.svg`: win rate and geomean by workload shape.
6. `fairness_throughput_pareto_64p64c.svg`: fairness/throughput supporting view.

## Application Targets To Discuss

The most favorable states from `grace_results` were:

- `1pNc` fan-out throughput: UBQ wins 6/6 scenarios, geomean 2.13x.
- balanced `NpNc` throughput: UBQ wins 6/6 scenarios, geomean 1.36x.
- high-MPMC throughput: UBQ wins 6/6 scenarios, geomean 1.42x.
- complex high-MPMC throughput: UBQ wins 5/6 scenarios, geomean 1.25x.
- data-latency operation cost: UBQ wins 45/49 push-elapsed scenarios and 39/49
  pop-elapsed scenarios.

Map those to:

- ingress-to-worker dispatch,
- runtime global task-injection queues,
- parallel graph/frontier worklists,
- small-message low-latency pipelines,
- unbounded FIFO burst buffers where bounded pre-sizing is operationally costly.

Do not lead with `Np1c` single-consumer throughput. That is not UBQ's strongest
state and should be framed as a caveat or avoided through sharding/batching.
