# BBQ ATC 2022 Reproduction Notes

This repo can now schedule the main microbenchmark shapes from Jiawei Wang et
al., "BBQ: A Block-based Bounded Queue for Exchanging Data and Profiling",
USENIX ATC 2022:

- `spsc`: `1p1c`
- `mpsc:N-M`: `Np1c` for each `N` in the inclusive range
- `spmc:N-M`: `1pNc` for each `N` in the inclusive range
- `mpmc:N-M`: `NpNc` for each `N` in the inclusive range
- `bbq-atc22-x86-88t`: `1p1c`, `Np1c`, and `1pNc` for `N=1..87`
- `bbq-atc22-oversub-x86-12t`: `Np1c` and `1pNc` for `N=1..59`

The BBQ paper's microbenchmark workloads map to these harness modes:

- `throughput`: simple workload, total consumed entries per second.
- `complex_throughput`: simple workload plus per-operation allocation/free and
  a deterministic busy loop of up to 100 spin iterations.
- `data_latency`: average time from enqueue to dequeue, using the complex-style
  allocation and busy loop.
- `fairness`: reports aggregate throughput plus producer and consumer
  max/min throughput ratios.

The paper also reports Linux `perf` L1 cache misses, bounded full/empty failed
operation latency, and drop-old overwrite mode. Those are intentionally not
forced into UBQ's unbounded API. Cache misses should be collected externally
with `perf` on Linux. Full enqueue and drop-old overwrite are bounded-queue
semantics, so UBQ does not have directly comparable operations.

## Plot coverage

The plotting helpers generate both generic per-scenario plots and BBQ-style
family scaling plots from the same schema-v6 JSON files:

- SPSC throughput/latency/fairness-style bars:
  `plots/<machine>/<mode>/1p1c_<metric>.png`
- Paper-style MPSC scaling:
  `plots/<machine>/<mode>/mpsc_line_<metric>.png`
- Paper-style SPMC scaling:
  `plots/<machine>/<mode>/spmc_line_<metric>.png`
- Combined scenario scaling:
  `plots/<machine>/<mode>/scenarios_line_<metric>.png`

These cover the paper's simple workload, complex workload, data latency, and
fairness families for SPSC/MPSC/SPMC when the corresponding harness modes are
run:

- `figure_cross_x86_{spsc,mpsc,spmc}_simple.pdf` -> `throughput`
- `figure_cross_x86_{spsc,mpsc,spmc}.pdf` -> `complex_throughput`
- `figure_cross_lat_x86_{spsc,mpsc,spmc}.pdf` -> `data_latency`
- `figure_cross_fairness_x86_{mpsc,spmc}_simple.pdf` ->
  `producer_fairness` and `consumer_fairness`
- `figure_cross_overload_{mpsc,spmc}_simple.pdf` ->
  run `bbq-atc22-oversub-x86-12t` and use the generated MPSC/SPMC line plots

This repository also emits extra plots for timing fields that the harness now
records, for example `throughput_push_elapsed`, `throughput_pop_elapsed`,
`data_latency_push_elapsed`, and `data_latency_pop_elapsed`.

The following BBQ paper figures are not reproduced directly by this harness:

- `figure_cross_perf_*`: L1 cache misses require an external `perf` collection
  pass and a separate merge step.
- `figure_cross_emptyfull_*` and `figure_cross_retrynew.pdf`: bounded
  full/empty retry latency has no direct UBQ equivalent; UBQ is unbounded and
  empty pop is a different operation shape.
- `figure_cross_dropold*` and `figure_self_feat_drop_old.pdf`: drop-old
  overwrite is bounded-queue behavior.
- `figure_self_feat_faa_lse.pdf` and `figure_self_feat_dynamic_size.pdf`:
  BBQ implementation feature ablations do not map to current UBQ knobs, though
  UBQ label sweeps can still be plotted as variant comparisons.
- `figure_self_blknum_{thpt,lat}_mpsc.pdf`: block-size sweeps can be approximated
  by running several `fastfifo_*` sizes and UBQ block labels, but there is not
  yet a dedicated block-size x-axis plot.
- `figure_dpdk.pdf` and `figure_disruptor_x86.pdf`: application benchmarks are
  outside the microbenchmark harness.
- `figure_io_uring_*.pdf`: covered by the dedicated three-thread SQ/CQ
  replacement benchmark in [io_uring_queue_benchmarks.md](io_uring_queue_benchmarks.md).

Example direct run:

```bash
cargo run --release --features bench_registry,bench_rbbq,bench_lfqueue,bench_wcq --bin bench_matrix -- \
  --machine-label local \
  --queues ubq,segqueue,concurrent-queue,rbbq,lfqueue,wcq \
  --ubq-label balanced,8,127,crossbeam \
  --scenarios bbq-atc22-x86-88t \
  --modes throughput,complex_throughput,data_latency,fairness \
  --items-per-producer 1000000 \
  --repeats 3
```

Example oversubscription run on a 12-hyperthread machine:

```bash
cargo run --release --features bench_registry,bench_rbbq,bench_lfqueue,bench_wcq --bin bench_matrix -- \
  --machine-label x86-12t \
  --queues ubq,segqueue,concurrent-queue,rbbq,lfqueue \
  --ubq-label balanced,8,127,crossbeam \
  --scenarios bbq-atc22-oversub-x86-12t \
  --modes throughput,complex_throughput \
  --items-per-producer 1000000 \
  --repeats 3 \
  --parallelism 60
```

Reproducible sparse-grid run:

```bash
cargo run --release --features bench_registry,bench_rbbq,bench_lfqueue,bench_wcq --bin bench_grid -- \
  --machine-label local \
  --queues ubq,segqueue,concurrent-queue,rbbq,lfqueue,wcq \
  --scenarios bbq-atc22-x86-88t \
  --modes throughput,complex_throughput,data_latency,fairness \
  --items-per-producer 1000000 \
  --repeats 3
```

For reproducible UBQ variation coverage, use `bench_grid`; it defaults to the
sparse grid, with `-d` selecting the dense grid. For fixed paper-style
comparisons, use `bench_matrix` with explicit `--ubq-label` values for each UBQ
variant you want in the paper plots.
