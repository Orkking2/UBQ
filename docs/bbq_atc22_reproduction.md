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

Fleet run:

```bash
cargo run --release --bin full_bench_fleet -- \
  --config bench_fleet_bbq_atc22.toml \
  --machines local,lab,hebrides \
  --repeats 3
```

For a large UBQ variant search, keep using `bench_frontier` or
`full_bench_fleet`. For fixed paper-style comparisons, use `bench_matrix` with
explicit `--ubq-label` values for each UBQ variant you want in the paper plots.
