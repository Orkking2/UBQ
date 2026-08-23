# io_uring Queue Replacement Benchmark

This benchmark reproduces the shape of the BBQ paper's io_uring experiment:

- Three threads:
  - submitter writes request batches to SQ
  - kernel thread drains SQ and immediately writes completions to CQ
  - completer drains CQ
- CQ size is `2 * SQ size`
- SQ sizes default to `32,1024`
- batch modes default to fixed `1` and random `1..32`
- each queue/mode/size point runs `10` repeats
- each repeat submits `1,000,000` requests

The benchmark is a userspace queue-replacement benchmark. It does not perform
I/O, and it does not model io_uring's overflow linked list. The `io_uring`
backend is an SPSC head/tail ring with power-of-two capacity, batched head/tail
publication, and the SQ-array-style entry indirection used by io_uring. The BBQ
backend uses this repo's RBBQ/FastFifo baseline. UBQ is wrapped with exact SQ/CQ
capacity counters so the submitter cannot run ahead of the configured ring
sizes.

Run the default matrix:

```bash
scripts/run_io_uring_queue_bench.sh
```

This writes:

- `bench_results/io_uring_queue/<timestamp>-<host>/samples.csv`
- `bench_results/io_uring_queue/<timestamp>-<host>/summary.csv`

Plot a completed run:

```bash
python3 scripts/plot_io_uring_queue.py \
  bench_results/io_uring_queue/<timestamp>-<host>/summary.csv
```

By default this writes SVG and PNG plots next to the summary:

- `io_uring_queue_submit_latency.{svg,png}`
- `io_uring_queue_submit_throughput.{svg,png}`
- `io_uring_queue_submit_speedup.{svg,png}`

Use `--metric total` to plot end-to-end latency and throughput instead of the
submitter's measured window.

For publication-style runs, pin the three benchmark threads:

```bash
scripts/run_io_uring_queue_bench.sh --pin
```

On platforms where thread affinity is unavailable, `--pin` prints a warning and
continues unpinned instead of blocking the run.

Useful overrides:

```bash
scripts/run_io_uring_queue_bench.sh \
  --queues io-uring,bbq,ubq \
  --sq-sizes 32,1024 \
  --batch-modes fixed1,random1to32 \
  --requests 1000000 \
  --repeats 10 \
  --bbq-block-size 64 \
  --ubq-label balanced,1,page,crossbeam
```

`summary.csv` reports median submit/end-to-end latency per request, median
submit/end-to-end requests per second, and speedups against the matching SQ size
and batch mode for the `io_uring` backend.
