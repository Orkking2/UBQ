# FAA versus CAS with backoff: throughput or cache-line retention?

`bench_atomic_updates` in its zero-work loop shows CAS with spin backoff, and
the SegQueue-style reservation that uses the same backoff, outrunning FAA at
low updater counts. UBQ reserves producer slots with FAA, so the result needs
an explanation before it is either dismissed or acted on. This note states the
hypothesis, what the benchmark now records to test it, the measurements taken
so far, and what is still missing.

## Hypothesis

In a loop that does nothing between updates, backoff does not make CAS cheaper.
It makes the losers wait, so the core that owns the counter's cache line keeps
it and issues a run of updates without a line transfer. Throughput rises
because fewer transfers happen per update, not because an update costs less.
The run length is a property of the loop: once an updater does any work with
its reservation before asking for the next one, the line moves anyway, the
runs end, and each failed CAS is pure waste. FAA cannot fail, so it should win
in every regime where the line actually moves.

The hypothesis makes three predictions that can each fail:

1. Where CAS with backoff beats FAA, nearly every successful update directly
   follows the same updater's previous one.
2. Plain CAS (no backoff) does not beat FAA anywhere.
3. Adding thread-local work between updates removes the advantage, and it
   removes the same-updater runs at the same point.

## What the benchmark records

Schema v4 of `bench_atomic_updates` adds, without changing the default sweep:

| Addition | Meaning |
| --- | --- |
| `--think-iters a,b,c` | Thread-local work after every successful update: a dependent xorshift chain of that many iterations. `0` (default) is the historical loop. `meta.think_iteration_ns` is the calibrated single-thread cost of one iteration. |
| `same_updater_fraction` | Fraction of successful updates for which no other thread wrote the counter since this updater's previous write. 1.0 for a lone updater; about 0 under fair alternation. |
| `min_updater_share`, `max_updater_share` | Smallest and largest share of a sample's updates taken by one updater. The fair share is `1 / updater_count`. |
| `--update-chunk n` | Successful updates between checks of the stop flag (default 4096, unchanged). When one updater completes only thousands of updates per second, a 4096-update chunk outlasts the sample and the updaters stop at different times; use a small chunk for those sweeps. |

`same_updater_fraction` is computed from values each updater already holds:
the word it observed when its update succeeded, compared with the word its own
previous write left. It adds one register compare per update and no shared
memory access.

## Running it

```bash
cargo run --release --features bench_tools --bin bench_atomic_updates -- \
  --machine-label hebrides \
  --block-sizes 4095 \
  --think-iters 0,32,128,512 \
  --update-chunk 64 \
  --repeats 5

python3 scripts/summarize_atomic_fairness.py \
  bench_results/runs/hebrides/atomic_updates/*.json \
  --csv bench_results/atomic_fairness_hebrides.csv
```

With one block size this is 7 cases x 5 repeats x 4 think levels per updater
count at 0.6 s each: about 11 minutes on a machine with eight power-of-two
updater counts. `scripts/plot_atomic_updates.py --think-iters N` renders the
existing plots for one think level at a time.

## Measurements so far

### hebrides, standalone counter (2026-11-12)

Two-socket Arm Neoverse-N1, 80 cores per socket, no SMT, Linux 6.8, LSE
atomics inlined. This run used a standalone single-file microbenchmark (one
padded `AtomicU64`, no block boundaries, crossbeam-shaped spin backoff), not
this repository's binary; it is the measurement that motivated the change and
should be repeated with `bench_atomic_updates`. Medians of three 0.5 s runs,
threads pinned to CPUs in order (so 96 and above cross sockets).

| Think | Updaters | FAA | CAS | CAS+backoff (Mops/s) | CAS/FAA | CAS+backoff/FAA | Same-updater FAA / CAS / CAS+backoff | CAS+backoff failures per update |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | ---: |
| 0 | 2 | 93.7 | 22.0 | 130.1 | 0.23 | 1.39 | 0.67 / 0.45 / 1.00 | 0.01 |
| 0 | 4 | 87.1 | 10.3 | 92.4 | 0.12 | 1.06 | 0.67 / 0.03 / 1.00 | 0.04 |
| 0 | 8 | 86.3 | 9.7 | 46.2 | 0.11 | 0.53 | 0.67 / 0.01 / 1.00 | 0.19 |
| 0 | 16 | 84.3 | 8.2 | 13.3 | 0.10 | 0.16 | 0.66 / 0.01 / 1.00 | 1.27 |
| 0 | 64 | 83.4 | 6.6 | 0.6 | 0.08 | 0.01 | 0.66 / 0.03 / 0.31 | 42.6 |
| ~60 ns | 2 | 27.5 | 15.8 | 14.0 | 0.57 | 0.51 | 0.00 / 0.02 / 1.00 | 0.10 |
| ~60 ns | 16 | 29.9 | 10.6 | 3.6 | 0.35 | 0.12 | 0.00 / 0.00 / 0.13 | 4.57 |
| ~60 ns | 64 | 29.2 | 9.0 | 0.5 | 0.31 | 0.02 | 0.00 / 0.00 / 0.02 | 50.0 |
| ~250 ns | 16 | 29.6 | 8.8 | 3.2 | 0.30 | 0.11 | 0.00 / 0.00 / 0.00 | 4.78 |
| ~1 us | 16 | 14.7 | 9.3 | 3.1 | 0.63 | 0.21 | 0.00 / 0.00 / 0.00 | 4.49 |

- Prediction 1 holds. CAS with backoff beats FAA only at 2 and 4 updaters with
  zero work, and there its same-updater fraction is 1.00; at 2 updaters one
  core took 72% of the updates.
- Prediction 2 holds. Plain CAS is between 0.04x and 1.0x FAA in every
  configuration measured.
- Prediction 3 holds in direction. With about 60 ns of work FAA wins at every
  updater count. The same-updater runs under backoff persist at 2 to 8
  updaters even then, but they no longer pay for the failed attempts.

### 4-vCPU cloud VM, this repository's binary (2026-11-12)

Intel Sapphire Rapids under KVM, noisy; included only to show that the
in-repository harness reproduces the shape, including for the SegQueue-style
reservation. Block 4095, `--update-chunk 64`, medians of three 0.3 s runs.

| Think | Updaters | FAA | CAS | CAS+backoff | SegQueue-style (Mops/s) | CAS+backoff/FAA | SegQueue/FAA | Same-updater FAA / CAS+backoff / SegQueue |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 0 | 2 | 38.3 | 30.0 | 81.7 | 55.2 | 2.13 | 1.44 | 0.54 / 1.00 / 1.00 |
| 0 | 4 | 35.1 | 24.0 | 65.5 | 43.8 | 1.87 | 1.25 | 0.47 / 1.00 / 1.00 |
| ~58 ns | 2 | 19.4 | 18.1 | 15.4 | 14.9 | 0.79 | 0.77 | 0.09 / 1.00 / 1.00 |
| ~58 ns | 4 | 20.5 | 17.8 | 14.1 | 13.7 | 0.69 | 0.67 | 0.03 / 1.00 / 1.00 |
| ~232 ns | 4 | 14.7 | 14.8 | 7.9 | 7.8 | 0.54 | 0.53 | 0.00 / 0.01 / 0.01 |
| ~929 ns | 4 | 3.8 | 3.9 | 3.9 | 4.0 | 1.01 | 1.04 | 0.00 / 0.00 / 0.00 |

## Limits

- One server-class machine, one architecture. The x86 point is a 4-vCPU VM.
- The hebrides numbers come from the standalone microbenchmark, not from this
  binary, and not from a queue. They say nothing yet about UBQ end to end.
- The think work is register-only. Real producers touch the slot they
  reserved, which also moves cache lines; that should strengthen the effect
  but is not measured.
- Only crossbeam's spin backoff is modelled. A backoff tuned per machine could
  move the crossover.
- Updater counts are powers of two, and threads are pinned in CPU order, so
  the cross-socket transition is confounded with updater count above 80 on
  hebrides.
- Whether the N1 executes these atomics near the core or far in the
  interconnect is unknown, and would change the mechanism's explanation.

## Next steps

1. Repeat on hebrides with this binary (command above), then on Grace and
   MareNostrum 5 through `slurm/focused_contention.sbatch`.
2. Figure: CAS+backoff/FAA and SegQueue/FAA throughput ratio against think
   time in ns, one line per updater count, with same-updater fraction on a
   second axis or as marker fill.
3. Queue-level check: build UBQ with the producer reservation switched to CAS
   with backoff and compare under `bench_grid` scenarios that carry payloads.

## Draft paragraph for the paper

> FAA is not unconditionally the faster reservation primitive. In a closed
> loop that performs no work between reservations, a CAS loop with exponential
> spin backoff outpaces FAA at low thread counts (1.39x at two threads on the
> 160-core Neoverse-N1 machine). The advantage is a property of the loop
> rather than of the primitive: in exactly these configurations, 99 to 100% of
> successful updates directly follow the same thread's previous update, so
> backoff is letting one core retain the cache line and issue a run of
> reservations while the others wait. A plain CAS loop never outperforms FAA,
> and with zero work FAA is ahead of both from eight threads up. Once each
> reservation is followed by as little as 60 ns of thread-local work, FAA is
> faster at every thread count: at 16 to 64 threads it is about 3x faster than
> plain CAS and between 5x and 58x faster than CAS with backoff, whose failed
> attempts grow to 50 per successful update. UBQ therefore reserves producer
> slots with FAA and treats zero-work microbenchmarks of the head word as an
> upper bound on contention, not as a predictor of queue throughput.

The numbers in this paragraph are from the standalone hebrides run above and
must be replaced by the `bench_atomic_updates` run before the paragraph is
used.
