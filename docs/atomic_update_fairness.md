# FAA versus CAS with backoff: throughput or cache-line retention?

`bench_atomic_updates` in its zero-work loop shows CAS with spin backoff, and
the SegQueue-style reservation that uses the same backoff, outrunning FAA.
UBQ reserves producer slots with FAA, so the result needs an explanation
before it is either dismissed or acted on. This note states the hypothesis,
what the benchmark records to test it, what the measurements on hebrides say,
and why none of the throughput numbers are ready for the paper yet.

**Status (2026-11-12).** The mechanism is confirmed: wherever the backoff
methods beat FAA, almost all of their updates come from the thread that made
the previous one. The claim that any work between updates removes the
advantage is **not** confirmed: in one of two throughput modes seen on
hebrides it survives 63 ns of work. Those two modes differ by about 10x, are
not controlled by the benchmark, and can reverse which method wins. Resolving
them is the next step.

## Hypothesis

In a loop that does nothing between updates, backoff does not make CAS cheaper.
It makes the losers wait, so the core that owns the counter's cache line keeps
it and issues a run of updates without a line transfer. Throughput rises
because fewer transfers happen per update, not because an update costs less.
FAA cannot fail and does not wait, so under FAA the line moves on nearly every
update.

Three predictions, each of which can fail:

1. Where a backoff method beats FAA, nearly every one of its successful
   updates directly follows the same updater's previous one.
2. Plain CAS (no backoff) does not beat FAA.
3. Thread-local work between updates removes the advantage, and removes the
   same-updater runs at the same point.

## What the benchmark records

Schema v4 of `bench_atomic_updates` adds, without changing the default sweep:

| Addition | Meaning |
| --- | --- |
| `--think-iters a,b,c` | Thread-local work after every successful update: a dependent xorshift chain of that many iterations. `0` (default) is the historical loop. `meta.think_iteration_ns` is the calibrated single-thread cost of one iteration. |
| `same_updater_fraction` | Fraction of successful updates for which no other thread wrote the counter since this updater's previous write. 1.0 for a lone updater; about 0 under fair alternation. |
| `min_updater_share`, `max_updater_share` | Smallest and largest share of a sample's updates taken by one updater. The fair share is `1 / updater_count`. |
| `counter_address_hex` | Virtual address of the sample's counter allocation, so throughput modes can be checked against placement. |
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
existing plots for one think level at a time. The summary CSV carries each
row's minimum and maximum throughput and its number of distinct counter
addresses.

## Measurements on hebrides

Two-socket Arm Neoverse-N1, 80 cores per socket, no SMT, Linux 6.8. Both runs
used this binary at `c16166d` with the command above: run A with the default
build flags (atomics through the outline helpers), five repeats; run B with
`-C target-cpu=native` (LSE inlined), three repeats. The full summary tables,
and the per-repeat values of the bimodal cells, are in
[`data/atomic_fairness_hebrides_2026-11-12.txt`](data/atomic_fairness_hebrides_2026-11-12.txt).
Wherever the two builds are in the same mode they agree to within a few
percent, so inlining is not what separates them.

### Two modes

With two or more updaters, every method's throughput falls into one of two
levels, most visibly for FAA:

| | FAA, zero work | FAA, 63 to 253 ns of work | FAA, about 1 us of work |
| --- | ---: | ---: | ---: |
| slow mode | 2.4 to 3.1 Mops/s | 1.25 to 1.6 Mops/s | 0.8 to 1.5 Mops/s |
| fast mode | about 12.5 Mops/s | 10 to 15 Mops/s | 11 to 12 Mops/s |

In the slow mode FAA's total throughput barely depends on the updater count
(2 to 64) or, up to 253 ns, on the think time, which means one update costs
roughly 0.3 to 0.8 us whoever makes it. Run A is in the slow mode almost throughout; run B
is in the fast mode for most cells at 16 to 64 updaters. Four cells switch
mode between repeats. A separate probe on the same machine found that the
mode followed the counter's address across repeats and process restarts; this
binary did not yet record the address, so that is not established here. The
cause is unknown. Candidates are the NUMA node of the page holding the counter
(the main thread that first touches it is not pinned) and where the line is
homed on the N1 mesh.

### Prediction 1: holds

Every cell in which a backoff method beats FAA by 1.5x or more has a
same-updater fraction of 0.82 or higher for that method, in both runs. FAA's
own fraction in the same cells is 0.21 to 0.42 with zero work and at most 0.01
with work.

| Run | Think | Updaters | FAA | CAS+backoff | SegQueue-style (Mops/s) | CAS+backoff/FAA | SegQueue/FAA | Same-updater FAA / CAS+backoff / SegQueue |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| A | 0 | 2 | 3.06 | 10.64 | 30.02 | 3.48 | 9.81 | 0.42 / 1.00 / 1.00 |
| A | 0 | 16 | 2.48 | 11.79 | 11.24 | 4.75 | 4.53 | 0.23 / 0.97 / 0.97 |
| A | 0 | 64 | 2.37 | 3.79 | 4.34 | 1.59 | 1.83 | 0.21 / 0.97 / 0.97 |
| B | 0 | 8 | 2.59 | 13.90 | 10.82 | 5.37 | 4.18 | 0.25 / 0.98 / 0.97 |
| A | 63 ns | 8 | 1.44 | 4.20 | 4.24 | 2.92 | 2.95 | 0.01 / 0.84 / 0.83 |
| A | 63 ns | 16 | 1.43 | 4.69 | 4.27 | 3.28 | 2.98 | 0.00 / 0.83 / 0.83 |

Conversely, where the runs are mostly gone so is the advantage: at 253 ns in
run A, with 4 to 64 updaters, the backoff methods' same-updater fraction is
0.07 to 0.29 and their ratio to FAA is 0.94 to 1.09.

The converse of the prediction does not hold: runs do not guarantee a win. At
63 ns with 2 updaters the backoff methods lose in both runs (0.53x to 0.55x)
with a same-updater fraction of 0.93.

### Prediction 2: holds where it can be judged

In every cell whose repeats agree, plain CAS is at or below FAA with zero,
63 ns or 253 ns of work (0.35x to 1.06x, the top of the range being a single
updater). The exceptions are a cell that switches mode between repeats (run
B, zero work, 32 updaters, 3.77x on the median) and about 1 us of work, where
plain CAS is ahead in run A at 16 to 64 updaters (1.20x to 1.45x) and in run
B at 8 updaters (1.16x) with no same-updater runs to explain it. Neither is
understood.

### Prediction 3: fails as stated

The advantage does not disappear with any work. It depends on the mode:

| Mode | Think | Updaters | CAS+backoff/FAA | Same-updater, CAS+backoff |
| --- | ---: | ---: | ---: | ---: |
| slow (run A) | 63 ns | 4, 8, 16, 64 | 2.09, 2.92, 3.28, 2.78 | 0.83 to 0.85 |
| slow (run A) | 253 ns | 4 to 64 | 0.94 to 1.07 | 0.09 to 0.29 |
| fast (run B) | 63 ns | 16, 32, 64 | 0.26, 0.10, 0.03 | 0.30, 0.04, 0.04 |
| fast (run B) | 253 ns | 16, 32, 64 | 0.33, 0.11, 0.03 | 0.01 |

A reading consistent with both rows: runs under backoff pay while the think
time is small compared with the cost of moving the line. In the slow mode
that cost is several hundred nanoseconds, so 63 ns of work does not break the
runs and backoff still wins; by 253 ns it does. In the fast mode the cost is
tens of nanoseconds, 63 ns of work is already enough, and FAA wins by 4x to
30x. This is a refinement suggested by the data, not something the data was
collected to test.

## Earlier measurements

A standalone single-file microbenchmark (one padded `AtomicU64`, a bare
`fetch_add` with no preceding load and no block boundaries) run on hebrides
earlier the same day showed FAA at 84 to 94 Mops/s from 2 to 64 threads with
zero work, CAS with backoff ahead only at 2 and 4 threads (1.39x and 1.06x),
and FAA ahead everywhere with 60 ns of work. It was a single process, so it
sampled one placement, and its FAA loop differs from the one here, which loads
the counter before every `fetch_add` as UBQ's producer path does. Its numbers
should not be combined with the tables above.

On a 4-vCPU Sapphire Rapids VM this binary shows the same qualitative shape
(CAS with backoff 2.1x and SegQueue-style 1.4x FAA at 2 updaters with zero
work, both below 0.8x with 58 ns of work, same-updater fraction 1.00 for the
backoff methods).

## Limits

- **Uncontrolled placement.** The two modes above differ by about 10x, flip
  the winner, and are not explained. Until the counter's placement is
  controlled or at least identified per sample, no throughput ratio from this
  benchmark on hebrides should be quoted. Samples in one process usually
  share one counter address, so repeats within a run do not sample placement.
- One machine, one architecture.
- A counter, not a queue. Nothing here measures UBQ end to end.
- The think work is register-only. Real producers touch the slot they
  reserved, which also moves cache lines.
- Only crossbeam's spin backoff is modelled.
- Updater counts are powers of two and threads are pinned in CPU order, so
  128 updaters is the only cross-socket point.
- Run B started with the machine still shedding load from run A.

## Next steps

1. Identify the modes. Run this binary under `numactl --membind=0` and
   `--membind=1` with updaters confined to socket 0. If the mode follows the
   memory node, pin the allocating thread or bind the counter explicitly and
   record the node per sample; if it does not, use `counter_address_hex` to
   test the address explanation.
2. Rerun the think sweep in each mode separately and report them separately.
3. Check whether `bench_grid` results on hebrides show the same two levels.
4. Figure, once the modes are controlled: backoff/FAA throughput ratio
   against think time in ns, one line per updater count, same-updater
   fraction as marker fill, one panel per mode.
5. Queue-level check: build UBQ with the producer reservation switched to CAS
   with backoff and compare under scenarios that carry payloads.

## What can be said in the paper today

Only the mechanism:

> Where a CAS loop with spin backoff, or SegQueue's reservation, outruns FAA
> on a contended head word, it does so by serialising access rather than by
> making updates cheaper: in every such configuration we measured on a
> 160-core Neoverse-N1 machine, between 82% and 100% of the backoff method's
> successful updates directly followed the same thread's previous update,
> against at most 42% for FAA.

Any sentence about by how much, at how many threads, or after how much work
the advantage disappears has to wait for step 1.
