# FAA versus CAS with backoff: throughput or cache-line retention?

`bench_atomic_updates` in its zero-work loop shows CAS with spin backoff, and
the SegQueue-style reservation that uses the same backoff, outrunning FAA.
UBQ's producer currently reserves slots with a CAS loop and spin backoff on
its packed head (`UBQ::push_batch` in `src/queue.rs`; `push` is a batch of
one), and its consumer reserves with CAS as well. The producer's CAS
reservation dates from the queue refactor of 2026-08-16 (`9025b3c`) and was
chosen on the strength of this benchmark's zero-work result. This note states
the hypothesis behind that result, what the benchmark records to test it, and
what the measurements on hebrides say.

**Status (2026-11-12).**

- The mechanism is confirmed: wherever the backoff methods beat FAA, almost
  all of their updates come from the thread that made the previous one.
- Which method wins on hebrides depends on which NUMA node holds the counter.
  With the counter on the updaters' node, the backoff methods win only at 2
  to 8 updaters with no work between updates; everywhere else they are level
  with FAA or behind it, by up to about 30x. With the counter on the other
  node, the backoff methods win at 4 to 64 updaters by 1.6x to 5x, with or
  without work.
- The measurement that motivated UBQ's CAS reservation mixes two effects.
  With the counter on the updaters' node, the backoff lead exists only with
  zero work at up to 8 updaters; with the counter on the other node, it
  exists at 4 or more. The reservation choice should be re-tested at queue
  level, with memory placement controlled.
- Placement is now part of the benchmark (schema v5): `--counter-node` binds
  the counter's page to a node, and every sample records the node the kernel
  reports for it, so the two cases, about 10x apart in FAA throughput, can no
  longer mix silently. Older unbound hebrides numbers are one or the other
  without saying which.

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
| `counter_address_hex` | Virtual address of the sample's counter allocation, so throughput levels can be checked against placement. |
| `--update-chunk n` | Successful updates between checks of the stop flag (default 4096, unchanged). When one updater completes only thousands of updates per second, a 4096-update chunk outlasts the sample and the updaters stop at different times; use a small chunk for those sweeps. |

Schema v5 makes the counter's NUMA placement part of the experiment:

| Addition | Meaning |
| --- | --- |
| `--counter-node a,b` | Placements swept like think levels: `unplaced` (default, the historical behaviour: wherever the first write lands), `local` (the first updater's node), or a node number. A placement the platform cannot honour is an error, not a silent fallback; binding needs Linux. |
| `counter_node`, `counter_node_after`, `counter_node_moved` | The node the kernel reports for the counter's page (`get_mempolicy` with `MPOL_F_NODE \| MPOL_F_ADDR`) before and after the measured interval, and whether it changed. Null where the OS does not report nodes. |
| `updater_nodes`, `counter_relation` | The distinct nodes of the updaters' pinned CPUs, and the sample's label: `local` (counter on the updaters' one node), `remote`, `mixed` (updaters span nodes), or `unknown`. |
| `meta.numa_nodes`, `meta.numa_balancing` | Each node's CPU list from `/sys/devices/system/node`, and the kernel's automatic NUMA balancing setting, which can migrate an unplaced page during a run. |

Every trial's counter has an anonymous mapping of its own rather than a heap
slot. For a placed counter the mapping is bound with `mbind(MPOL_BIND,
MPOL_MF_MOVE | MPOL_MF_STRICT)` before its first write, so the kernel allocates
the page on that node, nothing else shares the binding, and automatic
balancing leaves it there. `scripts/summarize_atomic_fairness.py` keeps rows
for different counter nodes apart and flags any configuration measured on more
than one node or whose page moved during a sample;
`scripts/plot_atomic_updates.py` requires `--counter-node` when its inputs
span nodes.

`same_updater_fraction` is computed from values each updater already holds:
the word it observed when its update succeeded, compared with the word its own
previous write left. It adds one register compare per update and no shared
memory access.

## Running it

On a multi-socket machine, choose the counter's node. Unplaced, its page
lands wherever its first write happens to run, so its node is arbitrary. That
write comes from the unpinned main thread and, since schema v5, goes to a
fresh mapping in every trial, so an unplaced run can change node from one
trial to the next; before v5 the node followed the heap address the counter
happened to reuse.

```bash
cargo build --release --features bench_tools --bin bench_atomic_updates

# Updaters are pinned to CPUs in order; keep them on one node and sweep the
# counter's node. On hebrides node 0 is CPUs 0-79.
./target/release/bench_atomic_updates \
  --machine-label hebrides \
  --block-sizes 4095 \
  --think-iters 0,32,128,512 \
  --update-chunk 64 \
  --max-updaters 64 \
  --counter-node 0,1 \
  --repeats 5

python3 scripts/summarize_atomic_fairness.py \
  bench_results/runs/hebrides/atomic_updates/*.json \
  --csv bench_results/atomic_fairness_hebrides.csv
```

With one block size this is 7 cases x 5 repeats x 4 think levels per updater
count and node at 0.6 s each: about 10 minutes per node at seven updater
counts. `numactl --membind=<node>` with the default `--counter-node unplaced`
remains a cross-check from outside: the recorded `counter_node` is the bound
node.
`scripts/plot_atomic_updates.py --think-iters N` renders the existing plots
for one think level at a time. The summary CSV carries each row's minimum and
maximum throughput and its number of distinct counter addresses.

## Measurements on hebrides

Two-socket Arm Neoverse-N1, 80 cores per socket, no SMT, Linux 6.8, automatic
NUMA balancing on. All tables are from this binary and are committed under
[`data/`](data/):

- [`atomic_fairness_hebrides_numa_2026-11-12.txt`](data/atomic_fairness_hebrides_numa_2026-11-12.txt):
  memory bound to node 0, to node 1, and unbound; updaters on CPUs 0 to 63
  (node 0); think 0 and 63 ns; three repeats. These are the controlled runs
  and the ones the conclusions below rest on.
- [`atomic_fairness_hebrides_counter_node_2026-11-12.txt`](data/atomic_fairness_hebrides_counter_node_2026-11-12.txt):
  the same sweep with placement done by the benchmark itself
  (`--counter-node 0,1`, no `numactl`), with the node recorded per sample.
  It reproduces the bound runs to within a few percent.
- [`atomic_fairness_hebrides_2026-11-12.txt`](data/atomic_fairness_hebrides_2026-11-12.txt):
  two earlier unbound runs with four think levels and up to 128 updaters, one
  with default flags and one with `-C target-cpu=native`. Where those two
  builds landed on the same placement they agree to within a few percent, so
  inlining the LSE atomics is not a factor.

### Placement decides the level

| Counter's node | FAA, zero work, 2 / 16 / 64 updaters | FAA, 63 ns of work, 2 / 16 / 64 updaters | Cells whose repeats differ by more than 2x |
| --- | ---: | ---: | ---: |
| same as updaters (`--membind=0`) | 36.6 / 13.3 / 11.8 Mops/s | 19.0 / 15.0 / 13.9 Mops/s | 0 of 56 |
| other node (`--membind=1`) | 3.1 / 2.5 / 2.3 Mops/s | 1.6 / 1.4 / 1.4 Mops/s | 0 of 56 |
| unbound | either, by counter address | either, by counter address | 2 of 56 |
| `--counter-node 0` (benchmark binds; recorded node 0, `local`) | 35.3 / 13.2 / 12.7 Mops/s | 17.6 / 15.0 / 15.3 Mops/s | 0 of 56 |
| `--counter-node 1` (benchmark binds; recorded node 1, `remote`) | 3.2 / 2.5 / 2.3 Mops/s | 1.6 / 1.4 / 1.4 Mops/s | 0 of 56 |

The benchmark's own binding reproduces the `numactl` runs, and every one of
its 588 samples recorded its counter on the requested node both before and
after the measurement. As cross-checks, an unplaced run under `numactl
--membind=1` recorded node 1 and `--counter-node local` resolved to node 0.

`/proc/<pid>/numa_maps` confirmed the heap was entirely on the bound node in
both bound runs. In the unbound run each counter address kept one level for
as long as it was used, and the only two cells that mixed levels are ones
where the address changed between repeats. Snapshots of that run's
`numa_maps` show the heap holding every counter address on both nodes (12
pages on node 1 early in the run, then 5 on node 0 and 23 on node 1). Those
counts are for the whole heap mapping, not per page, so they fit "the unbound
levels are the two nodes" without proving which page held which counter; the
`--counter-node` runs settle it. With the counter on the other node
one FAA update costs roughly 0.3 to 0.7 us whoever makes it, and total
throughput barely moves from 2 to 64 updaters.

### Prediction 1: holds

In every run, every cell in which a backoff method beats FAA by 1.5x or
more has a same-updater fraction of 0.82 or higher for that method. FAA's own
fraction in those cells is at most 0.64 with zero work and at most 0.01 with
work.

The converse does not hold: runs do not guarantee a win. With the counter
local and 63 ns of work, CAS with backoff keeps a same-updater fraction of
0.90 to 0.95 at 2 to 8 updaters and is between 0.62x and 1.04x FAA.

### Prediction 2: holds in the controlled runs

With the counter's node fixed, by `numactl` or by `--counter-node`, plain CAS
is between 0.36x and 1.01x FAA in every cell. The exceptions seen earlier were all in unbound runs: one cell
that changed placement between repeats, and cells at about 1 us of work, a
think level the bound runs did not include.

### Prediction 3: holds locally, fails remotely

| Counter's node | Think | CAS+backoff / FAA at 2, 4, 8, 16, 32, 64 updaters | SegQueue-style / FAA |
| --- | ---: | --- | --- |
| same as updaters | 0 | 2.85, 4.54, 3.68, 1.01, 0.11, 0.05 | 2.34, 3.75, 2.62, 0.73, 0.13, 0.05 |
| same as updaters | 63 ns | 0.67, 1.04, 0.62, 0.25, 0.11, 0.04 | 0.67, 1.03, 0.58, 0.23, 0.11, 0.04 |
| other node | 0 | 3.33, 2.28, 4.66, 4.76, 3.21, 2.29 | 9.86, 3.87, 4.37, 4.67, 3.98, 1.94 |
| other node | 63 ns | 0.54, 2.08, 2.94, 3.28, 3.25, 2.51 | 0.53, 2.13, 2.97, 3.34, 3.38, 3.29 |

With the counter local, 63 ns of work is enough: the backoff methods are at
best level with FAA and at 16 or more updaters FAA is 4x to 25x ahead. With
the counter remote, the same 63 ns changes nothing from 4 updaters up.

The table is from the `numactl` runs. The `--counter-node` sweep is an
independent repeat of all four rows and agrees in shape:

| Counter's node | Think | CAS+backoff / FAA at 2, 4, 8, 16, 32, 64 updaters | SegQueue-style / FAA |
| --- | ---: | --- | --- |
| `--counter-node 0` | 0 | 2.96, 4.47, 3.70, 1.08, 0.12, 0.05 | 2.41, 3.71, 2.65, 0.81, 0.14, 0.04 |
| `--counter-node 0` | 63 ns | 0.74, 1.02, 0.62, 0.26, 0.10, 0.03 | 0.72, 1.03, 0.61, 0.24, 0.10, 0.03 |
| `--counter-node 1` | 0 | 3.36, 2.31, 4.69, 4.76, 2.96, 1.61 | 9.67, 3.86, 4.35, 4.63, 3.38, 1.82 |
| `--counter-node 1` | 63 ns | 0.54, 2.11, 2.92, 3.31, 3.32, 2.31 | 0.55, 2.13, 2.94, 3.40, 3.60, 3.60 |

Two cells move the ranges quoted elsewhere in this note. Locally with work,
FAA's lead at 64 updaters is about 30x in this run against 25x in the first.
Remotely with zero work, the backoff methods' lead at 64 updaters is 1.6x to
1.8x in this run against 1.9x to 2.3x in the first. Three repeats do not
separate those. The
earlier unbound run suggests the remote advantage is gone by about 250 ns
(ratios of 0.94 to 1.09), but that run's placement was not recorded.

The reading that fits both: runs under backoff pay for themselves while the
work between updates is short compared with the cost of moving the line.
Locally that cost is tens of nanoseconds; across sockets on this machine it
is several hundred. This is a refinement suggested by the data, not something
the data was collected to test.

## Earlier measurements

A standalone single-file microbenchmark (one padded `AtomicU64`, a bare
`fetch_add` with no preceding load and no block boundaries) run on hebrides
the same day showed FAA at 84 to 94 Mops/s from 2 to 64 threads with zero
work, CAS with backoff ahead only at 2 and 4 threads, and FAA ahead everywhere
with 60 ns of work. It was a single unbound process, and its FAA loop differs
from the one here, which loads the counter before every update, as UBQ's
producer path loads its head before each CAS. Its numbers should not be combined with the tables above.

On a 4-vCPU Sapphire Rapids VM this binary shows the local-node shape (CAS
with backoff 2.1x and SegQueue-style 1.4x FAA at 2 updaters with zero work,
both below 0.8x with 58 ns of work).

## Limits

- One machine, one architecture. Cross-socket line transfers on this platform
  may be unusually expensive; nothing here says the remote result carries to
  other two-socket systems.
- The bound runs have three repeats, two think levels and at most 64
  updaters. The 250 ns and 1 us levels have only been run unbound.
- Only the counter is placed. The stop flag and each updater's stack land
  wherever they land; neither is touched in the update loop's hot path.
- Updaters were always on node 0. The symmetric case, and updaters spanning
  both nodes (labelled `mixed`), are untested.
- A counter, not a queue. Nothing here measures UBQ end to end, and whether
  `bench_grid` results on hebrides carry the same two levels is unchecked.
- The think work is register-only. Real producers touch the slot they
  reserved, which also moves cache lines.
- Only crossbeam's spin backoff is modelled.

## Next steps

1. Rerun all four think levels with five repeats per node
   (`--counter-node 0,1`), to locate where the remote advantage ends.
2. Check `bench_grid` on hebrides for the same two levels. UBQ's head words
   and blocks are allocated by whichever thread gets there first.
3. Figure: backoff/FAA throughput ratio against updater count, one panel per
   counter node, one line per think level, same-updater fraction as marker
   fill.
4. Repeat on Grace and MareNostrum 5 through `slurm/focused_contention.sbatch`
   with memory bound.
5. Queue-level check: compare UBQ's current CAS reservation with an FAA
   reservation of the same packed head, with memory placement controlled and
   under scenarios that carry payloads. No FAA variant of the head exists in
   the queue registry today.

## Draft paragraph for the paper

Supported by the `numactl` runs and the `--counter-node` repeat above; three
repeats each, one machine.

> Where a CAS loop with spin backoff, or SegQueue's reservation, outruns FAA
> on a contended head word, it does so by serialising access rather than by
> making updates cheaper: in every such configuration we measured, at least
> 82% of the backoff method's successful updates directly followed the same
> thread's previous update. Whether that serialisation pays depends on the
> cost of moving the cache line. On a two-socket, 160-core Neoverse-N1
> machine with the head word on the updaters' own NUMA node, the backoff
> methods lead only at up to eight threads and with no work between
> reservations (2.3x to 4.5x); with 63 ns of thread-local work they are at
> best level with FAA, and from 16 threads FAA is 4x to 30x faster. With the
> head word on the other node, where one update costs several hundred
> nanoseconds, the backoff methods lead by 1.6x to 5x from 4 to 64 threads with
> or without that work. Zero-work microbenchmarks of a head word, and any
> multi-socket measurement that does not fix memory placement, therefore say
> little about which primitive a queue should use.
