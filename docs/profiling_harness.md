# Queue bottleneck profiling on MareNostrum 5 and Grace

This is the operational runbook for the versioned profiling harness. It is
designed to answer three different questions without conflating them:

1. Where does throughput stop scaling as producer and consumer contention
   changes?
2. Which resource moves at that inflection—allocator traffic, faults, cache
   misses, branch behavior, CPU time, or atomic/exclusive-operation pressure?
3. Which functions and source lines account for the observed resource?

The Slurm option is spelled **`--constraint=perfparanoid`**. The common
`--constaint` spelling is a typo. Every submission helper in this harness
requests the correct constraint.

The workflow follows BSC's counter-access requirement and full-node resource
model. See the [BSC tools guidance](https://tools.bsc.es/mn_users) and
[MareNostrum 5 Slurm documentation](https://www.bsc.es/supportkc/docs/MareNostrum5/slurm/).

## Measurement architecture

The ready-to-run full contention workflow is documented separately in the
[Performix contention campaign](performix_contention_campaign.md). It selects
per-queue throughput winners and matched one-factor batch/backoff/block
contrasts, then imports self-identifying archives into the Performix app.

```text
campaign JSON ── deterministic expansion ── candidate JSONL plans
                                                    │
short schema-v7 bench_grid throughput frontier ─────┤
                                                    ├── evidence-driven selector
optional portable perf screen on MN5 + Grace ───────┤
                                                    │
                                                    ▼
                                      bounded representative APX plans
        │
        ├── code hotspots on supported MN5/Grace installations
        └── Arm microarchitecture/memory recipes on Grace
        │
        ▼
raw immutable case directories ── normalization ── CSV/JSON ── plot report
```

Each collected case has two core workload executions:

- An **unprofiled shadow** calibrates and measures a drained, empty-to-empty
  handoff. Its throughput is the only value used for performance comparisons.
- The collector receives the shadow's exact items-per-producer count. It sees
  no calibration loop, and every recipe performs the same logical work.

When a campaign supplies `items_per_producer` explicitly, the shadow runs that
same fixed count without a collector; it is never omitted. Thus neither APX nor
`perf stat` overhead can become the reported throughput in fixed-count plans.

This separation is mandatory. APX recipe overhead can differ by orders of
magnitude, so recipe-internal throughput is diagnostic metadata, not a queue
performance result.

Portable-screen stages add a third, exact-work execution with the intrusive
allocator counter enabled. Keeping that probe out of both the throughput
shadow and the perf/APX workload prevents its shared counters from becoming
the bottleneck being measured. It emits allocation calls,
allocated/deallocated bytes, live-byte delta, and peak additional live bytes.
The normal shadow independently records `getrusage` user/system CPU time, page
faults, and context switches. Metrics are normalized per item where useful.

## Capability matrix

| Layer | MN5 CPU nodes | Grace nodes | Interpretation |
|---|---:|---:|---|
| `perf stat` portable screen | yes | yes | Cross-machine event layer; inspect multiplex percentages |
| Allocator + OS interval probes | yes | yes | Exact handoff interval; excludes queue/thread construction |
| APX Code Hotspots | capability-gated | yes | Function/source attribution |
| APX CPU Microarchitecture | no by default | yes | Front-end/back-end/retiring/speculation diagnosis |
| APX Instruction Mix | no by default | yes | Loads, stores, branches, barriers, and exclusive operations |
| Focused atomic/head experiments | yes | yes | Mechanism evidence, not end-to-end queue proof |

Arm Performix is primarily for Arm Neoverse systems, although its target
support includes a narrower Linux-x64 path. The generator therefore permits
only `code_hotspots` for MN5 and records an `unavailable` result instead of
failing an entire array when the CLI/module or recipe is absent. Deep Arm PMU
recipes remain Grace-only. See [Arm Performix](https://developer.arm.com/servers-and-cloud-computing/arm-performix),
the [installation/platform guide](https://learn.arm.com/install-guides/performix/),
and Arm's [practical recipe guidance](https://developer.arm.com/community/arm-community-blogs/b/servers-and-cloud-computing-blog/posts/arm-performix-practical-performance-analysis-for-arm-based-servers).

APX cases default to 30 seconds because Arm recommends sufficiently long
captures (at least about 20 seconds) for stable sampling. The runner executes
`target prepare`, `recipe ready`, `recipe run --deploy-tools`, run inspection,
archive export, and daemon teardown for every case. Archives are parsed by
column names rather than fixed positions.

## Files added by the harness

- `profiling/campaign.example.json`: broad, versioned candidate matrix.
- `profiling/campaign.throughput-guided.json`: full primary scenario ladder
  used only to select a small evidence-backed Performix plan from a raw sweep.
- `profiling/campaign.frontier-guided.json`: Grace batch-256 LUBQ inquiry
  selected from an existing `bench_grid` frontier, with UBQ and Crossbeam
  controls.
- `scripts/make_profile_plan.py`: validates and deterministically expands the
  matrix into one JSON object per Slurm array element.
- `scripts/select_profile_plan.py`: reads either profiling summaries or one
  coherent schema-v7 `bench_grid` run, then bounds perf/APX work using observed
  cliffs and their predecessors.
- `scripts/run_profile_case.py`: shadow calibration, exact-work collection,
  provenance, APX export, and resumable status handling.
- `slurm/submit_profile_plan.sh` and `slurm/profile_case.sbatch`: full-node
  arrays for MN5 and Grace.
- `scripts/aggregate_profile_results.py`: dynamic APX/perf parsing, quality
  gates, long-form metrics, robust summaries, and variant effects.
- `scripts/plot_profile_results.py`: coverage, scaling, allocator pressure,
  contention, bottleneck fingerprint, hotspots, and variant-effect figures.
- `slurm/submit_focused_contention.sh`: focused atomic-update and head-reload
  experiments across powers-of-two thread counts.

## Phase 0: build and validate

Run these commands from the repository deployed on the BSC project filesystem:

```bash
export ACCOUNT=bsc18
export UBQ=/gpfs/projects/$ACCOUNT/$USER/UBQ
mkdir -p "$UBQ/profiling_results/plans"

mn5_build=$($UBQ/slurm/submit_build.sh mn5)
grace_build=$($UBQ/slurm/submit_build.sh grace)
printf 'MN5 build=%s Grace build=%s\n' "$mn5_build" "$grace_build"
```

The build installs `bench_profile`, the separately instrumented
`bench_profile_alloc`, `bench_atomic_updates`, and `bench_head_reload` beside
`bench_grid`. The normal profile binary contains no counting allocator;
`bench_profile_alloc` is invoked only for the diagnostic allocator pass. Both
keep frame pointers and line debug information separate from the comparative
grid binary.

Validate the declared campaign before allocating nodes:

```bash
python3 "$UBQ/scripts/make_profile_plan.py" \
  "$UBQ/profiling/campaign.example.json" --summary-only
```

The broad example contains 4,334 cases. It is a candidate universe, **not a
recommendation to submit every case at once**. Submit one phase and cluster at
a time. APX candidates should be pruned after the portable screen.

## Raw-throughput-guided Performix path

Use this path when a short schema-v7 `bench_grid` sweep already covers the
primary producer-, consumer-, and balanced-contention ladders. The candidate
plan deliberately declares every adjacent power-of-two scenario; it is a
selection universe and must not be submitted directly. Missing intermediate
raw points are not bridged and called local cliffs.

The frontier loader admits only completed positive-throughput handoffs with
authoritative affinity, the expected machine parallelism, one source commit,
and one protocol cohort. Timed-out and incomplete samples are recorded in the
selection sidecar as discards, never converted to zero. With
`--representatives-per-coordinate 3`, each chosen coordinate retains the
cliff-driving queue variation, the fastest evidenced control, and the slowest
or most throughput-distant evidenced contrast. Candidate variations without
enough successful repeats are recorded as unavailable and are not profiled.

Generate a Grace Code Hotspots candidate universe and select one-repeat pilots
from `grace-7`:

```bash
grace7_frontier="$UBQ/bench_results/runs/grace-7"

python3 "$UBQ/scripts/make_profile_plan.py" \
  "$UBQ/profiling/campaign.throughput-guided.json" \
  --stage throughput-apx-hotspots --cluster grace \
  --output "$UBQ/profiling_results/plans/grace7-hotspots-candidates.jsonl"

python3 "$UBQ/scripts/select_profile_plan.py" \
  "$UBQ/profiling_results/plans/grace7-hotspots-candidates.jsonl" \
  "$grace7_frontier" \
  --output "$UBQ/profiling_results/plans/grace7-hotspots-pilot.jsonl" \
  --score-queue ubq --score-queue lubq \
  --always-keep-queue-variant 'ubq:balanced,1,page,crossbeam' \
  --min-evidence-repeats 3 --plan-repeats 1 \
  --representatives-per-coordinate 3 --cliffs-per-shape 1 \
  --no-anchors --no-endpoints --max-cases 18
```

On the current local `grace-7` snapshot this produces 18 cases: three
variations at both sides of one cliff in each primary shape. The motivating
local transitions are:

- producer contention, scalar UBQ yield: `64p1c` 7.76 Mops/s to `128p1c`
  3.02 Mops/s, scaling efficiency 0.196;
- consumer contention, batch-256 LUBQ yield: `1p64c` 626.7 Mops/s to
  `1p128c` 226.6 Mops/s, scaling efficiency 0.182;
- balanced contention, batch-4096 UBQ crossbeam: `32p32c` 1.98 Gops/s to
  `64p64c` 440.0 Mops/s, scaling efficiency 0.111.

These are targeting observations, not bottleneck diagnoses. The sidecar pins
the 59-record input hash, machine label, commit `766866f`, dirty-tree flag,
protocol hash, 1,092 admitted samples, and 2,565 non-completed discards. A new
unprofiled fixed-work shadow beside every Performix capture supplies the
authoritative throughput for the current binary.

Submit Code Hotspots first, then generate the two hardware-recipe pilot using
the same selection policy:

```bash
$UBQ/slurm/submit_profile_plan.sh grace \
  "$UBQ/profiling_results/plans/grace7-hotspots-pilot.jsonl" \
  --after "$grace_build" --time 00:35:00 --max-concurrent 1 \
  --max-submit-tasks 18 \
  --results-root "$UBQ/profiling_results/throughput_guided_raw"

python3 "$UBQ/scripts/make_profile_plan.py" \
  "$UBQ/profiling/campaign.throughput-guided.json" \
  --stage throughput-apx-hardware --cluster grace \
  --output "$UBQ/profiling_results/plans/grace7-hardware-candidates.jsonl"

python3 "$UBQ/scripts/select_profile_plan.py" \
  "$UBQ/profiling_results/plans/grace7-hardware-candidates.jsonl" \
  "$grace7_frontier" \
  --output "$UBQ/profiling_results/plans/grace7-hardware-pilot.jsonl" \
  --score-queue ubq --score-queue lubq \
  --always-keep-queue-variant 'ubq:balanced,1,page,crossbeam' \
  --min-evidence-repeats 3 --plan-repeats 1 \
  --representatives-per-coordinate 3 --cliffs-per-shape 1 \
  --no-anchors --no-endpoints --max-cases 36

$UBQ/slurm/submit_profile_plan.sh grace \
  "$UBQ/profiling_results/plans/grace7-hardware-pilot.jsonl" \
  --after "$grace_build" --time 00:35:00 --max-concurrent 1 \
  --max-submit-tasks 36 \
  --results-root "$UBQ/profiling_results/throughput_guided_raw"
```

The 36 hardware cases are the same six coordinates and three variations, with
CPU Microarchitecture and Memory Access recipes. Inspect APX readiness,
archive validity, sample counts, and the fixed-work shadow ratios before
raising `--plan-repeats` to three. When `mn5-7` arrives, run the same hotspot
stage with `--cluster mn5`; the campaign automatically omits Grace-only deep
recipes and scenarios that exceed MN5's capacity.

## Historical grace-6 portable-screen path

Use this path when a broad `bench_grid` frontier already exists. It treats the
old throughput data only as targeting evidence; new fixed-work shadows and
portable counters remain the comparative measurements. Never turn timed-out
or failed frontier rows into zero throughput.

The archived `grace-6` records are source-revision coherent and identify two
batch-256 LUBQ transitions worth attributing:

- consumer contention at `1p16c` to `1p32c`;
- balanced MPMC at `32p32c` to `64p64c`.

The latter crosses Grace's 72-core NUMA boundary, so interpret it as both a
scaling and topology contrast. UBQ with crossbeam backoff and Crossbeam
BatchQueue are retained at every coordinate as controls.

On the machine containing the rsynced frontier, generate candidates and let
the selector retain both sides of those LUBQ cliffs:

```bash
python3 "$UBQ/scripts/make_profile_plan.py" \
  "$UBQ/profiling/campaign.frontier-guided.json" \
  --stage frontier-portable-screen --cluster grace \
  --output "$UBQ/profiling_results/plans/frontier-screen-grace-candidates.jsonl"

python3 "$UBQ/scripts/select_profile_plan.py" \
  "$UBQ/profiling_results/plans/frontier-screen-grace-candidates.jsonl" \
  "$UBQ/bench_results/runs/grace-6" \
  --output "$UBQ/profiling_results/plans/frontier-screen-grace.jsonl" \
  --score-queue-variant lubq \
  --shape consumer_contention --shape balanced \
  --batch-size 256 --min-evidence-repeats 3 \
  --no-anchors --no-endpoints --max-cases 60

wc -l "$UBQ/profiling_results/plans/frontier-screen-grace.jsonl"
# 60
```

The selection sidecar hashes the 59 input records and records their machine
label, git revision, dirty flag, protocol cohort, discarded failures, and the
evidence for every coordinate. The focused campaign has a new campaign ID so
it cannot inherit missing cases from an earlier broad attempt.

Versions of `submit_profile_plan.sh` before this change wrote `campaign.json`
before `sbatch` returned. A QOS-rejected attempt could therefore claim that all
1,360 cases had been submitted even when no case directory existed. The helper
now records only accepted jobs and stores their Slurm job IDs. Keep any such
old manifest for audit, but do not aggregate it with this focused result root.

Submit into an isolated result root. `--max-submit-tasks 60` is a local
preflight guard for accidentally passing the candidate plan; it does not
reserve QOS capacity or account for the user's other queued jobs.

```bash
frontier_raw="$UBQ/profiling_results/frontier_raw"
grace_screen=$($UBQ/slurm/submit_profile_plan.sh grace \
  "$UBQ/profiling_results/plans/frontier-screen-grace.jsonl" \
  --after "$grace_build" --time 00:12:00 --max-concurrent 4 \
  --max-submit-tasks 60 --results-root "$frontier_raw")
```

After those 60 cases finish, aggregate only the isolated campaign and create
a one-repeat Code Hotspots pilot from its new counter evidence:

```bash
python3 "$UBQ/scripts/aggregate_profile_results.py" \
  "$frontier_raw" \
  --out-dir "$UBQ/profiling_results/aggregate/frontier-screen"

python3 "$UBQ/scripts/make_profile_plan.py" \
  "$UBQ/profiling/campaign.frontier-guided.json" \
  --stage frontier-apx-hotspots --cluster grace \
  --output "$UBQ/profiling_results/plans/frontier-hotspots-grace-candidates.jsonl"

python3 "$UBQ/scripts/select_profile_plan.py" \
  "$UBQ/profiling_results/plans/frontier-hotspots-grace-candidates.jsonl" \
  "$UBQ/profiling_results/aggregate/frontier-screen/cell_summary.csv" \
  --output "$UBQ/profiling_results/plans/frontier-hotspots-grace-pilot.jsonl" \
  --score-queue-variant lubq \
  --shape consumer_contention --shape balanced \
  --batch-size 256 --min-evidence-repeats 5 --plan-repeats 1 \
  --no-anchors --no-endpoints --max-cases 12

$UBQ/slurm/submit_profile_plan.sh grace \
  "$UBQ/profiling_results/plans/frontier-hotspots-grace-pilot.jsonl" \
  --after "$grace_build" --time 00:35:00 --max-concurrent 1 \
  --max-submit-tasks 12 --results-root "$frontier_raw"
```

Inspect the pilot's APX readiness, archive, sample count, and hotspots before
adding repeats or the CPU Microarchitecture and Instruction Mix recipes. The
scalar UBQ backoff split and timed-out LUBQ scalar cells are separate follow-up
questions; they are deliberately absent from this first batch-256 inquiry.

## Phase 1: portable counter screen from scratch

Generate independent plans so each cluster can use its own build dependency:

```bash
python3 "$UBQ/scripts/make_profile_plan.py" \
  "$UBQ/profiling/campaign.example.json" \
  --stage portable-screen --cluster mn5 \
  --output "$UBQ/profiling_results/plans/screen-mn5.jsonl"

python3 "$UBQ/scripts/make_profile_plan.py" \
  "$UBQ/profiling/campaign.example.json" \
  --stage portable-screen --cluster grace \
  --output "$UBQ/profiling_results/plans/screen-grace.jsonl"
```

Submit them only when no compatible frontier exists. Perf-only arrays default
to four simultaneous full nodes; reduce that with `--max-concurrent` if
allocation pressure matters. A Slurm array throttle such as `%4` controls
running elements only: every array element is still a submitted job for
`QOSMaxSubmitJobPerUserLimit`. The Grace plan below has 1,360 elements, so it
must be narrowed or split according to the site's current submit allowance.

```bash
mn5_screen=$($UBQ/slurm/submit_profile_plan.sh mn5 \
  "$UBQ/profiling_results/plans/screen-mn5.jsonl" \
  --after "$mn5_build" --time 00:12:00 --max-concurrent 4)

grace_screen=$($UBQ/slurm/submit_profile_plan.sh grace \
  "$UBQ/profiling_results/plans/screen-grace.jsonl" \
  --after "$grace_build" --time 00:12:00 --max-concurrent 4)
```

The screen covers scalar and native-batch paths for UBQ, LUBQ,
Crossbeam SegQueue/BatchQueue, `concurrent-queue`, mutex+VecDeque, and the
Michael–Scott queue. Shapes distinguish producer contention (`Np1c`),
consumer contention (`1pNc`), and balanced MPMC (`NpNc`). Five repeats are the
minimum default for screen summaries.

`allocator_probe=true` on the portable and topology stages adds one intrusive
exact-count diagnostic execution per case. It is never used for throughput,
cycles, APX samples, or other performance claims. APX stages leave it disabled
because the corresponding screen cells already provide allocator counts.

Run topology stages next. They deliberately straddle the MN5 56-core and
full-node regions and Grace 72-core and full-node regions:

```bash
python3 "$UBQ/scripts/make_profile_plan.py" \
  "$UBQ/profiling/campaign.example.json" --stage mn5-topology --cluster mn5 \
  --output "$UBQ/profiling_results/plans/topology-mn5.jsonl"
python3 "$UBQ/scripts/make_profile_plan.py" \
  "$UBQ/profiling/campaign.example.json" --stage grace-topology --cluster grace \
  --output "$UBQ/profiling_results/plans/topology-grace.jsonl"

$UBQ/slurm/submit_profile_plan.sh mn5 \
  "$UBQ/profiling_results/plans/topology-mn5.jsonl" --after "$mn5_screen"
$UBQ/slurm/submit_profile_plan.sh grace \
  "$UBQ/profiling_results/plans/topology-grace.jsonl" --after "$grace_screen"
```

After the screen and topology jobs finish, normalize them:

```bash
python3 "$UBQ/scripts/aggregate_profile_results.py" \
  "$UBQ/profiling_results/raw" \
  --out-dir "$UBQ/profiling_results/aggregate/screen"
```

Inspect `coverage.json` and `quality.csv` before selecting APX work. An
authoritative case requires completed status, authoritative worker affinity,
the fixed-work collector result, matching shadow/collector item counts, valid
APX status where relevant, and any stage-specific timing/sample threshold.
Perf multiplexing below 90% is retained with a warning and its running
percentage, not silently treated as an exact counter.

## Phase 2: evidence-driven APX hotspots

Generate broad candidates, then reduce them with the screen evidence:

```bash
for cluster in mn5 grace; do
  python3 "$UBQ/scripts/make_profile_plan.py" \
    "$UBQ/profiling/campaign.example.json" \
    --stage apx-hotspots --cluster "$cluster" \
    --output "$UBQ/profiling_results/plans/hotspots-$cluster-candidates.jsonl"

  python3 "$UBQ/scripts/select_profile_plan.py" \
    "$UBQ/profiling_results/plans/hotspots-$cluster-candidates.jsonl" \
    "$UBQ/profiling_results/aggregate/screen/cell_summary.csv" \
    --output "$UBQ/profiling_results/plans/hotspots-$cluster.jsonl" \
    --cliffs-per-shape 1 --max-cases 500
done
```

The selection sidecar records every retained coordinate and why it was
selected. Selection happens at `(machine, scenario, batch)` coordinates and
keeps all candidate queues, variants, repeats, and recipes at that coordinate,
so cross-queue attribution remains meaningful. For expensive APX plans, add
`--representatives-per-coordinate 3` to retain only evidenced cliff, fast, and
slow/span representatives; use `--always-keep-queue-variant` when one exact
control must remain at every compatible coordinate.

Submit APX arrays conservatively. Their default concurrency is one:

```bash
$UBQ/slurm/submit_profile_plan.sh grace \
  "$UBQ/profiling_results/plans/hotspots-grace.jsonl" \
  --after "$grace_build" --time 00:35:00 --max-concurrent 1

$UBQ/slurm/submit_profile_plan.sh mn5 \
  "$UBQ/profiling_results/plans/hotspots-mn5.jsonl" \
  --after "$mn5_build" --time 00:35:00 --max-concurrent 1
```

On a site without an MN5 APX module, those cases become `unavailable` rather
than corrupting coverage. To use a project-staged x64 CLI, set
`APX_MODULE=none` and `APX_BIN_DIR=/path/to/apx/bin` at submission time. Recipe
readiness remains authoritative.

## Phase 3: Grace deep attribution

Use deep recipes only on cells justified by the screen/hotspot evidence:

```bash
python3 "$UBQ/scripts/make_profile_plan.py" \
  "$UBQ/profiling/campaign.example.json" \
  --stage apx-grace-deep --cluster grace \
  --output "$UBQ/profiling_results/plans/deep-grace-candidates.jsonl"

python3 "$UBQ/scripts/select_profile_plan.py" \
  "$UBQ/profiling_results/plans/deep-grace-candidates.jsonl" \
  "$UBQ/profiling_results/aggregate/screen/cell_summary.csv" \
  --output "$UBQ/profiling_results/plans/deep-grace.jsonl" \
  --cliffs-per-shape 1 --max-cases 500

$UBQ/slurm/submit_profile_plan.sh grace \
  "$UBQ/profiling_results/plans/deep-grace.jsonl" \
  --after "$grace_build" --time 00:45:00 --max-concurrent 1
```

Start with Code Hotspots. Use CPU Microarchitecture to decide whether a cliff
is front-end, back-end, retirement, or speculation limited. Use Instruction
Mix to test an atomic-contention hypothesis through barrier/exclusive
instruction shares and store-exclusive failures. Do not compare throughput
between those recipes.

## Focused atomic experiments

The end-to-end queue grid is the primary evidence. The focused binaries answer
narrow mechanism questions across powers-of-two thread counts:

```bash
mn5_focused=$($UBQ/slurm/submit_focused_contention.sh mn5 --after "$mn5_build")
grace_focused=$($UBQ/slurm/submit_focused_contention.sh grace --after "$grace_build")
```

Render their existing dedicated reports after completion:

```bash
python3 "$UBQ/scripts/plot_atomic_updates.py" \
  --runs-dir "$UBQ/profiling_results/focused" \
  --out-dir "$UBQ/profiling_results/plots/focused"

python3 "$UBQ/scripts/plot_head_reload.py" \
  "$UBQ"/profiling_results/focused/*/*/head_reload.json \
  --out-dir "$UBQ/profiling_results/plots/focused"
```

`bench_atomic_updates` separates FAA, CAS, CAS+backoff, and SegQueue-style
updates while varying updater count and block boundary frequency.
`bench_head_reload` measures the upper-bound benefit of avoiding wide head
reloads after low-word CAS failures. Their mixed-width overlapping atomics are
explicit hardware experiments outside Rust's supported memory model; use them
to generate hypotheses, never as proof that an end-to-end queue is correct or
faster.

## Final aggregation and visual report

Install plotting dependencies once on the analysis machine:

```bash
python3 -m pip install -r "$UBQ/requirements-plot.txt"
```

Then aggregate the complete raw root and render:

```bash
python3 "$UBQ/scripts/aggregate_profile_results.py" \
  "$UBQ/profiling_results/raw" \
  --out-dir "$UBQ/profiling_results/aggregate/full"

python3 "$UBQ/scripts/plot_profile_results.py" \
  "$UBQ/profiling_results/aggregate/full" \
  --out-dir "$UBQ/profiling_results/plots/full"
```

Open `profiling_results/plots/full/index.md` first. The generated report has:

- coverage and quality disposition;
- unprofiled throughput scaling by traffic shape and machine;
- allocator, page-fault, and CPU-pressure scaling;
- portable shared-state contention symptoms (throughput, cycles/op, IPC, and
  cache misses);
- a cross-queue bottleneck fingerprint normalized per metric;
- APX self-sample hotspot heatmaps;
- independent-repeat build-variant effect intervals, or true paired effects
  only when genuinely paired data was imported.

Use `--machine`, `--recipe`, `--scenario`, and `--variant` repeatedly to make
focused reports. `--stress-scenario` overrides the automatically chosen,
well-covered balanced-MPMC fingerprint case.

## How to read the signals

| Observation at the same scaling cliff | Likely next hypothesis | Confirmation |
|---|---|---|
| Alloc calls or bytes/item rise; `malloc`, `sysmalloc`, `mprotect`, or allocator locks dominate hotspots | Block/page creation or reclamation pressure | Compare scalar vs batch, block size, allocator variant, and faults/Mitem |
| Allocation metrics stay flat while cycles/op and CPU ns/item rise and IPC falls with thread count | Shared atomic/cache-line contention | APX exclusive failures/barrier mix; focused CAS/FAA curves; hot atomic helper symbols |
| Producer-contention shape collapses but consumer-contention does not | Producer head/reservation serialization | Compare UBQ/LUBQ sharding and producer-heavy scenarios |
| Consumer-contention shape collapses, especially in batch mode | Consumer head, slot publication waits, or per-item consumed accounting | Consumer-heavy hotspots and batch-size sweep |
| Cache misses/Mop and back-end-bound percentage rise without allocator traffic | Metadata/data locality, false sharing, or pointer chasing | Queue/block layout variant and APX source attribution |
| Faults/Mitem and system CPU rise at block boundaries | Page provisioning/first-touch behavior | Pre-faulted or magazine distributor variant; NUMA placement control |
| Larger batches improve throughput but not allocations/item | Atomic reservation amortization | Cycles/op and exclusive-operation metrics should fall |
| Larger batches reduce allocations/item and allocator hotspots | Allocation/block turnover amortization | Block-size and page-distributor variant |

Correlations are not root causes. Require the cliff, counter movement, and code
attribution to agree, then validate one change at a time.

### Preliminary evidence already in this repository

The legacy Grace Code Hotspots archive for batched Crossbeam BatchQueue
(`1p1c`, batch 256) is a useful hypothesis generator: approximately 34.3% of
self samples land in `__aarch64_ldadd8_acq_rel`, 20.4% in the batch iterator,
11.7% in `mprotect`, 8.7% in `send_batch`, 7.1% in the allocator's private lock
wait, and 3.8% in `sysmalloc`. That is simultaneous evidence for shared atomic
traffic and page/allocator pressure, even with only two workers. It is not a
comparative conclusion: it is one legacy capture whose calibration/warmup was
inside APX. The fixed-work, multi-repeat protocol above exists to test whether
those signals track end-to-end cliffs across queues and thread counts.

## Allocator and page-distributor experiments

Do not begin with a custom page distributor merely because allocator symbols
appear somewhere in a profile. Prototype it only when all of these hold:

1. allocator/system calls account for material self samples at a repeatable
   end-to-end scaling cliff;
2. allocation calls, bytes/item, faults, or system CPU move with that cliff;
3. the movement follows block turnover (batch size, block size, or producer
   count), not just thread startup;
4. the result reproduces on fixed work with adequate APX samples;
5. a cheaper allocator/pre-fault experiment improves the same signals.

A defensible first distributor is a bounded, sharded magazine of raw queue
pages, not an unbounded global freelist. Record at least shard count, magazine
capacity, refill batch, page size, first-touch policy, NUMA policy, retained
bytes, refill/return counts, and fallback allocations. Preserve alignment and
the original allocation layout, and define teardown/reclamation before
optimizing the fast path. Watch for ABA, cross-thread ownership, remote-NUMA
reuse, memory retention at low load, and a new refcount/CAS bottleneck.

Represent each implementation as a campaign variant rather than changing a
label after collection:

```json
{
  "variants": [
    {
      "name": "global-pages",
      "is_baseline": true,
      "binary": "artifacts/{cluster}/variants/global/bench_profile",
      "allocator_binary": "artifacts/{cluster}/variants/global/bench_profile_alloc",
      "factors": {
        "allocator": "system",
        "page_distributor": "global",
        "shards": 1
      }
    },
    {
      "name": "sharded-magazine",
      "binary": "artifacts/{cluster}/variants/sharded/bench_profile",
      "allocator_binary": "artifacts/{cluster}/variants/sharded/bench_profile_alloc",
      "factors": {
        "allocator": "system",
        "page_distributor": "sharded-magazine",
        "shards": 16,
        "magazine_pages": 32,
        "first_touch": "producer"
      }
    }
  ]
}
```

For an allocator available through dynamic interposition, a variant may also
declare workload-only environment variables:

```json
{
  "name": "jemalloc-preload",
  "binary": "artifacts/{cluster}/bench_profile",
  "allocator_binary": "artifacts/{cluster}/bench_profile_alloc",
  "env": {
    "LD_PRELOAD": "/project/lib/libjemalloc.so",
    "MALLOC_CONF": "background_thread:false,dirty_decay_ms:-1,muzzy_decay_ms:-1"
  },
  "factors": {"allocator": "jemalloc", "page_distributor": "global"}
}
```

The environment is placed on the workload command, not the APX daemon. Prefer
separately hashed binaries when interposition behavior is uncertain. Mark
exactly one variant `is_baseline`; the aggregator reports independent-repeat
median ratios with bootstrap intervals. The generator deliberately rejects
`pair_variants=true` because separate Slurm array elements are not genuine
pairs. Paired ratios require both treatments in the same randomized allocation
block.

## Result contract and recovery

Raw layout:

```text
profiling_results/raw/<campaign>/<case-id>/
  case.json
  provenance.json
  commands.json
  shadow.json
  allocator-benchmark.json       # optional, intrusive exact-work probe
  benchmark.json
  profiled-benchmark.json        # perf path
  perf.csv                       # perf path
  <run-id>.zip                   # APX path
  status.json
  *.stdout.log / *.stderr.log
```

`case.json` is the immutable experimental identity. Provenance includes binary
hash, source revision/dirty hash, affinity, node, CPU description, Slurm
allocation, module log, APX/perf versions, and `perf_event_paranoid`. The
submission manifest is the union of submitted case IDs, so coverage can report
missing work rather than treating absence as success.

Completed cases are reused. Failed or partial directories require an explicit
runner `--force`; preserve the original directory first if it contains an APX
archive. Capability failures are status `unavailable` with exit code zero so a
single unsupported recipe does not cancel unrelated array elements.

Normalized outputs:

- `trials.csv`: one row per case and authoritative disposition;
- `metrics_long.csv`: raw and derived benchmark, perf, APX, allocator, and OS
  metrics with names/units preserved;
- `hotspots.csv`: function self samples and fractions;
- `quality.csv`: every rejection/warning reason;
- `cell_summary.csv`: median, MAD, quartiles, and deterministic 95% bootstrap
  interval for provenance-identical cells;
- `variant_effects.csv`: independent-repeat treatment/baseline effects;
- `paired_effects.csv`: only populated by explicit, genuinely paired inputs;
- `coverage.json`: planned, observed, authoritative, rejected, and missing
  counts.

Never merge MN5 and Grace into one statistical cell, mix APX recipes, compare
different source/binary hashes as repeats, or interpret a single APX capture as
a stable performance estimate.
