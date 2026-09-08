# Throughput-guided contention campaign for Performix

For a revision-safe, executable remote driver around this procedure, use
[`scripts/performix_campaign_workflow.py`](../scripts/performix_campaign_workflow.py)
and the [remote workflow guide](remote_performix_workflow.md). The commands
below remain the lower-level components used by that driver.

This campaign measures the feasible `Np1c`, `1pNc`, and `NpNc` cases for
`N in {1,2,4,8,16,32,63,64,72,128,143}` without sending the full Cartesian
product through Performix. A short raw-throughput frontier chooses the best
observed configuration of each queue. Performix then collects only those
winners and explicit one-factor tuning contrasts.

The queue set is UBQ, LUBQ, Crossbeam SegQueue/BatchQueue,
`concurrent-queue`, Mutex+VecDeque, Michael–Scott, RBBQ/BBQ, LSCQ through the
`lfqueue` crate, and Moodycamel's `ConcurrentQueue`. The repository does not
currently contain an LCRQ adapter. LSCQ results must not be labelled LCRQ.
wCQ remains excluded because its current dependency has known wrapped-ring
emptiness and split-CAS correctness problems.

UBQ and LUBQ use one host base page per block by design, so their block size is
recorded as `page`, not swept as if it were a tunable integer. The block-size
contrast applies to RBBQ (`64,256,1024,4096`); the analogous LSCQ structural
contrast is segment size (`32,256,1024`). Batch size and backoff are varied for
the implementations that actually expose those operations.

## 1. Build and collect the short throughput frontier

Run from the repository deployed on the BSC project filesystem:

```bash
export ACCOUNT=bsc18
export UBQ=/gpfs/projects/$ACCOUNT/$USER/UBQ
mkdir -p "$UBQ/profiling_results/plans"

grace_build=$($UBQ/slurm/submit_build.sh grace)
mn5_build=$($UBQ/slurm/submit_build.sh mn5)

grace_sweep=$($UBQ/slurm/submit_contention_sweep.sh \
  grace grace-contention-bulk-v1 --after "$grace_build")
mn5_sweep=$($UBQ/slurm/submit_contention_sweep.sh \
  mn5 mn5-contention-bulk-v1 --after "$mn5_build")

printf 'Grace sweep=%s MN5 sweep=%s\n' "$grace_sweep" "$mn5_sweep"
```

`submit_contention_sweep.sh` uses three 500 ms measured repetitions, a 100 ms
warmup, and a 50 ms pilot. Scalar is always included. Native batch sizes are
`8,64,256,1024,4096`; both UBQ and LUBQ backoffs are included. Every scenario
owns one exclusive full node, and each successful repetition is checkpointed.
Rerunning the same command with the same machine label resumes missing cells.

The exact feasible scenario lists are
`manifests/contention-grace-144.txt` (29 unique scenarios) and
`manifests/contention-mn5-112.txt` (22). Grace can run `143p1c`, `1p143c`, and
`72p72c`. MN5 cannot run `128p1c`, `143p1c`, their consumer mirrors, or a
balanced case above `32p32c` without oversubscription, so those cells are
explicitly absent rather than silently run unpinned.

For a longer raw frontier, override only the timing while preserving the
matrix identity:

```bash
UBQ_BENCH_WARMUP_MS=250 UBQ_BENCH_PILOT_MS=100 \
UBQ_BENCH_PHASE_MS=1000 UBQ_BENCH_REPEATS=5 \
  $UBQ/slurm/submit_contention_sweep.sh \
    grace grace-contention-bulk-v1 --after "$grace_build"
```

## 2. Select and collect the best queue configurations

Generate the candidate universe, then select exactly one evidence-backed
throughput winner per queue and scenario. The candidate file is never a
submission plan.

```bash
grace_frontier="$UBQ/bench_results/runs/grace-contention-bulk-v1"
grace_candidates="$UBQ/profiling_results/plans/contention-grace-hotspots-candidates.jsonl"
grace_winners="$UBQ/profiling_results/plans/contention-grace-hotspots-winners-r1.jsonl"

python3 "$UBQ/scripts/make_profile_plan.py" \
  "$UBQ/profiling/campaign.contention-performix.json" \
  --stage contention-apx-hotspots --cluster grace \
  --output "$grace_candidates"

python3 "$UBQ/scripts/select_profile_variations.py" \
  "$grace_candidates" "$grace_frontier" \
  --policy winners --min-evidence-repeats 3 --plan-repeats 1 \
  --max-cases 300 --output "$grace_winners"

grace_hotspots=$($UBQ/slurm/submit_profile_plan.sh grace "$grace_winners" \
  --after "$grace_build" --time 00:35:00 --max-concurrent 1 \
  --max-submit-tasks 300 \
  --results-root "$UBQ/profiling_results/contention_raw")
```

With complete raw coverage, the Grace winner plan contains 261 Code Hotspots
cases: 29 scenarios times 9 queue families, one repeat each. Failed or timed
out raw configurations are never treated as zero; they appear under
`unavailable_configurations` in the `.selection.json` sidecar.

The corresponding feasible MN5 Code Hotspots plan is:

```bash
mn5_frontier="$UBQ/bench_results/runs/mn5-contention-bulk-v1"
mn5_candidates="$UBQ/profiling_results/plans/contention-mn5-hotspots-candidates.jsonl"
mn5_winners="$UBQ/profiling_results/plans/contention-mn5-hotspots-winners-r1.jsonl"

python3 "$UBQ/scripts/make_profile_plan.py" \
  "$UBQ/profiling/campaign.contention-performix.json" \
  --stage contention-apx-hotspots --cluster mn5 \
  --output "$mn5_candidates"
python3 "$UBQ/scripts/select_profile_variations.py" \
  "$mn5_candidates" "$mn5_frontier" \
  --policy winners --min-evidence-repeats 3 --plan-repeats 1 \
  --max-cases 225 --output "$mn5_winners"

$UBQ/slurm/submit_profile_plan.sh mn5 "$mn5_winners" \
  --after "$mn5_build" --time 00:35:00 --max-concurrent 1 \
  --max-submit-tasks 225 \
  --results-root "$UBQ/profiling_results/contention_raw"
```

MN5 APX remains capability-gated. If the x64 APX CLI/module or recipe is not
available, cases are recorded as unavailable. The throughput frontier is
still valid and can select a later collection when that capability exists.

## 3. Select matched batch, backoff, and block/segment contrasts

The one-factor policy starts from each queue's throughput winner at a scenario
and changes one requested dimension while holding the others fixed. This is
the hardware A/B set; it avoids comparing an arbitrary slow configuration to
a winner that differs in several ways.

Start with Code Hotspots at known cliffs and full-node endpoints:

```bash
grace_tuning="$UBQ/profiling_results/plans/contention-grace-hotspots-tuning-r1.jsonl"

python3 "$UBQ/scripts/select_profile_variations.py" \
  "$grace_candidates" "$grace_frontier" \
  --policy one-factor \
  --dimension batch_size --dimension backoff --dimension block_size \
  --scenario 64p1c --scenario 128p1c --scenario 143p1c \
  --scenario 1p64c --scenario 1p128c --scenario 1p143c \
  --scenario 32p32c --scenario 64p64c --scenario 72p72c \
  --min-evidence-repeats 3 --plan-repeats 1 --max-cases 400 \
  --output "$grace_tuning"

$UBQ/slurm/submit_profile_plan.sh grace "$grace_tuning" \
  --after "$grace_build" --time 00:35:00 --max-concurrent 1 \
  --max-submit-tasks 400 \
  --results-root "$UBQ/profiling_results/contention_raw"
```

Completed winner cases have the same deterministic case IDs and are reused.
Each selected case records `selection_evidence`, `comparison_group_id`,
`comparison_role`, and `comparison_sets`. A control belongs to the same
dimension-specific comparison set as all of its batch/backoff/block contrasts.

After Code Hotspots establishes which queue/scenario pairs warrant deeper
work, generate the Grace-only hardware candidates. This example retains the
best queue configuration at the six high-contention endpoints:

```bash
grace_hardware_candidates="$UBQ/profiling_results/plans/contention-grace-hardware-candidates.jsonl"
grace_hardware_winners="$UBQ/profiling_results/plans/contention-grace-hardware-winners-r1.jsonl"

python3 "$UBQ/scripts/make_profile_plan.py" \
  "$UBQ/profiling/campaign.contention-performix.json" \
  --stage contention-apx-hardware --cluster grace \
  --output "$grace_hardware_candidates"

python3 "$UBQ/scripts/select_profile_variations.py" \
  "$grace_hardware_candidates" "$grace_frontier" \
  --policy winners \
  --scenario 128p1c --scenario 143p1c \
  --scenario 1p128c --scenario 1p143c \
  --scenario 64p64c --scenario 72p72c \
  --min-evidence-repeats 3 --plan-repeats 1 --max-cases 200 \
  --output "$grace_hardware_winners"

$UBQ/slurm/submit_profile_plan.sh grace "$grace_hardware_winners" \
  --after "$grace_build" --time 00:45:00 --max-concurrent 1 \
  --max-submit-tasks 200 \
  --results-root "$UBQ/profiling_results/contention_raw"
```

The three recipes are CPU Microarchitecture, Instruction Mix, and Memory
Access. Do not compare their collector-perturbed execution rates. The new
unprofiled shadow beside every capture is the throughput measurement.

To promote a stable pilot, regenerate the same selection with
`--plan-repeats 3`; repeat-one cases are reused and only the missing repeat IDs
perform real collection.

## 4. Transfer and import into the Performix app

Copy raw result directories and the matching raw frontier to the local
machine. Preserve the directory hierarchy and sidecars:

```bash
rsync -a grace-login:"$UBQ/profiling_results/contention_raw/" \
  "$LOCAL_UBQ/profiling_results/contention_raw/"
rsync -a grace-login:"$UBQ/bench_results/runs/grace-contention-bulk-v1/" \
  "$LOCAL_UBQ/bench_results/runs/grace-contention-bulk-v1/"
```

On the local machine, aggregate the shadow/case metadata and inventory the
archives before modifying the Performix library:

```bash
cd "$LOCAL_UBQ"
python3 scripts/aggregate_profile_results.py \
  profiling_results/contention_raw \
  --out-dir profiling_results/aggregate/contention

python3 scripts/import_performix_runs.py \
  profiling_results/contention_raw \
  --trials-csv profiling_results/aggregate/contention/trials.csv
```

Open Arm Performix so its local daemon is available, then import, rename, and
attach the local source tree in one resumable operation:

```bash
APX='/Applications/Arm Performix.app/Contents/assets/apx/apx'
python3 scripts/import_performix_runs.py \
  profiling_results/contention_raw \
  --import --apx "$APX" --source "$LOCAL_UBQ/src" \
  --trials-csv profiling_results/aggregate/contention/trials.csv
```

The importer resumes archives already marked `imported` in its manifest; use
`--force-import` only when duplicate imports are intentional. It writes:

- `apx-import-manifest.csv`: archive hash, remote and local run IDs, stable
  name, queue parameters, shadow throughput, and comparison identity;
- `performix-mcp-context.json`: the same correlations in a form convenient for
  MCP analysis;
- `profiling_results/aggregate/contention/trials.csv`: quality-gated cases.

Join the Performix/MCP `run.id` to `local_run_id` in the import manifest. The
original exported ID remains in `apx_run_id` and joins directly to
`trials.csv`. Stable names have the form
`machine__queue-variant__scenario__batch__recipe__repeat__group__case-hash`,
so the app is usable even without opening the CSV first.

## 5. Interpret in Performix rather than custom plots

Use `comparison_group_id` to keep queue-winner comparisons at the same
scenario, recipe, and repeat. Use `comparison_sets_json` for matched tuning
contrasts. In the app:

1. Start with Code Hotspots and compare function and call-path shares. Locate
   the contended CAS/FAA, retry/backoff, allocator, or syscall path.
2. Open the identical case in CPU Microarchitecture and determine whether the
   added time is retiring, front-end, bad-speculation, or back-end pressure.
3. Use Instruction Mix to test the mechanism: loads/stores, barriers,
   exclusive/atomic instructions, branches, and failed retry work.
4. Use Memory Access only after a source hotspot exists, to distinguish cache
   locality, shared-line traffic, and memory latency from instruction-side
   overhead.
5. Quote throughput only from the unprofiled shadow in `trials.csv` or the
   import manifest. Recipe-internal throughput is diagnostic because collector
   overhead differs by recipe.

For MCP-assisted analysis, provide `performix-mcp-context.json` and
`trials.csv`, then ask for all runs in one `comparison_group_id` or
`comparison_set_id`. That gives the MCP an exact run-to-workload join instead
of relying on import order or anonymous run titles.
