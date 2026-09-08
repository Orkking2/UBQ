# Remote throughput-guided Performix workflow

This is the recommended entry point for a new Grace or MN5 measurement
campaign. It transfers only source and configuration to the cluster, runs a
short throughput frontier, selects APX targets from that frontier, and prints
the exact paths to copy home.

The workflow never submits APX collection before throughput selection. It also
never submits the deeper hardware recipes automatically: inspect Code Hotspots
first, then explicitly submit the hardware plan if the selected pairs warrant
the additional collection time.

## 1. Copy the working tree to BSC

Set the paths on the local machine. The destination should be an existing clone
so the remote build can retain Git provenance. This command does not delete
remote results or artifacts.

```bash
export LOCAL_UBQ="$HOME/GitHub/UBQ"
export REMOTE_HOST=grace-login
export REMOTE_UBQ=/gpfs/projects/bsc18/$USER/UBQ

rsync -az \
  --exclude '/.git/' \
  --exclude '/target/' \
  --exclude '/artifacts/' \
  --exclude '/logs/' \
  --exclude '/bench_results/' \
  --exclude '/profiling_results/' \
  "$LOCAL_UBQ/" "$REMOTE_HOST:$REMOTE_UBQ/"
```

Do not reuse a machine label after source or benchmark parameters change. The
label is deliberately both the workflow identity and the raw frontier
identity. A useful convention is `grace-bulk-consumed-v1`.

## 2. Submit the complete dependency graph

Log in to the target cluster and use `auto` for unattended execution:

```bash
ssh "$REMOTE_HOST"
export ACCOUNT=bsc18
export UBQ=/gpfs/projects/$ACCOUNT/$USER/UBQ
cd "$UBQ"

python3 scripts/performix_campaign_workflow.py \
  auto grace grace-bulk-consumed-v1
```

This initially submits:

```text
build -> full throughput frontier[] -> selection/controller
```

The frontier array runs all 29 feasible Grace scenarios (or all 22 feasible
MN5 scenarios) and the complete configured queue, batch, backoff, and
block/segment matrix. After every frontier array element succeeds, the
controller evaluates those results and extends the graph:

```text
selection/controller -> Code Hotspots part 1 -> ... -> part N
                     -> hardware part 1 -> ... -> part N
```

The hardware chain depends on the final Code Hotspots part. APX parts contain
at most 300 cases and are serialized with `afterok`; individual arrays also
default to one running APX case. The selected Code Hotspots plan contains the
best measured configuration of every queue at every feasible scenario, plus
one-factor cases at discovered cliffs and fixed UBQ/LUBQ controls. The deeper
three-recipe hardware plan remains limited to the discovered/fixed study
scenarios.

No manual stage initiation is required. Progress is always available through:

```bash
python3 scripts/performix_campaign_workflow.py \
  status grace grace-bulk-consumed-v1
```

If the continuation job itself is interrupted after the frontier completed,
rerun its idempotent body from the login node. It reuses a frozen selection,
recognizes fully submitted chunks, and serializes any replacement chunks after
the last accepted one:

```bash
python3 scripts/performix_campaign_workflow.py \
  continue grace grace-bulk-consumed-v1
```

To build the full frontier and Code Hotspots chain but omit the automatic
hardware tail, add `--no-hardware`. Selection controls can also be supplied to
`auto`, for example:

```bash
python3 scripts/performix_campaign_workflow.py \
  auto grace grace-bulk-consumed-v1 \
  --score-queue ubq --tuning-queue ubq --tuning-queue lubq
```

For MN5, replace both occurrences of `grace`. The MN5 campaign automatically
omits the Grace-only hardware-recipe tail.

## 3. Manual stage control (optional)

Log in to the target cluster and run:

```bash
ssh "$REMOTE_HOST"
export ACCOUNT=bsc18
export UBQ=/gpfs/projects/$ACCOUNT/$USER/UBQ
cd "$UBQ"

python3 scripts/performix_campaign_workflow.py \
  start grace grace-bulk-consumed-v1
```

For MN5, replace both occurrences of `grace` appropriately, for example:

```bash
python3 scripts/performix_campaign_workflow.py \
  start mn5 mn5-bulk-consumed-v1
```

`start` submits a build job followed by the feasible contention array with an
`afterok` dependency. It records the job IDs, a content fingerprint of the
deployed benchmark/profiling source, the resolved campaign configuration, and
all output paths in:

```text
profiling_results/workflows/<machine-label>/workflow.json
```

Check progress without changing anything:

```bash
python3 scripts/performix_campaign_workflow.py \
  status grace grace-bulk-consumed-v1
```

If cells failed for an environmental reason, resubmit only against the same
unchanged source. Completed cells are resumed rather than recomputed:

```bash
python3 scripts/performix_campaign_workflow.py \
  start grace grace-bulk-consumed-v1 --resume
```

The command refuses a resume if any executable source, profiling script,
manifest, or campaign configuration changed. Use a new label in that case.

## 4. Select the APX targets dynamically

After the frontier job has left the queue:

```bash
python3 scripts/performix_campaign_workflow.py \
  select grace grace-bulk-consumed-v1
```

On Grace, the default selection produces two frozen plans:

- `hotspots-selected.jsonl` contains the best measured configuration for every
  queue/scenario, one-factor variations at throughput-discovered scaling
  cliffs, and a fixed UBQ/LUBQ attribution panel.
- `hardware-selected.jsonl` contains the throughput winners at the dynamically
  selected and fixed study scenarios for CPU Microarchitecture, Instruction
  Mix, and Memory Access.

The fixed Grace attribution panel is `64p1c`, `143p1c`, `1p64c`, `1p143c`,
`64p64c`, and `72p72c`. It prevents a pre/post optimization comparison from
silently changing both the implementation and its chosen parameters. All
other tuning coordinates come from measured scaling cliffs and their immediate
predecessors.

Useful selection controls include:

```bash
# Let only UBQ throughput choose the cliff coordinates.
python3 scripts/performix_campaign_workflow.py \
  select grace grace-bulk-consumed-v1 --score-queue ubq

# Expand one-factor tuning only for UBQ and LUBQ while retaining all queue winners.
python3 scripts/performix_campaign_workflow.py \
  select grace grace-bulk-consumed-v1 \
  --tuning-queue ubq --tuning-queue lubq

# Use only dynamically discovered scenarios, without the fixed attribution panel.
python3 scripts/performix_campaign_workflow.py \
  select grace grace-bulk-consumed-v1 --no-static-controls
```

Every selection requires three authoritative raw repeats by default. Review
`selection-summary.json` before submitting; it records the dynamic scenarios,
fixed controls, input frontier, source fingerprint, plan hashes, and case
counts. Selection normally refuses an active or incomplete scenario frontier;
`--allow-partial` is available when using incomplete evidence is deliberate and
will record that decision in the summary. MN5 produces the Code Hotspots plan
only because the deeper hardware stage is Grace-only.

## 5. Submit Code Hotspots, then optionally hardware recipes

Submit the single deduplicated hotspot plan:

```bash
python3 scripts/performix_campaign_workflow.py \
  submit grace grace-bulk-consumed-v1 hotspots
```

The default is one simultaneous APX case. Plans larger than 300 cases are
frozen into immutable arrays of at most 300 tasks and chained with `afterok`,
so site submission limits do not cause the arrays to compete with each other.
Use `--chunk-size` only when the target QoS requires a smaller ceiling. Check
progress with `status`. A second submission is rejected unless `--resubmit` is
given, in which case completed cases resume and only missing work should
collect again.

After reviewing the Code Hotspots captures, explicitly submit deeper recipes:

```bash
python3 scripts/performix_campaign_workflow.py \
  submit grace grace-bulk-consumed-v1 hardware
```

When a hotspot job was recorded, the hardware submission automatically gains
an `afterok` dependency on its latest job ID. Use `--after JOBID` to provide an
explicit dependency instead.

MN5 APX remains capability-gated by the installed module and recipes. The raw
throughput and selection stages remain useful even if APX collection is not
available there.

## 6. Copy results home and import into Performix

On the remote machine, print copy-and-import commands for the exact workflow:

```bash
python3 scripts/performix_campaign_workflow.py \
  paths grace grace-bulk-consumed-v1
```

The three directories to preserve are:

```text
profiling_results/workflows/<machine-label>/
bench_results/runs/<machine-label>/
profiling_results/contention_raw/<machine-label>/
```

After copying them to the same relative paths in the local clone:

```bash
cd "$LOCAL_UBQ"
python3 scripts/aggregate_profile_results.py \
  profiling_results/contention_raw/grace-bulk-consumed-v1 \
  --out-dir profiling_results/aggregate/grace-bulk-consumed-v1

python3 scripts/import_performix_runs.py \
  profiling_results/contention_raw/grace-bulk-consumed-v1 \
  --trials-csv profiling_results/aggregate/grace-bulk-consumed-v1/trials.csv
```

Review `apx-import-manifest.csv`, then open the Performix app and perform the
resumable import:

```bash
APX='/Applications/Arm Performix.app/Contents/assets/apx/apx'
python3 scripts/import_performix_runs.py \
  profiling_results/contention_raw/grace-bulk-consumed-v1 \
  --import --apx "$APX" --source "$LOCAL_UBQ/src" \
  --trials-csv profiling_results/aggregate/grace-bulk-consumed-v1/trials.csv
```

The generated `performix-mcp-context.json`, import manifest, and `trials.csv`
provide the exact join from local Performix run IDs to throughput evidence and
matched comparison sets.
