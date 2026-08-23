# Profiling UBQ queues with Arm Performix

This workflow answers a narrow question at a time: where does one queue spend
CPU time for one producer/consumer shape and one operation size? It runs the
same queue handoff shape used by the comparative harness, but as a foreground
process that Arm Performix can launch and sample directly.

## Build once

On a BSC login node with the repository deployed at `$PROJ/UBQ`:

```bash
export PROJ=/gpfs/projects/$ACCOUNT/$USER
build_id=$($PROJ/UBQ/slurm/submit_build.sh grace)
```

The build produces two deliberately separate release artifacts:

- `artifacts/grace/bench_grid` remains the normal comparative binary.
- `artifacts/grace/bench_profile` has line debug information and frame
  pointers for attribution without changing `bench_grid` code generation.

## Profile an exact case

The smallest useful comparison keeps scenario and batch size identical:

```bash
seg_job=$($PROJ/UBQ/slurm/submit_performix.sh grace segqueue 1p1c \
  --batch-size 256 --after "$build_id")

lubq_job=$($PROJ/UBQ/slurm/submit_performix.sh grace lubq 1p1c \
  --batch-size 256 --after "$build_id")

ubq_job=$($PROJ/UBQ/slurm/submit_performix.sh grace ubq 1p1c \
  --batch-size 256 \
  --ubq-label balanced,1,page,crossbeam \
  --after "$build_id")
```

All three jobs may run independently after the build. Each requests an
exclusive Grace node, and `bench_profile` pins only the requested producer and
consumer workers within that allocation. Omit `--batch-size` for scalar queue
operations. Increase the scenario in the usual form (`8p1c`, `1p8c`, `8p8c`,
and so on) to distinguish producer, consumer, and MPMC contention.

The default measured duration is 30 seconds after a two-second warmup. Override
these with `--duration-secs` and `--warmup-secs`. The workload first calibrates
with drained handoff rounds, then performs one long, fixed-item, empty-to-empty
round sized for the requested duration. This avoids sampling the full grid's
worker protocol and avoids measuring a large post-deadline queue drain.

## Select recipes in this order

Start with Code Hotspots:

```bash
$PROJ/UBQ/slurm/submit_performix.sh grace lubq 1p1c \
  --batch-size 256 --recipe code_hotspots --after "$build_id"
```

It identifies hot functions and source lines. Once those are known, run the
same case with:

```bash
--recipe cpu_microarchitecture
--recipe instruction_mix
```

Use CPU Microarchitecture to separate front-end, back-end, speculation, and
retirement pressure; use Instruction Mix to see the balance of loads/stores,
branches, atomics, integer operations, and vector instructions. Keep recipe,
scenario, batch size, source revision, and node type fixed when comparing two
queue implementations.

The BSC installation is unprivileged. The submission job runs `apx recipe
ready` before collection and stops immediately if the chosen recipe is not
usable. Memory Access, Syscall Trace Summary, and System Characterization are
not part of this workflow because the installation notice says they require
elevated privileges.

## Results

Each job writes to:

```text
performix_results/grace/<queue>-<scenario>-batch-<size>-<recipe>/<job-id>/
```

The directory contains:

- `run-info.txt`: node, CPU, binary hash, exact workload, Slurm allocation,
  and APX version.
- `apx-prepare.log`, `apx-ready.log`, and `apx-run.log`: target-agent
  deployment, readiness, and collection diagnostics.
- `run-id.txt` and `apx-run-info.log`: the local Performix run identity and
  metadata.
- `apx-export.log` plus the exported portable run archive.

The job runs `apx target prepare --target localhost` before the readiness check
so the mandatory target agent is deployed or verified on the compute node. It
then runs the recipe with `--deploy-tools`, allowing APX to deploy missing
recipe-specific tools such as `sl-record` and `sl-analyze`. It stops its APX
daemon at job exit and exports the run to GPFS before the allocation ends.
Exported archives can be imported into another Arm Performix CLI or opened
through the GUI for flame graphs, function tables, and source attribution.

## Run the workload without Performix

For a fast validation inside a Grace allocation:

```bash
srun --ntasks=1 --cpus-per-task=144 --cpu-bind=cores \
  $PROJ/UBQ/artifacts/grace/bench_profile \
    --queue lubq --scenario 1p1c --batch-size 256 \
    --warmup-secs 2 --duration-secs 5
```

The final JSON records the exact case, item count, elapsed time, throughput,
and whether worker affinity succeeded. Treat its throughput as a sanity check;
the repeated `bench_grid` records remain the source for comparative performance
claims.

The command syntax follows the Arm Performix 2026.3.1 CLI guide: local
collection uses `apx recipe run <recipe> --workload <command>
--target=localhost`, and persistence uses `apx run export <run-id>
<directory>`. The submission script records the exact APX version at runtime.
