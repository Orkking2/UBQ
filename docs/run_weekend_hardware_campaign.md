# Run the weekend hardware campaign

The executable entry point is `scripts/hardware_campaign.py`. It freezes the current worktree (including uncommitted changes and vendored dependencies), builds two symbolized binaries on the cluster, runs correctness and collector gates, and dispatches the finite campaign. Use a new directory for each cluster. The older `auto` / v4 workflow is independent.

From the updated UBQ checkout on the **Grace login node**:

```bash
python3 scripts/hardware_campaign.py start grace profiling_results/weekend-hw-v1-grace
```

From the updated UBQ checkout on the **MN5 login node**:

```bash
python3 scripts/hardware_campaign.py start mn5 profiling_results/weekend-hw-v1-mn5
```

Copy the changed source, scripts, Slurm wrapper and `profiling/` design/matrix to both checkouts first. The required vendored dependencies are already part of the repository. The `start` command checks for Slurm commands before creating anything; it does not connect to another machine.

Defaults are account `bsc18` (or `$ACCOUNT`), Grace `ngpp/ngp_bsccs`, MN5 `gpp/gp_bsccs`, constraint `perfparanoid`, and 48 hours from launch including queueing. Override `--account`, `--partition`, `--qos`, `--constraint`, or `--hours` on `start` if your allocation requires it. Preserve the standard GPP cohort on MN5; deliberately running other memory cohorts requires a separate campaign label. [BSC's scheduling documentation](https://www.bsc.es/supportkc/docs/MareNostrum5/slurm/) describes the partitions and account QoS names.

`start` returns after starting a detached, lightweight Python submitter on the login node. This process polls Slurm every 30 seconds and survives an SSH disconnect; all compilation and measurements run in Slurm. Its PID, hostname and log path are printed and saved. Keep that login-node process alive for the weekend. The existing `gcc` and `rust` modules must provide the repository's Rust toolchain (1.92 or later). Grace also tries `arm_performix/2026.3.1`; set `APX_MODULE` and/or `APX_BIN_DIR` before launching when the installed module differs. A failed APX module or recipe disables its deep panel through an actual capture gate.

## Preview and monitor

These previews need neither Slurm nor a build:

```bash
python3 scripts/hardware_campaign.py plan grace
python3 scripts/hardware_campaign.py plan mn5
python3 scripts/hardware_campaign.py plan grace --json > /tmp/weekend-grace-plan.json
```

On each cluster, use its campaign directory:

```bash
python3 scripts/hardware_campaign.py status profiling_results/weekend-hw-v1-grace
tail -f profiling_results/weekend-hw-v1-grace/controller.log
```

Status consults accounting, including array element IDs and failed batch steps. A completed sidecar with an OOM/timeout/nonzero scheduler exit is failed. A stale `running` sidecar cannot make a terminal task live; missing accounting is `unknown`, not proof of completion. Controller heartbeat age is reported separately. This uses the array-aware `JobID` field, rather than `JobIDRaw`, which denotes a different identity for arrays. [Slurm's `sacct` documentation](https://slurm.schedmd.com/sacct.html) defines those fields.

If the submitter exits, inspect `controller.log`, then restart it on the **same cluster**:

```bash
python3 scripts/hardware_campaign.py resume profiling_results/weekend-hw-v1-grace
```

The shared controller lock prevents two dispatchers. Resume preserves the original deadline, source, binaries and accepted jobs. It does not rerun failed or successful measurements. If a crash interrupted submission itself, `submit-intent.json` is reconciled by its unique Slurm job name. Ambiguous acceptance stops submission rather than risking duplicates; inspect that file and accounting before changing it. Non-quota submission failures stop the controller with the scheduler error in its log.

If the initial build failed with `can't find push_batch bench`, the original snapshot code omitted `benches/`. The corrected runner copies all standard Cargo target directories and package/toolchain inputs. Sync the corrected `scripts/hardware_campaign.py` and ensure `benches/push_batch.rs` exists in the remote checkout. Start a new campaign directory on the login node so the fix enters the frozen source and the 48-hour budget restarts:

```bash
python3 scripts/hardware_campaign.py start grace profiling_results/weekend-hw-v2-grace
```

Use `mn5` and `weekend-hw-v2-mn5` for the corresponding MN5 retry. Keep the failed directory for diagnostics; `resume` does not rebuild it or refresh its source snapshot.

The controller submits unthrottled arrays and halves submission chunks when the site's actual submit quota rejects a request, down to a single task. It tries again as tasks finish. There are no failed-`afterok` chains and no `%N` running-task limit. Near the deadline it stops admitting work that cannot fit its budget; at 48 hours it cancels this campaign's still-pending tasks. Already-running tasks finish within their allocation limits. Unsubmitted cases are explicitly `not_admitted`.

## What will run

Each measurement repetition gets its own exclusive allocation, with all 144 Grace or 112 MN5 physical worker cores available. Small scenarios remain on exclusive full nodes. CPU IDs come from the allocated cpuset and `lscpu`; one hardware thread per physical core is selected, ordered by NUMA node/socket/core. Producer and consumer roles are interleaved in that ordering. This is a recorded baseline placement, not an intervention that forces remote memory.

| Panel | Grace allocations | MN5 allocations | Work |
| --- | ---: | ---: | --- |
| Correctness gates | 17 | 17 | All 14 configurations plus backoff and completion controls; repeated reuse and exact-once native-batch checks |
| Collector pilots | 7 | 6 | Three PMU groups plus four APX recipes / three perf sampling modes |
| Core matrix | 504 | 504 | 14 configurations × 12 shapes × 3 repetitions |
| Queue memory | 90 | 90 | 10 implementations × 3 producer counts × 3 repetitions; 3 fresh depth processes per allocation |
| Memory scaffolding control | 9 | 9 | Same worker scaffolding, occupancy counter only, no stored payloads |
| Portable software A/B | 18 | 18 | Six pairs × 3 repetitions; both arms share each allocation |
| Deep attribution | 744 | 558 | 56 baseline coordinates plus six paired comparisons × enabled collectors × 3 repetitions |
| **Maximum, plus one build** | **1,389** | **1,202** | Gates and the elapsed deadline can reduce coverage |

The queue lifecycle panel contains 540 fresh processes across both clusters, plus 54 scaffolding controls. The deep panel contains at most 816 Grace captures and 612 MN5 captures, since each A/B allocation captures both arms. Independent allocations may reuse a physical node; node identity is retained for analysis. Replicate zero calibrates the round count; later repetitions replay its frozen schedule.

All handoffs use `u64` values and at most 8,388,608 offered items per round, rounded down to a producer/batch multiple. Workers and handles persist across rounds. Calibration adjusts **round count**, targeting 30 seconds; it never expands the burst cap. A/B calibration chooses the faster arm's schedule, then both arms use identical work, prewarm and placement. Arm order alternates by repetition. Timing counts completed handoffs, not push plus pop as two operations.

The primary executable has no per-item verification counters on its timing path. Count checks happen after every drained round. A separate diagnostic mode checks every value exactly once, including native batch paths. The UBQ gates additionally exercise batch 1024 and the full burst cap, addressing the v4 failure shape. Passing these tests is evidence, not a proof of concurrent queue correctness. A failing configuration or A/B control quarantines its dependent work.

There is an 8 GiB process RSS soft watchdog and a 300-second per-workload timeout. A watcher is not an instantaneous allocation cap. The Slurm allocation requests node memory for workload plus collectors. Ordinary bundles have a 55-minute internal deadline and a 60-minute allocation; deep bundles have a 115-minute deadline and a 120-minute allocation. Preparation, recording, export and cleanup all share that internal budget. Output streams to disk during commands; timeout/signal handling terminates local process groups and preserves diagnostics. Slurm remains the final bound for collector daemons.

## Hardware and memory outputs

The PMU pilot tests these separate, simultaneous user-space groups: cycles/instructions, cache references/misses, and dTLB loads/misses. Each member must produce a numeric result with at least 90% time-running coverage. Unsupported or heavily multiplexed groups are omitted, without blocking the shadow or lifecycle panels. Exact generic-event meanings remain architecture dependent; `perf list` and machine provenance are saved. No automatic claim of equivalent Arm/Intel cache events is made.

Grace attempts `code_hotspots`, `cpu_microarchitecture`, `instruction_mix`, and `memory_access`. MN5 attempts `perf record` with frame-pointer stacks, `perf mem` loads, and `perf c2c`. Each must first produce a complete, matching workload result and at least 100 decodable samples. APX archives also pass the existing archive parser. Pilot success does not establish sample loss rates or symbol resolution quality: inspect raw captures before attribution claims. Missing SPE/PEBS support leaves an explicit coverage gap.

The memory process parks workers at exact empty/full/drained/idle-1s/idle-10s/refilled/redrained/destroyed checkpoints. Depths are 4,096, 65,536 and 524,288 items, with producer handles alive through refill. It records RSS, PSS, private pages, virtual size/high water, fill/drain times, CPU time and faults. These are **process footprints**, including worker stacks and allocator retention. The memory-scaffolding control helps estimate that overhead; its throughput is not a queue result. RSS after destruction is not automatically a leak. C++ allocations are included in process footprint; no Rust-only allocator count is presented as total allocation coverage.

The two software interventions are crossbeam/yield backoff on UBQ/LUBQ and per-item/per-segment completion accounting on UBQ. The latter is a same-source `probe_item_completion` build control. LUBQ retention limits are explicitly deferred because safe excess-block reclamation is not implemented. Exact CAS retry counts, queue-owned cached bytes, allocation-call interception, memory-controller bandwidth, syscall tracing, explicit NUMA interventions and extra repetitions beyond three are also outside this executable iteration.

## Export results

First refresh accounting on the cluster; then generate CSVs there or from a copied campaign directory:

```bash
python3 scripts/hardware_campaign.py status profiling_results/weekend-hw-v1-grace
python3 scripts/hardware_campaign.py report profiling_results/weekend-hw-v1-grace
```

Outputs are `trials.csv`, `memory.csv`, `counters.csv`, and `effects.csv`, alongside source/binary hashes, per-run workload JSON, logs, provenance, perf data and APX ZIP archives. `effects.csv` gives paired throughput and CPU-time ratios for the portable software controls.

Use `valid_bundle=True` and `collector=shadow` for throughput comparisons. `quality_ok` additionally requires at least five measured seconds, an empty-round barrier-overhead proxy ≤5%, and collector/shadow elapsed ratio ≤3. Inspect excluded and unavailable cases; failed bundles retain partial results but are not authoritative comparisons. Hardware counter rows retain their running fraction and whole-process scope: startup, two warmup rounds and teardown are included, and normalization uses measured plus warmup handoffs. Sample shares are not elapsed-time shares. The empty-round proxy estimates harness overhead; it is not an exact subtraction from queue time.

For the next analysis, copy the campaign's `campaign.json`, `identity.json`, `state.json`, `accounting.json`, `progress.json`, `controller.log`, `results/`, `artifacts/`, and generated CSVs. Preserve `source/` and its manifest with the campaign for source attribution and reproduction. The new campaign does not modify the imported v4 results.
