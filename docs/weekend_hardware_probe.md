# Weekend probe: hardware evidence for queue software decisions

Status: implemented as `scripts/hardware_campaign.py`. Follow the [runnable campaign guide](run_weekend_hardware_campaign.md) for launch commands, exact implemented coverage, gates and output interpretation. The design below records the broader experimental intent; the run guide specifies the executable first iteration and its explicit deferrals. No cluster work has been submitted. Use a new campaign label; keep `grace-bulk-consumed-v4` as the exploratory first iteration.

The intended result is a set of defensible statements of the form: “this algorithmic choice creates this hardware cost; changing this one software choice moves the predicted hardware metric and changes throughput or memory use.” A slowdown or high cache-miss percentage alone is not such a statement. A negative result or a measured tradeoff is also useful.

## 1. What iteration one contributes

The v4 results nominate mechanisms, rather than determine the next winners:

- UBQ's producer-heavy samples show substantial allocator/memory-management activity; consumer-heavy samples show substantial yielding.
- LUBQ's producer-owned shards and retained SPMC block caches suggest a synchronization-versus-memory-retention tradeoff.
- Fifty-one OOMs happened in the shadow, before APX. The next workload must bound burst work instead of extrapolating a fast pilot into a huge single handoff.
- Three UBQ exact-count failures require a separate correctness investigation. PMU counters cannot establish queue correctness.
- CPU Microarchitecture and Instruction Mix succeeded in older Grace captures. Memory Access has not been validated here. Arm's [SPE setup guidance](https://learn.arm.com/learning-paths/servers-and-cloud-computing/spe-on-performix/) explains its platform/kernel requirements.

The present source tree is already different from v4. Establish the new baseline from a frozen, correctness-approved source snapshot; do not call a v4/current comparison a one-factor experiment.

## 2. Questions, predictions, and interventions

| Question | Hardware prediction to test | Matched intervention | Evidence against the hypothesis |
| --- | --- | --- | --- |
| Does per-item completion accounting create unnecessary shared writes? | Fewer completion RMWs per item, less CPU work and possibly less coherence pressure with bulk accounting | Same UBQ source, external batch, memory ordering and reclamation protocol; change completion updates from per item to per block segment | Update count falls but cycles/item and throughput do not improve, or another path takes the saved work |
| Is retry policy wasting CPU, or is yielding itself expensive? | Backoff changes retry/branch work, CPU seconds/item and scheduling activity; cache effects are a secondary prediction | UBQ and LUBQ crossbeam versus yield, holding batch, placement and work fixed | A large sample-share change without lower CPU/item or improved throughput; excessive yielding can lose |
| Does block retention buy speed with retained memory? | Reuse reduces allocation/fault/syscall activity; a retention cap reduces empty-after-drain footprint but can increase refill cost | Current LUBQ retention versus a bounded, correctness-approved retained-block policy | No reduction in retained footprint, or negligible reuse benefit despite a large footprint |
| How do different queue structures move work between cores and memory? | Shared MPMC structures, producer shards, linked nodes, preallocated rings and a mutex should have different CPU, cache/TLB, allocation and waiting costs | Fixed workload and explicit scalar/native-batch configuration; compare within each machine | Cost attribution follows the adapter, allocator or offered backlog rather than the queue mechanism |

Completion aggregation and capped retention need diagnostic/build variants; they do not currently have supported experiment switches. If either cannot pass correctness before launch, omit that intervention explicitly. Backoff and native-batch comparisons already expose useful software changes. Do not silently substitute an unrelated old binary for a missing control.

Retries need direct, thread-local diagnostic counting when an exact retry claim matters. Exclusive-store failures do not measure every CAS retry, especially when Arm LSE atomics are used. Instruction Mix proportions alone are not counts of failed atomic operations.

## 3. Machines and the core matrix

Use exclusive full nodes on both machines. Do not impose an application concurrency throttle: Slurm should schedule all eligible queued jobs. Bound outstanding submissions according to actual QoS headroom, which is a different constraint. Record node model, cpuset, NUMA map, page size, SMT, frequency policy, compiler flags, source tree hash and binary hashes.

- Grace: 144 benchmark worker cores, 72 per socket in the recorded topology.
- MN5: standard DDR5 GPP cohort, 112 physical cores. Keep HBM/high-memory nodes as separate cohorts if deliberately used. [BSC describes these different node classes](https://www.bsc.es/supportkc/docs/MareNostrum5/overview/).

| Shape | Both machines | Grace full-node endpoint | MN5 full-node endpoint |
| --- | --- | --- | --- |
| Reference | 1p1c | — | — |
| Producer contention | 8p1c, 16p1c, 32p1c, 64p1c | 143p1c | 111p1c |
| Consumer contention | 1p16c, 1p32c, 1p64c | 1p143c | 1p111c |
| Balanced | 32p32c | 72p72c | 56p56c |

This is 12 scenarios per machine, including the true MN5 endpoints omitted by the old power-of-two-oriented manifest. Use topology-derived physical core lists, not an assumption that adjacent CPU IDs imply the intended NUMA placement. Full-node worker endpoints leave no dedicated collector core; report their collection overhead and keep unprofiled throughput separate.

Core configurations:

- Nine scalar configurations: UBQ, LUBQ, Crossbeam SegQueue, ConcurrentQueue unbounded, Mutex+VecDeque, Michael–Scott, Moodycamel, RBBQ and LSCQ.
- Five batch-256 configurations: UBQ, LUBQ, Crossbeam BatchQueue, Mutex+VecDeque and Moodycamel.
- UBQ/LUBQ baseline backoff: crossbeam. UBQ/LUBQ block size: host page, recorded in bytes. RBBQ block size: 256, requested capacity: 1,048,576. LSCQ segment size: 256.

There are 14 configurations × 12 scenarios × 2 machines = **336 core cells**, each with three independent repetitions. Crossbeam BatchQueue is a distinct implementation, not just a batch switch on SegQueue; this yields ten underlying implementations across the 14 configurations. Report ordering guarantees, boundedness, backpressure, adapter copying and per-producer tokens alongside comparisons. Producer-sharded and globally ordered queues do not necessarily promise identical ordering.

The exact planning matrix is [weekend_hardware_probe.matrix.csv](../profiling/weekend_hardware_probe.matrix.csv). The machine-readable design is [weekend_hardware_probe.design.json](../profiling/weekend_hardware_probe.design.json); its schema is intentionally not the old harness's campaign schema.

## 4. Workload protocol: bounded work, sustained collection

Keep u64 payloads. Count one completed producer-to-consumer handoff as one item; do not count push and pop as two completed items.

Implement an in-process sequence of bounded, empty-to-empty rounds with persistent worker threads and producer/consumer handles. Start with a cap of 8,388,608 total items per round (64 MiB of payload if all were resident). Align work to the same producer count and batch multiple for each matched comparison. A round cap bounds offered burst work; it does not make instantaneous queue occupancy equal across algorithms.

Calibrate the number of rounds to approximately 30 seconds, rather than increasing the number of items in one round. Replay the exact items-per-round, round count, prewarm and placement for the shadow, each collector, and both A/B arms. Choose an A/B schedule using the faster arm's pilot so both captures are sufficiently long; record every actual duration and classify excessively perturbed captures. Do not compare collector-internal throughput between recipes.

Before freezing the protocol, check total round caps 2^18, 2^20 and 2^23 on a small representative panel. Require that barriers/control logic contribute less than about 5% of sampled CPU work and that conclusions are not a round-size artifact. If the largest cap still makes control overhead dominant, revise the persistent-worker protocol; do not grow it without a memory bound. Calibrate/freeze each pair before measured repetitions and record total work for normalization across algorithms.

Use a workload RSS watchdog with an initial 8 GiB soft threshold, plus an overall allocation budget that includes the profiler. A soft watcher is not an instantaneous allocation cap. Record a budget stop explicitly and retain its partial diagnostics. Never turn it into a zero-throughput sample. Track high-water/retained memory across rounds to detect growth even when every individual round is bounded.

Correctness runs use tagged producer/sequence IDs and exact-once verification on bounded workloads, separate from performance collection. Total count alone misses a duplicate balanced by a loss. Include boundary sizes, abandoned/partially consumed iterators, repeated block reuse, many consumers, and the v4 batch-1024 failures. Correctness instrumentation must not appear in the primary hardware/throughput executable.

## 5. Three complementary measurement panels

### Core counters on both machines

For every core cell, collect an unprofiled shadow and small, separate PMU groups against the same frozen work schedule. Prefer three groups: instructions/cycles/branches; data-cache refills or misses; data-TLB accesses/walks. Resolve supported names and meanings from the installed PMU and event documentation during the capability pilot. Require adequate time-running coverage (target at least 95%); split groups if necessary. Missing/unsupported counters remain missing.

Report:

- Handoffs/s, summed worker CPU seconds/item and cycles/item.
- Instructions/item; IPC/CPI as supporting descriptions, not the target of optimization.
- Cache events and TLB walks per item, with their exact event semantics.
- User/system CPU, page faults, context switches and migrations per item.
- Memory-controller bytes/item and local/remote data-source attribution only where the permissions, PMU scope and tools support them.

Do not equate Arm and Intel cache event names or top-down percentage definitions. Compare A/B ratios within each machine and ask whether the direction and mechanism replicate. Node bandwidth includes background activity; collect an idle reference and report its scope. High backend-bound percentage alone does not identify DRAM, cache-line contention or atomic latency.

### Memory footprint and reuse

Run a separate, low-perturbation lifecycle probe so “more memory” is not confounded with “more items happened to be queued.” For each of the ten implementations and producer-handle counts 1, 16 and maximum-producers (143/111), use exact depths 4,096; 65,536; and 524,288 items. These are below the requested bounded-ring capacity; validate actual capacity before using a depth. Use a fresh process for every depth/repetition.

Record checkpoints: constructed empty → fully enqueued and touched → fully drained → idle for 1 second and 10 seconds → refilled → destroyed. Keep the producer handles alive until the intended destruction checkpoint. Quiescent checkpoints make occupancy exact without adding a shared per-item occupancy counter to the timed hot path. Include a repeat of the same fill/drain cycle to expose reuse.

Measure and distinguish:

1. Empty structure/handle footprint and its growth with producer count.
2. Additional private resident bytes at equal occupancy, plus bytes per resident payload item.
3. Queue-owned live/cached/reserved storage, where explicit instrumentation exists.
4. Retained bytes after drain and residual process memory after queue destruction.
5. Allocation calls/bytes, page faults and mapping activity; refill CPU cost and throughput in a separate clean timing pass.

Use `/proc` RSS/private/PSS checkpoints and a no-queue baseline with the same scaffolding, alongside queue/allocator diagnostics. `ru_maxrss` cannot show memory falling after drain, and the old Slurm MaxRSS values did not represent the killed workload's peak reliably. RSS remaining after destruction may be allocator retention, not a queue leak.

The existing Rust GlobalAlloc probe covers Rust allocation paths, including UBQ block allocations in the current source, but does not cover C++ allocations made inside Moodycamel. It also perturbs timing through shared counters. Label allocation coverage; process-wide footprint supplies the cross-implementation comparison, and any additional allocator interception is a separate diagnostic pass.

Thirty bundles per machine (ten implementations × three producer counts), each containing three depths and three fresh-process repetitions: **60 bundles / 540 lifecycle process runs** across both machines.

### Deep hardware attribution and software A/B

Freeze a baseline attribution panel of all 14 configurations at 1p1c, 16p1c, 1p32c and full-balanced: **56 coordinates per machine**. This panel spans structures and regimes without selecting only the fastest or only the survivors.

Add these eight matched pairs per machine, all at external batch 256:

- Backoff: UBQ and LUBQ, each at 1p32c and full-balanced: four pairs.
- Completion accounting: UBQ at 1p32c and full-balanced: two pairs, conditional on validated switches.
- Retention: LUBQ at 8p1c and full-balanced: two pairs, conditional on validated safe retention controls.

Each paired replicate runs both arms on the same exclusive node in alternating/randomized order. Keep allocator, work, placement, compiler options and all other code fixed. Replicates should span separate allocations and, where available, distinct nodes; retain node identity. Freeze the retention cap from a small unprofiled pilot, documenting the chosen cap in both pages and bytes, and do not retune it on the reported repetitions.

Grace: Code Hotspots, CPU Microarchitecture, Instruction Mix, and Memory Access **only if an actual SPE capture succeeds**. A ready check alone is insufficient. Three repetitions of up to 72 coordinates × four recipes gives **at most 864 captures**. Arm describes the roles of these recipes and recommends adequate sampling duration in its [practical profiling guidance](https://developer.arm.com/community/arm-community-blogs/b/servers-and-cloud-computing-blog/posts/arm-performix-practical-performance-analysis-for-arm-based-servers). Recipe passes must run separately.

MN5: core counters/top-down groups from the portable panel, plus `perf record` source/stack attribution; add validated `perf mem` or `perf c2c` for the memory/coherence questions. Do not depend on an unproven MN5 APX installation. Up to 72 coordinates × two sampling collectors × three repetitions gives **at most 432 captures**; if memory sampling is unavailable, retain counter and footprint evidence and report the attribution gap.

Successful memory-sampling capability must mean meaningful non-empty samples, acceptable lost-sample counts and resolvable workload attribution. Preserve binary images, debug symbols and raw event definitions. Sample fractions are not wall-clock fractions; sampled load latency and hardware traffic estimates must retain their tool-specific definitions. A syscall hotspot can be allocator/kernel behavior, not a direct queue syscall.

Optional topology validation after the core panels: UBQ and LUBQ at 16p16c, all workers/memory on one NUMA node versus producers and producer-touched memory on node 0 and consumers on node 1, then reverse roles. Change placement only. Explicit NUMA maps and first-touch/memory policy are required; `core_offset` alone is insufficient. This can test whether a software improvement is particularly useful when sharing crosses sockets.

## 6. Weekend execution and bounded recovery

Priority is scientific value, independent of how many nodes Slurm grants:

1. Before unattended execution: build/provenance, correctness gate, low-contention and full-node smoke cases, memory observer validation, and one complete capture per enabled recipe on each machine. A failure disables only the affected queue/variant/capability, with an explicit coverage record. Correctness failures quarantine the affected implementation from performance claims.
2. First wave: core counters and controlled memory lifecycle on both machines. These produce useful results even if a deep collector fails.
3. Second wave: the fixed algorithm-attribution panel and validated matched A/B pairs, prioritized across mechanisms and queues rather than exhausting one queue first.
4. Final available window: extra paired repetitions (aim five total for the primary contrasts), the optional topology panel, and a single retry of clearly transient tool failures. Deterministic integrity/OOM failures are not blindly retried.

Submit exclusive jobs without a `%N` array throttle. The dispatcher must inspect/retry boundedly on submission headroom, persist accepted job IDs immediately, and enqueue additional chunks as tasks leave the system. [Slurm's submit limits count pending and running jobs](https://slurm.schedmd.com/resource_limits.html); dependency chains alone do not solve that limit. Size chunks to actual current allowance rather than assuming 300 is the site's ceiling.

Use successful build/correctness dependencies for eligibility. Independent experiment bundles may continue after unrelated cases fail, but they must read the relevant gate result. Never attach replacement work to an already failed array using `afterok`. [Slurm documents that a failed dependency will not release the dependent job](https://slurm.schedmd.com/sbatch.html).

Write logs incrementally; preserve errors and partial artifacts. Mark complete only after the collected workload result, exact work identity and archive validation succeed. Reconcile terminal Slurm states with saved status. Retries get new attempt directories; successful evidence is immutable.

Budget the complete allocation: shadow/pilot, preparation/readiness, collector, export and cleanup. Inner timeouts must end before Slurm's limit. Provisional per-capture outer ceilings are 45 minutes on Grace and 20 minutes on MN5, subject to site QoS and pilot timing; these are planning ceilings, not current helper defaults. If worst-case phase allowances cannot fit, shorten/restructure that bundle before submission.

Use a 48-hour campaign deadline, including queueing, and estimate end-to-end duration from pilot measurements before unattended launch. Stop admitting new low-priority jobs early enough for their allowed duration; checkpoint existing work. Slurm availability cannot be predicted from requested concurrency.

Illustrative cost, not a promise: 336 core bundles averaging six minutes ≈34 node-hours; 60 memory bundles averaging five minutes ≈5; 864 Grace captures averaging six minutes ≈86; 432 MN5 captures averaging three minutes ≈22; paired portable passes and setup add roughly 5–15. Plan around **150–170 node-hours before retries**, with **250–350 node-hours of working headroom** if desired. Recalculate from the pilot. At the stated per-capture ceilings the full deep matrix alone can consume about 792 node-hours, so priority/deadline logic is essential. Exclusive small-scenario jobs are still billed as whole nodes; [BSC explains whole-node core-hour accounting](https://www.bsc.es/supportkc/docs/MareNostrum5/slurm/).

## 7. Implementation and launch gates

The new runner implements the following requirements, with per-cluster correctness and capability gates remaining to be executed remotely:

- Persistent-worker bounded-round workload, exact-schedule export/replay, and a memory watchdog.
- A quiescent, controlled-occupancy lifecycle mode with process-wide memory checkpoints.
- Strong completion validation, incremental logs, whole-job timeout accounting and Slurm state reconciliation.
- Submission dispatch that handles actual QoS headroom and partial failure.
- Exact-once correctness reproduction for UBQ; diagnostic-only accounting/retention switches if those interventions are included.
- Per-machine capability manifests, explicit CPU/memory placement and the new MN5 full endpoints.

Use `hardware_campaign.py start` as documented in the run guide. The older `auto` workflow and `submit_profile_plan.sh` remain separate; do not pass the custom weekend design schema to them. Retention controls, exact allocator/retry diagnostics and the explicit NUMA intervention are deferred in this executable iteration.

## 8. What we should be able to say afterward

Produce an algorithm hardware-cost matrix, memory footprint-versus-occupancy curves, empty-after-drain/refill tradeoffs, and paired A/B effect plots. Keep error/coverage panels beside them, including correctness exclusions and memory-budget stops.

A software improvement is supported when the predicted mechanism metric changes, unprofiled throughput/CPU or memory use changes in the expected direction, correctness remains intact, and the paired direction is repeatable. Record counterexamples and architecture-specific reversals. Use paired ratios within each node/block and summarize across independent repetitions; three repetitions provide exploratory spread, not precise confidence. Extend primary pairs to five if time permits.

Example target conclusions (hypotheses, not current findings):

- “Aggregating completion updates lowered shared RMWs/item and CPU cost, with a larger benefit at the full consumer count.”
- “A bounded reuse policy reduced retained bytes after draining, at a measured allocation and refill-throughput cost.”
- “Yield-only backoff reduced retry work on one architecture but increased scheduler overhead on the other.”

The value of this weekend is a causal software/hardware explanation and a reproducible next decision, even if no single queue or tuning choice wins every workload.
