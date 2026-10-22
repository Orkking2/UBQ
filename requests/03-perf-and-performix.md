# 03: the profiling data

Guillem asked for `perf` or Performix data. The runs of `02-paper-matrix.md` already collect it, so this request is about what to send and how to ask for more; it submits no jobs of its own.

## What every run captures

When a run's measurement stage ends, a second stage profiles representative cells at the queue's fastest configuration there: the shapes `1p1c`, `Np1c` (every core but one producing into one consumer), `1pNc` and `HpHc` (half the cores on each side), at scalar operations and, where the queue batches, at batches of 256 and 4096 items, with 8-byte items. Each capture runs beside an unprofiled twin on the same node, so the collector's own slowdown is known.

- On MareNostrum 5: `perf stat` counter groups (cycles and instructions, cache references and misses, TLB, branches), `perf record` of cycles, `perf mem` loads (PEBS) and `perf c2c` for false and true sharing.
- On Grace: the same `perf stat` groups plus stall counters, `perf record`, `perf mem` loads through Arm SPE, and Arm Performix's recipes Code Hotspots, CPU Microarchitecture and Instruction Mix (the job loads the `arm_performix/2026.3.1` module).

Each capture lives in its task directory, `$ROOT/runs/<id>/<cluster>/tasks/<task>/`: the counter CSV, the `perf report` text summaries (symbols, memory levels, cache lines), the SPE event histogram, and on Grace the Performix export archive of the capture. The raw `perf.data` files stay out of the archives of `02` because they are large; the summaries and the Performix exports come home with them.

## What to send

The archives of `02` already contain the summaries and the Performix exports. If Guillem wants the raw captures of particular cells for his own dissection, add them for those cells only:

```sh
cd $ROOT/runs/<id>/<cluster>/tasks
ls -d s2-*                       # the profiling tasks, named s2-<cell>-<collector>-r<replicate>
du -sh s2-*/perf.data 2>/dev/null
tar czf ~/<id>-<cluster>-perf.tar.gz s2-<cell>-perf_record-r0 s2-<cell>-perf_mem-r0
```

A `perf.data` from a wide shape can be gigabytes; check the size before packaging.

## Asking for more cells

Any cell of a finished run can be profiled afterwards, at the queue's fastest configuration unless an arm is named:

```sh
python3 $ROOT/runs/<id>/tool/ab.py remote deep $ROOT/runs/<id>/<cluster> --cell 72p72c-b256-u64 --cell 143p1c-scalar-u64
python3 $ROOT/runs/<id>/tool/ab.py remote deep $ROOT/runs/<id>/<cluster> --cell 16p1c-scalar-u64 --collector perf_c2c
```

Cell names are `<shape>-<batch>-<item>` as listed in `$ROOT/runs/<id>/<cluster>/plan.json`; collectors are `counters`, `perf_record`, `perf_mem`, `perf_c2c` (MareNostrum 5) and `code_hotspots`, `cpu_microarchitecture`, `instruction_mix` (Grace). Each capture is one more exclusive allocation of about twenty minutes.

## What Nicolas does with it

The lab's report puts each collector's metrics per handoff side by side with the unprofiled twin: cycles and instructions per item, cache misses per item, the share of samples in the queue's own functions, the memory level each sampled load hit, and the cache lines that bounced between cores. Those tables are what the paper's mechanism explanations rest on.
