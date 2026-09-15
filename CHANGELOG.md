# Changelog

All notable changes to UBQ are documented in this file.

## Unreleased

### Added

- `./ab` (`lab/`): staged A/B studies of the queue. Stage 1 measures
  throughput with paired, exact-once-checked runs in exclusive allocations;
  stage 2 captures each candidate's biggest wins and losses with perf and Arm
  Performix. Arm sources are content-addressed and their builds cached per
  cluster, every Slurm job advances an idempotent state machine instead of a
  login-node daemon, and a new study cancels the one it supersedes.
- `bench_hardware` accepts `ubq-v8-s<slots>-v<vblocks>` to set v8's VBlocks
  per allocation as well as its VBlock size. Without a geometry suffix an
  owned-buffer queue uses its implementation's default geometry.
- Lab studies take any number of arms, generated from a `grid` of queues and
  geometries if wanted, and group them into families; the report ranks every
  arm per cell and names each family's best variation, the deep stage
  selects per family, and `./ab plot` draws scaling curves, block-size
  sweeps, memory footprints and an effect heatmap. Every run records its
  end-of-run RSS. The default matrix grew to 23 shapes and four batch sizes,
  and the RSS guard to 32 GiB.
- `v8::UBQ<T, B>` and `dynblock::UBQ<T, B>` take the same `BackoffPolicy`
  parameter as `UBQ<T, B>` (`backoff::Crossbeam` by default, `backoff::Yield`
  as the alternative), so `bench_hardware`'s `backoff` now selects the
  policy for every registered queue instead of rejecting owned-buffer
  queues with anything but crossbeam. Lab studies treat a backoff as a
  variation within a family, like a geometry: `dynblock-vs-ubq.json` and
  `v8-vs-ubq.json` compare each queue at both policies, the spec rejects
  unknown policies, and the report and figures name the policy behind each
  family's best.
- Lab figures now use absolute units (items/s with K/M/B suffixes, bytes)
  and name every series; nothing is divided by the control arm. The report
  and CSVs gain a geometry matrix (every arm's throughput as a share of the
  batch's best arm, and its largest and typical end-of-run RSS), and
  `./ab plot` draws geometry, memory and layout matrices plus a
  throughput-by-geometry figure per family. `lab/studies/geometry-matrix.json`
  sweeps the full slots x VBlocks matrix for v8;
  `strict-baselines.json` and `relaxed-baselines.json` show how external
  queues join a study within one FIFO class.

- `ubq::bitmask::UBQ` (`src/bitmask/`, harness name `ubq-bitmask[-s64-v<VBlocks>]`):
  v8's reservations over VBlocks of exactly 64 slots, each published through
  a `produced` and a `consumed` 64-bit atomic mask with one `fetch_or` per
  segment on each side. Consumers read slots in order as their bits appear
  and never consult the producer index to tell an initialized slot from a
  hole. Default geometry 64 x 2048 slots, v8's block capacity.
- The attach-all freelist in `bitmask`: the producer index carries
  the number of blocks attached after the producer's block in its low byte,
  so a reservation that fits in that runway is one CAS with no freelist
  access; the producer crossing a block boundary attaches everything the
  freelist holds; and the freelist is either empty or a chain taken whole,
  with no busy token and no block dropped while the queue lives.
  `lab/studies/bitmask-vs-ubq.json` compares the original UBQ, v8 and
  bitmask at both backoff policies.
- The lab report opens each cluster with a *throughput by shape* section:
  per batch size, one row per shape and one column per family with the
  median throughput of the family's fastest arm, the fastest family in bold,
  plus a summary of each family's geometric mean, its ratio to the control's
  family, and the shapes it led (`csv/shape_matrix.csv`,
  `csv/family_summary.csv`); `./ab status` and `report` print the summary.
- `bench_hardware` takes separate `producer_batch` and `consumer_batch`
  sizes, and a lab `batch_sizes` entry may be a `[producer, consumer]` pair
  (`143p1c-b1x256`); the report and `./ab plot` add a batch matrix of every
  pair's throughput per arm and shape. The default matrix measures three
  uneven pairs (`throughput.skewed_batch_sizes`, push 16 / pop 256, 16 / 4096
  and 256 / 4096) at every shape whose sides differ, mirrored at
  consumer-heavy shapes so the lone side takes the larger batch, and none at
  balanced shapes: 140 cells instead of 92.

### Changed

- UBQ block capacity is derived from the number of `Slot<T>` values that fit
  in one system base page. `UBQ<T, B>` now has only the element and backoff type
  parameters.
- Benchmark and JNI registries expose only the page-sized Crossbeam and yield
  backoff variants. Historical numeric result labels remain readable and
  normalize to the corresponding `page` label.
- `./ab plot`'s scaling figure draws each family at its best variation for
  each batch size, chosen as the geometry matrix's ring, instead of one
  variation over all cells, and when a family has several arms names the
  variation drawn in a legend inside every panel.

### Removed

- The public `DEFAULT_BLOCK_SIZE` constant and the `BLOCK_SIZE` const generic.
- The misleading `huge-heap` UBQ plot family and live huge-block benchmark
  variant.
- The `bench_grid`, `bench_matrix` and `bench_profile` binaries, the weekend
  hardware campaign, the throughput-guided Performix workflow, and their Slurm
  scripts, scenario manifests, campaign designs, aggregation and plotting
  scripts. `./ab sync` replaces `scripts/push.sh` and `scripts/pull.sh`. The
  lab reads none of their result formats.
- The `profile_allocator_probe` feature.
- The `aos-to-soa` variant's study results; the variant itself was removed
  earlier.

## 5.0.0 - 2026-07-17

### Added

- `no_std + alloc` support through `default-features = false`.
- An explicit compile-time diagnostic for targets without native 8-bit and
  pointer-width atomics.
- A `bench_tools` feature that contains the benchmark harness, benchmark
  binaries, and their host-only dependencies.

### Changed

- The queue core now depends only on `core`, `alloc`, and `crossbeam-utils`.
- `crossbeam-utils` is built without its `std` feature in `no_std` builds.
- `backoff::Yield` falls back to processor spinning when `std` is disabled.
- JNI dynamic libraries are built explicitly with
  `cargo rustc --lib --crate-type cdylib --features jni`.
- The minimum supported Rust version is now 1.92.

### Removed

- Unused no-op Cargo features from the 4.x package.
- Obsolete README instructions for removed queue variants and Loom tests.

### Migration from 4.x

- Normal queue users do not need to change their code.
- Users of `ubq::bench_harness` must enable the `bench_tools` feature.
- Embedded users should disable default features and provide a global
  allocator.
