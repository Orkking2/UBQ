# Changelog

All notable changes to UBQ are documented in this file.

## Unreleased

### Added

- `bench_atomic_updates` accepts `--think-iters` (thread-local work after each
  update) and `--update-chunk`, and records `same_updater_fraction`, the
  per-updater share of updates and each sample's counter address. Results use
  schema v4; the default sweep is unchanged.
  `scripts/summarize_atomic_fairness.py` tabulates throughput, the ratio to
  FAA and fairness per think level. On multi-socket machines bind memory with
  `numactl`: which method wins depends on the counter's NUMA node. See
  `docs/atomic_update_fairness.md`.
- `bench_atomic_updates --counter-node unplaced|local|<node>,...` places each
  trial's counter in a page of its own bound to a NUMA node with `mbind`, and
  sweeps placements like think levels. Every sample records the counter's node
  before and after the measurement, whether it moved, the updaters' nodes and
  a local/remote/mixed label (schema v5; the default sweep stays unplaced).
  The summary script keeps counter nodes apart and flags mixed placement, and
  the plot script selects one node with `--counter-node`.

### Changed

- UBQ block capacity is derived from the number of `Slot<T>` values that fit
  in one system base page. `UBQ<T, B>` now has only the element and backoff type
  parameters.
- Benchmark and JNI registries expose only the page-sized Crossbeam and yield
  backoff variants. Historical numeric result labels remain readable and
  normalize to the corresponding `page` label.

### Removed

- The public `DEFAULT_BLOCK_SIZE` constant and the `BLOCK_SIZE` const generic.
- The misleading `huge-heap` UBQ plot family and live huge-block benchmark
  variant.

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
