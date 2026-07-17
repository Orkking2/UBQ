# Changelog

All notable changes to UBQ are documented in this file.

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
