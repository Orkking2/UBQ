//! Additional baseline queues for the comparative benchmark grid.
//!
//! Each submodule is a self-contained `QueueKind` baseline: the queue type
//! itself plus its `BenchQueueOps`/`BenchQueue`/`LogQueueOps`/`LogQueue`
//! trait impls. Wiring into `QueueKind`, `job_factory_for_spec`, and the
//! plan-expansion functions lives back in `bench_harness::mod` alongside the
//! existing baselines, to keep one single source of truth for the registry.

#[cfg(feature = "bench_moodycamel")]
pub mod moodycamel_cq;
pub mod ms_queue;
pub mod mutex_vecdeque;
pub mod naive_faa_queue;
