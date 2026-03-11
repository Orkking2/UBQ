//! Queue variant policies used to specialize UBQ's hot paths.

/// Controls when a producer should allocate or prepare the successor block.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PrepareMode {
    /// Only prepare when advancing at the current block boundary.
    BoundaryOnly,
    /// Prepare at the block boundary when the recycle pool has an empty slot.
    BoundaryIfPoolHasVacancy,
    /// Prepare at the block boundary only when the recycle pool is empty.
    BoundaryIfPoolEmpty,
    /// Prepare either at the block boundary or whenever the recycle pool has an
    /// empty slot.
    BoundaryOrPoolHasVacancy,
}

/// Compile-time queue policy used to specialize producer/consumer behavior.
pub trait Variant {
    /// Policy for successor-block preparation.
    const PREPARE_MODE: PrepareMode;
    /// Whether an unused producer spare block should be returned to the pool.
    const RECYCLE_PRODUCER_SPARE: bool;
    /// Whether a fully-consumed block should be returned to the pool.
    const RECYCLE_CONSUMED: bool;
}

/// The eager-preparation legacy variant (`v3`).
#[derive(Clone, Copy, Debug, Default)]
pub struct AggressivePrepare;

impl Variant for AggressivePrepare {
    const PREPARE_MODE: PrepareMode = PrepareMode::BoundaryOrPoolHasVacancy;
    const RECYCLE_PRODUCER_SPARE: bool = true;
    const RECYCLE_CONSUMED: bool = true;
}

/// The balanced legacy variant (`v4`).
#[derive(Clone, Copy, Debug, Default)]
pub struct Balanced;

impl Variant for Balanced {
    const PREPARE_MODE: PrepareMode = PrepareMode::BoundaryIfPoolHasVacancy;
    const RECYCLE_PRODUCER_SPARE: bool = true;
    const RECYCLE_CONSUMED: bool = true;
}

/// The pool-conservative legacy variant (`v5`).
#[derive(Clone, Copy, Debug, Default)]
pub struct PoolConservative;

impl Variant for PoolConservative {
    const PREPARE_MODE: PrepareMode = PrepareMode::BoundaryIfPoolEmpty;
    const RECYCLE_PRODUCER_SPARE: bool = true;
    const RECYCLE_CONSUMED: bool = true;
}

/// The no-pool legacy variant (`v6`).
#[derive(Clone, Copy, Debug, Default)]
pub struct NoPool;

impl Variant for NoPool {
    const PREPARE_MODE: PrepareMode = PrepareMode::BoundaryOnly;
    const RECYCLE_PRODUCER_SPARE: bool = false;
    const RECYCLE_CONSUMED: bool = false;
}

/// The consumer-only recycle legacy variant (`v7`).
#[derive(Clone, Copy, Debug, Default)]
pub struct ConsumerPoolOnly;

impl Variant for ConsumerPoolOnly {
    const PREPARE_MODE: PrepareMode = PrepareMode::BoundaryIfPoolEmpty;
    const RECYCLE_PRODUCER_SPARE: bool = false;
    const RECYCLE_CONSUMED: bool = true;
}
