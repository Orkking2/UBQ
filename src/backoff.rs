//! Backoff policies used to specialize UBQ's retry loops.

use std::thread::yield_now;

/// A backoff policy used by UBQ retry loops.
pub trait BackoffPolicy: Sized {
    /// Creates a new backoff state for the current operation.
    fn new() -> Self;

    /// Invoked after a failed, tight retry.
    fn spin(&self);

    /// Invoked after a slower retry path.
    fn snooze(&self);
}

/// Backoff policy backed by `crossbeam_utils::Backoff`.
#[derive(Debug)]
pub struct Crossbeam(crossbeam_utils::Backoff);

impl Default for Crossbeam {
    fn default() -> Self {
        Self::new()
    }
}

impl BackoffPolicy for Crossbeam {
    #[inline]
    fn new() -> Self {
        Self(crossbeam_utils::Backoff::new())
    }

    #[inline]
    fn spin(&self) {
        self.0.spin();
    }

    #[inline]
    fn snooze(&self) {
        self.0.snooze();
    }
}

/// Minimal backoff policy that yields on `snooze` and does nothing on `spin`.
#[derive(Clone, Copy, Debug, Default)]
pub struct Yield;

impl BackoffPolicy for Yield {
    #[inline]
    fn new() -> Self {
        Self
    }

    #[inline]
    fn spin(&self) {}

    #[inline]
    fn snooze(&self) {
        yield_now();
    }
}
