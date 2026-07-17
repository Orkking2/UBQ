//! Backoff policies used to specialize UBQ's retry loops.

/// A backoff policy used by UBQ retry loops.
pub trait BackoffPolicy: Sized {
    /// Creates a new backoff state for the current operation.
    fn new() -> Self;

    /// We must wait, because another thread made progress.
    fn spin(&self);

    /// We must wait until another thread makes progress.
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
///
/// Without the `std` feature, `snooze` uses [`core::hint::spin_loop`] because
/// there is no host scheduler to yield to.
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
        #[cfg(feature = "std")]
        std::thread::yield_now();

        #[cfg(not(feature = "std"))]
        core::hint::spin_loop();
    }
}
