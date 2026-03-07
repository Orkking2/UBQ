use std::thread::yield_now;

pub struct Backoff {}

impl Backoff {
    #[inline]
    pub fn new() -> Self {
        Self {}
    }

    #[inline]
    pub fn spin(&self) {}

    #[inline]
    pub fn snooze(&self) {
        yield_now();
    }
}
