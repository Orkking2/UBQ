//! Naive floor baseline: a `VecDeque` behind a single `Mutex`.
//!
//! This is the textbook "what everyone reaches for by default" queue that
//! nearly every lock-free queue paper gestures at as the thing being
//! improved on. It is also, deliberately, the one baseline in this file that
//! gets a genuinely batch-native override: locking once and draining/
//! extending the whole batch under that one lock is cheap, legitimate, and
//! answers a real question ("does batching help even a coarse lock?") that's
//! otherwise invisible from the scalar-only baselines already in the grid.

use std::collections::VecDeque;
use std::sync::Mutex;

use crate::bench_harness::{BenchQueue, BenchQueueOps, LogQueue, LogQueueOps, LogRecord};

pub struct MutexQueue<T> {
    inner: Mutex<VecDeque<T>>,
}

impl<T> MutexQueue<T> {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(VecDeque::new()),
        }
    }
}

impl<T> Default for MutexQueue<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl BenchQueueOps for MutexQueue<u64> {
    fn visit_recv_batch(&self, size: usize, visit: &mut dyn FnMut(u64)) -> usize {
        let mut guard = self.inner.lock().expect("mutex poisoned");
        let available = guard.len().min(size);
        guard.drain(..available).for_each(visit);
        available
    }
    fn try_send_value(&self, value: u64) -> bool {
        self.inner.lock().expect("mutex poisoned").push_back(value);
        true
    }

    fn send_batch(&self, base: u64, offsets: std::ops::Range<usize>) {
        let mut guard = self.inner.lock().expect("mutex poisoned");
        guard.extend(offsets.map(|offset| base + offset as u64));
    }

    fn try_recv_value(&self) -> Option<u64> {
        self.inner.lock().expect("mutex poisoned").pop_front()
    }

    fn try_recv_batch(&self, request_size: usize) -> usize {
        let mut guard = self.inner.lock().expect("mutex poisoned");
        let available = guard.len().min(request_size);
        guard.drain(..available).count()
    }
}

impl BenchQueue for MutexQueue<u64> {
    fn new_queue() -> std::sync::Arc<Self> {
        std::sync::Arc::new(Self::new())
    }
}

impl LogQueueOps for MutexQueue<LogRecord> {
    fn send_log(&self, record: LogRecord) {
        self.inner.lock().expect("mutex poisoned").push_back(record);
    }

    fn try_recv_log(&self) -> Option<LogRecord> {
        self.inner.lock().expect("mutex poisoned").pop_front()
    }
}

impl LogQueue for MutexQueue<LogRecord> {
    fn new_log_queue() -> std::sync::Arc<Self> {
        std::sync::Arc::new(Self::new())
    }
}
