//! Blocking queue wrapper backed by a condition variable.

use crate::{ConfiguredUBQ, backoff::BackoffPolicy};
use std::{
    fmt,
    marker::PhantomData,
    sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError},
};

/// Nonblocking queue operations required by [`SleepQ`].
///
/// Implement this trait for queue types that can be wrapped by [`SleepQ`].
/// The `pop` method must return immediately with [`None`] when the queue is
/// empty.
pub trait NonBlockingQueue<T>: Sized {
    /// Creates a new, empty queue.
    fn new() -> Self;

    /// Returns `true` if this queue contains no values.
    fn is_empty(&self) -> bool;

    /// Removes and returns the front element, or [`None`] if the queue is empty.
    fn pop(&self) -> Option<T>;

    /// Pushes `val` onto the back of the queue.
    fn push(&self, val: T);
}

impl<T, B, const POOL: usize, const BLOCK_SIZE: usize, A> NonBlockingQueue<T>
    for ConfiguredUBQ<T, B, POOL, BLOCK_SIZE, A>
where
    B: BackoffPolicy,
{
    #[inline]
    fn new() -> Self {
        ConfiguredUBQ::<T, B, POOL, BLOCK_SIZE, A>::new()
    }

    #[inline]
    fn is_empty(&self) -> bool {
        ConfiguredUBQ::is_empty(self)
    }

    #[inline]
    fn pop(&self) -> Option<T> {
        ConfiguredUBQ::pop(self)
    }

    #[inline]
    fn push(&self, val: T) {
        ConfiguredUBQ::push(self, val)
    }
}

impl<T> NonBlockingQueue<T> for crossbeam_queue::SegQueue<T> {
    #[inline]
    fn new() -> Self {
        crossbeam_queue::SegQueue::new()
    }

    #[inline]
    fn is_empty(&self) -> bool {
        crossbeam_queue::SegQueue::is_empty(self)
    }

    #[inline]
    fn pop(&self) -> Option<T> {
        crossbeam_queue::SegQueue::pop(self)
    }

    #[inline]
    fn push(&self, val: T) {
        crossbeam_queue::SegQueue::push(self, val)
    }
}

/// A blocking wrapper around a nonblocking queue.
///
/// `SleepQ` preserves nonblocking [`try_pop`](Self::try_pop) access while adding
/// a blocking [`pop`](Self::pop) that parks the caller on a condition variable
/// when the wrapped queue is empty. Each [`push`](Self::push) wakes one blocked
/// caller. Producers must push through `SleepQ` to wake blocked callers; pushing
/// through another handle to the wrapped queue cannot notify this condition
/// variable.
///
/// ```rust
/// use std::thread;
/// use ubq::SleepQ;
///
/// let q = SleepQ::<u64>::new_arc();
/// let consumer = {
///     let q = q.clone();
///     thread::spawn(move || q.pop())
/// };
///
/// q.push(42);
/// assert_eq!(consumer.join().unwrap(), 42);
/// ```
pub struct SleepQ<T, Q = ConfiguredUBQ<T>> {
    queue: Q,
    // Protects the empty-check/sleep protocol. The wrapped queue remains
    // responsible for its own producer/consumer synchronization.
    sleep: Mutex<()>,
    not_empty: Condvar,
    _item: PhantomData<fn() -> T>,
}

impl<T, Q> SleepQ<T, Q>
where
    Q: NonBlockingQueue<T>,
{
    /// Creates a new, empty sleeping queue.
    #[inline]
    pub fn new() -> Self {
        Self::from_queue(Q::new())
    }

    /// Creates a new, empty sleeping queue wrapped in an [`Arc`].
    #[inline]
    pub fn new_arc() -> Arc<Self> {
        Arc::new(Self::new())
    }

    /// Wraps an existing queue.
    #[inline]
    pub fn from_queue(queue: Q) -> Self {
        Self {
            queue,
            sleep: Mutex::new(()),
            not_empty: Condvar::new(),
            _item: PhantomData,
        }
    }

    /// Returns the wrapped queue, consuming this wrapper.
    #[inline]
    pub fn into_inner(self) -> Q {
        self.queue
    }

    /// Returns `true` if the wrapped queue contains no values.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    /// Removes and returns the front element, or [`None`] if the queue is empty.
    ///
    /// This method never parks the caller.
    #[inline]
    #[doc(alias = "try_recv")]
    pub fn try_pop(&self) -> Option<T> {
        self.queue.pop()
    }

    /// Removes and returns the front element, blocking while the queue is empty.
    ///
    /// Since `SleepQ` has no closed state, this method waits until a value is
    /// available.
    #[doc(alias = "recv")]
    pub fn pop(&self) -> T {
        loop {
            if let Some(val) = self.queue.pop() {
                return val;
            }

            let mut guard = lock_unpoisoned(&self.sleep);

            while self.queue.is_empty() {
                guard = wait_unpoisoned(&self.not_empty, guard);
            }
        }
    }

    /// Pushes `val` onto the back of the queue and wakes one blocked caller.
    #[inline]
    #[doc(alias = "send")]
    pub fn push(&self, val: T) {
        {
            let _guard = lock_unpoisoned(&self.sleep);
            self.queue.push(val);
        }

        self.not_empty.notify_one();
    }
}

impl<T, Q> Default for SleepQ<T, Q>
where
    Q: NonBlockingQueue<T>,
{
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl<T, Q> fmt::Debug for SleepQ<T, Q>
where
    Q: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SleepQ")
            .field("queue", &self.queue)
            .finish_non_exhaustive()
    }
}

fn lock_unpoisoned<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

fn wait_unpoisoned<'a, T>(condvar: &Condvar, guard: MutexGuard<'a, T>) -> MutexGuard<'a, T> {
    condvar.wait(guard).unwrap_or_else(PoisonError::into_inner)
}
