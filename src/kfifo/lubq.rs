use super::queue::SPMC;
use crate::backoff::{BackoffPolicy, Crossbeam};
use alloc::{
    boxed::Box,
    sync::Arc,
    vec::{IntoIter, Vec},
};
use core::{
    cell::UnsafeCell,
    fmt,
    hint::spin_loop,
    ops::{Deref, DerefMut},
    ptr::{NonNull, null_mut},
    sync::atomic::{
        AtomicBool, AtomicPtr, AtomicUsize,
        Ordering::{AcqRel, Acquire, Relaxed, Release},
    },
};
use crossbeam_utils::CachePadded;

/// Creates a linked-shard channel using the default backoff policy.
///
/// ```rust
/// use ubq::kfifo::{TryRecvError, channel};
///
/// let (mut sender, mut receiver) = channel();
/// sender.send(7);
/// drop(sender);
///
/// assert_eq!(receiver.try_recv(), Ok(7));
/// assert_eq!(receiver.try_recv(), Err(TryRecvError::Disconnected));
/// ```
pub fn channel<T>() -> (Sender<T>, Receiver<T>) {
    channel_with()
}

/// Creates a linked-shard channel with an explicit backoff policy.
///
/// # Panics
///
/// Panics if one `T` does not fit in or requires greater alignment than a
/// system base page.
pub fn channel_with<T, B: BackoffPolicy>() -> (Sender<T, B>, Receiver<T, B>) {
    let core = Arc::new(Core::new());
    let shard = core.registry.head().expect("LUBQ starts with one shard");

    (
        Sender {
            core: Arc::clone(&core),
            shard,
        },
        Receiver { core, cursor: None },
    )
}

/// The result of attempting to receive from a linked-shard channel.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TryRecvError {
    /// At least one sender remains, but no value was available during the scan.
    Empty,
    /// Every sender has been dropped and all retained values have been drained.
    Disconnected,
}

impl fmt::Display for TryRecvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Empty => "the channel is temporarily empty",
            Self::Disconnected => "the channel is disconnected and drained",
        })
    }
}

#[cfg(feature = "std")]
impl std::error::Error for TryRecvError {}

/// A single-producer handle for a linked-shard channel.
///
/// Every sender owns one private queue shard. Cloning a sender reuses an
/// inactive empty shard when possible and otherwise appends a permanent shard.
/// Sending requires mutable access, making the intended one-producer-per-shard
/// discipline explicit.
pub struct Sender<T, B: BackoffPolicy = Crossbeam> {
    core: Arc<Core<T, B>>,
    shard: ShardPtr<T, B>,
}

/// A round-robin receiver for a linked-shard channel.
///
/// Receivers keep the core alive after the final sender exits so values in
/// inactive producer shards can still be drained. Cloning a receiver creates an
/// independent scan cursor and does not allocate a producer shard.
pub struct Receiver<T, B: BackoffPolicy = Crossbeam> {
    core: Arc<Core<T, B>>,
    cursor: Option<ShardPtr<T, B>>,
}

type PaddedShard<T, B> = CachePadded<Shard<T, B>>;

/// Non-owning pointer into the stable shard arena owned by `Core`.
struct ShardPtr<T, B>(NonNull<PaddedShard<T, B>>);

impl<T, B> Copy for ShardPtr<T, B> {}

impl<T, B> Clone for ShardPtr<T, B> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T, B> ShardPtr<T, B> {
    fn new(shard: &mut PaddedShard<T, B>) -> Self {
        Self(NonNull::from(shard))
    }

    fn as_ptr(self) -> *mut PaddedShard<T, B> {
        self.0.as_ptr()
    }
}

// SAFETY: `ShardPtr` is used only while a strong `Arc<Core>` keeps its target
// allocated. The pointed-to shard is safe to access from another thread when T
// is Send because its mutable state is atomic and UBQ is Send + Sync for T.
unsafe impl<T: Send, B> Send for ShardPtr<T, B> {}
// SAFETY: shared access has the same stable-allocation and UBQ synchronization
// requirements as the Send implementation above.
unsafe impl<T: Send, B> Sync for ShardPtr<T, B> {}

struct Shard<T, B> {
    id: usize,
    producer_active: AtomicBool,
    next: AtomicPtr<PaddedShard<T, B>>,
    queue: SPMC<T, B>,
}

impl<T, B: BackoffPolicy> Shard<T, B> {
    fn new(id: usize) -> Self {
        Self {
            id,
            producer_active: AtomicBool::new(true),
            next: AtomicPtr::new(null_mut()),
            queue: SPMC::new(),
        }
    }

    fn queue(&self) -> &SPMC<T, B> {
        &self.queue
    }

    fn try_acquire_producer(&self) -> bool {
        if self.producer_active.load(Acquire) || !self.queue.is_empty() {
            return false;
        }

        // The registry lock serializes shard acquisition, while this CAS
        // synchronizes with the previous sender's Release retirement.
        self.producer_active
            .compare_exchange(false, true, AcqRel, Acquire)
            .is_ok()
    }
}

struct Core<T, B> {
    registry: Registry<T, B>,
    senders: AtomicUsize,
}

impl<T, B: BackoffPolicy> Core<T, B> {
    fn new() -> Self {
        Self {
            registry: Registry::new(),
            senders: AtomicUsize::new(1),
        }
    }

    fn acquire_sender_shard(&self) -> ShardPtr<T, B> {
        self.registry.acquire_sender_shard()
    }

    fn shard(&self, shard: ShardPtr<T, B>) -> &PaddedShard<T, B> {
        self.registry.shard(shard)
    }

    fn pop_from(&self, shard: ShardPtr<T, B>) -> Option<T> {
        self.shard(shard).queue().pop()
    }

    fn pop_batch_from(&self, shard: ShardPtr<T, B>, size: usize, values: &mut Vec<T>) -> usize {
        debug_assert!(size != 0);

        let previous_len = values.len();
        {
            let reservation = self.shard(shard).queue().pop_batch(size);
            reservation.append_to_vec(values);
        }

        values.len() - previous_len
    }

    fn add_sender(&self) {
        self.senders
            .try_update(Relaxed, Relaxed, |count| count.checked_add(1))
            .expect("linked UBQ sender count overflowed");
    }
}

/// The structural owner of all published nodes.
///
/// Published node addresses remain stable until `Core` is destroyed. Receiver
/// traversal therefore uses non-owning pointers without per-hop reference-count
/// traffic. Registration is serialized, but no steady-state operation removes
/// a node or mutates a published forward link.
struct Registry<T, B> {
    head: AtomicPtr<PaddedShard<T, B>>,
    tail: AtomicPtr<PaddedShard<T, B>>,
    state: SpinLock<RegistryState<T, B>>,
}

// Each box gives published shards a stable address even if the ownership
// vector reallocates. Storing shards inline would invalidate raw cursors.
#[allow(clippy::vec_box)]
struct RegistryState<T, B> {
    owned: Vec<Box<PaddedShard<T, B>>>,
    next_id: usize,
}

impl<T, B: BackoffPolicy> Registry<T, B> {
    fn new() -> Self {
        let mut first = Box::new(CachePadded::new(Shard::new(0)));
        let pointer = ShardPtr::new(first.as_mut()).as_ptr();
        Self {
            head: AtomicPtr::new(pointer),
            tail: AtomicPtr::new(pointer),
            state: SpinLock::new(RegistryState {
                owned: alloc::vec![first],
                next_id: 1,
            }),
        }
    }

    fn acquire_sender_shard(&self) -> ShardPtr<T, B> {
        let mut state = self.state.lock();

        for shard in &mut state.owned {
            if shard.try_acquire_producer() {
                return ShardPtr::new(shard.as_mut());
            }
        }

        let id = state.next_id;
        state.next_id = state
            .next_id
            .checked_add(1)
            .expect("linked UBQ exhausted its producer shard identifiers");
        let mut shard = Box::new(CachePadded::new(Shard::new(id)));
        let shard_ptr = ShardPtr::new(shard.as_mut());

        // Store ownership before publishing the raw address.
        state.owned.push(shard);

        let tail = self.tail.load(Relaxed);
        debug_assert!(!tail.is_null());
        // SAFETY: the registry lock serializes append, and `owned` retains every
        // published allocation until Registry destruction.
        unsafe { &*tail }.next.store(shard_ptr.as_ptr(), Release);
        self.tail.store(shard_ptr.as_ptr(), Release);
        shard_ptr
    }

    fn head(&self) -> Option<ShardPtr<T, B>> {
        NonNull::new(self.head.load(Acquire)).map(ShardPtr)
    }

    fn tail(&self) -> Option<ShardPtr<T, B>> {
        NonNull::new(self.tail.load(Acquire)).map(ShardPtr)
    }

    fn shard(&self, shard: ShardPtr<T, B>) -> &PaddedShard<T, B> {
        // SAFETY: the returned borrow is tied to this Registry, whose boxed
        // arena owns every published shard until the Registry is dropped.
        unsafe { shard.0.as_ref() }
    }

    fn next(&self, shard: ShardPtr<T, B>) -> Option<ShardPtr<T, B>> {
        let pointer = self.shard(shard).next.load(Acquire);
        NonNull::new(pointer).map(ShardPtr)
    }

    #[cfg(test)]
    fn routed_len(&self) -> usize {
        let state = self.state.lock();
        debug_assert_eq!(state.owned.len(), state.next_id);
        let mut len = 0;
        let mut current = self.head.load(Relaxed);
        while !current.is_null() {
            len += 1;
            // SAFETY: all nodes remain owned for the Registry lifetime.
            current = unsafe { &*current }.next.load(Relaxed);
        }
        len
    }
}

impl<T, B: BackoffPolicy> Sender<T, B> {
    fn shard(&self) -> &PaddedShard<T, B> {
        self.core.shard(self.shard)
    }

    /// Sends a value through this sender's private shard.
    pub fn send(&mut self, value: T) {
        self.shard().queue().push(value);
    }

    /// Sends an exact batch through this sender's private shard.
    pub fn send_batch<I>(&mut self, values: I)
    where
        I: IntoIterator<Item = T>,
        I::IntoIter: ExactSizeIterator,
    {
        self.shard().queue().push_batch(values);
    }

    /// Alias for [`send`](Self::send).
    pub fn push(&mut self, value: T) {
        self.send(value);
    }

    /// Alias for [`send_batch`](Self::send_batch).
    pub fn push_batch<I>(&mut self, values: I)
    where
        I: IntoIterator<Item = T>,
        I::IntoIter: ExactSizeIterator,
    {
        self.send_batch(values);
    }
}

impl<T, B: BackoffPolicy> Clone for Sender<T, B> {
    /// Acquires an inactive empty shard or appends a new producer shard.
    fn clone(&self) -> Self {
        let core = Arc::clone(&self.core);
        let shard = core.acquire_sender_shard();
        core.add_sender();
        Self { core, shard }
    }
}

impl<T, B: BackoffPolicy> Drop for Sender<T, B> {
    fn drop(&mut self) {
        // Mutable sending means the final send through this handle is complete.
        // The release makes the dormant producer lease reusable without
        // reclaiming the shard or resetting its warmed UBQ.
        self.shard().producer_active.store(false, Release);

        // AcqRel chains final sender publication before a receiver observes
        // sender count zero and performs its closed-channel confirmation scan.
        let previous = self.core.senders.fetch_sub(1, AcqRel);
        debug_assert!(previous != 0);
    }
}

impl<T, B: BackoffPolicy> Receiver<T, B> {
    /// Returns `true` after every sender has been dropped.
    ///
    /// A closed channel can still contain values in inactive shards. Use
    /// [`try_recv`](Self::try_recv) until it returns
    /// [`TryRecvError::Disconnected`] to prove it is drained.
    pub fn is_closed(&self) -> bool {
        self.core.senders.load(Acquire) == 0
    }

    /// Attempts to receive one value in work-conserving round-robin order.
    pub fn try_recv(&mut self) -> Result<T, TryRecvError> {
        if let Some(value) = self.scan_once() {
            return Ok(value);
        }

        if self.core.senders.load(Acquire) != 0 {
            return Err(TryRecvError::Empty);
        }

        // The Acquire zero observation synchronizes with the final sender's
        // Release decrement. Scan again so a send which occurred after its
        // shard was visited in the first scan cannot be mistaken for drain.
        self.scan_once().ok_or(TryRecvError::Disconnected)
    }

    /// Appends up to `size` values to `values` using native shard batch claims.
    ///
    /// Existing elements in `values` are preserved and do not count toward
    /// `size`. A batch favors the receiver's current producer shard, then
    /// visits later shards only when more values are needed. This producer-local
    /// burst policy amortizes shard traversal and reservation bookkeeping.
    ///
    /// Reusing the same vector across calls avoids allocation after its capacity
    /// reaches the caller's usual batch size.
    pub fn try_recv_batch_into(
        &mut self,
        values: &mut Vec<T>,
        size: usize,
    ) -> Result<usize, TryRecvError> {
        if size == 0 {
            return Ok(0);
        }

        let received = self.scan_batch_once(values, size);
        if received != 0 {
            return Ok(received);
        }

        if self.core.senders.load(Acquire) != 0 {
            return Err(TryRecvError::Empty);
        }

        // Match scalar closed detection: observing the final sender's release
        // decrement orders its last publication, and a second scan prevents
        // that final batch from being mistaken for a drained channel.
        let received = self.scan_batch_once(values, size);
        if received == 0 {
            Err(TryRecvError::Disconnected)
        } else {
            Ok(received)
        }
    }

    /// Receives up to `size` values into an eager owning iterator.
    ///
    /// This convenience method allocates an output vector. Use
    /// [`try_recv_batch_into`](Self::try_recv_batch_into) to reuse caller-owned
    /// storage across operations.
    pub fn try_recv_batch(&mut self, size: usize) -> Result<IntoIter<T>, TryRecvError> {
        let mut values = Vec::new();
        self.try_recv_batch_into(&mut values, size)?;
        Ok(values.into_iter())
    }

    /// Removes one value, returning `None` when empty or disconnected.
    pub fn pop(&mut self) -> Option<T> {
        self.try_recv().ok()
    }

    /// Eagerly removes up to `size` values.
    pub fn pop_batch(&mut self, size: usize) -> IntoIter<T> {
        self.try_recv_batch(size)
            .unwrap_or_else(|_| Vec::new().into_iter())
    }

    /// Appends up to `size` values to `values`, returning zero when the channel
    /// is empty or disconnected.
    pub fn pop_batch_into(&mut self, values: &mut Vec<T>, size: usize) -> usize {
        self.try_recv_batch_into(values, size).unwrap_or(0)
    }

    fn scan_once(&mut self) -> Option<T> {
        let boundary = self.core.registry.tail()?;
        let boundary_id = self.core.shard(boundary).id;
        let start = self
            .cursor
            .take()
            .filter(|shard| self.core.shard(*shard).id <= boundary_id)
            .or_else(|| self.core.registry.head())?;
        let start_id = self.core.shard(start).id;

        // Scan from the cursor through the tail captured at operation start.
        let mut current = Some(start);
        while let Some(shard) = current {
            if self.core.shard(shard).id > boundary_id {
                break;
            }

            let next = self.core.registry.next(shard);
            if let Some(value) = self.core.pop_from(shard) {
                self.cursor = next.or_else(|| self.core.registry.head());
                return Some(value);
            }

            if self.core.shard(shard).id == boundary_id {
                break;
            }

            current = next.filter(|candidate| self.core.shard(*candidate).id <= boundary_id);
        }

        // Wrap once and scan the prefix which precedes the original cursor.
        let mut current = self.core.registry.head();
        while let Some(shard) = current {
            let shard_id = self.core.shard(shard).id;
            if shard_id >= start_id || shard_id > boundary_id {
                break;
            }

            let next = self.core.registry.next(shard);
            if let Some(value) = self.core.pop_from(shard) {
                self.cursor = next.or_else(|| self.core.registry.head());
                return Some(value);
            }
            current = next.filter(|candidate| self.core.shard(*candidate).id <= boundary_id);
        }

        // Empty scans rotate too so receivers do not continually favor the
        // same inactive shard.
        self.cursor = self
            .core
            .registry
            .next(start)
            .or_else(|| self.core.registry.head());
        None
    }

    fn scan_batch_once(&mut self, values: &mut Vec<T>, size: usize) -> usize {
        debug_assert!(size != 0);

        let Some(boundary) = self.core.registry.tail() else {
            return 0;
        };
        let boundary_id = self.core.shard(boundary).id;
        let Some(start) = self
            .cursor
            .take()
            .filter(|shard| self.core.shard(*shard).id <= boundary_id)
            .or_else(|| self.core.registry.head())
        else {
            return 0;
        };
        let start_id = self.core.shard(start).id;
        let mut received = 0;

        // Prefer a contiguous reservation from the current shard. If it cannot
        // fill the request, continue through the captured producer prefix and
        // ask each later shard for the remaining range exactly once.
        let mut current = Some(start);
        while let Some(shard) = current {
            if self.core.shard(shard).id > boundary_id {
                break;
            }

            let next = self.core.registry.next(shard);
            let popped = self.core.pop_batch_from(shard, size - received, values);
            if popped != 0 {
                received += popped;
                self.cursor = next.or_else(|| self.core.registry.head());
                if received == size {
                    return received;
                }
            }

            if self.core.shard(shard).id == boundary_id {
                break;
            }
            current = next.filter(|candidate| self.core.shard(*candidate).id <= boundary_id);
        }

        // Wrap once to cover shards which preceded the starting cursor.
        let mut current = self.core.registry.head();
        while let Some(shard) = current {
            let shard_id = self.core.shard(shard).id;
            if shard_id >= start_id || shard_id > boundary_id {
                break;
            }

            let next = self.core.registry.next(shard);
            let popped = self.core.pop_batch_from(shard, size - received, values);
            if popped != 0 {
                received += popped;
                self.cursor = next.or_else(|| self.core.registry.head());
                if received == size {
                    return received;
                }
            }
            current = next.filter(|candidate| self.core.shard(*candidate).id <= boundary_id);
        }

        if received == 0 {
            // Rotate after an empty scan just as the scalar path does.
            self.cursor = self
                .core
                .registry
                .next(start)
                .or_else(|| self.core.registry.head());
        }

        received
    }
}

impl<T, B: BackoffPolicy> Clone for Receiver<T, B> {
    fn clone(&self) -> Self {
        Self {
            core: Arc::clone(&self.core),
            cursor: None,
        }
    }
}

/// Minimal no_std structural lock used only for rare list mutations.
struct SpinLock<T> {
    locked: AtomicBool,
    value: UnsafeCell<T>,
}

impl<T> SpinLock<T> {
    fn new(value: T) -> Self {
        Self {
            locked: AtomicBool::new(false),
            value: UnsafeCell::new(value),
        }
    }

    fn lock(&self) -> SpinLockGuard<'_, T> {
        while self
            .locked
            .compare_exchange_weak(false, true, Acquire, Relaxed)
            .is_err()
        {
            while self.locked.load(Relaxed) {
                spin_loop();
            }
        }

        SpinLockGuard { lock: self }
    }
}

// SAFETY: access to value is serialized by `locked`, with Acquire/Release
// synchronization. Moving a lock is safe exactly when moving T is safe.
unsafe impl<T: Send> Send for SpinLock<T> {}
// SAFETY: shared access to value is available only through the exclusive guard.
unsafe impl<T: Send> Sync for SpinLock<T> {}

struct SpinLockGuard<'a, T> {
    lock: &'a SpinLock<T>,
}

impl<T> Deref for SpinLockGuard<'_, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        // SAFETY: this guard owns the lock until Drop.
        unsafe { &*self.lock.value.get() }
    }
}

impl<T> DerefMut for SpinLockGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        // SAFETY: only one guard can exist at a time.
        unsafe { &mut *self.lock.value.get() }
    }
}

impl<T> Drop for SpinLockGuard<'_, T> {
    fn drop(&mut self) {
        self.lock.locked.store(false, Release);
    }
}

#[cfg(test)]
mod tests {
    use super::{TryRecvError, channel};
    use alloc::{sync::Arc, vec::Vec};
    use core::sync::atomic::{
        AtomicUsize,
        Ordering::{Acquire, Relaxed},
    };
    use std::thread;

    #[test]
    fn channel_starts_open_and_empty() {
        let (_sender, mut receiver) = channel::<usize>();
        assert_eq!(receiver.try_recv(), Err(TryRecvError::Empty));
        assert!(!receiver.is_closed());
    }

    #[test]
    fn receiver_drains_after_final_sender_exits() {
        let (mut sender, mut receiver) = channel();
        sender.send_batch(0..32);
        drop(sender);

        assert!(receiver.is_closed());
        assert_eq!(
            receiver.pop_batch(32).collect::<Vec<_>>(),
            (0..32).collect::<Vec<_>>()
        );
        assert_eq!(receiver.try_recv(), Err(TryRecvError::Disconnected));
    }

    #[test]
    fn inactive_shard_remains_drainable_while_other_sender_runs() {
        let (mut first, mut receiver) = channel();
        let mut anchor = first.clone();
        first.send_batch(0..20);
        drop(first);

        assert_eq!(
            receiver.pop_batch(20).collect::<Vec<_>>(),
            (0..20).collect::<Vec<_>>()
        );
        assert_eq!(receiver.try_recv(), Err(TryRecvError::Empty));

        anchor.send(42);
        assert_eq!(receiver.try_recv(), Ok(42));
        drop(anchor);
        assert_eq!(receiver.try_recv(), Err(TryRecvError::Disconnected));
    }

    #[test]
    fn drained_inactive_shard_remains_routed_and_is_reused() {
        let (mut first, mut receiver) = channel();
        let second = first.clone();
        let first_shard = first.shard;
        first.send(7);
        drop(first);

        assert_eq!(receiver.try_recv(), Ok(7));
        assert_eq!(receiver.core.registry.routed_len(), 2);

        let reused = second.clone();
        assert_eq!(reused.shard.as_ptr(), first_shard.as_ptr());
        assert_eq!(receiver.core.registry.routed_len(), 2);

        drop(reused);
        drop(second);
        assert_eq!(receiver.try_recv(), Err(TryRecvError::Disconnected));
    }

    #[test]
    fn drained_inactive_tail_is_reused_without_growing_the_list() {
        let (first, mut receiver) = channel::<usize>();
        let mut retired_tail = first.clone();
        let retired_tail_shard = retired_tail.shard;
        retired_tail.send(9);
        drop(retired_tail);

        assert_eq!(receiver.try_recv(), Ok(9));
        assert_eq!(receiver.core.registry.routed_len(), 2);

        let mut new_tail = first.clone();
        assert_eq!(new_tail.shard.as_ptr(), retired_tail_shard.as_ptr());
        new_tail.send(10);
        assert_eq!(receiver.try_recv(), Ok(10));
        assert_eq!(receiver.core.registry.routed_len(), 2);

        drop(new_tail);
        drop(first);
        assert_eq!(receiver.try_recv(), Err(TryRecvError::Disconnected));
    }

    #[test]
    fn buffered_inactive_shard_is_not_reused_until_drained() {
        let (mut first, mut receiver) = channel();
        let anchor = first.clone();
        let buffered_shard = first.shard;
        first.send(7);
        drop(first);

        let temporary = anchor.clone();
        assert_ne!(temporary.shard.as_ptr(), buffered_shard.as_ptr());
        assert_eq!(receiver.core.registry.routed_len(), 3);
        drop(temporary);

        assert_eq!(receiver.try_recv(), Ok(7));
        let reused = anchor.clone();
        assert_eq!(reused.shard.as_ptr(), buffered_shard.as_ptr());
        assert_eq!(receiver.core.registry.routed_len(), 3);

        drop(reused);
        drop(anchor);
        assert_eq!(receiver.try_recv(), Err(TryRecvError::Disconnected));
    }

    #[test]
    fn receiver_clone_is_strong_but_does_not_add_a_shard() {
        let (sender, receiver) = channel::<usize>();
        let routed = receiver.core.registry.routed_len();
        let strong = Arc::strong_count(&receiver.core);

        let receiver_two = receiver.clone();
        assert_eq!(Arc::strong_count(&receiver.core), strong + 1);
        assert_eq!(receiver.core.registry.routed_len(), routed);

        drop(receiver_two);
        assert_eq!(Arc::strong_count(&receiver.core), strong);
        drop(sender);
    }

    #[test]
    fn sender_clone_allocates_and_appends_a_distinct_shard() {
        let (first, receiver) = channel::<usize>();
        let second = first.clone();

        assert_ne!(first.shard().id, second.shard().id);
        assert_eq!(receiver.core.senders.load(Relaxed), 2);
        assert_eq!(receiver.core.registry.routed_len(), 2);

        drop(second);
        drop(first);
        assert_eq!(receiver.core.senders.load(Acquire), 0);
    }

    #[test]
    fn repeated_sender_churn_keeps_only_anchor_and_current_tail_routed() {
        let (anchor, mut receiver) = channel();
        let mut reusable_shard = None;

        for value in 0..500 {
            let mut sender = anchor.clone();
            let shard = sender.shard.as_ptr();
            assert_eq!(*reusable_shard.get_or_insert(shard), shard);
            sender.send(value);
            drop(sender);

            assert_eq!(receiver.try_recv(), Ok(value));
            assert_eq!(receiver.core.registry.routed_len(), 2);
        }

        drop(anchor);
        assert_eq!(receiver.try_recv(), Err(TryRecvError::Disconnected));
    }

    #[test]
    fn native_batch_preserves_fifo_and_rotates_between_producer_bursts() {
        let (mut first, mut receiver) = channel();
        let mut second = first.clone();
        first.send_batch(0..4);
        second.send_batch(10..14);

        assert_eq!(
            receiver.pop_batch(8).collect::<Vec<_>>(),
            [0, 1, 2, 3, 10, 11, 12, 13]
        );

        drop(first);
        drop(second);
        assert_eq!(receiver.try_recv(), Err(TryRecvError::Disconnected));
    }

    #[test]
    fn caller_owned_batch_appends_up_to_the_requested_size() {
        let (mut sender, mut receiver) = channel();
        sender.send_batch(0..8);

        let mut values = alloc::vec![99];
        assert_eq!(receiver.try_recv_batch_into(&mut values, 3), Ok(3));
        assert_eq!(values, [99, 0, 1, 2]);

        values.clear();
        assert_eq!(receiver.pop_batch_into(&mut values, 16), 5);
        assert_eq!(values, [3, 4, 5, 6, 7]);

        values.clear();
        assert_eq!(receiver.try_recv_batch_into(&mut values, 0), Ok(0));
        assert_eq!(
            receiver.try_recv_batch_into(&mut values, 1),
            Err(TryRecvError::Empty)
        );
        assert!(values.is_empty());

        drop(sender);
        assert_eq!(
            receiver.try_recv_batch_into(&mut values, 1),
            Err(TryRecvError::Disconnected)
        );
    }

    #[test]
    fn native_batch_crosses_inner_ubq_blocks() {
        let (mut sender, mut receiver) = channel();
        let len = sender.shard().queue().block_length() + 17;
        sender.send_batch(0..len);

        let mut values = Vec::new();
        assert_eq!(receiver.try_recv_batch_into(&mut values, len), Ok(len));
        assert_eq!(values, (0..len).collect::<Vec<_>>());

        drop(sender);
        assert_eq!(receiver.try_recv(), Err(TryRecvError::Disconnected));
    }

    #[test]
    fn values_drop_after_receivers_and_senders_are_gone() {
        struct CountDrop(Arc<AtomicUsize>);

        impl Drop for CountDrop {
            fn drop(&mut self) {
                self.0.fetch_add(1, Relaxed);
            }
        }

        let dropped = Arc::new(AtomicUsize::new(0));
        let (mut sender, receiver) = channel();
        sender.send_batch((0..17).map(|_| CountDrop(Arc::clone(&dropped))));

        drop(receiver);
        assert_eq!(dropped.load(Relaxed), 0);
        drop(sender);
        assert_eq!(dropped.load(Relaxed), 17);
    }

    #[test]
    fn drained_inactive_shard_keeps_its_ubq_until_root_drop() {
        let (mut first, mut receiver) = channel();
        let anchor = first.clone();
        let shard = first.shard;
        first.send_batch(0..8);
        drop(first);

        assert_eq!(
            receiver.pop_batch(8).collect::<Vec<_>>(),
            (0..8).collect::<Vec<_>>()
        );
        assert!(receiver.core.shard(shard).queue().is_empty());
        assert_eq!(receiver.core.registry.routed_len(), 2);

        let reused = anchor.clone();
        assert_eq!(reused.shard.as_ptr(), shard.as_ptr());
        assert_eq!(receiver.core.registry.routed_len(), 2);

        drop(reused);
        drop(anchor);
    }

    #[test]
    fn inactive_buffered_values_remain_until_root_drop() {
        struct CountDrop(Arc<AtomicUsize>);

        impl Drop for CountDrop {
            fn drop(&mut self) {
                self.0.fetch_add(1, Relaxed);
            }
        }

        let dropped = Arc::new(AtomicUsize::new(0));
        let (mut first, receiver) = channel();
        let anchor = first.clone();
        first.send_batch((0..11).map(|_| CountDrop(Arc::clone(&dropped))));
        drop(first);

        assert_eq!(dropped.load(Relaxed), 0);
        drop(receiver);
        assert_eq!(dropped.load(Relaxed), 0);

        drop(anchor);
        assert_eq!(dropped.load(Relaxed), 11);
    }

    #[test]
    fn final_send_is_not_missed_by_closed_detection() {
        for value in 0..1_000 {
            let (mut sender, mut receiver) = channel();
            let producer = thread::spawn(move || {
                sender.send(value);
            });

            let observed = loop {
                match receiver.try_recv() {
                    Ok(value) => break value,
                    Err(TryRecvError::Empty) => thread::yield_now(),
                    Err(TryRecvError::Disconnected) => panic!("final send was missed"),
                }
            };

            producer.join().unwrap();
            assert_eq!(observed, value);
            assert_eq!(receiver.try_recv(), Err(TryRecvError::Disconnected));
        }
    }

    #[test]
    fn concurrent_fan_in_delivers_every_value_once() {
        const PRODUCERS: usize = 4;
        const CONSUMERS: usize = 3;
        const ITEMS: usize = 2_000;
        const TOTAL: usize = PRODUCERS * ITEMS;

        let (first, receiver) = channel();
        let mut senders = Vec::with_capacity(PRODUCERS);
        senders.push(first);
        while senders.len() < PRODUCERS {
            senders.push(senders[0].clone());
        }

        let mut receivers = Vec::with_capacity(CONSUMERS);
        receivers.push(receiver);
        while receivers.len() < CONSUMERS {
            receivers.push(receivers[0].clone());
        }

        let seen = Arc::new((0..TOTAL).map(|_| AtomicUsize::new(0)).collect::<Vec<_>>());
        let producer_threads = senders
            .into_iter()
            .enumerate()
            .map(|(producer, mut sender)| {
                thread::spawn(move || {
                    sender.send_batch((0..ITEMS).map(|item| producer * ITEMS + item));
                })
            })
            .collect::<Vec<_>>();

        let consumer_threads = receivers
            .into_iter()
            .map(|mut receiver| {
                let seen = Arc::clone(&seen);
                thread::spawn(move || {
                    loop {
                        match receiver.try_recv() {
                            Ok(value) => {
                                assert_eq!(seen[value].fetch_add(1, Relaxed), 0);
                            }
                            Err(TryRecvError::Empty) => thread::yield_now(),
                            Err(TryRecvError::Disconnected) => break,
                        }
                    }
                })
            })
            .collect::<Vec<_>>();

        for producer in producer_threads {
            producer.join().unwrap();
        }
        for consumer in consumer_threads {
            consumer.join().unwrap();
        }

        assert!(seen.iter().all(|count| count.load(Relaxed) == 1));
    }

    #[test]
    fn concurrent_native_batches_deliver_every_value_once() {
        const PRODUCERS: usize = 4;
        const CONSUMERS: usize = 3;
        const ITEMS: usize = 2_000;
        const BATCH: usize = 64;
        const TOTAL: usize = PRODUCERS * ITEMS;

        let (first, receiver) = channel();
        let mut senders = Vec::with_capacity(PRODUCERS);
        senders.push(first);
        while senders.len() < PRODUCERS {
            senders.push(senders[0].clone());
        }

        let mut receivers = Vec::with_capacity(CONSUMERS);
        receivers.push(receiver);
        while receivers.len() < CONSUMERS {
            receivers.push(receivers[0].clone());
        }

        let seen = Arc::new((0..TOTAL).map(|_| AtomicUsize::new(0)).collect::<Vec<_>>());
        let producer_threads = senders
            .into_iter()
            .enumerate()
            .map(|(producer, mut sender)| {
                thread::spawn(move || {
                    sender.send_batch((0..ITEMS).map(|item| producer * ITEMS + item));
                })
            })
            .collect::<Vec<_>>();

        let consumer_threads = receivers
            .into_iter()
            .map(|mut receiver| {
                let seen = Arc::clone(&seen);
                thread::spawn(move || {
                    let mut values = Vec::with_capacity(BATCH);
                    loop {
                        values.clear();
                        match receiver.try_recv_batch_into(&mut values, BATCH) {
                            Ok(received) => {
                                assert_eq!(received, values.len());
                                for value in values.drain(..) {
                                    assert_eq!(seen[value].fetch_add(1, Relaxed), 0);
                                }
                            }
                            Err(TryRecvError::Empty) => thread::yield_now(),
                            Err(TryRecvError::Disconnected) => break,
                        }
                    }
                })
            })
            .collect::<Vec<_>>();

        for producer in producer_threads {
            producer.join().unwrap();
        }
        for consumer in consumer_threads {
            consumer.join().unwrap();
        }

        assert!(seen.iter().all(|count| count.load(Relaxed) == 1));
    }
}
