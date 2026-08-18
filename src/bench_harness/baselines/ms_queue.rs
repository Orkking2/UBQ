//! The textbook Michael & Scott linked-list queue — the "why not just
//! allocate per element" floor baseline. Nothing else currently in the
//! comparative grid represents pure per-element allocation: `SegQueue`,
//! `ConcurrentQueue`, and UBQ are all already block-structured.
//!
//! Reclamation uses `crossbeam-epoch` rather than an intentional leak.
//! Leaking is not actually free here: the benchmark harness reuses one
//! worker subprocess across every successful job in a run (only respawning
//! after a `Failed`/`TimedOut` outcome), so a leaking queue would accumulate
//! unreclaimed nodes across however many jobs that worker happens to handle
//! — risking multi-GB growth or an OOM that surfaces as an unrelated later
//! job's spurious failure. `crossbeam-epoch` is the standard, well-trodden
//! choice for exactly this queue shape and adds no build-time complexity.
//!
//! No `send_batch`/`try_recv_batch` override: MS-queue has no structural
//! bulk-reservation primitive (each push is one CAS on one node), so this
//! stays scalar-only, on par with `FastFifo`/`LfQueue`/`Wcq`.

use std::sync::atomic::Ordering;

use crossbeam_epoch::{self as epoch, Atomic, Owned, Shared};

use crate::bench_harness::{BenchQueue, BenchQueueOps, LogQueue, LogQueueOps, LogRecord};

struct Node<T> {
    // `None` only for the permanent sentinel node at the head of the list.
    // Behind an `Atomic`-guarded pointer that is only ever dereferenced by
    // the single thread that just won the CAS retiring this node as the new
    // head (see `pop`), so a plain, non-atomic `Option<T>` is sound here —
    // no concurrent access to the same node's `data` is possible.
    data: Option<T>,
    next: Atomic<Node<T>>,
}

pub struct MsQueue<T> {
    head: Atomic<Node<T>>,
    tail: Atomic<Node<T>>,
}

// SAFETY: values are moved through the list, never shared by reference
// across threads; `T: Send` is sufficient, matching UBQ's own reasoning.
unsafe impl<T: Send> Send for MsQueue<T> {}
unsafe impl<T: Send> Sync for MsQueue<T> {}

impl<T> MsQueue<T> {
    pub fn new() -> Self {
        let sentinel = Owned::new(Node {
            data: None,
            next: Atomic::null(),
        });
        let guard = epoch::pin();
        let sentinel = sentinel.into_shared(&guard);
        Self {
            head: Atomic::from(sentinel),
            tail: Atomic::from(sentinel),
        }
    }

    pub fn push(&self, value: T) {
        let new_node = Owned::new(Node {
            data: Some(value),
            next: Atomic::null(),
        });
        let guard = &epoch::pin();
        let mut new_node = new_node;
        loop {
            let tail = self.tail.load(Ordering::Acquire, guard);
            // SAFETY: `tail` is never null and always points at a live node
            // (either the sentinel or a pushed node); this queue never frees
            // a node still reachable from `tail`.
            let tail_ref = unsafe { tail.deref() };
            let next = tail_ref.next.load(Ordering::Acquire, guard);
            if next.is_null() {
                match tail_ref.next.compare_exchange(
                    Shared::null(),
                    new_node,
                    Ordering::Release,
                    Ordering::Relaxed,
                    guard,
                ) {
                    Ok(new_node) => {
                        let _ = self.tail.compare_exchange(
                            tail,
                            new_node,
                            Ordering::Release,
                            Ordering::Relaxed,
                            guard,
                        );
                        return;
                    }
                    Err(err) => {
                        new_node = err.new;
                    }
                }
            } else {
                // Tail lagged behind; help swing it forward before retrying.
                let _ = self.tail.compare_exchange(
                    tail,
                    next,
                    Ordering::Release,
                    Ordering::Relaxed,
                    guard,
                );
            }
        }
    }

    pub fn try_pop(&self) -> Option<T> {
        let guard = &epoch::pin();
        loop {
            let head = self.head.load(Ordering::Acquire, guard);
            // SAFETY: `head` always points at a live node; nodes are only
            // reclaimed via `guard.defer_destroy` after being unlinked here.
            let head_ref = unsafe { head.deref() };
            let next = head_ref.next.load(Ordering::Acquire, guard);
            // SAFETY: `next`, if non-null, points at a live node linked by
            // the current head; it cannot be concurrently freed while
            // reachable from `head`.
            let next_ref = unsafe { next.as_ref() }?;
            if self
                .head
                .compare_exchange(head, next, Ordering::Release, Ordering::Relaxed, guard)
                .is_ok()
            {
                // SAFETY: this thread uniquely won the CAS retiring `head`
                // as the old sentinel and promoting `next` to the new
                // sentinel; per the MS-queue convention the dequeued value
                // lives in the *new* sentinel's `data`, and this is the only
                // thread that will ever read it (a second dequeuer's `head`
                // load can no longer observe `head` as current). The old
                // node is retired via `defer_destroy`, not freed here.
                let value = unsafe {
                    let next_mut = &next_ref.data as *const Option<T> as *mut Option<T>;
                    (*next_mut).take()
                };
                unsafe {
                    guard.defer_destroy(head);
                }
                return value;
            }
        }
    }
}

impl<T> Default for MsQueue<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Drop for MsQueue<T> {
    fn drop(&mut self) {
        // No concurrent access is possible once we have `&mut self`; drain
        // and free every remaining node directly rather than through the
        // epoch-deferred path.
        while self.try_pop().is_some() {}
        // SAFETY: `&mut self` guarantees no other thread holds a reference;
        // `unprotected` is sound in single-threaded teardown.
        unsafe {
            let guard = epoch::unprotected();
            let sentinel = self.head.load(Ordering::Relaxed, guard);
            if !sentinel.is_null() {
                drop(sentinel.into_owned());
            }
        }
    }
}

impl BenchQueueOps for MsQueue<u64> {
    fn try_send_value(&self, value: u64) -> bool {
        self.push(value);
        true
    }

    fn try_recv_value(&self) -> Option<u64> {
        self.try_pop()
    }
}

impl BenchQueue for MsQueue<u64> {
    fn new_queue() -> std::sync::Arc<Self> {
        std::sync::Arc::new(Self::new())
    }
}

impl LogQueueOps for MsQueue<LogRecord> {
    fn send_log(&self, record: LogRecord) {
        self.push(record);
    }

    fn try_recv_log(&self) -> Option<LogRecord> {
        self.try_pop()
    }
}

impl LogQueue for MsQueue<LogRecord> {
    fn new_log_queue() -> std::sync::Arc<Self> {
        std::sync::Arc::new(Self::new())
    }
}
