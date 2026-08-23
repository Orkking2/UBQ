//! `moodycamel::ConcurrentQueue` (`cameron314/concurrentqueue`), wrapped via
//! the small `extern "C"` shim in `third_party/moodycamel/shim.cpp` rather
//! than reimplemented — this is the canonical C++ MPMC queue with real
//! `enqueue_bulk`/`try_dequeue_bulk`, and wrapping the actual upstream
//! algorithm gets that comparison with none of the "did I port the CAS
//! retry logic correctly" risk a reimplementation would carry.
//!
//! Every producer and consumer thread gets its own `ProducerToken`/
//! `ConsumerToken`, created once and reused for that thread's whole run —
//! the usage moodycamel's own header recommends for known, persistent
//! threads (see the comment in `shim.cpp`). This harness always knows the
//! scenario's producer/consumer thread shape ahead of time, so there's no
//! reason to pay the token-free API's per-call "implicit producer"
//! registration cost. That means `MoodycamelQueue` implements
//! [`BenchQueueHandleFactory`]/[`LogQueueHandleFactory`] rather than
//! [`BenchQueueOps`]/[`LogQueueOps`] directly: the thread-scoped token pair
//! lives on the per-thread handle, not on the shared queue.
//!
//! The shim is monomorphized to `uint64_t` only. `LogRecord` doesn't fit in
//! a `u64`, so the log path boxes it and passes the pointer through as a
//! `u64` — the same smuggling technique already used elsewhere in this
//! harness to shuttle non-`u64` payloads through a `u64`-typed channel (see
//! `bench_data_latency_with_queue` in `crate::bench_harness`).

use std::cell::UnsafeCell;
use std::os::raw::c_void;
use std::sync::Arc;

use crate::bench_harness::{
    BenchQueueHandleFactory, BenchQueueThreadOps, LogQueueHandleFactory, LogQueueThreadOps,
    LogRecord,
};

unsafe extern "C" {
    fn mc_queue_new() -> *mut c_void;
    fn mc_queue_free(handle: *mut c_void);
    fn mc_queue_create_producer_token(handle: *mut c_void) -> *mut c_void;
    fn mc_queue_free_producer_token(token: *mut c_void);
    fn mc_queue_enqueue_with_token(handle: *mut c_void, token: *mut c_void, value: u64) -> bool;
    fn mc_queue_enqueue_bulk_with_token(
        handle: *mut c_void,
        token: *mut c_void,
        items: *const u64,
        count: usize,
    ) -> bool;
    fn mc_queue_create_consumer_token(handle: *mut c_void) -> *mut c_void;
    fn mc_queue_free_consumer_token(token: *mut c_void);
    fn mc_queue_try_dequeue_with_token(
        handle: *mut c_void,
        token: *mut c_void,
        out: *mut u64,
    ) -> bool;
    fn mc_queue_try_dequeue_bulk_with_token(
        handle: *mut c_void,
        token: *mut c_void,
        out: *mut u64,
        max: usize,
    ) -> usize;
}

pub struct MoodycamelQueue {
    handle: *mut c_void,
}

// SAFETY: moodycamel::ConcurrentQueue is documented upstream as safe for
// concurrent multi-producer/multi-consumer access from any thread without
// external synchronization. The raw handle here is only ever touched through
// the shim's C API (never dereferenced directly), so sharing and sending it
// across threads is sound.
unsafe impl Send for MoodycamelQueue {}
unsafe impl Sync for MoodycamelQueue {}

impl MoodycamelQueue {
    pub fn new_handle() -> Arc<Self> {
        let handle = unsafe { mc_queue_new() };
        assert!(!handle.is_null(), "moodycamel queue allocation failed");
        Arc::new(Self { handle })
    }
}

impl Drop for MoodycamelQueue {
    fn drop(&mut self) {
        unsafe { mc_queue_free(self.handle) };
    }
}

struct ProducerTokenHandle(*mut c_void);

// SAFETY: this token is created once in `thread_handle()`/`log_thread_handle()`
// and moved into the one benchmark thread that will use it, before that
// thread makes its first call — matching moodycamel's own guidance ("ideally
// there should be a maximum of one token per thread"). It is never shared or
// accessed by more than one thread at a time, so moving it across the
// spawning thread boundary once, before use, is sound.
unsafe impl Send for ProducerTokenHandle {}

impl Drop for ProducerTokenHandle {
    fn drop(&mut self) {
        unsafe { mc_queue_free_producer_token(self.0) };
    }
}

struct ConsumerTokenHandle(*mut c_void);

// SAFETY: see ProducerTokenHandle above; the same single-owning-thread
// lifecycle applies to consumer tokens.
unsafe impl Send for ConsumerTokenHandle {}

impl Drop for ConsumerTokenHandle {
    fn drop(&mut self) {
        unsafe { mc_queue_free_consumer_token(self.0) };
    }
}

enum MoodycamelThreadRole {
    Producer {
        token: ProducerTokenHandle,
        batch: UnsafeCell<Vec<u64>>,
    },
    Consumer {
        token: ConsumerTokenHandle,
        batch: UnsafeCell<Vec<u64>>,
    },
}

/// Per-thread moodycamel handle with exactly the token required by its role.
/// Batch storage is retained for the thread's full benchmark lifetime.
pub struct MoodycamelThreadHandle {
    queue: Arc<MoodycamelQueue>,
    role: MoodycamelThreadRole,
}

fn new_producer_handle(queue: &Arc<MoodycamelQueue>) -> MoodycamelThreadHandle {
    let producer_token = unsafe { mc_queue_create_producer_token(queue.handle) };
    assert!(
        !producer_token.is_null(),
        "moodycamel producer token allocation failed"
    );
    MoodycamelThreadHandle {
        queue: queue.clone(),
        role: MoodycamelThreadRole::Producer {
            token: ProducerTokenHandle(producer_token),
            batch: UnsafeCell::new(Vec::new()),
        },
    }
}

fn new_consumer_handle(queue: &Arc<MoodycamelQueue>) -> MoodycamelThreadHandle {
    let consumer_token = unsafe { mc_queue_create_consumer_token(queue.handle) };
    assert!(
        !consumer_token.is_null(),
        "moodycamel consumer token allocation failed"
    );
    MoodycamelThreadHandle {
        queue: queue.clone(),
        role: MoodycamelThreadRole::Consumer {
            token: ConsumerTokenHandle(consumer_token),
            batch: UnsafeCell::new(Vec::new()),
        },
    }
}

impl BenchQueueHandleFactory for MoodycamelQueue {
    type ThreadHandle = MoodycamelThreadHandle;

    fn thread_handle(self: &Arc<Self>) -> Self::ThreadHandle {
        panic!("moodycamel handles must be requested for a producer or consumer role")
    }

    fn producer_thread_handle(self: &Arc<Self>) -> Self::ThreadHandle {
        new_producer_handle(self)
    }

    fn consumer_thread_handle(self: &Arc<Self>) -> Self::ThreadHandle {
        new_consumer_handle(self)
    }
}

impl LogQueueHandleFactory for MoodycamelQueue {
    type ThreadHandle = MoodycamelThreadHandle;

    fn log_thread_handle(self: &Arc<Self>) -> Self::ThreadHandle {
        panic!("moodycamel log handles must be requested for a producer or consumer role")
    }

    fn log_producer_thread_handle(self: &Arc<Self>) -> Self::ThreadHandle {
        new_producer_handle(self)
    }

    fn log_consumer_thread_handle(self: &Arc<Self>) -> Self::ThreadHandle {
        new_consumer_handle(self)
    }
}

impl BenchQueueThreadOps for MoodycamelThreadHandle {
    fn try_send_value(&self, value: u64) -> bool {
        let MoodycamelThreadRole::Producer { token, .. } = &self.role else {
            panic!("attempted to send through a moodycamel consumer handle");
        };
        unsafe { mc_queue_enqueue_with_token(self.queue.handle, token.0, value) }
    }

    fn send_batch(&self, base: u64, offsets: std::ops::Range<usize>) {
        let MoodycamelThreadRole::Producer { token, batch } = &self.role else {
            panic!("attempted to send a batch through a moodycamel consumer handle");
        };
        // SAFETY: the harness moves this non-Sync handle into one worker and
        // never overlaps calls on it.
        let items = unsafe { &mut *batch.get() };
        items.clear();
        items.extend(offsets.map(|offset| base + offset as u64));
        let backoff = crossbeam_utils::Backoff::new();
        while !unsafe {
            mc_queue_enqueue_bulk_with_token(
                self.queue.handle,
                token.0,
                items.as_ptr(),
                items.len(),
            )
        } {
            backoff.snooze();
        }
    }

    fn try_recv_value(&self) -> Option<u64> {
        let MoodycamelThreadRole::Consumer { token, .. } = &self.role else {
            panic!("attempted to receive through a moodycamel producer handle");
        };
        let mut value = 0_u64;
        let ok = unsafe { mc_queue_try_dequeue_with_token(self.queue.handle, token.0, &mut value) };
        ok.then_some(value)
    }

    fn try_recv_batch(&self, request_size: usize) -> usize {
        let MoodycamelThreadRole::Consumer { token, batch } = &self.role else {
            panic!("attempted to receive a batch through a moodycamel producer handle");
        };
        // SAFETY: the harness moves this non-Sync handle into one worker and
        // never overlaps calls on it. Keeping len at the requested size makes
        // every FFI output slot valid without re-zeroing an established buffer.
        let buf = unsafe { &mut *batch.get() };
        buf.resize(request_size, 0);
        unsafe {
            mc_queue_try_dequeue_bulk_with_token(
                self.queue.handle,
                token.0,
                buf.as_mut_ptr(),
                request_size,
            )
        }
    }
}

impl LogQueueThreadOps for MoodycamelThreadHandle {
    fn send_log(&self, record: LogRecord) {
        let MoodycamelThreadRole::Producer { token, .. } = &self.role else {
            panic!("attempted to send a log through a moodycamel consumer handle");
        };
        let ptr = Box::into_raw(Box::new(record)) as usize as u64;
        let backoff = crossbeam_utils::Backoff::new();
        while !unsafe { mc_queue_enqueue_with_token(self.queue.handle, token.0, ptr) } {
            backoff.snooze();
        }
    }

    fn try_recv_log(&self) -> Option<LogRecord> {
        let MoodycamelThreadRole::Consumer { token, .. } = &self.role else {
            panic!("attempted to receive a log through a moodycamel producer handle");
        };
        let mut ptr = 0_u64;
        let ok = unsafe { mc_queue_try_dequeue_with_token(self.queue.handle, token.0, &mut ptr) };
        ok.then(|| *unsafe { Box::from_raw(ptr as usize as *mut LogRecord) })
    }
}
