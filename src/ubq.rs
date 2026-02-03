use crate::{cursor::Cursor, head::Head};
use crossbeam_utils::{Backoff, CachePadded};
use std::{
    alloc::{Layout, alloc_zeroed, handle_alloc_error},
    cell::UnsafeCell,
    mem::MaybeUninit,
    ptr::NonNull,
    sync::atomic::{AtomicPtr, AtomicUsize, Ordering, fence},
};

/// The maximum number of threads that can increment allocated
/// in a particular block at one time.
pub(crate) const OFFSET_PAD: usize = 32;

pub struct UBQ<T> {
    copies: NonNull<AtomicUsize>,

    phead: Head<T>,
    chead: Head<T>,
}

impl<T> Drop for UBQ<T> {
    fn drop(&mut self) {
        let left = unsafe { self.copies.as_ref() }.fetch_sub(1, Ordering::Relaxed);

        if left == 1 {
            // We are the last UBQ being dropped
            let (_, original) = self.phead.load();

            for head in [&mut self.phead, &mut self.chead] {
                // SAFETY: We are the last UBQ being dropped
                unsafe { head.destroy() };
            }

            let mut cur = original;

            loop {
                let next_ptr = unsafe { cur.as_ref() }.next.load(Ordering::Relaxed);

                // SAFETY:
                unsafe { drop(Box::from_raw(cur.as_ptr())) };

                if next_ptr.is_null() {
                    break;
                }

                let next = unsafe { NonNull::new_unchecked(next_ptr) };

                if next == original {
                    break;
                }

                cur = next;
            }
        }
    }
}

impl<T> Clone for UBQ<T> {
    fn clone(&self) -> Self {
        // SAFETY: self.copies is created by Box::into_raw
        unsafe { self.copies.as_ref() }.fetch_add(1, Ordering::Relaxed);

        Self {
            copies: self.copies.clone(),
            phead: self.phead.clone(),
            chead: self.chead.clone(),
        }
    }
}

unsafe impl<T> Sync for UBQ<T> {}
unsafe impl<T> Send for UBQ<T> {}

enum PushState<T> {
    Success {
        #[cfg(test)]
        slot: usize,
    },
    GotoNext {
        next: NonNull<Block<T>>,
        version: usize,
        #[cfg(test)]
        slot: usize,
    },
    // AwaitNext {
    //     e: T,
    // },
    Reload {
        e: T,
    },
}

enum PopState<T> {
    Empty,
    Success {
        e: T,
        #[cfg(test)]
        slot: usize,
    },
    GotoNext {
        e: T,
        next: NonNull<Block<T>>,
        version: usize,
        #[cfg(test)]
        slot: usize,
    },
    Reload,
}

impl<T> UBQ<T> {
    pub fn new() -> Self {
        let root = unsafe { NonNull::new_unchecked(Box::into_raw(Block::new_zero())) };

        Self {
            copies: unsafe { NonNull::new_unchecked(Box::into_raw(Box::new(AtomicUsize::new(1)))) },
            phead: Head::new(root),
            chead: Head::new(root),
        }
    }

    pub fn push(&self, e: T) {
        let (mut version, mut phead) = self.phead.load();
        let backoff = Backoff::new();

        let mut e_opt = Some(e);

        '_outer: loop {
            // SAFETY: e_opt is always Some(e) when entering 'outer
            match Self::push_to(phead, version, unsafe { e_opt.take().unwrap_unchecked() }) {
                PushState::Success { .. } => return,
                PushState::GotoNext { version, next, .. } => {
                    self.phead.store(version, next);
                    return;
                }
                PushState::Reload { e } => {
                    backoff.snooze();

                    (version, phead) = self.phead.load();
                    e_opt = Some(e);

                    continue;
                }
            }
        }
    }

    #[cfg(test)]
    fn push_with_block(&self, e: T) -> (NonNull<Block<T>>, usize) {
        let (mut version, mut phead) = self.phead.load();
        let backoff = Backoff::new();
        let mut e_opt = Some(e);

        '_outer: loop {
            // SAFETY: e_opt is always Some(e) when entering 'outer
            match Self::push_to(phead, version, unsafe { e_opt.take().unwrap_unchecked() }) {
                PushState::Success { slot } => return (phead, slot),
                PushState::GotoNext {
                    version,
                    next,
                    slot,
                } => {
                    self.phead.store(version, next);
                    return (phead, slot);
                }
                PushState::Reload { e } => {
                    backoff.snooze();

                    (version, phead) = self.phead.load();
                    e_opt = Some(e);

                    continue;
                }
            }
        }
    }

    fn push_to(this: NonNull<Block<T>>, version: usize, e: T) -> PushState<T> {
        // SAFETY: `this` is always created from a NonNull::from_ref or Box::into_raw
        let this_ref = unsafe { this.as_ref() };
        let allocated = Cursor::from_raw(this_ref.allocated.load(Ordering::Relaxed));

        crate::ubq_log!(
            tag: "push.enter",
            "block={:p} allocated={} committed={}",
            this,
            allocated,
            this_ref.committed.load(Ordering::Relaxed)
        );

        if allocated.vsn() > version || allocated.off() >= this_ref.len() {
            // TODO: Fix log -- if allocated.vsn() > version, somehow we have been paused and now have invalid data,
            // thus we need to reload. If otherwise, this block is fully allocated, we are also out of sync and
            // should reload.
            crate::ubq_log!(
                tag: "push.await_full",
                "block={:p} allocated={} len={}",
                this,
                allocated,
                this_ref.len()
            );
            PushState::Reload { e }
        } else {
            let old = this_ref.allocated.fetch_add(1, Ordering::Relaxed);

            crate::ubq_log!(
                tag: "push.allocate",
                "block={:p} slot={} len={}",
                this,
                old,
                this_ref.len()
            );

            if old >= this_ref.len() {
                // The following was commented out because it is not necessary
                // for allocated to equal committed, if committed is >= BLOCK_CAP
                //
                // Ordering: We are not writing anything into this block,
                // so we do not need a Release fence here, or any fance at all.
                // this_ref.committed.fetch_add(1, Ordering::Relaxed);

                crate::ubq_log!(
                    tag: "push.over_alloc",
                    "block={:p} slot={} len={}",
                    this,
                    old,
                    this_ref.len()
                );

                PushState::Reload { e }
            } else {
                // SAFETY: We reserved this slot when we fetch_add'ed earlier.
                unsafe { this_ref.array[old].get().write(MaybeUninit::new(e)) };
                // Ordering: We need the write above to be visible to any consumers,
                // so we need at least a Release ordering here.
                this_ref.committed.fetch_add(1, Ordering::Release);
                crate::ubq_log!(
                    tag: "push.write",
                    "block={:p} slot={}",
                    this,
                    old
                );

                // Note: We are already synchronized at this point, so we do
                // not have to worry about our fellow producers contending to
                // reset next.
                if old + 1 == this_ref.len() {
                    let next = this_ref.next.load(Ordering::Acquire);

                    // Note: When we have just one block, it will not point anywhere. We also
                    // always expect to have >=2 blocks, so the null case always triggers the
                    // first time around, in which we allocate a new block every time.
                    if next.is_null() {
                        let new = Block::new(this);

                        this_ref.next.store(new.as_ptr(), Ordering::Release);

                        PushState::GotoNext {
                            next: new,
                            version,
                            #[cfg(test)]
                            slot: old,
                        }
                    } else {
                        match unsafe { next.as_ref().unwrap_unchecked() }.p_reset(version) {
                            usize::MAX => {
                                let new = Block::new(unsafe { NonNull::new_unchecked(next) });

                                this_ref.next.store(new.as_ptr(), Ordering::Release);

                                PushState::GotoNext {
                                    next: new,
                                    version,
                                    #[cfg(test)]
                                    slot: old,
                                }
                            }
                            version => PushState::GotoNext {
                                next: unsafe { NonNull::new_unchecked(next) },
                                version,
                                #[cfg(test)]
                                slot: old,
                            },
                        }
                    }
                } else {
                    crate::ubq_log!(
                        tag: "push.success",
                        "block={:p} slot={}",
                        this,
                        old
                    );
                    // We have succeeded, return.
                    PushState::Success {
                        #[cfg(test)]
                        slot: old,
                    }
                }
            }
        }
    }

    pub fn pop(&self) -> Option<T> {
        let (mut version, mut chead) = self.chead.load();
        let backoff = Backoff::new();

        '_outer: loop {
            match Self::pop_from(chead, version) {
                PopState::Empty => return None,
                PopState::Success { e, .. } => return Some(e),
                PopState::GotoNext {
                    e, version, next, ..
                } => {
                    self.chead.store(version, next);
                    return Some(e);
                }
                PopState::Reload => {
                    backoff.snooze();

                    (version, chead) = self.chead.load();

                    continue;
                }
            }
        }
    }

    #[cfg(test)]
    fn pop_with_block(&self) -> (Option<T>, NonNull<Block<T>>, Option<usize>) {
        let (mut version, mut chead) = self.chead.load();
        let backoff = Backoff::new();

        '_outer: loop {
            match Self::pop_from(chead, version) {
                PopState::Empty => return (None, chead, None),
                PopState::Success { e, slot } => return (Some(e), chead, Some(slot)),
                PopState::GotoNext {
                    e,
                    version,
                    next,
                    slot,
                } => {
                    self.chead.store(version, next);
                    return (Some(e), chead, Some(slot));
                }
                PopState::Reload => {
                    backoff.snooze();

                    (version, chead) = self.chead.load();

                    continue;
                }
            }
        }
    }

    fn pop_from(this: NonNull<Block<T>>, version: usize) -> PopState<T> {
        let backoff = Backoff::new();

        loop {
            // SAFETY: `this` is always created from a NonNull::from_ref or Box::into_raw
            let this_ref = unsafe { this.as_ref() };

            let reserved = Cursor::from_raw(this_ref.reserved.load(Ordering::Relaxed));

            if reserved.vsn() > version {
                return PopState::Reload;
            }

            if reserved.off() < BLOCK_CAP {
                // Ordering: committed is the signal for finished writes, we need
                // those writes to be visible when we read it later (if we read it).
                let committed = Cursor::from_raw(this_ref.committed.load(Ordering::Acquire));

                crate::ubq_log!(
                    tag: "pop.check",
                    "block={:p} reserved={} committed={}",
                    this,
                    reserved,
                    committed
                );

                if committed.off() <= reserved.off() {
                    crate::ubq_log!(
                        tag: "pop.empty",
                        "block={:p} reserved={} committed={}",
                        this,
                        reserved,
                        committed
                    );
                    crate::debug::maybe_flush();
                    return PopState::Empty;
                }

                if committed.off() < BLOCK_CAP {
                    let allocated = Cursor::from_raw(this_ref.allocated.load(Ordering::Acquire));

                    // Is allocated.off() < committed.off() ever possible?
                    if allocated.off() > /* != */ committed.off() {
                        crate::ubq_log!(
                            tag: "pop.wait_commit",
                            "block={:p} allocated={} committed={}",
                            this,
                            allocated,
                            committed
                        );

                        backoff.snooze();
                        crate::debug::maybe_flush();
                        continue;
                    }
                }

                if this_ref.reserved.fetch_max(
                    reserved.incr_off().into_raw(),
                    Ordering::SeqCst, /* Relaxed */
                ) == reserved.into_raw()
                {
                    crate::ubq_log!(
                        tag: "pop.reserve",
                        "block={:p} slot={} committed={}",
                        this,
                        reserved,
                        committed
                    );

                    let e = unsafe { this_ref.array[reserved.off()].get().read().assume_init() };
                    let consumed = this_ref.consumed.fetch_add(1, Ordering::Release);

                    if consumed + 1 == BLOCK_CAP {
                        loop {
                            let next = this_ref.next.load(Ordering::Acquire);

                            if next.is_null() {
                                backoff.snooze();
                                crate::debug::maybe_flush();
                                continue;
                            } else {
                                crate::ubq_log!(
                                    tag: "pop.goto_next",
                                    "block={:p} next={:p}",
                                    this,
                                    next
                                );
                                // SAFETY: next is not null
                                return PopState::GotoNext {
                                    e,
                                    version: unsafe { next.as_ref().unwrap_unchecked() }
                                        .c_reset(version),
                                    next: unsafe { NonNull::new_unchecked(next) },
                                    #[cfg(test)]
                                    slot: reserved.off(),
                                };
                            }
                        }
                    } else {
                        crate::ubq_log!(
                            tag: "pop.success",
                            "block={:p} slot={}",
                            this,
                            reserved
                        );
                        return PopState::Success {
                            e,
                            #[cfg(test)]
                            slot: reserved.off(),
                        };
                    }
                } else {
                    crate::ubq_log!(
                        tag: "pop.reserve_fail",
                        "block={:p} slot={}",
                        this,
                        reserved
                    );
                    backoff.spin();
                    crate::debug::maybe_flush();
                    continue;
                }
            }

            // We are not the one that needs to reset next.
            crate::ubq_log!(
                tag: "pop.await_next",
                "block={:p} reserved={}",
                this,
                reserved
            );
            return PopState::Reload;
        }
    }
}

#[cfg(all(feature = "bench_small", feature = "bench_medium"))]
compile_error!("Only one of bench_small, bench_medium, bench_large can be enabled at a time.");
#[cfg(all(feature = "bench_small", feature = "bench_large"))]
compile_error!("Only one of bench_small, bench_medium, bench_large can be enabled at a time.");
#[cfg(all(feature = "bench_medium", feature = "bench_large"))]
compile_error!("Only one of bench_small, bench_medium, bench_large can be enabled at a time.");

#[cfg(feature = "bench_small")]
#[doc(hidden)]
pub const BLOCK_CAP: usize = 8;
#[cfg(feature = "bench_medium")]
#[doc(hidden)]
pub const BLOCK_CAP: usize = 32;
#[cfg(feature = "bench_large")]
#[doc(hidden)]
pub const BLOCK_CAP: usize = 128;
#[cfg(not(any(
    feature = "bench_small",
    feature = "bench_medium",
    feature = "bench_large"
)))]
#[doc(hidden)]
pub const BLOCK_CAP: usize = 32;

pub struct Block<T> {
    allocated: CachePadded<AtomicUsize>,
    committed: CachePadded<AtomicUsize>,
    reserved: CachePadded<AtomicUsize>,
    consumed: CachePadded<AtomicUsize>,

    next: CachePadded<AtomicPtr<Self>>,

    array: [UnsafeCell<MaybeUninit<T>>; BLOCK_CAP],
}

impl<T> Drop for Block<T> {
    fn drop(&mut self) {
        let allocated = self.allocated.load(Ordering::SeqCst);
        let committed = self.committed.load(Ordering::SeqCst);
        let reserved = self.reserved.load(Ordering::SeqCst);
        let consumed = self.consumed.load(Ordering::SeqCst);

        debug_assert!(
            allocated == committed || (allocated > committed && committed >= BLOCK_CAP),
            "allocated {allocated} == {committed} committed || (allocated {allocated} < {committed} committed && committed {committed} >= {BLOCK_CAP} BLOCK_CAP)"
        );
        debug_assert!(
            reserved == consumed,
            "reserved {reserved} == {consumed} consumed"
        );

        (consumed..committed)
            .filter(|i| BLOCK_CAP.gt(i))
            .for_each(|i| unsafe { self.array[i].get().cast::<T>().drop_in_place() });
    }
}

impl<T> Block<T> {
    const LAYOUT: Layout = {
        let layout = Layout::new::<Self>();

        assert!(layout.size() != 0, "Block cannot be zero-sized");

        layout
    };

    fn new_zero() -> Box<Self> {
        let ptr = unsafe { alloc_zeroed(Self::LAYOUT) };

        if ptr.is_null() {
            handle_alloc_error(Self::LAYOUT)
        }

        unsafe { Box::from_raw(ptr.cast()) }
    }

    pub fn new(next: NonNull<Self>) -> NonNull<Self> {
        let out = Self::new_zero();

        // if version != 0 {
        //     for atomic in [&out.allocated, &out.committed, &out.reserved, &out.consumed] {
        //         atomic.store(
        //             Cursor::for_version(version).into_raw(),
        //             Ordering::SeqCst, /* Relaxed */
        //         );
        //     }
        // }

        out.next.store(next.as_ptr(), Ordering::Release);

        unsafe { NonNull::new_unchecked(Box::into_raw(out)) }
    }

    pub fn p_reset(&self, mut version: usize) -> usize {
        let consumed = Cursor::from_raw(self.consumed.load(Ordering::Acquire));
        let len = self.len();

        crate::ubq_log!(
            tag: "reset.attempt",
            "block={:p} consumed={} len={}",
            self as *const Self,
            consumed,
            len
        );

        version = version.max(consumed.vsn() + 1);

        if consumed.off() >= len {
            for push_atomic in [&self.allocated, &self.committed] {
                push_atomic.fetch_max(
                    Cursor::for_version(version).into_raw(),
                    Ordering::SeqCst, /* Relaxed */
                );
            }

            fence(Ordering::SeqCst);

            crate::ubq_log!(
                tag: "reset.success",
                "block={:p} consumed={} len={}",
                self as *const Self,
                consumed,
                len
            );

            version
        } else {
            crate::ubq_log!(
                tag: "reset.skip",
                "block={:p} consumed={} len={}",
                self as *const Self,
                consumed,
                len
            );
            usize::MAX
        }
    }

    pub fn c_reset(&self, version: usize) -> usize {
        let allocated = Cursor::from_raw(self.allocated.load(Ordering::Acquire));
        let version = version.max(allocated.vsn());

        for pop_atomic in [&self.reserved, &self.committed] {
            pop_atomic.fetch_max(
                Cursor::for_version(version).into_raw(),
                Ordering::SeqCst, /* Relaxed */
            );
        }

        version
    }

    pub const fn len(&self) -> usize {
        self.array.len()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::debug;
    use std::path::PathBuf;
    use std::sync::{
        Arc, Barrier, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
        mpsc,
    };
    use std::thread;

    struct LogGuard {
        tx: Option<mpsc::Sender<Vec<debug::LogEntry>>>,
        out_dir: Option<PathBuf>,
    }

    impl LogGuard {
        fn new(tx: mpsc::Sender<Vec<debug::LogEntry>>, out_dir: Option<PathBuf>) -> Self {
            Self {
                tx: Some(tx),
                out_dir,
            }
        }
    }

    impl Drop for LogGuard {
        fn drop(&mut self) {
            let entries = debug::take();
            if let Some(dir) = &self.out_dir {
                let _ = debug::write_to_dir(&entries, dir);
            }
            if let Some(tx) = self.tx.take() {
                let _ = tx.send(entries);
            }
        }
    }

    #[allow(dead_code)]
    struct TraceState<T> {
        allocated: usize,
        committed: usize,
        reserved: usize,
        consumed: usize,
        next: *mut Block<T>,
        phead: NonNull<Block<T>>,
        phead_vsn: usize,
        chead: NonNull<Block<T>>,
        chead_vsn: usize,
    }

    fn trace_state<T>(queue: &UBQ<T>, block: NonNull<Block<T>>) -> TraceState<T> {
        let block_ref = unsafe { block.as_ref() };

        let (phead_vsn, phead) = queue.phead.load();
        let (chead_vsn, chead) = queue.chead.load();

        TraceState {
            allocated: block_ref.allocated.load(Ordering::Relaxed),
            committed: block_ref.committed.load(Ordering::Relaxed),
            reserved: block_ref.reserved.load(Ordering::Relaxed),
            consumed: block_ref.consumed.load(Ordering::Relaxed),
            next: block_ref.next.load(Ordering::Relaxed),
            phead,
            phead_vsn,
            chead,
            chead_vsn,
        }
    }

    #[allow(unused_variables)]
    fn log_trace_push(queue: &UBQ<u64>, value: u64, block: NonNull<Block<u64>>, slot: usize) {
        let state = trace_state(queue, block);
        crate::ubq_log!(
            tag: "test.pop",
            "value={value} block={block:p} slot={slot} alloc={} committed={} reserved={} consumed={} next={:p} phead={:p} vsn={} chead={:p} vsn={}",
            state.allocated,
            state.committed,
            state.reserved,
            state.consumed,
            state.next,
            state.phead.as_ptr(),
            state.phead_vsn,
            state.chead.as_ptr(),
            state.chead_vsn
        );
    }

    #[allow(unused_variables)]
    fn log_trace_pop(queue: &UBQ<u64>, value: u64, block: NonNull<Block<u64>>, slot: usize) {
        let state = trace_state(queue, block);
        crate::ubq_log!(
            tag: "test.pop",
            "value={value} block={block:p} slot={slot} alloc={} committed={} reserved={} consumed={} next={:p} phead={:p} vsn={} chead={:p} vsn={}",
            state.allocated,
            state.committed,
            state.reserved,
            state.consumed,
            state.next,
            state.phead.as_ptr(),
            state.phead_vsn,
            state.chead.as_ptr(),
            state.chead_vsn
        );
    }

    fn record_error(
        error_count: &AtomicUsize,
        first_error: &Mutex<Option<String>>,
        message: String,
    ) {
        error_count.fetch_add(1, Ordering::Relaxed);
        let mut slot = first_error.lock().expect("first_error lock");
        if slot.is_none() {
            *slot = Some(message);
        }
    }

    fn dump_logs(logs: &[debug::LogEntry]) {
        if logs.is_empty() {
            eprintln!("ubq debug: no log entries collected");
            return;
        }

        let max_entries = 50_000usize;
        let mut count = 0usize;
        println!("ubq debug: dumping {} entries", logs.len());
        for entry in logs.iter().take(max_entries) {
            println!(
                "[{}] tid={} {:?} {} {}",
                entry.ts_ns, entry.thread_id, entry.thread_label, entry.tag, entry.message
            );
            count += 1;
        }
        if logs.len() > max_entries {
            println!("ubq debug: truncated after {count} entries");
        }
    }

    #[test]
    fn push_pop_1_000_000() {
        let ubq = UBQ::<i32>::new();

        for i in 0..1_000_000 {
            ubq.push(i);
        }

        for i in 0..1_000_000 {
            assert_eq!(ubq.pop(), Some(i));
        }
    }

    /*
    UBQ_DEBUG_TAGS=test.,pop.goto_next,push.goto_next,reset.,push.new_block,pop.success,push.success \
    UBQ_DEBUG_SAMPLE=1 \
    UBQ_TEST_PRODUCERS=4 \
    UBQ_TEST_CONSUMERS=4 \
    UBQ_DEBUG_MAX=20000 \
    UBQ_TEST_ITEMS=5000 \
    UBQ_TEST_TRACE_MAX=1024 \
    UBQ_DEBUG_FLUSH_MS=500 \
    UBQ_DEBUG_DIR=ubq_logs \
    cargo test --features ubq_debug mpmc_integrity_smoke -- --nocapture
         */

    #[test]
    fn mpmc_integrity_smoke() {
        const DEFAULT_PRODUCERS: usize = 4;
        const DEFAULT_CONSUMERS: usize = 4;
        const DEFAULT_ITEMS_PER_PRODUCER: usize = 20_000;
        const SENTINEL: u64 = u64::MAX;

        let producers = std::env::var("UBQ_TEST_PRODUCERS")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(DEFAULT_PRODUCERS);
        let consumers = std::env::var("UBQ_TEST_CONSUMERS")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(DEFAULT_CONSUMERS);
        let items_per_producer = std::env::var("UBQ_TEST_ITEMS")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(DEFAULT_ITEMS_PER_PRODUCER);
        let trace_max = std::env::var("UBQ_TEST_TRACE_MAX")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(0);

        let total = producers * items_per_producer;
        let queue = Arc::new(UBQ::<u64>::new());
        let seen = Arc::new(
            (0..total)
                .map(|_| AtomicBool::new(false))
                .collect::<Vec<_>>(),
        );

        let (log_tx, log_rx) = mpsc::channel::<Vec<debug::LogEntry>>();
        let log_dir = std::env::var_os("UBQ_DEBUG_DIR").map(PathBuf::from);
        let _stdout_guard = if let Some(dir) = &log_dir {
            if let Err(err) = debug::prepare_log_dir(dir) {
                eprintln!(
                    "ubq debug: failed to prepare log dir {}: {err}",
                    dir.display()
                );
            }
            match debug::capture_stdout(dir) {
                Ok(guard) => {
                    debug::install_stdout_panic_hook();
                    Some(guard)
                }
                Err(err) => {
                    eprintln!(
                        "ubq debug: failed to capture stdout in {}: {err}",
                        dir.display()
                    );
                    None
                }
            }
        } else {
            None
        };
        let _stderr_guard = if let Some(dir) = &log_dir {
            match debug::capture_stderr(dir) {
                Ok(guard) => {
                    debug::install_stdout_panic_hook();
                    Some(guard)
                }
                Err(err) => {
                    eprintln!(
                        "ubq debug: failed to capture stderr in {}: {err}",
                        dir.display()
                    );
                    None
                }
            }
        } else {
            None
        };
        let error_count = Arc::new(AtomicUsize::new(0));
        let first_error = Arc::new(Mutex::new(None::<String>));
        let start = Arc::new(Barrier::new(producers + consumers + 1));

        let mut producer_handles = Vec::with_capacity(producers);
        for producer_id in 0..producers {
            let queue = queue.clone();
            let start = start.clone();
            let log_tx = log_tx.clone();
            let log_dir = log_dir.clone();
            let trace_max = trace_max;
            let items_per_producer = items_per_producer;
            producer_handles.push(thread::spawn(move || {
                debug::set_thread_label(format!("producer-{producer_id}"));
                let _guard = LogGuard::new(log_tx, log_dir);
                start.wait();
                let base = producer_id * items_per_producer;
                for offset in 0..items_per_producer {
                    let value = (base + offset) as u64;
                    if trace_max != 0 && value < trace_max {
                        let (block, slot) = queue.push_with_block(value);
                        log_trace_push(&queue, value, block, slot);
                    } else {
                        queue.push(value);
                    }
                }
            }));
        }

        let mut consumer_handles = Vec::with_capacity(consumers);
        for consumer_id in 0..consumers {
            let queue = queue.clone();
            let start = start.clone();
            let seen = seen.clone();
            let log_tx = log_tx.clone();
            let log_dir = log_dir.clone();
            let error_count = error_count.clone();
            let first_error = first_error.clone();
            let trace_max = trace_max;
            consumer_handles.push(thread::spawn(move || {
                debug::set_thread_label(format!("consumer-{consumer_id}"));
                let _guard = LogGuard::new(log_tx, log_dir);
                start.wait();
                loop {
                    let (value_opt, block, slot_opt) = if trace_max != 0 {
                        queue.pop_with_block()
                    } else {
                        (queue.pop(), NonNull::dangling(), None)
                    };
                    if let Some(value) = value_opt {
                        if value == SENTINEL {
                            break;
                        }
                        if trace_max != 0 && value < trace_max {
                            if let Some(slot) = slot_opt {
                                log_trace_pop(&queue, value, block, slot);
                            } else {
                                crate::ubq_log!(
                                    tag: "test.pop",
                                    "value={value} block={block:p} slot=?"
                                );
                            }
                        }
                        let idx = value as usize;
                        if idx >= seen.len() {
                            record_error(
                                &error_count,
                                &first_error,
                                format!("out-of-range value {value}"),
                            );
                            continue;
                        }
                        let was_set = seen[idx].swap(true, Ordering::AcqRel);
                        if was_set {
                            record_error(
                                &error_count,
                                &first_error,
                                format!("duplicate value {value}"),
                            );
                        }
                    }
                }
            }));
        }

        start.wait();

        let mut had_panic = false;
        for handle in producer_handles {
            if handle.join().is_err() {
                had_panic = true;
            }
        }

        for _ in 0..consumers {
            queue.push(SENTINEL);
        }

        for handle in consumer_handles {
            if handle.join().is_err() {
                had_panic = true;
            }
        }

        drop(log_tx);
        let mut logs: Vec<debug::LogEntry> = Vec::new();
        for mut chunk in log_rx {
            logs.append(&mut chunk);
        }
        logs.append(&mut debug::take());
        logs.sort_by_key(|entry| entry.ts_ns);

        let mut missing_count = 0usize;
        let mut missing_samples = Vec::new();
        for (idx, flag) in seen.iter().enumerate() {
            if !flag.load(Ordering::Acquire) {
                missing_count += 1;
                if missing_samples.len() < 10 {
                    missing_samples.push(idx);
                }
            }
        }

        let errors = error_count.load(Ordering::Relaxed);
        let dump_always = std::env::var_os("UBQ_DEBUG_DUMP").is_some();
        let has_failure = had_panic || errors > 0 || missing_count > 0;
        if has_failure || dump_always {
            dump_logs(&logs);
        }

        if had_panic {
            panic!("worker thread panicked");
        }
        if errors > 0 {
            let first = first_error.lock().expect("first_error lock");
            let message = first.as_deref().unwrap_or("unknown consumer error");
            panic!("consumer errors: {errors} (first: {message})");
        }
        if missing_count > 0 {
            panic!(
                "missing values: {missing_count} (samples: {:?})",
                missing_samples
            );
        }
    }

    #[test]
    fn block_reset_success() {
        // let block = Block::<u64>::new_zero();
        // let len = block.len();
        // assert!(len > 0);

        // block.allocated.store(len, Ordering::Relaxed);
        // block.committed.store(len, Ordering::Relaxed);
        // block.reserved.store(len, Ordering::Relaxed);
        // block.consumed.store(len, Ordering::Relaxed);

        // assert!(block.reset());
        // assert_eq!(block.allocated.load(Ordering::Relaxed), 0);
        // assert_eq!(block.committed.load(Ordering::Relaxed), 0);
        // assert_eq!(block.reserved.load(Ordering::Relaxed), 0);
        // assert_eq!(block.consumed.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn block_reset_skips_when_consumed_low() {
        // let block = Block::<u64>::new_zero();
        // let len = block.len();
        // assert!(len > 0);

        // block.allocated.store(len, Ordering::Relaxed);
        // block.committed.store(len, Ordering::Relaxed);
        // block
        //     .reserved
        //     .store(len - 1, Ordering::Relaxed);
        // block
        //     .consumed
        //     .store(len - 1, Ordering::Relaxed);

        // assert!(!block.reset());
        // assert_eq!(block.allocated.load(Ordering::Relaxed), len);
        // assert_eq!(block.committed.load(Ordering::Relaxed), len);
        // assert_eq!(block.reserved.load(Ordering::Relaxed), len - 1);
        // assert_eq!(block.consumed.load(Ordering::Relaxed), len - 1);
    }
}
