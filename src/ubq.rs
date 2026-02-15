use crate::{
    cursor::{AtomicCursor, Cursor},
    head::Head,
};
use crossbeam_utils::{Backoff, CachePadded};
use std::{
    alloc::{Layout, alloc_zeroed, handle_alloc_error},
    cell::UnsafeCell,
    fmt::{Debug, Write},
    mem::MaybeUninit,
    num::NonZeroUsize,
    ptr::NonNull,
    sync::atomic::{AtomicPtr, AtomicUsize, Ordering},
};

/// The maximum number of threads that can increment allocated
/// in a particular block at one time.
pub(crate) const OFFSET_PAD: usize = 32;

pub struct UBQ<T> {
    copies: NonNull<AtomicUsize>,

    phead: Head<T>,
    chead: Head<T>,
}

impl<T> Debug for UBQ<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let original = self.phead.load().1;
        let chead = self.chead.load().1;

        let mut out_str = String::new();
        writeln!(out_str, "\tphead: {:p}", original)?;
        writeln!(out_str, "\tchead: {:p}", chead)?;

        let mut current = unsafe { original.as_ref() };
        writeln!(out_str, "\tblocks: [")?;
        loop {
            let allocated = current.allocated.load(Ordering::Relaxed);
            let committed = current.committed.load(Ordering::Relaxed);
            let reserved = current.reserved.load(Ordering::Relaxed);
            let consumed = current.consumed.load(Ordering::Relaxed);

            writeln!(
                out_str,
                "\t\t{:p} ACRC {allocated}, {committed}, {reserved}, {consumed}",
                current as *const Block<T>
            )?;

            let next = unsafe { NonNull::new_unchecked(current.next.load(Ordering::Relaxed)) };

            if next == original {
                break;
            }

            current = unsafe { next.as_ref() }
        }
        writeln!(out_str, "\t]")?;

        write!(f, "UBQ {{\n{out_str}}}")
    }
}

impl<T> Drop for UBQ<T> {
    fn drop(&mut self) {
        // SAFETY: self.copies is created by Box::into_raw
        let left = unsafe { self.copies.as_ref() }.fetch_sub(1, Ordering::Relaxed);

        if left == 1 {
            // We are the last UBQ being dropped
            let (_, original) = self.phead.load();

            unsafe { self.phead.destroy() };
            unsafe { self.chead.destroy() };

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
    Goto {
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
    Goto {
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
                PushState::Reload { e } => {
                    e_opt = Some(e);

                    '_inner: loop {
                        let (new_version, new_phead) = self.phead.load();

                        if version != new_version || phead != new_phead {
                            (version, phead) = (new_version, new_phead);
                            break;
                        }

                        backoff.snooze();
                    }

                    continue;
                }
                PushState::Goto {
                    version: new_vsn,
                    next,
                    ..
                } => {
                    self.phead.store(new_vsn, version, next);
                    return;
                }
                PushState::Success { .. } => return,
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
                PushState::Reload { e } => {
                    e_opt = Some(e);

                    '_inner: loop {
                        let (new_version, new_phead) = self.phead.load();

                        if version != new_version || phead != new_phead {
                            (version, phead) = (new_version, new_phead);
                            break;
                        }

                        backoff.snooze();
                    }

                    continue;
                }
                PushState::Goto {
                    version: new_vsn,
                    next,
                    slot,
                } => {
                    self.phead.store(new_vsn, version, next);
                    return (phead, slot);
                }
                PushState::Success { slot } => return (phead, slot),
            }
        }
    }

    fn push_to(this: NonNull<Block<T>>, version: usize, e: T) -> PushState<T> {
        // SAFETY: `this` is always created by Box::into_raw
        let this_ref = unsafe { this.as_ref() };
        let allocated = this_ref.allocated.load(Ordering::Relaxed);

        crate::log!(
            tag: "push.enter",
            "block={:p} allocated={} version={}",
            this,
            allocated,
            version,
        );

        if allocated.vsn() != version {
            crate::log!(
                tag: "push.reload_stale",
                "block={:p} allocated={} local_version={} reason=stale_version",
                this,
                allocated,
                version,
            );

            PushState::Reload { e }
        } else if allocated.off() >= BLOCK_CAP {
            crate::log!(
                tag: "push.await_full",
                "block={:p} allocated={} len={} version={} reason=full_block",
                this,
                allocated,
                BLOCK_CAP,
                version,
            );

            PushState::Reload { e }
        } else {
            let old_allocated = this_ref.allocated.fetch_add(1, Ordering::Relaxed);

            crate::log!(
                tag: "push.allocate",
                "block={:p} slot={} len={}",
                this,
                old_allocated,
                BLOCK_CAP
            );

            if old_allocated.off() >= BLOCK_CAP {
                crate::log!(
                    tag: "push.over_alloc",
                    "block={:p} slot={} len={}",
                    this,
                    old_allocated.off(),
                    BLOCK_CAP
                );

                PushState::Reload { e }
            } else {
                // SAFETY: We reserved this slot when we fetch_add'ed earlier.
                unsafe {
                    this_ref.array[old_allocated.off()]
                        .get()
                        .write(MaybeUninit::new(e))
                };
                // Ordering: We need the write above to be visible to any consumers,
                // so we need at least a Release ordering here.
                this_ref.committed.fetch_add(1, Ordering::Release);
                crate::log!(
                    tag: "push.write",
                    "block={:p} slot={}",
                    this,
                    old_allocated.off(),
                );

                // Note: We are already synchronized at this point, so we do
                // not have to worry about our fellow producers contending to
                // reset next.
                if old_allocated.off() + 1 == BLOCK_CAP {
                    if let Some(next) = NonNull::new(this_ref.next.load(Ordering::Acquire)) {
                        if let Some(version) = unsafe { next.as_ref() }.reset_p(version) {
                            PushState::Goto {
                                next,
                                version,
                                #[cfg(test)]
                                slot: old_allocated.off(),
                            }
                        } else {
                            let new = Block::new_with_version(next, version);

                            this_ref.next.store(new.as_ptr(), Ordering::Release);

                            PushState::Goto {
                                next: new,
                                version,
                                #[cfg(test)]
                                slot: old_allocated.off(),
                            }
                        }
                    } else {
                        // Note: When we have just one block, it will not point anywhere. We also
                        // always expect to have >=2 blocks, so the null case always triggers the
                        // first time around, in which we allocate a new block every time.
                        let new = Block::new(this);

                        this_ref.next.store(new.as_ptr(), Ordering::Release);

                        PushState::Goto {
                            next: new,
                            version,
                            #[cfg(test)]
                            slot: old_allocated.off(),
                        }
                    }
                } else {
                    crate::log!(
                        tag: "push.success",
                        "block={:p} slot={}",
                        this,
                        old_allocated.off(),
                    );
                    // We have succeeded, return.
                    PushState::Success {
                        #[cfg(test)]
                        slot: old_allocated.off(),
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
                PopState::Reload => {
                    '_inner: loop {
                        let (new_version, new_chead) = self.chead.load();

                        if version != new_version || chead != new_chead {
                            (version, chead) = (new_version, new_chead);
                            break;
                        }

                        backoff.snooze();
                    }

                    continue;
                }
                PopState::Goto {
                    e,
                    version: new_vsn,
                    next,
                    ..
                } => {
                    self.chead.store(new_vsn, version, next);
                    return Some(e);
                }
                PopState::Success { e, .. } => return Some(e),
                PopState::Empty => return None,
            }
        }
    }

    #[cfg(test)]
    fn pop_with_block(&self) -> (Option<T>, NonNull<Block<T>>, Option<usize>) {
        let (mut version, mut chead) = self.chead.load();
        let backoff = Backoff::new();

        '_outer: loop {
            match Self::pop_from(chead, version) {
                PopState::Reload => {
                    '_inner: loop {
                        let (new_version, new_chead) = self.chead.load();

                        if version != new_version || chead != new_chead {
                            (version, chead) = (new_version, new_chead);
                            break;
                        }

                        backoff.snooze();
                    }

                    continue;
                }
                PopState::Goto {
                    e,
                    version: new_vsn,
                    next,
                    slot,
                } => {
                    self.chead.store(new_vsn, version, next);
                    return (Some(e), chead, Some(slot));
                }
                PopState::Success { e, slot } => return (Some(e), chead, Some(slot)),
                PopState::Empty => return (None, chead, None),
            }
        }
    }

    fn pop_from(this: NonNull<Block<T>>, version: usize) -> PopState<T> {
        let backoff = Backoff::new();

        crate::log!(
            tag: "pop.enter",
            "block={:p} version={}",
            this,
            version,
        );

        loop {
            // SAFETY: `this` is always created from a NonNull::from_ref or Box::into_raw
            let this_ref = unsafe { this.as_ref() };

            let reserved = this_ref.reserved.load(Ordering::Relaxed);

            if reserved.vsn() > version {
                return PopState::Reload;
            }

            if reserved.off() < BLOCK_CAP {
                // Ordering: committed is the signal for finished writes, we need
                // those writes to be visible when we read it later (if we read it).
                let committed = this_ref.committed.load(Ordering::Acquire);

                if committed.off() <= reserved.off() {
                    crate::log!(
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
                    let allocated = this_ref.allocated.load(Ordering::Acquire);

                    // Is allocated.off() < committed.off() ever possible?
                    if allocated.off() > /* != */ committed.off() {
                        crate::log!(
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

                if this_ref
                    .reserved
                    .fetch_max(reserved.incr_off(), Ordering::SeqCst) /* Relaxed */
                    == reserved
                {
                    crate::log!(
                        tag: "pop.reserve",
                        "block={this:p} slot={reserved} committed={committed}, BLOCK_CAP={BLOCK_CAP}",
                    );

                    let e = unsafe { this_ref.array[reserved.off()].get().read().assume_init() };
                    let consumed = this_ref.consumed.fetch_add(1, Ordering::Release);

                    if consumed.off() + 1 == BLOCK_CAP {
                        // We don't actually need to print consumed + 1, as we do know for certain they are equal
                        crate::log!(
                            tag: "pop.ilc",
                            "block={this:p}",
                        );

                        loop {
                            let mut next = this_ref.next.load(Ordering::Acquire);

                            if next.is_null() {
                                backoff.snooze();
                                crate::debug::maybe_flush();
                                continue;
                            }

                            crate::log!(
                                tag: "pop.goto_next",
                                "block={:p} next={:p}",
                                this,
                                next
                            );

                            loop {
                                // SAFETY (loop 0):   next is not null
                                // SAFETY (loop >=1): next cannot be updated from !null -> null
                                unsafe {
                                    if let Some(version) =
                                        next.as_ref().unwrap_unchecked().reset_c()
                                    {
                                        return PopState::Goto {
                                            e,
                                            next: NonNull::new_unchecked(next),
                                            version,
                                            #[cfg(test)]
                                            slot: reserved.off(),
                                        };
                                    }
                                }

                                // It is theoretically possible for a producer who is trying to
                                // push a new next to be paused for enough time for us to reach
                                // here as a consumer, so we DO actually have to reload next.
                                next = this_ref.next.load(Ordering::Acquire);
                                backoff.snooze();
                            }
                        }
                    } else {
                        crate::log!(
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
                    crate::log!(
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
            crate::log!(
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
    allocated: CachePadded<AtomicCursor>,
    committed: CachePadded<AtomicCursor>,
    reserved: CachePadded<AtomicCursor>,
    consumed: CachePadded<AtomicCursor>,

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
            allocated == committed || (allocated > committed && committed.into_raw() >= BLOCK_CAP),
            "allocated {allocated} == {committed} committed || (allocated {allocated} < {committed} committed && committed {committed} >= {BLOCK_CAP} BLOCK_CAP)"
        );
        debug_assert!(
            reserved == consumed,
            "reserved {reserved} == {consumed} consumed"
        );

        let consumed = consumed.into_raw();
        let committed = committed.into_raw();

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

        // SAFETY: We have exclusive access to out.next until
        // `out` is stored (with a release)
        unsafe { out.next.as_ptr().write(next.as_ptr()) };

        // SAFETY: into_raw produces a valid nonzero pointer.
        unsafe { NonNull::new_unchecked(Box::into_raw(out)) }
    }

    pub fn new_with_version(next: NonNull<Self>, version: usize) -> NonNull<Self> {
        let cursor = Cursor::for_version(version);

        let new = Box::new(Self {
            allocated: CachePadded::new(AtomicCursor::new(cursor)),
            committed: CachePadded::new(AtomicCursor::new(cursor)),
            reserved: CachePadded::new(AtomicCursor::new(cursor)),
            consumed: CachePadded::new(AtomicCursor::new(cursor)),
            next: CachePadded::new(AtomicPtr::new(next.as_ptr())),
            array: core::array::from_fn(|_| UnsafeCell::new(MaybeUninit::uninit())),
        });

        // SAFETY: into_raw produces a valid nonzero pointer.
        unsafe { NonNull::new_unchecked(Box::into_raw(new)) }
    }
    /*
    Order: alloc, commit, reserve, consume

    [0]
    [[0]] 0:full, 0:full, 0:full, 0:full <- chead(vsn=0)
    [[1]] 0:full, 0:full, 0:0, 0:0, <- phead(vsn=0)

    [1]
    [[0]] 1:0, 1:0, 0:full, 0:full <- phead(vsn=0->1), chead(vsn=0)
    [[1]] 0:full, 0:full, 0:0, 0:0

    [2]
    [[0]] 1:0, 1:0, 0:full, 0:full <- phead(vsn=1)
    [[1]] 0:full, 0:full, 0:0, 0:0 <- chead(vsn=0)

    [2.0]
    [[0]] 1:full, 1:full, 0:full, 0:full
    [[2]] 1:0, 1:0, 0:0, 0:0 <- phead(vsn=1)
    [[1]] 0:full, 0:full, 0:0, 0:0 <- chead(vsn=0)

    [2.1]
    [[0]] 1:full, 1:full, 0:full, 0:full
    [[2]] 1:full, 1:full, 0:0, 0:0 <- phead(vsn=1)
    [[1]] 0:full, 0:full, 0:full, 0:full <- chead(vsn=0)

    [2.2]
    [[0]] 1:full, 1:full, 1:0, 1:0 <- chead(vsn=0->1)
    [[2]] 1:full, 1:full, 0:0, 0:0
    [[1]] 1:0, 1:0, 0:full, 0:full <- phead(vsn=1)

    [3]
    [[0]] 1:full, 1:full, 0:full, 0:full <- phead(vsn=1)
    [[1]] 0:full, 0:full, 0:full, 0:full <- chead(vsn=0)

    [4]


    */

    /// Returns None if we fail to reset (consumers not done)
    pub fn reset_p(&self, mut version: usize) -> Option<usize> {
        // Let us assume that reset_p is not called on fresh blocks.
        // Fresh blocks are constructed for the current phead version.
        //
        // version does eventually roll over (in ~3e4 years at maximum throughput)

        let consumed = self.consumed.load(Ordering::Acquire);

        if consumed.off() < BLOCK_CAP {
            return None;
        }

        let allocated = self.allocated.load(Ordering::Relaxed);

        debug_assert!(
            allocated.off() >= BLOCK_CAP,
            "We should not be calling reset on fresh blocks"
        );

        version = version.max(allocated.vsn() + 1);

        // In case version is going to roll over. It's perfectly acceptable for version
        // to roll over, but we must account for it here in this bigger version
        if version >= Cursor::MAX_VSN {
            version = 0;
        }

        let new = Cursor::for_version(version);

        self.allocated.store(new, Ordering::Relaxed);
        self.committed.store(new, Ordering::Relaxed);

        Some(version)
    }

    /*
    [0]
    [[0]] 0:0, 0:0, 0:0, 0:0 <- phead(vsn=0), chead(vsn=0)

    [1]
    [[0]] 0:full, 0:full, 0:0, 0:0 <- chead(vsn=0)
    [[1]] 0:0, 0:0, 0:0, 0:0 <- phead(vsn=0)

    [2]
    [[0]] 0:full, 0:full, 0:full, 0:full <- chead(vsn=0)
    [[1]] 0:full, 0:full, 0:full, 0:full <-phead(vsn=0)
    */

    #[allow(unused_variables)]
    /// Returns None if the we have not been reset by the consumer yet.
    pub fn reset_c(&self) -> Option<usize> {
        let allocated = self.allocated.load(Ordering::Relaxed);
        let reserved = self.reserved.load(Ordering::Relaxed);

        // reset_c DOES get called on blocks created by Block::new_with_version(alloc.vsn()),
        // for these we take no action (version is reserved.vsn())
        if reserved.off() == 0 {
            Some(reserved.vsn())
        } else if allocated.vsn() == reserved.vsn() {
            None
        } else {
            let version = allocated.vsn();

            let new = Cursor::for_version(version);

            self.reserved.store(new, Ordering::Relaxed);
            self.consumed.store(new, Ordering::Release);

            Some(version)
        }
    }

    // pub const fn len(&self) -> usize {
    //     self.array.len()
    // }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::debug;
    use std::panic::{AssertUnwindSafe, resume_unwind};
    use std::path::PathBuf;
    use std::sync::{
        Arc, Barrier, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
        mpsc::{self, RecvTimeoutError},
    };
    use std::thread;
    use std::time::Duration;

    #[allow(dead_code)]
    struct TraceState<T> {
        allocated: Cursor,
        committed: Cursor,
        reserved: Cursor,
        consumed: Cursor,
        next: *mut Block<T>,
        phead: NonNull<Block<T>>,
        phead_vsn: usize,
        chead: NonNull<Block<T>>,
        chead_vsn: usize,
    }

    const DEFAULT_TEST_TIMEOUT_SECS: u64 = 30;

    fn test_timeout() -> Duration {
        std::env::var("UBQ_TEST_TIMEOUT_SECS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .map(Duration::from_secs)
            .unwrap_or(Duration::from_secs(DEFAULT_TEST_TIMEOUT_SECS))
    }

    fn run_with_timeout(name: &'static str, f: impl FnOnce() + Send + 'static) {
        let timeout = test_timeout();
        let (tx, rx) = mpsc::channel();

        thread::spawn(move || {
            let result = std::panic::catch_unwind(AssertUnwindSafe(f));
            let _ = tx.send(result);
        });

        match rx.recv_timeout(timeout) {
            Ok(Ok(())) => {}
            Ok(Err(payload)) => resume_unwind(payload),
            Err(RecvTimeoutError::Timeout) => {
                panic!("test `{name}` timed out after {}s", timeout.as_secs())
            }
            Err(RecvTimeoutError::Disconnected) => {
                panic!("test `{name}` ended unexpectedly before reporting")
            }
        }
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
        crate::log!(
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
        crate::log!(
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
        run_with_timeout("push_pop_1_000_000", || {
            let ubq = UBQ::<i32>::new();

            for i in 0..1_000_000 {
                ubq.push(i);
            }

            for i in 0..1_000_000 {
                assert_eq!(ubq.pop(), Some(i));
            }
        });
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
    UBQ_DEBUG_DIR=logs \
    cargo test --features ubq_debug mpmc_integrity_smoke -- --nocapture
         */

    #[test]
    fn mpmc_integrity_smoke() {
        run_with_timeout("mpmc_integrity_smoke", || {
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

            let log_dir = std::env::var_os("UBQ_DEBUG_DIR").map(PathBuf::from);
            if let Some(dir) = &log_dir {
                if let Err(err) = debug::prepare_log_dir(dir) {
                    eprintln!(
                        "ubq debug: failed to prepare log dir {}: {err}",
                        dir.display()
                    );
                }
            }
            debug::init();
            let _stdout_guard = if let Some(dir) = &log_dir {
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
            crate::log!(
                tag: "test.start",
                "producers={producers} consumers={consumers} items_per_producer={items_per_producer} trace_max={trace_max}"
            );
            let error_count = Arc::new(AtomicUsize::new(0));
            let produced_count = Arc::new(AtomicUsize::new(0));
            let consumed_count = Arc::new(AtomicUsize::new(0));
            let first_error = Arc::new(Mutex::new(None::<String>));
            let start = Arc::new(Barrier::new(producers + consumers + 1));
            let _progress_log = debug::register(Duration::from_millis(500), {
                let produced_count = Arc::clone(&produced_count);
                let consumed_count = Arc::clone(&consumed_count);
                let error_count = Arc::clone(&error_count);
                move || {
                    let produced = produced_count.load(Ordering::Relaxed);
                    let consumed = consumed_count.load(Ordering::Relaxed);
                    let errors = error_count.load(Ordering::Relaxed);
                    crate::log!(
                        tag: "test.progress",
                        "produced={produced} consumed={consumed} errors={errors}"
                    );
                }
            });

            let _ubq_log = debug::register(Duration::from_millis(50), {
                let queue = queue.clone();
                move || {
                    crate::log!(
                        tag: "test.ubq",
                        "{:?}",
                        &*queue
                    );
                }
            });

            let mut producer_handles = Vec::with_capacity(producers);
            for producer_id in 0..producers {
                let queue = queue.clone();
                let start = start.clone();
                let produced_count = Arc::clone(&produced_count);
                let trace_max = trace_max;
                let items_per_producer = items_per_producer;
                producer_handles.push(thread::spawn(move || {
                    debug::set_thread_label(format!("producer-{producer_id}"));
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
                        produced_count.fetch_add(1, Ordering::Relaxed);
                    }
                }));
            }

            let mut consumer_handles = Vec::with_capacity(consumers);
            for consumer_id in 0..consumers {
                let queue = queue.clone();
                let start = start.clone();
                let seen = seen.clone();
                let error_count = error_count.clone();
                let first_error = first_error.clone();
                let consumed_count = Arc::clone(&consumed_count);
                let trace_max = trace_max;
                consumer_handles.push(thread::spawn(move || {
                    debug::set_thread_label(format!("consumer-{consumer_id}"));
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
                                    crate::log!(
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
                            consumed_count.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }));
            }

            start.wait();

            crate::log!(tag: "test.start_wait", "passed start.wait() on the main thread");

            let mut had_panic = false;

            crate::log!(
                tag: "test.producers",
                "entering the producer handle loop to check for panics"
            );

            for (i, handle) in producer_handles.into_iter().enumerate() {
                crate::log!(tag: "test.producers", "trying to join producer thread {i}");

                let mut loc = false;

                if handle.join().is_err() {
                    had_panic = true;
                    loc = true;
                }

                if loc {
                    crate::log!(tag: "test.producers", "producer thread {i} panic'ed");
                }
            }

            crate::log!(tag: "test.consumers", "pushing sentinels to the queue");

            for _ in 0..consumers {
                queue.push(SENTINEL);
            }

            crate::log!(tag: "test.consumers", "sentinels pushed, attempting to join");

            for (i, handle) in consumer_handles.into_iter().enumerate() {
                crate::log!(tag: "test.consumers", "trying to join consumer thread {i}");

                let mut loc = false;

                if handle.join().is_err() {
                    had_panic = true;
                    loc = true;
                }

                if loc {
                    crate::log!(tag: "test.consumers", "consumer thread {i} panic'ed");
                }
            }

            crate::log!(tag: "test.consumers", "consumer threads joined");

            drop(_progress_log);
            debug::flush();
            let mut logs: Vec<debug::LogEntry> = debug::snapshot();
            logs.sort_by_key(|entry| entry.ts_ns);
            debug::shutdown();

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
        });
    }
}
