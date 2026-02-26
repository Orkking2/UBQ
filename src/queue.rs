use crate::{
    BLOCK_LENGTH,
    block::{Block, DESTROY, READ, WRITE},
    util::{HIGHEST_BIT, INCR, acquire_nonnull},
};
use crossbeam_utils::{Backoff, CachePadded};
use std::{
    marker::PhantomData,
    mem::{ManuallyDrop, MaybeUninit},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

/// A lock-free, unbounded multi-producer/multi-consumer (MPMC) queue.
///
/// See the [crate-level documentation](crate) for an overview and quick-start
/// example. `UBQ<T>` itself is not clonable; share it with
/// [`Arc<UBQ<T>>`](std::sync::Arc).
pub struct UBQ<T> {
    /// Atomic pointer to phead: the block currently accepting producer pushes.
    pub(crate) phead: CachePadded<AtomicUsize>,
    /// Atomic pointer to chead: the block currently being drained by consumers.
    pub(crate) chead: CachePadded<AtomicUsize>,
    _marker: PhantomData<T>,
}

const MASK: usize = 255;

struct Head<T> {
    block: *mut Block<T>,
    index: usize,
}

impl<T> Copy for Head<T> {}
impl<T> Clone for Head<T> {
    fn clone(&self) -> Self {
        Self {
            block: self.block.clone(),
            index: self.index.clone(),
        }
    }
}

impl<T> Head<T> {
    fn new(u: usize) -> Self {
        Self {
            block: (u & !MASK) as *mut Block<T>,
            index: u & MASK,
        }
    }

    fn is_zero(&self) -> bool {
        self.index == 0 && self.block.is_null()
    }

    fn pack(self) -> usize {
        self.block.addr() | self.index
    }
}

// SAFETY: Slot ownership is assigned with atomic counters, and producer/consumer
// commits are synchronized with Release/Acquire ordering before cross-thread reads.
unsafe impl<T: Sync> Sync for UBQ<T> {}
unsafe impl<T: Send> Send for UBQ<T> {}

impl<T> UBQ<T> {
    /// Creates a new, empty queue.
    ///
    /// No blocks are allocated until the first call to [`push`](Self::push).
    #[inline]
    pub fn new() -> Self {
        Self {
            phead: CachePadded::new(AtomicUsize::new(0)),
            chead: CachePadded::new(AtomicUsize::new(0)),
            _marker: PhantomData,
        }
    }

    /// Creates a new queue, like [`new`](Self::new), but using [`Arc::new_zeroed`].
    pub fn new_arc() -> Arc<Self> {
        unsafe { Arc::new_zeroed().assume_init() }
    }

    // /// Returns `true` if this UBQ contains no values.
    // pub fn is_empty(&self) -> bool {
    //     todo!()
    // }

    /// Pushes `e` onto the back of the queue.
    #[doc(alias = "enqueue")]
    #[doc(alias = "send")]
    pub fn push(&self, e: T) {
        let backoff = Backoff::new();
        let mut phead = Head::new(0);
        let mut block = None;

        // This is the only time the ptr part of phead is invalid.
        if self.phead.load(Ordering::Acquire) == 0 {
            let ptr = Box::into_raw(Block::new_zeroed());

            match self.phead.compare_exchange(
                0,
                ptr.addr() + 1,
                Ordering::Release,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    self.chead.store(ptr.addr(), Ordering::Release);
                    phead = Head {
                        index: 0,
                        block: ptr,
                    };
                }
                Err(_) => block = Some(unsafe { Box::from_raw(ptr) }),
            }
        }

        if phead.is_zero() {
            loop {
                if Head::<T>::new(self.phead.load(Ordering::Acquire)).index >= BLOCK_LENGTH {
                    backoff.snooze();
                    continue;
                }

                phead = Head::new(self.phead.fetch_add(1, Ordering::Acquire));

                if phead.index >= BLOCK_LENGTH {
                    backoff.snooze();
                    continue;
                }

                break;
            }
        }

        if phead.index + 1 == BLOCK_LENGTH {
            let new = Box::into_raw(block.take().unwrap_or_else(Block::new_zeroed));

            unsafe { (*phead.block).next.store(new, Ordering::Release) };
            self.phead.store(new.addr(), Ordering::Release);
        }

        let slot = unsafe { (*phead.block).array.get_unchecked(phead.index) };
        unsafe { slot.value.get().write(MaybeUninit::new(e)) };

        slot.state.store(WRITE, Ordering::Release);
    }

    /// Removes and returns the front element, or [`None`] if the queue is empty.
    #[doc(alias = "dequeue")]
    #[doc(alias = "recv")]
    pub fn pop(&self) -> Option<T> {
        let backoff = Backoff::new();

        // Cheap hint if queue is empty.
        if self.chead.load(Ordering::Relaxed) == 0 {
            return None;
        }

        let mut chead = Head::new(self.chead.load(Ordering::Acquire));

        loop {
            if chead.index >> 1 >= BLOCK_LENGTH {
                backoff.snooze();
                chead = Head::new(self.chead.load(Ordering::Acquire));
                continue;
            }

            let mut new_index = chead.index + 2;

            if chead.index & 1 == 0 {
                let phead = Head::<T>::new(self.phead.load(Ordering::Acquire));

                if phead.block.addr() == chead.block.addr() {
                    if chead.index >> 1 >= phead.index {
                        return None;
                    }
                } else {
                    new_index |= 1;
                }
            }

            let new_chead = Head {
                block: chead.block,
                index: new_index,
            };

            match self.chead.compare_exchange_weak(
                chead.pack(),
                new_chead.pack(),
                Ordering::Acquire,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(head) => chead = Head::new(head),
            }

            backoff.spin();
        }

        chead.index >>= 1;

        if chead.index + 1 == BLOCK_LENGTH {
            self.chead.store(
                acquire_nonnull(unsafe { &(*chead.block).next }).addr(),
                Ordering::Release,
            );
        }

        let array = unsafe { &(*chead.block).array };

        let slot = unsafe { array.get_unchecked(chead.index) };
        slot.await_write();

        let e = unsafe { slot.value.get().read().assume_init() };

        if chead.index + 1 == BLOCK_LENGTH
            || slot.state.fetch_or(READ, Ordering::AcqRel) & DESTROY != 0
        {
            let mut free = true;

            for i in (chead.index + 1) % BLOCK_LENGTH..BLOCK_LENGTH - 1 {
                let slot = unsafe { array.get_unchecked(i) };

                if slot.state.load(Ordering::Acquire) & READ == 0
                    && slot.state.fetch_or(DESTROY, Ordering::AcqRel) & READ == 0
                {
                    free = false;
                    break;
                }
            }

            if free {
                let _ = unsafe { Box::from_raw(chead.block.cast::<ManuallyDrop<Block<T>>>()) };
            }
        }

        Some(e)
    }
}

impl<T> Drop for UBQ<T> {
    fn drop(&mut self) {
        let mut p = Head::<T>::new(*self.chead.get_mut()).block;

        while !p.is_null() {
            let mut b = unsafe { Box::from_raw(p) };
            p = *b.next.get_mut();
        }
    }
}
