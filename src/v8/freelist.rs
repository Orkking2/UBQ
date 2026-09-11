use super::block::{Block, BlockList, Geometry, OwnedBlock};
use core::{
    ptr,
    sync::atomic::{
        AtomicPtr,
        Ordering::{Acquire, Relaxed, Release},
    },
};

pub(super) struct FreeList<T> {
    head: AtomicPtr<Block<T>>,
}

impl<T> FreeList<T> {
    // An address token only: never dereferenced. Block alignment excludes MAX.
    fn busy() -> *mut Block<T> {
        ptr::without_provenance_mut(usize::MAX)
    }

    pub(super) const fn new() -> Self {
        Self {
            head: AtomicPtr::new(ptr::null_mut()),
        }
    }

    /// Detach up to `limit` cached blocks. A busy producer causes an immediate
    /// miss, so allocation can proceed independently of its progress.
    pub(super) fn take(&self, limit: usize) -> BlockList<T> {
        if limit == 0 {
            return BlockList::new();
        }

        let mut head = self.head.load(Acquire);
        loop {
            if head.is_null() || head == Self::busy() {
                return BlockList::new();
            }

            match self
                .head
                .compare_exchange_weak(head, Self::busy(), Relaxed, Acquire)
            {
                Ok(_) => break,
                Err(actual) => head = actual,
            }
        }

        // SAFETY: BUSY grants exclusive ownership. No pointer is dereferenced
        // before the CAS, so address reuse between load and CAS is harmless.
        let mut rest = head;
        let mut tail = ptr::null_mut();
        let mut len = 0;
        while len < limit && !rest.is_null() {
            tail = rest;
            rest = unsafe { *(*rest).next.get_mut() };
            len += 1;
        }

        // SAFETY: a successful CAS detached a nonempty chain. Only the prefix
        // was traversed; sever its last link before publishing the suffix.
        unsafe { *(*tail).next.get_mut() = ptr::null_mut() };
        self.head.store(rest, Release);
        unsafe { BlockList::from_raw_parts(head, tail, len) }
    }

    /// Return a fully consumed (or unused) detached block. If a producer owns
    /// the freelist, free immediately. CAS races with other returns may retry.
    pub(super) fn recycle(&self, mut block: OwnedBlock<T>) {
        block.reset();

        let mut head = self.head.load(Relaxed);
        loop {
            if head == Self::busy() {
                // OwnedBlock drops here.
                return;
            }

            // SAFETY: block is exclusively owned until the successful CAS.
            unsafe { (*block.as_ptr()).next.store(head, Relaxed) };
            match self
                .head
                .compare_exchange_weak(head, block.as_ptr(), Release, Relaxed)
            {
                Ok(_) => {
                    block.forget();
                    return;
                }
                Err(actual) => head = actual,
            }
        }
    }

    /// Recheck the cache between allocations, as consumers may return blocks
    /// while the allocator is working.
    pub(super) fn acquire(&self, count: usize, geometry: Geometry) -> BlockList<T> {
        let mut blocks = BlockList::new();
        let mut remaining = count;

        while remaining != 0 {
            let cached = self.take(remaining);
            remaining -= cached.len();
            blocks += cached;

            if remaining != 0 {
                blocks.push(OwnedBlock::new(geometry));
                remaining -= 1;
            }
        }

        blocks
    }

    /// Best effort under concurrency: new returns may refill the cache, and a
    /// producer holding BUSY may retain its detached list. Never flush live data.
    pub(super) fn flush(&self) -> usize {
        let mut blocks = self.take(usize::MAX);
        let mut freed = 0;

        while let Some(block) = blocks.pop() {
            drop(block);
            freed += 1;
        }

        freed
    }
}

impl<T> Drop for FreeList<T> {
    fn drop(&mut self) {
        let head = *self.head.get_mut();
        debug_assert_ne!(head, Self::busy());

        // SAFETY: exclusive access excludes all cache operations.
        drop(unsafe { BlockList::from_raw(head) });
    }
}

#[cfg(test)]
#[path = "tests/freelist.rs"]
mod tests;
