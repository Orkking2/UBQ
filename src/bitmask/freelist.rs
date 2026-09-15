//! A two-state cache of empty blocks: null, or a chain taken whole.
//!
//! The original v8 freelist has a third state, a busy token that a taker
//! holds while it splits the chain; a return that observes it drops its
//! block on the floor. Here a taker swaps the whole chain out in one step,
//! so returns only ever race with other returns, and no block is freed
//! while the queue lives (except through `flush`).

use super::block::{Block, BlockList, OwnedBlock};
#[cfg(test)]
use super::block::Geometry;
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
    pub(super) const fn new() -> Self {
        Self {
            head: AtomicPtr::new(ptr::null_mut()),
        }
    }

    /// Detach every cached block. The swap leaves the cache empty, so a
    /// return that races with it starts a new chain instead of waiting.
    pub(super) fn take_all(&self) -> BlockList<T> {
        let head = self.head.swap(ptr::null_mut(), Acquire);
        // SAFETY: the swap transfers exclusive ownership of the whole chain.
        unsafe { BlockList::from_raw(head) }
    }

    /// Return a detached chain. Its tail is relinked to the current head;
    /// the CAS only retries against other returns and a concurrent take.
    pub(super) fn give(&self, list: BlockList<T>) {
        if list.len() == 0 {
            return;
        }
        let (first, last, _) = list.into_raw_parts();
        let mut head = self.head.load(Relaxed);
        loop {
            // SAFETY: the chain is exclusively owned until the CAS succeeds,
            // and `head` is never dereferenced.
            unsafe { (*last).next.store(head, Relaxed) };
            match self
                .head
                .compare_exchange_weak(head, first, Release, Relaxed)
            {
                Ok(_) => return,
                Err(actual) => head = actual,
            }
        }
    }

    /// Return a fully consumed (or unused) detached block for reuse.
    pub(super) fn recycle(&self, mut block: OwnedBlock<T>) {
        block.reset();
        let mut list = BlockList::new();
        list.push(block);
        self.give(list);
    }

    /// Free every cached block and return how many. Concurrent returns may
    /// refill the cache; live queue blocks are never touched.
    pub(super) fn flush(&self) -> usize {
        let blocks = self.take_all();
        let freed = blocks.len();
        drop(blocks);
        freed
    }
}

impl<T> Drop for FreeList<T> {
    fn drop(&mut self) {
        let head = *self.head.get_mut();
        // SAFETY: exclusive access excludes all cache operations.
        drop(unsafe { BlockList::from_raw(head) });
    }
}

#[cfg(test)]
#[path = "tests/freelist.rs"]
mod tests;
