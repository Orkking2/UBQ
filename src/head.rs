use crate::Block;
use crossbeam_utils::CachePadded;
use std::{
    ptr::NonNull,
    sync::atomic::{AtomicPtr, AtomicUsize, Ordering},
};

pub struct Head<T> {
    version: NonNull<CachePadded<AtomicUsize>>,
    block: NonNull<CachePadded<AtomicPtr<Block<T>>>>,
}

impl<T> Clone for Head<T> {
    fn clone(&self) -> Self {
        Self {
            version: self.version.clone(),
            block: self.block.clone(),
        }
    }
}

impl<T> Head<T> {
    pub fn new(root: NonNull<Block<T>>) -> Self {
        Self {
            version: unsafe {
                NonNull::new_unchecked(Box::into_raw(Box::new(CachePadded::new(AtomicUsize::new(
                    0,
                )))))
            },
            block: unsafe {
                NonNull::new_unchecked(Box::into_raw(Box::new(CachePadded::new(AtomicPtr::new(
                    root.as_ptr(),
                )))))
            },
        }
    }

    pub fn load(&self) -> (usize, NonNull<Block<T>>) {
        let version = unsafe { self.version.as_ref() }.load(Ordering::Acquire);
        let block = unsafe { self.block.as_ref() }.load(Ordering::Relaxed);

        (
            version,
            // SAFETY: self.atomic is created by NonNull::from(Box::into_raw)
            unsafe { NonNull::new_unchecked(block) },
        )
    }

    pub fn store(&self, version: usize, next: NonNull<Block<T>>) {
        // Publish block first, then publish version.
        // SAFETY: self.atomic is created by NonNull::from(Box::into_raw)
        unsafe { self.block.as_ref() }.store(next.as_ptr(), Ordering::Relaxed);
        unsafe { self.version.as_ref() }.fetch_max(version, Ordering::Release);
    }

    /// Deallocate self.atomic
    pub unsafe fn destroy(&mut self) {
        // SAFETY: self.atomic is created by NonNull::from(Box::into_raw)
        drop(unsafe { Box::from_raw(self.block.as_ptr()) });
        drop(unsafe { Box::from_raw(self.version.as_ptr()) });
    }
}
