use portable_atomic::{AtomicU64, AtomicU128};

// One 128-bit atomic allocation with views over both the whole value and its
/// numeric low 64 bits.
///
/// This exists only for the mixed-width benchmark. Rust's atomic memory model
/// does not permit concurrent overlapping atomic accesses of different sizes,
/// even when the target hardware provides coherent instructions for both.
#[repr(transparent)]
pub(crate) struct AtomicInt {
    inner: AtomicU128,
}

impl AtomicInt {
    pub(crate) fn new(value: u128) -> Self {
        Self {
            inner: AtomicU128::new(value),
        }
    }

    #[inline]
    pub(crate) fn as_u128(&self) -> &AtomicU128 {
        &self.inner
    }

    #[inline]
    pub(crate) fn as_u64(&self) -> &AtomicU64 {
        let first_word = self.inner.as_ptr().cast();

        #[cfg(target_endian = "little")]
        let low_word = first_word;
        #[cfg(target_endian = "big")]
        let low_word = unsafe { first_word.add(1) };

        // SAFETY: AtomicU128 is 16-byte aligned and owns writable storage for
        // the returned reference's lifetime. Selecting the numeric low word is
        // endian-aware. The caller deliberately performs mixed-size atomic
        // accesses, which is outside Rust's supported memory model; this type
        // is confined to the explicitly caveated benchmark.
        unsafe { AtomicU64::from_ptr(low_word) }
    }
}
