use std::fmt::{Debug, Display};
use std::sync::atomic::{AtomicUsize, Ordering};
use crate::{BLOCK_CAP, OFFSET_PAD};

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Cursor {
    inner: usize,
}

impl Display for Cursor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl Debug for Cursor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.vsn(), self.off())
    }
}

const fn ceil_log2(value: usize) -> u32 {
    if value <= 1 {
        0
    } else {
        usize::BITS - (value - 1).leading_zeros()
    }
}

const OFFSET_BITS: u32 = ceil_log2(BLOCK_CAP + OFFSET_PAD);

const OFFSET_MASK: usize = if OFFSET_BITS >= usize::BITS {
    panic!("OFFSET_BITS >= usize::BITS");
} else {
    (1usize << OFFSET_BITS) - 1
};

impl Cursor {
    pub const MAX_VSN: usize = usize::MAX >> OFFSET_BITS;

    #[inline]
    /// Builds a cursor from `version` with `off() == 0` (stores `version` in the upper bits).
    /// Specifically, `version` is shifted up by [OFFSET_BITS].
    pub const fn for_version(version: usize) -> Self {
        Self {
            inner: version << OFFSET_BITS,
        }
    }

    #[inline]
    pub const fn from_raw(value: usize) -> Self {
        Self { inner: value }
    }

    #[inline]
    pub const fn into_raw(self) -> usize {
        self.inner
    }

    #[inline]
    pub const fn off(self) -> usize {
        self.inner & OFFSET_MASK
    }

    #[inline]
    pub const fn incr_off(self) -> Self {
        Self {
            inner: self.inner + 1,
        }
    }

    #[inline]
    pub const fn vsn(self) -> usize {
        self.inner >> OFFSET_BITS
    }

    // #[inline]
    // Clears self.off()
    // pub const fn incr_vsn(self) -> Self {
    //     Self {
    //         inner: (self.inner & !OFFSET_MASK) + (1usize << OFFSET_BITS),
    //     }
    // }
}

#[repr(transparent)]
pub struct AtomicCursor {
    inner: AtomicUsize,
}

impl AtomicCursor {
    #[inline]
    pub const fn new(value: Cursor) -> Self {
        Self {
            inner: AtomicUsize::new(value.into_raw()),
        }
    }

    #[inline]
    pub fn load(&self, order: Ordering) -> Cursor {
        Cursor::from_raw(self.inner.load(order))
    }

    #[inline]
    pub fn store(&self, value: Cursor, order: Ordering) {
        self.inner.store(value.into_raw(), order)
    }

    #[inline]
    pub fn fetch_add(&self, value: usize, order: Ordering) -> Cursor {
        Cursor::from_raw(self.inner.fetch_add(value, order))
    }

    #[inline]
    pub fn fetch_max(&self, value: Cursor, order: Ordering) -> Cursor {
        Cursor::from_raw(self.inner.fetch_max(value.into_raw(), order))
    }
}

// #[cfg(test)]
// mod test {
//     use super::*;

//     #[test]
//     fn incr_vsn_clears_off() {
//         for i in (0..OFFSET_MASK).map(Cursor::from_raw) {
//             assert!(i.incr_vsn().off() == 0);
//         }
//     }
// }
