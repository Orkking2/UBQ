use crate::{backoff::BackoffPolicy, dynamic::block::Block};
use core::ptr::{NonNull, null_mut, with_exposed_provenance_mut};
use portable_atomic::Ordering;

pub(crate) const MAX_BLOCK_LENGTH: usize = u16::MAX as usize;

pub(crate) const INDEX_MASK: u128 = u16::MAX as u128;
pub(crate) const BLOCK_LENGTH_SHIFT: u32 = u16::BITS;
pub(crate) const BLOCK_LENGTH_MASK: u128 = INDEX_MASK << BLOCK_LENGTH_SHIFT;
pub(crate) const MIDDLE_SHIFT: u32 = u32::BITS;
pub(crate) const PTR_SHIFT: u32 = u64::BITS;
pub(crate) const PTR_MASK: u128 = (u64::MAX as u128) << PTR_SHIFT;

// Numeric layout of a producer head:
//
//   127                 64 63        48 47        32 31        16 15         0
//   +---------------------+------------+------------+------------+-----------+
//   |     pointer (u64)   | token(u16) |excess(u16) |blk_len(u16)| index(u16)|
//   +---------------------+------------+------------+------------+-----------+
//
// `AtomicInt::as_u64()` aliases bits 0..64, so narrow stores update the
// producer's block-local fields while leaving the pointer untouched. The token
// changes whenever the pointer does, tying a low-word reservation to the block
// whose pointer was obtained by the preceding full-width load.
pub(crate) const EXCESS_MASK: u128 = (u16::MAX as u128) << MIDDLE_SHIFT;
pub(crate) const PHEAD_TOKEN_SHIFT: u32 = MIDDLE_SHIFT + u16::BITS;
pub(crate) const PHEAD_TOKEN_MASK: u128 = (u16::MAX as u128) << PHEAD_TOKEN_SHIFT;

/// A conservative lower bound on the capacity of already-linked successor
/// blocks, excluding the block addressed by `phead`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[repr(transparent)]
pub(crate) struct Excess(u16);

impl Excess {
    pub(crate) const ZERO: Self = Self(0);

    /// Returns the number of successor slots that the packed head can prove
    /// exist. The real linked capacity may be greater after saturation.
    pub(crate) fn known_slots(self) -> usize {
        self.0 as usize
    }

    /// Records newly linked successor capacity, saturating so the packed field
    /// remains a conservative lower bound even for very large chains.
    pub(crate) fn add_capacity(self, capacity: usize) -> Self {
        Self(
            self.known_slots()
                .saturating_add(capacity)
                .min(u16::MAX as usize) as u16,
        )
    }

    /// Removes the block that the head is about to enter from the count of
    /// successor-only slots. Saturation at zero preserves conservatism when an
    /// earlier large chain could not be represented exactly.
    pub(crate) fn remove_block(self, block_length: u16) -> Self {
        Self(self.0.saturating_sub(block_length))
    }
}

pub(crate) struct PHead<T> {
    pub(crate) ptr: *mut Block<T>,
    pub(crate) token: u16,
    pub(crate) excess: Excess,
    pub(crate) block_length: u16,
    pub(crate) index: u16,
}

impl<T> PartialEq for PHead<T> {
    fn eq(&self, other: &Self) -> bool {
        self.ptr == other.ptr
            && self.token == other.token
            && self.excess == other.excess
            && self.block_length == other.block_length
            && self.index == other.index
    }
}

impl<T> Copy for PHead<T> {}

impl<T> Clone for PHead<T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T> PHead<T> {
    /// The sole pointer-less producer-head state. It means that no producer has
    /// yet installed the queue's first block.
    pub(crate) const ZERO: Self = Self {
        ptr: null_mut(),
        token: 0,
        excess: Excess::ZERO,
        block_length: 0,
        index: 0,
    };

    pub(crate) fn from_block(block: &Block<T>) -> Self {
        Self::from_ptr(NonNull::from_ref(block))
    }

    /// Constructs a normalized head at index zero for one block. Successor
    /// capacity is intentionally zero and must be added separately when this
    /// block is the head of a newly allocated `BlockChain`.
    pub(crate) fn from_ptr(block: NonNull<Block<T>>) -> Self {
        Self {
            ptr: block.as_ptr(),
            block_length: u16::try_from(unsafe { block.as_ref() }.len())
                .expect("block length must fit in the packed u16 field"),
            ..Self::ZERO
        }
    }

    #[inline]
    pub(crate) fn from_u128(v: u128) -> Self {
        let address = ((v & PTR_MASK) >> PTR_SHIFT) as usize;

        Self {
            ptr: with_exposed_provenance_mut(address),
            token: ((v & PHEAD_TOKEN_MASK) >> PHEAD_TOKEN_SHIFT) as u16,
            excess: Excess(((v & EXCESS_MASK) >> MIDDLE_SHIFT) as u16),
            block_length: ((v & BLOCK_LENGTH_MASK) >> BLOCK_LENGTH_SHIFT) as u16,
            index: (v & INDEX_MASK) as u16,
        }
    }

    #[inline]
    pub(crate) fn from_u64(v: u64) -> Self {
        let v = v as u128;

        Self {
            token: ((v & PHEAD_TOKEN_MASK) >> PHEAD_TOKEN_SHIFT) as u16,
            excess: Excess(((v & EXCESS_MASK) >> MIDDLE_SHIFT) as u16),
            block_length: ((v & BLOCK_LENGTH_MASK) >> BLOCK_LENGTH_SHIFT) as u16,
            index: (v & INDEX_MASK) as u16,
            ..Self::ZERO
        }
    }

    #[inline]
    pub(crate) fn is_zero(&self) -> bool {
        self == &Self::ZERO
    }

    #[inline]
    pub(crate) fn pack_u128(self) -> u128 {
        ((self.ptr as u128) << PTR_SHIFT)
            | ((self.token as u128) << PHEAD_TOKEN_SHIFT)
            | ((self.excess.0 as u128) << MIDDLE_SHIFT)
            | ((self.block_length as u128) << BLOCK_LENGTH_SHIFT)
            | (self.index as u128)
    }

    #[inline]
    pub(crate) fn pack_u64(self) -> u64 {
        ((self.token as u64) << PHEAD_TOKEN_SHIFT)
            | ((self.excess.0 as u64) << MIDDLE_SHIFT)
            | ((self.block_length as u64) << BLOCK_LENGTH_SHIFT)
            | (self.index as u64)
    }

    #[inline]
    /// Loads the already-linked successor and advances the local traversal
    /// cursor to index zero in that block.
    ///
    /// Callers must ensure the current block cannot be reclaimed before this
    /// method has loaded its `next` pointer.
    pub(crate) fn next(self) -> Self {
        let block = unsafe { self.ptr.as_ref_unchecked() };
        let ptr = block.next().load(Ordering::Acquire);
        let block_length = u16::try_from(unsafe { ptr.as_ref_unchecked() }.len())
            .expect("block length must fit in the packed u16 field");

        Self {
            ptr,
            token: self.token.wrapping_add(1),
            excess: self.excess.remove_block(block_length),
            block_length,
            index: 0,
        }
    }

    /// Advances to a successor whose pointer the caller has already acquired.
    /// This is used by the boundary owner while building the final full-width
    /// producer head and avoids loading the old block's link a second time.
    pub(crate) fn with_block(self, ptr: NonNull<Block<T>>) -> Self {
        let block_length =
            u16::try_from(unsafe { ptr.as_ref() }.len()).expect("block length must fit in u16");

        Self {
            ptr: ptr.as_ptr(),
            token: self.token.wrapping_add(1),
            excess: self.excess.remove_block(block_length),
            block_length,
            index: 0,
        }
    }
}

// Numeric layout of a consumer head:
//
//   127                 64 63 62              32 31        16 15         0
//   +---------------------+--+------------------+------------+-----------+
//   |     pointer (u64)   |N | token (u31)      |blk_len(u16)| index(u16)|
//   +---------------------+--+------------------+------------+-----------+
//
// `N` records whether the producer is known to be in a later block. The token
// distinguishes low-word CAS results belonging to different consumer blocks.
pub(crate) const TOKEN_SHIFT: u32 = MIDDLE_SHIFT;
pub(crate) const U32_TOP: u32 = 1 << (u32::BITS - 1);
pub(crate) const TOKEN_VALUE_MASK: u32 = !U32_TOP;
pub(crate) const TOKEN_MASK: u128 = (TOKEN_VALUE_MASK as u128) << TOKEN_SHIFT;
pub(crate) const HAS_NEXT_SHIFT: u32 = u64::BITS - 1;
pub(crate) const HAS_NEXT_MASK: u128 = 1 << HAS_NEXT_SHIFT;

pub(crate) struct CHead<T> {
    pub(crate) ptr: *mut Block<T>,
    pub(crate) block_length: u16,
    pub(crate) has_next: bool,
    pub(crate) index: u16,
    pub(crate) token: u32,
}

impl<T> PartialEq for CHead<T> {
    fn eq(&self, other: &Self) -> bool {
        self.ptr == other.ptr
            && self.block_length == other.block_length
            && self.has_next == other.has_next
            && self.index == other.index
            && self.token == other.token
    }
}

impl<T> Copy for CHead<T> {}

impl<T> Clone for CHead<T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T> CHead<T> {
    pub(crate) const ZERO: Self = Self {
        ptr: null_mut(),
        block_length: 0,
        has_next: false,
        index: 0,
        token: 0,
    };

    pub(crate) fn from_block(block: &Block<T>) -> Self {
        Self {
            ptr: block as *const _ as *mut _,
            block_length: u16::try_from(block.len())
                .expect("block length must fit in the packed u16 field"),
            ..Self::ZERO
        }
    }

    pub(crate) fn from_ptr(block: NonNull<Block<T>>) -> Self {
        Self::from_block(unsafe { block.as_ref() })
    }

    #[inline]
    pub(crate) fn from_u128(v: u128) -> Self {
        let address = ((v & PTR_MASK) >> PTR_SHIFT) as usize;

        Self {
            ptr: with_exposed_provenance_mut(address),
            has_next: v & HAS_NEXT_MASK != 0,
            token: ((v & TOKEN_MASK) >> TOKEN_SHIFT) as u32,
            block_length: ((v & BLOCK_LENGTH_MASK) >> BLOCK_LENGTH_SHIFT) as u16,
            index: (v & INDEX_MASK) as u16,
        }
    }

    #[inline]
    pub(crate) fn from_u64(v: u64) -> Self {
        let v = v as u128;

        Self {
            has_next: v & HAS_NEXT_MASK != 0,
            token: ((v & TOKEN_MASK) >> TOKEN_SHIFT) as u32,
            block_length: ((v & BLOCK_LENGTH_MASK) >> BLOCK_LENGTH_SHIFT) as u16,
            index: (v & INDEX_MASK) as u16,
            ..Self::ZERO
        }
    }

    #[inline]
    pub(crate) fn is_zero(&self) -> bool {
        self == &Self::ZERO
    }

    #[inline]
    pub(crate) fn full_addr_eq(&self, other: &Self) -> bool {
        self.ptr == other.ptr && self.index == other.index
    }

    #[inline]
    pub(crate) fn pack_u128(self) -> u128 {
        debug_assert_eq!(self.token & U32_TOP, 0, "token must fit in 31 bits");

        ((self.ptr as u128) << PTR_SHIFT)
            | ((self.has_next as u128) << HAS_NEXT_SHIFT)
            | (((self.token & TOKEN_VALUE_MASK) as u128) << TOKEN_SHIFT)
            | ((self.block_length as u128) << BLOCK_LENGTH_SHIFT)
            | (self.index as u128)
    }

    #[inline]
    pub(crate) fn pack_u64(self) -> u64 {
        debug_assert_eq!(self.token & U32_TOP, 0, "token must fit in 31 bits");

        ((self.has_next as u64) << HAS_NEXT_SHIFT)
            | (((self.token & TOKEN_VALUE_MASK) as u64) << TOKEN_SHIFT)
            | ((self.block_length as u64) << BLOCK_LENGTH_SHIFT)
            | (self.index as u64)
    }

    #[inline]
    pub(crate) fn next(self) -> Self {
        let block = unsafe { self.ptr.as_ref_unchecked() };
        let ptr = block.next().load(Ordering::Acquire);
        let block_length = u16::try_from(unsafe { ptr.as_ref_unchecked() }.len())
            .expect("block length must fit in the packed u16 field");

        Self {
            ptr,
            block_length,
            token: self.token.wrapping_add(1) & TOKEN_VALUE_MASK,
            ..Self::ZERO
        }
    }

    pub(crate) fn await_next_head<B: BackoffPolicy>(self, backoff: &B) -> Self {
        let block = unsafe { self.ptr.as_ref_unchecked() };
        let (next, block_length) = block.await_next(backoff);

        Self {
            ptr: next.as_ptr(),
            block_length,
            token: self.token.wrapping_add(1) & TOKEN_VALUE_MASK,
            ..Self::ZERO
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn producer_head_round_trips_token_and_excess() {
        let head = PHead::<()> {
            ptr: with_exposed_provenance_mut(0x1234_5678),
            token: 0xabcd,
            excess: Excess(u16::MAX),
            block_length: 0x1357,
            index: 0x2468,
        };

        assert!(PHead::from_u128(head.pack_u128()) == head);

        let low = PHead::<()>::from_u64(head.pack_u64());
        assert!(low.ptr.is_null());
        assert_eq!(low.token, head.token);
        assert_eq!(low.excess, head.excess);
        assert_eq!(low.block_length, head.block_length);
        assert_eq!(low.index, head.index);
        assert_eq!(head.pack_u64(), 0xabcd_ffff_1357_2468);
    }

    #[test]
    fn producer_token_advances_with_the_pointer() {
        let first = Block::<()>::new_boxed(7, None);
        let second = Block::<()>::new_boxed(11, None);
        let second_ptr = NonNull::from_ref(second.as_ref());
        let head = PHead {
            token: u16::MAX,
            excess: Excess(19),
            ..PHead::from_block(first.as_ref())
        };

        let advanced = head.with_block(second_ptr);

        assert_eq!(advanced.ptr, second_ptr.as_ptr());
        assert_eq!(advanced.token, 0);
        assert_eq!(advanced.excess, Excess(8));
        assert_eq!(advanced.block_length, 11);
        assert_eq!(advanced.index, 0);
    }

    #[test]
    fn consumer_head_round_trips_has_next_and_token() {
        let head = CHead::<()> {
            ptr: with_exposed_provenance_mut(0x1234_5678),
            block_length: 0x1357,
            has_next: true,
            index: 0x2468,
            token: TOKEN_VALUE_MASK,
        };

        assert!(CHead::from_u128(head.pack_u128()) == head);

        let low = CHead::<()>::from_u64(head.pack_u64());
        assert!(low.ptr.is_null());
        assert!(low.has_next);
        assert_eq!(low.token, head.token);
        assert_eq!(low.block_length, head.block_length);
        assert_eq!(low.index, head.index);
    }

    #[test]
    fn excess_saturates() {
        let excess = Excess::ZERO.add_capacity(17).add_capacity(usize::MAX);

        assert_eq!(excess.known_slots(), u16::MAX as usize);
        assert_eq!(excess.remove_block(u16::MAX), Excess::ZERO);
    }
}
