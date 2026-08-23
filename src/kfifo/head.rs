use crate::{block::SpmcBlock as Block, page::page_size};
use core::ptr;

#[derive(Clone, Copy, Debug)]
pub(crate) struct HeadCodec {
    block_length: usize,
    pointer_mask: usize,
    index_mask: usize,
    has_next_mask: usize,
}

impl HeadCodec {
    pub(crate) fn new<T>() -> Self {
        let page_size = page_size();
        let block_length = Block::<T>::length();
        let page_bits = page_size.trailing_zeros();
        let index_bits = usize::BITS - block_length.leading_zeros();

        assert!(
            index_bits <= page_bits,
            "the block index does not fit in the page-aligned pointer tag"
        );

        let index_mask = usize::MAX >> (usize::BITS - index_bits);
        let has_next_mask = if index_bits < page_bits {
            1usize << index_bits
        } else {
            // With one-byte slots the index can consume every alignment bit.
            // Treating has_next as an uncached hint preserves correctness; the
            // queue simply reloads phead more often at this element size.
            0
        };

        Self {
            block_length,
            pointer_mask: !(page_size - 1),
            index_mask,
            has_next_mask,
        }
    }

    #[inline]
    pub(crate) const fn block_length(self) -> usize {
        self.block_length
    }

    #[cfg(test)]
    const fn has_next_enabled(self) -> bool {
        self.has_next_mask != 0
    }
}

pub(crate) struct Head<T> {
    pub block: *mut Block<T>,
    pub index: usize,
    pub has_next: bool,
}

impl<T> Head<T> {
    pub const ZERO: Self = Self {
        block: ptr::null_mut(),
        index: 0,
        has_next: false,
    };

    pub const fn from_ptr(block: *mut Block<T>) -> Self {
        Self {
            block,
            ..Self::ZERO
        }
    }

    #[inline]
    pub fn from_usize(value: usize, codec: HeadCodec) -> Self {
        Self {
            block: ptr::with_exposed_provenance_mut(value & codec.pointer_mask),
            index: value & codec.index_mask,
            has_next: codec.has_next_mask != 0 && value & codec.has_next_mask != 0,
        }
    }

    #[inline]
    pub fn to_usize(self, codec: HeadCodec) -> usize {
        debug_assert_eq!(self.block.addr() & !codec.pointer_mask, 0);
        debug_assert!(self.index <= codec.block_length);

        self.block.expose_provenance()
            | (self.index & codec.index_mask)
            | (if self.has_next {
                codec.has_next_mask
            } else {
                0
            })
    }

    pub const fn is_zero(&self) -> bool {
        self.block.is_null() && self.index == 0 && !self.has_next
    }
}

impl<T> Copy for Head<T> {}

impl<T> Clone for Head<T> {
    fn clone(&self) -> Self {
        *self
    }
}

#[cfg(test)]
mod tests {
    use super::{Head, HeadCodec};
    use crate::block::SpmcBlock as Block;

    #[test]
    fn codec_round_trips_every_head_field_when_hint_bit_is_available() {
        let codec = HeadCodec::new::<u64>();
        assert!(codec.has_next_enabled());

        let block = Block::<u64>::new();
        let head = Head {
            block,
            index: codec.block_length(),
            has_next: true,
        };
        let decoded = Head::from_usize(head.to_usize(codec), codec);

        assert_eq!(decoded.block, block);
        assert_eq!(decoded.index, codec.block_length());
        assert!(decoded.has_next);
        // SAFETY: the test block is unpublished and contains no values.
        unsafe { Block::free(block, 0) };
    }

    #[test]
    fn zero_round_trips() {
        let codec = HeadCodec::new::<u64>();
        assert!(Head::<u64>::from_usize(0, codec).is_zero());
        assert_eq!(Head::<u64>::ZERO.to_usize(codec), 0);
    }

    #[test]
    fn one_byte_slots_use_all_tag_bits_for_the_index() {
        let codec = HeadCodec::new::<()>();
        assert!(!codec.has_next_enabled());
    }
}
