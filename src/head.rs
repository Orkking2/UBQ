use crate::block::Block;
use core::ptr;

pub struct Head<T, const BLOCK_SIZE: usize> {
    pub block: *mut Block<T, BLOCK_SIZE>,
    pub index: usize,
    pub has_next: bool,
}

impl<T, const BLOCK_SIZE: usize> Head<T, BLOCK_SIZE> {
    const PTR_MASK: usize = usize::MAX << Block::<T, BLOCK_SIZE>::LSHFT;
    const IDX_MASK: usize = usize::MAX >> (Self::PTR_MASK.leading_ones() + 1);
    const HNX_MASK: usize = !(Self::PTR_MASK | Self::IDX_MASK);

    pub const ZERO: Self = Self::from_usize(0);

    pub const fn from_ptr(block: *mut Block<T, BLOCK_SIZE>) -> Self {
        Self {
            block,
            ..Self::ZERO
        }
    }

    pub const fn from_usize(value: usize) -> Self {
        Self {
            block: ptr::with_exposed_provenance_mut(value & Self::PTR_MASK),
            index: value & Self::IDX_MASK,
            has_next: value & Self::HNX_MASK != 0,
        }
    }

    pub fn to_usize(self) -> usize {
        self.block.expose_provenance()
            | (self.index & Self::IDX_MASK)
            | (if self.has_next { Self::HNX_MASK } else { 0 })
    }

    pub const fn is_zero(&self) -> bool {
        self.block.is_null() && self.index == 0 && !self.has_next
    }
}

impl<T, const BLOCK_SIZE: usize> Copy for Head<T, BLOCK_SIZE> {}
impl<T, const BLOCK_SIZE: usize> Clone for Head<T, BLOCK_SIZE> {
    fn clone(&self) -> Self {
        Self {
            block: self.block.clone(),
            index: self.index.clone(),
            has_next: self.has_next.clone(),
        }
    }
}
