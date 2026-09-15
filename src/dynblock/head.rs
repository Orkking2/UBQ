use super::block::{Geometry, VBlock};
use core::ptr;

/// One atomic word contains both the vblock address and its reservation index.
pub(super) struct Head<T> {
    pub(super) block: *mut VBlock<T>,
    pub(super) index: usize,
}

impl<T> Copy for Head<T> {}
impl<T> Clone for Head<T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T> Head<T> {
    pub(super) fn new(block: *mut VBlock<T>) -> Self {
        Self { block, index: 0 }
    }

    pub(super) fn decode(word: usize, geometry: Geometry) -> Self {
        let index_mask = geometry.vblock_alignment - 1;
        Self {
            block: ptr::with_exposed_provenance_mut(word & !index_mask),
            index: word & index_mask,
        }
    }

    pub(super) fn encode(self, geometry: Geometry) -> usize {
        debug_assert_eq!(self.block.addr() % geometry.vblock_alignment, 0);
        debug_assert!(self.index <= geometry.config.slots_per_vblock);
        self.block.expose_provenance() | self.index
    }
}
