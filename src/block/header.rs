use std::sync::atomic::Ordering;

#[cfg(feature = "ubq_debug")]
use std::fmt::Debug;

#[cfg(feature = "ubq_debug")]
use crate::block::WrappedU64Components;

use super::atomic::WrappedAtomicU64;
use super::wrapped_u64::WrappedU64;

pub(crate) struct HeaderControl {
    pub(crate) take: WrappedAtomicU64,
    pub(crate) give: WrappedAtomicU64,
}

#[cfg(feature = "ubq_debug")]
pub(crate) struct HeaderConDebug {
    take: WrappedU64,
    give: WrappedU64,
}

#[cfg(feature = "ubq_debug")]
impl Debug for HeaderConDebug {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let WrappedU64Components {
            bits,
            version: take_vsn,
            index: take_idx,
        } = self.take.get_components();

        let WrappedU64Components {
            version: give_vsn,
            index: give_idx,
            ..
        } = self.give.get_components();

        if take_vsn == give_vsn {
            write!(
                f,
                "{}({})|{}:{}/{} (t/g)",
                bits,
                WrappedU64::max_index_for_bits(bits),
                take_vsn,
                take_idx,
                give_idx
            )
        } else {
            write!(
                f,
                "{}({})|{}:{}/{}:{} (t/g)",
                bits,
                WrappedU64::max_index_for_bits(bits),
                take_vsn,
                take_idx,
                give_vsn,
                give_idx
            )
        }
    }
}

impl HeaderControl {
    pub(crate) fn for_capacity(capacity: usize) -> Self {
        Self {
            take: WrappedAtomicU64::from(WrappedU64::for_capacity(capacity)),
            give: WrappedAtomicU64::from(WrappedU64::for_capacity(capacity)),
        }
    }

    pub(crate) fn update_max_or_compare_exchange(&self, unbumped: WrappedU64) {
        match unbumped.bump_version_wrapping() {
            Ok(fetch) => self.fetch_max_both(fetch, Ordering::AcqRel),
            Err(cas) => self.cx_both(
                unbumped,
                cas,
                Ordering::AcqRel,
                Ordering::Acquire
            ),
        }
    }

    #[cfg(feature = "ubq_debug")]
    pub(crate) fn debug(&self) -> HeaderConDebug {
        HeaderConDebug {
            take: self.take.load(Ordering::Relaxed),
            give: self.give.load(Ordering::Relaxed),
        }
    }

    fn fetch_max_both(&self, val: WrappedU64, order: Ordering) {
        self.take.fetch_max(val, order);
        self.give.fetch_max(val, order);
    }

    fn cx_both(&self, current: WrappedU64, new: WrappedU64, success: Ordering, failure: Ordering) {
        let _ = self.take.compare_exchange(current, new, success, failure);
        let _ = self.give.compare_exchange(current, new, success, failure);
    }
}
