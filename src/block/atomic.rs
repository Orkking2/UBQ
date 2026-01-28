use std::sync::atomic::{AtomicU64, Ordering};

use super::wrapped_u64::WrappedU64;

pub(crate) struct WrappedAtomicU64 {
    inner: AtomicU64,
}

impl From<WrappedU64> for WrappedAtomicU64 {
    fn from(value: WrappedU64) -> Self {
        Self::new(value.get_raw())
    }
}

impl WrappedAtomicU64 {
    fn new(v: u64) -> Self {
        Self {
            inner: AtomicU64::new(v),
        }
    }

    pub(crate) fn set_bits_for_capacity(&self, capacity: usize) {
        let bits = WrappedU64::bits_for_capacity(capacity);

        loop {
            let old = WrappedU64::from_raw(self.inner.load(Ordering::Relaxed));

            if old.get_bits() < bits {
                if let Some(new) = old.set_bits(bits) {
                    match self.inner.compare_exchange(
                        old.get_raw(),
                        new.get_raw(),
                        Ordering::Relaxed,
                        Ordering::Relaxed,
                    ) {
                        Ok(_) => {}, // break v
                        Err(_) => continue,
                    }
                } else {
                    // break v
                }
            } else {
                // break v
            }

            break;
        }
    }

    // pub(crate) fn inc_top(&self) {
    //     let _ = self.inner.fetch_update(
    //         Ordering::Relaxed
    //         Ordering::Relaxed
    //         |v| WrappedU64::new_raw(v).inc_bits().map(WrappedU64::into_inner),
    //     );
    // }

    // pub(crate) fn dec_top(&self) {
    //     let _ = self.inner.fetch_update(
    //         Ordering::Relaxed
    //         Ordering::Relaxed
    //         |v| WrappedU64::new_raw(v).dec_bits().map(WrappedU64::into_inner),
    //     );
    // }

    pub(crate) fn fetch_add(&self, val: u64, order: Ordering) -> WrappedU64 {
        WrappedU64::from_raw(self.inner.fetch_add(val, order))
    }

    pub(crate) fn fetch_update<F>(
        &self,
        set_order: Ordering,
        fetch_order: Ordering,
        mut f: F,
    ) -> Result<WrappedU64, WrappedU64>
    where
        F: FnMut(WrappedU64) -> Option<WrappedU64>,
    {
        self.inner
            .fetch_update(set_order, fetch_order, |raw| {
                f(WrappedU64::from_raw(raw)).map(WrappedU64::get_raw)
            })
            .map(WrappedU64::from_raw)
            .map_err(WrappedU64::from_raw)
    }

    pub(crate) fn load(&self, order: Ordering) -> WrappedU64 {
        WrappedU64::from_raw(self.inner.load(order))
    }

    pub(crate) fn fetch_max(&self, val: WrappedU64, order: Ordering) -> WrappedU64 {
        WrappedU64::from_raw(self.inner.fetch_max(val.get_raw(), order))
    }

    pub(crate) fn compare_exchange(
        &self,
        current: WrappedU64,
        new: WrappedU64,
        success: Ordering,
        failure: Ordering,
    ) -> Result<WrappedU64, WrappedU64> {
        self.inner
            .compare_exchange(current.get_raw(), new.get_raw(), success, failure)
            .map(WrappedU64::from_raw)
            .map_err(WrappedU64::from_raw)
    }
}
