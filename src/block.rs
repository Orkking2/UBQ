use std::{
    cell::UnsafeCell,
    fmt::Debug,
    iter,
    mem::MaybeUninit,
    sync::atomic::{AtomicU64, Ordering},
};

#[non_exhaustive]
#[derive(Debug)]
pub enum ReserveError {
    NoEntry,
    NotAvailable,
    BlockDone,
}

struct Reservation<'a, T> {
    index: usize,
    block: &'a Block<T>,
}

impl<'a, T> Reservation<'a, T> {
    pub unsafe fn write(&self, val: T) {
        unsafe {
            self.block.array[self.index]
                .get()
                .write(MaybeUninit::new(val))
        };
    }

    pub unsafe fn read(&self) -> T {
        unsafe { self.block.array[self.index].get().read().assume_init() }
    }
}

pub struct UninitRes<'a, T> {
    inner: Reservation<'a, T>,
}

impl<'a, T> UninitRes<'a, T> {
    pub fn write(self, val: T) {
        unsafe {
            self.inner.write(val);
        }
    }
}

impl<'a, T> From<Reservation<'a, T>> for UninitRes<'a, T> {
    fn from(value: Reservation<'a, T>) -> Self {
        Self { inner: value }
    }
}

impl<'a, T> Drop for UninitRes<'a, T> {
    fn drop(&mut self) {
        self.inner.block.pcon.give.fetch_add(1, Ordering::Release);
    }
}

pub struct InitRes<'a, T> {
    inner: Reservation<'a, T>,
}

impl<'a, T> InitRes<'a, T> {
    pub fn read(self) -> T {
        unsafe { self.inner.read() }
    }
}

impl<'a, T> From<Reservation<'a, T>> for InitRes<'a, T> {
    fn from(value: Reservation<'a, T>) -> Self {
        Self { inner: value }
    }
}

impl<'a, T> Drop for InitRes<'a, T> {
    fn drop(&mut self) {
        self.inner.block.ccon.give.fetch_add(1, Ordering::Relaxed);
    }
}

struct WrappedAtomicU64 {
    inner: AtomicU64,
}

impl From<WrappedU64> for WrappedAtomicU64 {
    fn from(value: WrappedU64) -> Self {
        Self::new(value.into_inner())
    }
}

impl WrappedAtomicU64 {
    fn new(v: u64) -> Self {
        Self {
            inner: AtomicU64::new(v),
        }
    }

    fn inc_top(&self) {
        let _ = self
            .inner
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                Some(WrappedU64::new_raw(v).inc_bits().ok()?.into_inner())
            });
    }

    fn dec_top(&self) {
        let _ = self
            .inner
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                Some(WrappedU64::new_raw(v).dec_bits().ok()?.into_inner())
            });
    }

    fn fetch_add(&self, val: u64, order: Ordering) -> WrappedU64 {
        WrappedU64::new_raw(self.inner.fetch_add(val, order))
    }

    fn fetch_update<F>(
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
                f(WrappedU64::new_raw(raw)).map(WrappedU64::into_inner)
            })
            .map(WrappedU64::new_raw)
            .map_err(WrappedU64::new_raw)
    }

    fn load(&self, order: Ordering) -> WrappedU64 {
        WrappedU64::new_raw(self.inner.load(order))
    }

    fn fetch_max(&self, val: WrappedU64, order: Ordering) -> WrappedU64 {
        WrappedU64::new_raw(self.inner.fetch_max(val.into_inner(), order))
    }

    fn compare_exchange(
        &self,
        current: WrappedU64,
        new: WrappedU64,
        success: Ordering,
        failure: Ordering,
    ) -> Result<WrappedU64, WrappedU64> {
        self.inner
            .compare_exchange(current.into_inner(), new.into_inner(), success, failure)
            .map(WrappedU64::new_raw)
            .map_err(WrappedU64::new_raw)
    }
}

#[derive(PartialEq, Eq, Clone, Copy)]
struct WrappedU64 {
    inner: u64,
}

#[derive(Debug, PartialEq, Eq)]
pub struct WrappedU64Components {
    pub bits: u8,
    pub version: u64,
    pub index: u64,
}

impl Debug for WrappedU64 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let WrappedU64Components {
            bits,
            version,
            index,
        } = self.get_components();

        f.debug_struct("WrappedU64")
            .field("bits", &bits)
            .field("version", &version)
            .field("index", &index)
            .finish()
    }
}

#[derive(Debug)]
enum WrappedU64Err {
    IndexBitsExceedAvailablePayloadBits,
    IndexTooLargeForAllottedWidth,
    VersionTooLargeForAllottedWidth,
    IncBitsOverflows,
    DecBitsUnderflows,
}

impl WrappedU64 {
    fn for_capacity(capacity: usize) -> Self {
        let bits = capacity
            .saturating_add(1)
            .next_power_of_two()
            .trailing_zeros() as u8;

        unsafe { Self::new_unchecked(bits, 0, 0) }
    }

    fn new_raw(inner: u64) -> Self {
        Self { inner }
    }

    fn into_inner(self) -> u64 {
        self.inner
    }

    fn new(top: u8, version: u64, index: u64) -> Result<Self, WrappedU64Err> {
        let bits = top as u64;
        let index_mask = (1u64 << bits) - 1;
        let version_mask = (1u64 << (56 - bits)) - 1;

        if bits > 56 {
            Err(WrappedU64Err::IndexBitsExceedAvailablePayloadBits)
        } else if index & !index_mask != 0 {
            Err(WrappedU64Err::IndexTooLargeForAllottedWidth)
        } else if version & !version_mask != 0 {
            Err(WrappedU64Err::VersionTooLargeForAllottedWidth)
        } else {
            let payload = (version & version_mask) << bits | (index & index_mask);
            Ok(Self::new_raw(((top as u64) << 56) | payload))
        }
    }

    unsafe fn new_unchecked(top: u8, version: u64, index: u64) -> Self {
        let bits = top as u64;

        let index_mask = (1u64 << bits) - 1;
        let version_mask = (1u64 << (56 - bits)) - 1;

        let payload = (version & version_mask) << bits | (index & index_mask);
        Self::new_raw(((top as u64) << 56) | payload)
    }

    #[inline]
    fn top_u8(self) -> u8 {
        (self.inner >> (u64::BITS - u8::BITS)) as u8
    }

    #[inline]
    fn get_index(self) -> usize {
        self.get_index_given_bits(self.top_u8()).try_into().unwrap()
    }

    #[inline]
    fn get_index_given_bits(self, bits: u8) -> u64 {
        self.inner & ((1u64 << bits.min((u64::BITS - 1) as u8)) - 1)
    }

    #[inline]
    fn get_version(self) -> u64 {
        self.get_version_given_bits(self.top_u8())
    }

    #[inline]
    fn get_version_given_bits(self, bits: u8) -> u64 {
        (self.inner & 0x00FF_FFFF_FFFF_FFFF) >> bits
    }

    /// (bits, index, version)
    #[inline]
    fn get_components(self) -> WrappedU64Components {
        let bits = self.top_u8();

        let index = self.get_index_given_bits(bits);
        let version = self.get_version_given_bits(bits);

        WrappedU64Components {
            bits,
            version,
            index,
        }
    }

    #[inline]
    fn bump_index(self) -> Self {
        Self::new_raw(self.inner + 1)
    }

    #[inline]
    fn inc_bits(self) -> Result<Self, WrappedU64Err> {
        let WrappedU64Components {
            bits,
            version,
            index,
        } = self.get_components();

        Self::new(
            bits.checked_add(1).ok_or(WrappedU64Err::IncBitsOverflows)?,
            version,
            index,
        )
    }

    #[inline]
    fn inc_bits_unchecked(self) -> Self {
        let WrappedU64Components {
            bits,
            version,
            index,
        } = self.get_components();

        unsafe { Self::new_unchecked(bits + 1, version, index) }
    }

    #[inline]
    fn dec_bits(self) -> Result<Self, WrappedU64Err> {
        let WrappedU64Components {
            bits,
            version,
            index,
        } = self.get_components();

        Self::new(
            bits.checked_sub(1)
                .ok_or(WrappedU64Err::DecBitsUnderflows)?,
            version,
            index,
        )
    }

    #[inline]
    fn dec_bits_unchecked(self) -> Self {
        let WrappedU64Components {
            bits,
            version,
            index,
        } = self.get_components();

        unsafe { Self::new_unchecked(bits - 1, version, index) }
    }

    /// Ok(fetch_maxable), Err(must-CAS)
    #[inline]
    fn bump_version_wrapping(self) -> Result<Self, Self> {
        let WrappedU64Components { bits, version, .. } = self.get_components();

        let new_version = version + 1;

        if Self::max_version_for_bits(bits) > new_version {
            Ok(unsafe { Self::new_unchecked(bits, new_version, 0) })
        } else {
            Err(unsafe { Self::new_unchecked(bits, 0, 0) })
        }
    }

    // #[inline]
    // fn bump_version_unchecked(self) -> Self {
    //     let (top, version, ..) = self.get_components();

    //     unsafe { Self::new_unchecked(top, version + 1, 0) }
    // }

    #[inline]
    fn max_version_for_bits(bits: u8) -> u64 {
        (1u64 << (56 - bits)) - 1
    }

    // #[inline]
    // fn get_max_index(&self) -> u64 {
    //     (1u64 << self.top_u8()) - 1
    // }
}

#[cfg(test)]
mod wrapped_u64_tests {
    use super::*;

    #[test]
    fn new_to_get_components_symmetry() {
        for bits in 0u8..=56 {
            let index_max = (1u64 << bits) - 1;
            let version_max = (1u64 << (56 - bits)) - 1;

            let mut index_candidates = vec![0, index_max];
            if index_max > 1 {
                index_candidates.push(index_max / 2);
            }

            let mut version_candidates = vec![0, version_max];
            if version_max > 1 {
                version_candidates.push(version_max / 2);
            }

            index_candidates.sort_unstable();
            index_candidates.dedup();
            version_candidates.sort_unstable();
            version_candidates.dedup();

            for index in index_candidates.iter().copied() {
                for version in version_candidates.iter().copied() {
                    let wrapped = WrappedU64::new(bits, version, index).unwrap_or_else(|err| {
                        panic!("unexpected error {err:?} for bits={bits}, version={version}, index={index}")
                    });

                    assert_eq!(
                        WrappedU64Components {
                            bits,
                            version,
                            index
                        },
                        wrapped.get_components(),
                        "mismatch for bits={bits}, version={version}, index={index}"
                    );
                }
            }
        }
    }

    #[test]
    fn bump_index() {}

    #[test]
    fn capacity_fits_index() {
        fn assert_capacity(c: usize) {
            let bits = WrappedU64::for_capacity(c).top_u8();
            let mask = if bits == 0 { 0 } else { (1usize << bits) - 1 };
            assert_eq!(
                c & mask,
                c,
                "capacity {c} does not fit mask {mask:#x} (bits={bits})"
            );
        }

        // Exhaustive over small capacities to catch simple regressions.
        for c in 0usize..=0x1000 {
            assert_capacity(c);
        }

        // Sample around every power-of-two boundary that fits in 56 bits (the
        // maximum supported index width).
        let max_shift = usize::BITS.min(56) - 1;
        let mut shift = 1u32;
        while shift <= max_shift {
            let pow = 1usize << shift;
            for c in [
                pow.saturating_sub(2),
                pow.saturating_sub(1),
                pow,
                pow.saturating_add(1),
                pow.saturating_add(2),
            ] {
                assert_capacity(c);
            }
            shift += 1;
        }

        // Hit the upper edge of the supported range explicitly.
        let max_supported = if usize::BITS >= 56 {
            (1usize << 56) - 1
        } else {
            usize::MAX
        };
        for c in [
            max_supported.saturating_sub(2),
            max_supported.saturating_sub(1),
            max_supported,
        ] {
            assert_capacity(c);
        }
    }
}

struct HeaderControl {
    take: WrappedAtomicU64,
    give: WrappedAtomicU64,
}

impl HeaderControl {
    fn for_capacity(capacity: usize) -> Self {
        Self {
            take: WrappedAtomicU64::from(WrappedU64::for_capacity(capacity)),
            give: WrappedAtomicU64::from(WrappedU64::for_capacity(capacity)),
        }
    }

    fn update_max_or_compare_exchange(&self, unbumped: WrappedU64) {
        match unbumped.bump_version_wrapping() {
            Ok(fetch) => self.fetch_max_both(fetch, Ordering::Relaxed),
            Err(cas) => self.cx_both(unbumped, cas, Ordering::Relaxed, Ordering::Relaxed),
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

pub struct Block<T> {
    pcon: HeaderControl,
    ccon: HeaderControl,

    array: Box<[UnsafeCell<MaybeUninit<T>>]>,
}

impl<T> Block<T> {
    // /// `max_access` is the maximum number of threads that can simultaneously
    // /// call `allocate` or `reserve` at once (take the max of the two).
    // /// This is because we add to an index, potentially with each thread that
    // /// can possibly request a slot for any reason. But we need to know ahead
    // /// of time what the max of this index can be. We end up needing exactly
    // /// `size + max_access` capacity in our index, and the only one who knows
    // /// the max access and size is you my friend, the caller.
    pub fn new(size: usize) -> Self {
        Self {
            pcon: HeaderControl::for_capacity(size),
            ccon: HeaderControl::for_capacity(size),

            array: iter::from_fn(|| Some(UnsafeCell::new(MaybeUninit::uninit())))
                .take(size)
                .collect(),
        }
    }

    pub fn reset_pcon(&self) -> bool {
        let (pgive, cgive) = (
            self.pcon.give.load(Ordering::Relaxed),
            self.ccon.give.load(Ordering::Relaxed),
        );

        if pgive.get_version() == cgive.get_version() && cgive.get_index() > self.len() {
            self.pcon.update_max_or_compare_exchange(pgive);

            true
        } else {
            false
        }
    }

    pub fn reset_ccon(&self) -> bool {
        let (pgive, cgive) = (
            self.pcon.give.load(Ordering::Relaxed),
            self.ccon.give.load(Ordering::Relaxed),
        );

        // #[cfg(test)]
        // println!("Resetting ccon with pgive({pgive:?}) and cgive({cgive:?})");

        let diff = cgive.get_version() - pgive.get_version();

        // #[cfg(test)]
        // println!("diff({diff})");

        if cgive.get_index() != 0 && diff <= 1 {
            self.ccon.update_max_or_compare_exchange(cgive);

            true
        } else {
            false
        }
    }

    fn get_all_wrapped_atomics(&self) -> [&WrappedAtomicU64; 4] {
        [
            &self.ccon.give,
            &self.ccon.take,
            &self.pcon.give,
            &self.pcon.take,
        ]
    }

    #[inline]
    pub(crate) fn maybe_double_max_access(&self, new_max_access: usize) {
        if is_power_of_two(self.len() + new_max_access) {
            self.get_all_wrapped_atomics()
                .map(WrappedAtomicU64::inc_top);
        }
    }

    #[inline]
    pub(crate) fn maybe_halve_max_access(&self, new_max_access: usize) {
        if is_power_of_two(self.len() + new_max_access + 1) {
            self.get_all_wrapped_atomics()
                .map(WrappedAtomicU64::dec_top);
        }
    }

    #[inline]
    pub fn is_focus_of_consumers(&self) -> bool {
        self.ccon.give.load(Ordering::Relaxed).get_index() < self.len()
    }

    #[inline]
    pub const fn len(&self) -> usize {
        self.array.len()
    }

    // #[inline]
    // fn get_version_of(&self, raw: usize) -> usize {
    //     self.idx_control.version(raw)
    // }

    pub fn allocate(&self) -> Option<UninitRes<'_, T>> {
        // Every thread is guaranteed an extra index capacity to account for overflow. A thread will never
        // call this function, receive a BlockDone, and then immediately re-call this function, as that
        // could overflow into a version increment. This is because when a thread receives a BlockDone error
        // during allocation, it will try to allocate a new block.
        let res = self
            .pcon
            .take
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                (current.get_index() < self.len()).then(|| current.bump_index())
            });

        res.ok().map(|old| {
            Reservation {
                index: old.get_index(),
                block: self,
            }
            .into()
        })
    }

    pub fn reserve(&self) -> Result<InitRes<'_, T>, ReserveError> {
        loop {
            let reserved = self.ccon.take.load(Ordering::Relaxed);

            #[cfg(test)]
            println!("reserved({reserved:?})");

            if reserved.get_index() >= self.len() {
                #[cfg(test)]
                println!(
                    "Early BlockDone with res.idx({}), len({})",
                    reserved.get_index(),
                    self.len()
                );
                break Err(ReserveError::BlockDone);
            } else {
                // All previous writes in this block must be visible before this load.
                let committed = self.pcon.give.load(Ordering::Acquire);

                if committed.get_index() == reserved.get_index()
                    || committed.get_version() != reserved.get_version()
                {
                    break Err(ReserveError::NoEntry);
                }

                if committed.get_index() != self.len() {
                    let allocated = self.pcon.take.load(Ordering::Relaxed);

                    if allocated.get_index() != committed.get_index() {
                        break Err(ReserveError::NotAvailable);
                    }
                }

                if self
                    .ccon
                    .take
                    .fetch_max(reserved.bump_index(), Ordering::Relaxed)
                    == reserved
                {
                    break Ok(Reservation {
                        index: reserved.get_index() as usize,
                        block: self,
                    })
                    .map(InitRes::from);
                }
            }
        }
    }
}

#[inline]
fn is_power_of_two(n: usize) -> bool {
    n != 0 && (n & (n - 1)) == 0
}

#[test]
fn are_powers_of_two() {
    for power_of_two in (0..usize::BITS).map(|i| 1usize << i) {
        assert!(is_power_of_two(power_of_two))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn header_control_with(value: WrappedU64) -> HeaderControl {
        HeaderControl {
            take: WrappedAtomicU64::from(value),
            give: WrappedAtomicU64::from(value),
        }
    }

    #[test]
    fn update_max_or_compare_exchange_fetch_max_path() {
        let unbumped = WrappedU64::new(1, 0, 1).expect("valid starting value");
        let expected = match unbumped.bump_version_wrapping() {
            Ok(val) => val,
            Err(_) => panic!("should not wrap for small version"),
        };

        assert_eq!(
            expected,
            WrappedU64::new(1, 1, 0).expect("valid starting value")
        );

        let header = header_control_with(unbumped);

        header.update_max_or_compare_exchange(unbumped);

        for atomic in [&header.take, &header.give] {
            assert!(
                atomic.load(Ordering::Relaxed) == expected,
                "fetch_max path should bump version for both atomics"
            );
        }
    }

    #[test]
    fn update_max_or_compare_exchange_compare_exchange_path() {
        // bits=55 leaves a 1-bit version field, so version=1 is the maximum.
        let max_version = WrappedU64::max_version_for_bits(55);
        let unbumped = WrappedU64::new(55, max_version, 0).expect("max version fits bits");
        let expected = match unbumped.bump_version_wrapping() {
            Ok(_) => panic!("should wrap and force compare_exchange"),
            Err(val) => val,
        };

        let header = header_control_with(unbumped);

        header.update_max_or_compare_exchange(unbumped);

        for atomic in [&header.take, &header.give] {
            assert!(
                atomic.load(Ordering::Relaxed) == expected,
                "compare_exchange path should reset both atomics on wrap"
            );
        }
    }
}
