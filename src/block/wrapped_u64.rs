use std::fmt::Debug;

/// |<-64-------------------->|\
/// |<-8-->|<-i----->|<-bits->| where i = 64 - 8 - bits\
/// +------+---------+--------+\
/// | bits | version | index  |\
/// +------+---------+--------+
#[derive(PartialEq, Eq, Clone, Copy)]
pub(crate) struct WrappedU64 {
    inner: u64,
}

#[derive(PartialEq, Eq, Clone, Copy)]
pub struct WrappedU64Components {
    pub bits: u8,
    pub version: u64,
    pub index: u64,
}

impl Debug for WrappedU64Components {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self {
            bits,
            version,
            index,
        } = self;

        write!(
            f,
            "{}({}):{}:{}",
            bits,
            WrappedU64::max_index_for_bits(*bits),
            version,
            index
        )
    }
}

impl Debug for WrappedU64 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.get_components().fmt(f)
    }
}

impl WrappedU64 {
    #[inline]
    pub const fn bits_for_capacity(capacity: usize) -> u8 {
        capacity
            .saturating_add(1)
            .next_power_of_two()
            .trailing_zeros() as u8
    }

    #[inline]
    pub(crate) const fn for_capacity(capacity: usize) -> Self {
        let bits = Self::bits_for_capacity(capacity);

        unsafe { Self::new_unchecked(bits, 0, 0) }
    }

    #[inline]
    pub(crate) const fn from_raw(inner: u64) -> Self {
        Self { inner }
    }

    #[inline]
    pub(crate) const fn get_raw(self) -> u64 {
        self.inner
    }

    #[inline]
    pub(crate) fn new(bits: u8, version: u64, index: u64) -> Option<Self> {
        Self::raw_from_components(bits, version, index).map(Self::from_raw)
    }

    #[inline]
    const unsafe fn new_unchecked(bits: u8, version: u64, index: u64) -> Self {
        Self::from_raw(Self::raw_from_components_unchecked(bits, version, index))
    }

    #[inline]
    pub(crate) const fn get_bits(self) -> u8 {
        self.get_components().bits
    }

    #[inline]
    pub(crate) const fn get_index(self) -> usize {
        self.get_components().index as usize
    }

    #[inline]
    pub(crate) const fn get_version(self) -> u64 {
        self.get_components().version
    }

    #[inline]
    pub(crate) const fn payload(self) -> u64 {
        self.inner & (u64::MAX >> u8::BITS)
    }

    /// (bits, index, version)
    #[inline]
    pub(crate) const fn get_components(self) -> WrappedU64Components {
        Self::components_from_raw(self.inner)
    }

    #[inline]
    pub(crate) const fn bump_index(self) -> Option<Self> {
        let WrappedU64Components { bits, index, .. } = self.get_components();

        let new_index = index + 1;

        if new_index <= Self::max_index_for_bits(bits) {
            Some(Self::from_raw(self.inner + 1))
        } else {
            None
        }
    }

    #[inline]
    pub(crate) fn set_bits(self, bits: u8) -> Option<Self> {
        let WrappedU64Components { version, index, .. } = self.get_components();

        Self::new(bits, version, index)
    }

    /// Ok(fetch_maxable), Err(must-CAS)
    #[inline]
    pub(crate) const fn bump_version_wrapping(self) -> Result<Self, Self> {
        let WrappedU64Components { bits, version, .. } = self.get_components();

        let new_version = version + 1;

        if Self::max_version_for_bits(bits) > new_version {
            Ok(unsafe { Self::new_unchecked(bits, new_version, 0) })
        } else {
            Err(unsafe { Self::new_unchecked(bits, 0, 0) })
        }
    }

    #[inline]
    const fn max_version_for_bits(bits: u8) -> u64 {
        (1u64 << (u64::BITS as u8 - u8::BITS as u8 - bits)) - 1
    }

    #[inline]
    pub(crate) const fn max_index_for_bits(bits: u8) -> u64 {
        (1u64 << bits) - 1
    }

    /// Packs `bits`, `version`, and `index` into the raw `u64` layout, returning `None` if any
    /// value violates the encoding invariants (bits > 56, or version/index exceed their masks).
    /// If those invariants are already guaranteed by the caller, use
    /// `raw_from_components_unchecked`, which is a branchless equivalent.
    #[inline]
    const fn raw_from_components(bits: u8, version: u64, index: u64) -> Option<u64> {
        if bits > u64::BITS as u8 - u8::BITS as u8 {
            return None;
        }

        let bits_u64 = bits as u64;
        let index_mask = Self::max_index_for_bits(bits);
        let version_mask = Self::max_version_for_bits(bits);

        if index & !index_mask != 0 || version & !version_mask != 0 {
            return None;
        }

        let payload = (version & version_mask) << bits_u64 | (index & index_mask);
        Some((bits_u64 << (u64::BITS - u8::BITS)) | payload)
    }

    /// Packs `bits`, `version`, and `index` into the raw `u64` layout without validating inputs.
    /// This is "unchecked" in the sense that it assumes `bits <= 56` and that `version` and
    /// `index` already fit the masks derived from `bits`; violating those invariants will produce
    /// a truncated/aliased payload rather than an error.
    #[inline]
    const fn raw_from_components_unchecked(bits: u8, version: u64, index: u64) -> u64 {
        let bits_u64 = bits as u64;
        let index_mask = Self::max_index_for_bits(bits);
        let version_mask = Self::max_version_for_bits(bits);
        let payload = (version & version_mask) << bits_u64 | (index & index_mask);
        (bits_u64 << (u64::BITS - u8::BITS)) | payload
    }

    #[inline]
    const fn components_from_raw(inner: u64) -> WrappedU64Components {
        let bits = (inner >> (u64::BITS - u8::BITS)) as u8;
        let payload = Self { inner }.payload();
        let index_mask = Self::max_index_for_bits(bits);
        let index = payload & index_mask;
        let version = payload >> bits;

        WrappedU64Components {
            bits,
            version,
            index,
        }
    }
}

#[cfg(test)]
mod tests {
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
                    let wrapped = WrappedU64::new(bits, version, index).unwrap_or_else(|| {
                        panic!("invalid input bits={bits}, version={version}, index={index}")
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
    fn capacity_fits_index() {
        fn assert_capacity(c: usize) {
            let bits = WrappedU64::for_capacity(c).get_bits();
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

    #[test]
    fn get_components_matches_accessors() {
        for bits in 0u8..=56 {
            let index_max = (1u64 << bits) - 1;
            let version_max = (1u64 << (56 - bits)) - 1;

            let mut index_candidates = vec![0u64];
            if index_max <= usize::MAX as u64 {
                index_candidates.push(index_max);
                if index_max > 1 {
                    index_candidates.push(index_max / 2);
                }
            } else {
                let usize_max = usize::MAX as u64;
                index_candidates.push(usize_max);
                if usize_max > 1 {
                    index_candidates.push(usize_max / 2);
                }
            }

            let mut version_candidates = vec![0u64, version_max];
            if version_max > 1 {
                version_candidates.push(version_max / 2);
            }

            index_candidates.sort_unstable();
            index_candidates.dedup();
            version_candidates.sort_unstable();
            version_candidates.dedup();

            for index in index_candidates.iter().copied() {
                for version in version_candidates.iter().copied() {
                    let wrapped = WrappedU64::new(bits, version, index).unwrap_or_else(|| {
                        panic!("invalid input bits={bits}, version={version}, index={index}")
                    });
                    let components = wrapped.get_components();

                    assert_eq!(
                        components.bits,
                        wrapped.get_bits(),
                        "bits mismatch for bits={bits}, version={version}, index={index}"
                    );
                    assert_eq!(
                        components.version,
                        wrapped.get_version(),
                        "version mismatch for bits={bits}, version={version}, index={index}"
                    );
                    assert_eq!(
                        components.index as u64,
                        wrapped.get_index() as u64,
                        "index mismatch for bits={bits}, version={version}, index={index}"
                    );
                }
            }
        }
    }

    #[test]
    fn raw_layout_manual_cases() {
        let cases = [
            (0u8, 0u64, 0u64, 0x0000_0000_0000_0000u64),
            (1u8, 1u64, 1u64, 0x0100_0000_0000_0003u64),
            (8u8, 0x1234_5678_9ABCu64, 0x5Au64, 0x0812_3456_789A_BC5Au64),
            (
                56u8,
                0u64,
                0x00FF_FFFF_FFFF_FFFFu64,
                0x38FF_FFFF_FFFF_FFFFu64,
            ),
        ];

        for (bits, version, index, raw) in cases {
            let wrapped = WrappedU64::new(bits, version, index).unwrap_or_else(|| {
                panic!("invalid input bits={bits}, version={version}, index={index}")
            });
            assert_eq!(
                raw,
                wrapped.get_raw(),
                "raw mismatch for bits={bits}, version={version}, index={index}"
            );

            let components = WrappedU64::from_raw(raw).get_components();
            assert_eq!(
                WrappedU64Components {
                    bits,
                    version,
                    index
                },
                components,
                "components mismatch for raw={raw:#x}"
            );
        }
    }

    #[test]
    fn raw_round_trip_boundaries() {
        for bits in [0u8, 1, 2, 7, 8, 9, 31, 32, 55, 56] {
            let index_max = if bits == 0 { 0 } else { (1u64 << bits) - 1 };
            let version_max = (1u64 << (56 - bits)) - 1;

            let index_candidates = [0u64, index_max, index_max / 2];
            let version_candidates = [0u64, version_max, version_max / 2];

            for index in index_candidates {
                for version in version_candidates {
                    let wrapped = WrappedU64::new(bits, version, index).unwrap();
                    let raw = wrapped.get_raw();
                    let from_raw = WrappedU64::from_raw(raw);
                    assert_eq!(
                        wrapped.get_components(),
                        from_raw.get_components(),
                        "round trip mismatch bits={bits}, version={version}, index={index}"
                    );
                    assert_eq!(
                        raw,
                        WrappedU64::new(bits, version, index).unwrap().get_raw()
                    );
                }
            }
        }
    }
}
