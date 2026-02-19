use crate::packed::{A, F, L, high, low};
use crossbeam_utils::CachePadded;
use std::{cell::UnsafeCell, mem::MaybeUninit, sync::atomic::AtomicPtr};

/// A fixed-size ring-buffer segment.
///
/// `p` and `c` each pack two [`H`] counters into a [`A`] using the upper and lower
/// `H::BITS` bits respectively; see `high()`, `low()`, and `merge()`.
pub struct B<T> {
    /// Pointer to the next block in the ring.
    pub n: CachePadded<AtomicPtr<Self>>,
    /// Producer counter. `high(p)` = slots claimed; `low(p)` = slots committed.
    pub p: CachePadded<A>,
    /// Consumer counter. `high(c)` = slots claimed; `low(c)` = slots committed.
    /// The sentinel value `F::MAX` means the block is not yet open to consumers.
    pub c: CachePadded<A>,
    /// Element storage. Slot `i` is written by the unique producer that claims index
    /// `i` and read by the unique consumer that claims index `i`. See [C6].
    pub a: [UnsafeCell<MaybeUninit<T>>; L as usize],
}

impl<T> Drop for B<T> {
    fn drop(&mut self) {
        let (p, c) = unsafe { (*self.p.as_ptr(), *self.c.as_ptr()) };

        debug_assert!(
            high(p) == low(p) || low(p) == L,
            "all producers should be finished before dropping B (p = {}:{})",
            high(p),
            low(p)
        );
        debug_assert!(
            high(c) == low(c) || low(c) == L,
            "all consumers should be finished before dropping B (c = {}:{})",
            high(c),
            low(c),
        );

        // Drop unconsumed elements in this block. The live range of
        // initialized, unconsumed slots is:
        //	 · [0, 0) 			  when p == F::MAX: block is marked
        //	   for destruction during queue operation; it is guaranteed
        //     to be empty.
        //
        //   · [0, low(p))        when c == F::MAX: block was never
        //     opened to consumers; every produced value is unconsumed.
        //
        //   · [low(c), low(p))   else: low(c) committed consumer
        //     reads have already moved [0, low(c)) out; the remaining
        //     range [low(c), low(p)) holds initialized unconsumed Ts.
        let z = |u: F| if u == F::MAX { 0 } else { low(u) };

        for i in z(c)..z(p) {
            unsafe {
                self.a
                    .get_unchecked_mut(i as usize)
                    .get_mut()
                    .as_mut_ptr()
                    .drop_in_place();
            }
        }
    }
}
