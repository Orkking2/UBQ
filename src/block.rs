use std::{
    cell::UnsafeCell,
    mem::MaybeUninit,
    ptr::null_mut,
    sync::atomic::{AtomicPtr, AtomicU8, AtomicUsize},
};

/// Power of the alignment.
#[cfg(any(
    all(feature = "ubq_block_31", feature = "ubq_block_63"),
    all(feature = "ubq_block_31", feature = "ubq_block_127"),
    all(feature = "ubq_block_31", feature = "ubq_block_255"),
    all(feature = "ubq_block_31", feature = "ubq_block_511"),
    all(feature = "ubq_block_31", feature = "ubq_block_1023"),
    all(feature = "ubq_block_31", feature = "ubq_block_2047"),
    all(feature = "ubq_block_31", feature = "ubq_block_4095"),
    all(feature = "ubq_block_63", feature = "ubq_block_127"),
    all(feature = "ubq_block_63", feature = "ubq_block_255"),
    all(feature = "ubq_block_63", feature = "ubq_block_511"),
    all(feature = "ubq_block_63", feature = "ubq_block_1023"),
    all(feature = "ubq_block_63", feature = "ubq_block_2047"),
    all(feature = "ubq_block_63", feature = "ubq_block_4095"),
    all(feature = "ubq_block_127", feature = "ubq_block_255"),
    all(feature = "ubq_block_127", feature = "ubq_block_511"),
    all(feature = "ubq_block_127", feature = "ubq_block_1023"),
    all(feature = "ubq_block_127", feature = "ubq_block_2047"),
    all(feature = "ubq_block_127", feature = "ubq_block_4095"),
    all(feature = "ubq_block_255", feature = "ubq_block_511"),
    all(feature = "ubq_block_255", feature = "ubq_block_1023"),
    all(feature = "ubq_block_255", feature = "ubq_block_2047"),
    all(feature = "ubq_block_255", feature = "ubq_block_4095"),
    all(feature = "ubq_block_511", feature = "ubq_block_1023"),
    all(feature = "ubq_block_511", feature = "ubq_block_2047"),
    all(feature = "ubq_block_511", feature = "ubq_block_4095"),
    all(feature = "ubq_block_1023", feature = "ubq_block_2047"),
    all(feature = "ubq_block_1023", feature = "ubq_block_4095"),
    all(feature = "ubq_block_2047", feature = "ubq_block_4095"),
))]
compile_error!(
    "Select at most one block-length feature: ubq_block_31|63|127|255|511|1023|2047|4095"
);

pub const LSHIFT: usize = if cfg!(feature = "ubq_block_31") {
    5
} else if cfg!(feature = "ubq_block_63") {
    6
} else if cfg!(feature = "ubq_block_127") {
    7
} else if cfg!(feature = "ubq_block_255") {
    8
} else if cfg!(feature = "ubq_block_511") {
    9
} else if cfg!(feature = "ubq_block_1023") {
    10
} else if cfg!(feature = "ubq_block_2047") {
    11
} else if cfg!(feature = "ubq_block_4095") {
    12
} else {
    11
};

/// Number of element slots per block.
pub const BLOCK_LENGTH: usize = (1 << LSHIFT) - 1;
/// Packed-head alignment/mask tuning target.
///
/// For `T = usize`, `size_of::<Slot<T>>() == 9` and this yields:
/// `8 (next) + 1 (ready) + 255 * 9 = 2304`, rounded to 2560 with `align(512)`.
pub const BLOCK_ALIGN: usize = 1 << (LSHIFT + 1);
pub const BLOCK_MASK: usize = BLOCK_ALIGN - 1;

/// A fixed-size ring-buffer segment.
#[cfg_attr(feature = "ubq_block_31", repr(align(64)))]
#[cfg_attr(feature = "ubq_block_63", repr(align(128)))]
#[cfg_attr(feature = "ubq_block_127", repr(align(256)))]
#[cfg_attr(feature = "ubq_block_255", repr(align(512)))]
#[cfg_attr(feature = "ubq_block_511", repr(align(1024)))]
#[cfg_attr(feature = "ubq_block_1023", repr(align(2048)))]
#[cfg_attr(feature = "ubq_block_2047", repr(align(4096)))]
#[cfg_attr(feature = "ubq_block_4095", repr(align(8192)))]
#[cfg_attr(
    not(any(
        feature = "ubq_block_31",
        feature = "ubq_block_63",
        feature = "ubq_block_127",
        feature = "ubq_block_255",
        feature = "ubq_block_511",
        feature = "ubq_block_1023",
        feature = "ubq_block_2047",
        feature = "ubq_block_4095"
    )),
    repr(align(4096))
)]
pub struct Block<T> {
    /// Link to the successor block used by producer-head advancement/recycling.
    pub next: AtomicPtr<Self>,
    /// Per-slot storage for values in this block.
    pub slots: [Slot<T>; BLOCK_LENGTH],
    /// How many elements have been consumed from this block.
    pub consumed: AtomicUsize,
}

const _: () = {
    assert!(BLOCK_ALIGN.is_power_of_two());
    // `chead` encodes as `2 * slot + phase_bit`, so index bits must fit mask.
    assert!(BLOCK_LENGTH * 2 + 1 <= BLOCK_MASK);
};

// Bits indicating the state of a slot:
// * If a value has been written into the slot, `WRITE` is set.
// * If a value has been read from the slot, `READ` is set.
// * If the block is being destroyed, `DESTROY` is set.
pub const WRITE: u8 = 1;
// pub const READ: u8 = WRITE << 1;
// pub const DESTROY: u8 = READ << 1;

pub struct Slot<T> {
    pub value: UnsafeCell<MaybeUninit<T>>,
    pub state: AtomicU8,
}

impl<T> Block<T> {
    pub fn new_zeroed() -> Box<Self> {
        unsafe { Box::new_zeroed().assume_init() }

        // let mut b = unsafe { Box::<Self>::new_uninit().assume_init() };

        // *b.next.get_mut() = null_mut();

        // b.array
        //     .iter_mut()
        //     .for_each(|slot| *slot.state.get_mut() = 0);

        // b
    }

    pub fn reset(&mut self) {
        *self.next.get_mut() = null_mut();
        *self.consumed.get_mut() = 0;

        self.slots
            .iter_mut()
            .filter_map(|Slot { state, .. }| {
                let m = state.get_mut();
                (*m != 0).then_some(m)
            })
            .for_each(|state| *state = 0);
    }
}

impl<T> Drop for Block<T> {
    fn drop(&mut self) {
        self.slots
            .iter_mut()
            // .map(CachePadded::deref_mut)
            // .filter_map(|Slot { value, state }| {
            //     (*state.get_mut() & (WRITE | READ) == WRITE).then_some(value)
            // })
            // Elements in the block are reserved densly from low to high.
            .skip(*self.consumed.get_mut())
            .filter_map(|Slot { value, state }| (*state.get_mut() == WRITE).then_some(value))
            .for_each(|value| unsafe { value.get_mut().assume_init_drop() });
    }
}
