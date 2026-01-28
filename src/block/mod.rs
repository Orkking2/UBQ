use std::{cell::UnsafeCell, iter, mem::MaybeUninit, sync::atomic::Ordering};

#[cfg(feature = "ubq_debug")]
use std::fmt::Debug;

mod atomic;
mod header;
mod wrapped_u64;

use atomic::WrappedAtomicU64;
use header::HeaderControl;

pub use wrapped_u64::WrappedU64Components;

use crate::block::wrapped_u64::WrappedU64;
use crate::cache_padded::CachePadded;
#[cfg(feature = "ubq_debug")]
use crate::block::header::HeaderConDebug;
#[cfg(feature = "ubq_debug")]
use atomic_list::sync::Node;

#[non_exhaustive]
#[derive(Debug)]
pub enum ReserveError {
    NoEntry,
    NotAvailable,
    BlockDone,
}

struct Reservation<'a, T> {
    block: &'a Block<T>,
    index: usize,
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

    pub fn get_idx(&self) -> usize {
        self.index
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

    pub fn get_idx(&self) -> usize {
        self.inner.get_idx()
    }
}

impl<'a, T> Drop for UninitRes<'a, T> {
    fn drop(&mut self) {
        self.inner
            .block
            .pcon
            .give
            .fetch_add(1, Ordering::Release);
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
        self.inner
            .block
            .ccon
            .give
            .fetch_add(1, Ordering::Release);
    }
}

pub struct Block<T> {
    pcon: CachePadded<HeaderControl>,
    ccon: CachePadded<HeaderControl>,

    array: Box<[UnsafeCell<MaybeUninit<T>>]>,
}

#[cfg(feature = "ubq_debug")]
pub struct BlockDebug<T> {
    self_ptr: *const Block<T>,
    len: usize,
    pcon: HeaderConDebug,
    ccon: HeaderConDebug,
}

#[cfg(feature = "ubq_debug")]
impl<T> Debug for BlockDebug<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{:p}: {{l: {}, p: {:?}, c: {:?}}}",
            self.self_ptr, self.len, self.pcon, self.ccon
        )
    }
}

impl<T> Block<T> {
    pub fn new(size: usize, max_access: usize) -> Self {
        Self {
            pcon: CachePadded::new(HeaderControl::for_capacity(size + max_access)),
            ccon: CachePadded::new(HeaderControl::for_capacity(size + max_access)),

            array: iter::from_fn(|| Some(UnsafeCell::new(MaybeUninit::uninit())))
                .take(size)
                .collect(),
        }
    }

    #[inline]
    pub const fn len(&self) -> usize {
        self.array.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.pcon
            .take
            .load(Ordering::Acquire)
            .get_index()
            <= self
                .ccon
                .take
                .load(Ordering::Acquire)
                .get_index()
    }

    pub fn allocate(&self) -> Option<UninitRes<'_, T>> {
        // Every thread is guaranteed an extra index capacity to account for overflow. A thread will never
        // call this function, receive a BlockDone, and then immediately re-call this function, as that
        // could overflow into a version increment. This is because when a thread receives a BlockDone error
        // during allocation, it will try to allocate a new block.
        self.pcon
            .take
            .fetch_update(
                Ordering::AcqRel,
                Ordering::Acquire,
                |current| {
                    (current.get_index() < self.len()).then(|| {
                        current
                            .bump_index()
                            .expect("Bumping index should stay within bounds")
                    })
                },
            )
            .ok()
            .map(|old| {
                let index = old.get_index();

                UninitRes {
                    inner: Reservation { index, block: self },
                }
            })
    }

    pub fn reserve(&self) -> Result<InitRes<'_, T>, ReserveError> {
        loop {
            let reserved = self.ccon.take.load(Ordering::Acquire);

            if reserved.get_index() >= self.len() {
                #[cfg(feature = "ubq_debug")]
                log::warn!(
                    "BlockDone (reserved.get_index() = {}/{})",
                    reserved.get_index(),
                    self.len()
                );
                break Err(ReserveError::BlockDone);
            } else {
                // All previous writes in this block must be visible before this load.
                let committed = self.pcon.give.load(Ordering::Acquire);

                if committed.get_version() < reserved.get_version() {
                    #[cfg(feature = "ubq_debug")]
                    log::warn!(
                        "BlockDone (committed.get_version() {} < {} reserved.get_version())",
                        committed.get_version(),
                        reserved.get_version()
                    );
                    break Err(ReserveError::BlockDone);
                }

                if committed.get_index() == reserved.get_index() {
                    // If the block is brand new (no producer progress, no consumer progress),
                    // treat it as empty so consumers don't skip a block that may later be produced into.
                    if committed.payload() == 0 && reserved.payload() == 0 {
                        #[cfg(feature = "ubq_debug")]
                        log::warn!(
                            "NoEntry (brand new block: committed.payload() {} | {} reserved.payload())",
                            committed.payload(),
                            reserved.payload()
                        );
                        break Err(ReserveError::NoEntry);
                    }
                    #[cfg(feature = "ubq_debug")]
                    log::warn!(
                        "NoEntry (committed.get_index() {} == {} reserved.get_index())",
                        committed.get_index(),
                        reserved.get_index()
                    );
                    break Err(ReserveError::NoEntry);
                }

                if committed.get_index() != self.len() {
                    let allocated = self.pcon.take.load(Ordering::Acquire);

                    if allocated.get_index() != committed.get_index() {
                        #[cfg(feature = "ubq_debug")]
                        log::warn!(
                            "NotAvailable (allocated.get_index() {} != {} committed.get_index())",
                            allocated.get_index(),
                            committed.get_index()
                        );
                        break Err(ReserveError::NotAvailable);
                    }
                }

                if self.ccon.take.fetch_max(
                    reserved.bump_index().unwrap(),
                    Ordering::AcqRel
                ) == reserved
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

    pub fn reset_pcon(&self) -> bool {
        let (pgive, cgive) = (
            self.pcon.give.load(Ordering::Acquire),
            self.ccon.give.load(Ordering::Acquire),
        );

        let WrappedU64Components {
            version: cgive_vsn,
            index: cgive_idx,
            ..
        } = cgive.get_components();

        let pgive_vsn = pgive.get_version();
        let len = self.len();

        if pgive_vsn < cgive_vsn || (pgive_vsn == cgive_vsn && cgive_idx as usize >= len) {
            self.pcon.update_max_or_compare_exchange(pgive);
            true
        } else {
            false
        }
    }

    pub fn reset_ccon(&self) -> bool {
        let cgive = self.ccon.give.load(Ordering::Acquire);

        if cgive.get_index() >= self.len() {
            self.ccon.update_max_or_compare_exchange(cgive);
            true
        } else {
            false
        }
    }

    #[cfg(feature = "ubq_debug")]
    pub fn debug(this: &Node<Self>) -> BlockDebug<T> {
        BlockDebug {
            self_ptr: Node::as_ptr(this),
            len: this.len(),
            pcon: this.pcon.debug(),
            ccon: this.ccon.debug(),
        }
    }

    #[inline]
    pub fn not_available_for_producing(&self) -> bool {
        !self.available_for_producing()
    }

    #[inline]
    pub fn available_for_producing(&self) -> bool {
        let [cgive, ctake, pgive, ptake] = self.get_all_wrapped_atomics();

        let [cgive, ctake, pgive, ptake] = [
            cgive.load(Ordering::Relaxed),
            ctake.load(Ordering::Relaxed),
            pgive.load(Ordering::Relaxed),
            ptake.load(Ordering::Relaxed),
        ];

        let all_equal = pgive == ptake && pgive == cgive && pgive == ctake;

        if all_equal {
            return true;
        }

        let len = self.len();
        let cgive_vsn = cgive.get_version();
        let pgive_vsn = pgive.get_version();

        pgive_vsn < cgive_vsn || (pgive_vsn == cgive_vsn && cgive.get_index() >= len)
    }

    pub(crate) fn set_max_access(&self, max_access: usize) {
        for atomic in self.get_all_wrapped_atomics().into_iter() {
            atomic.set_bits_for_capacity(self.len() + max_access)
        }
    }

    pub(crate) fn is_brand_new(&self) -> bool {
        let [cgive, ctake, pgive, ptake] = self.get_all_wrapped_atomics();
        let [cgive, ctake, pgive, ptake] = [
            cgive.load(Ordering::Acquire),
            ctake.load(Ordering::Acquire),
            pgive.load(Ordering::Acquire),
            ptake.load(Ordering::Acquire),
        ];

        [cgive, ctake, pgive, ptake]
            .into_iter()
            .map(WrappedU64::payload)
            .all(|payload| payload == 0)
    }

    fn get_all_wrapped_atomics(&self) -> [&WrappedAtomicU64; 4] {
        [
            &self.ccon.give,
            &self.ccon.take,
            &self.pcon.give,
            &self.pcon.take,
        ]
    }
}

#[test]
fn test() {
    // {l: 256, p: {t: 9(511):0:256, g: 9(511):0:256}, c: {t: 9(511):0:256, g: 9(511):0:256}, resulting in
    // {l: 256, p: {t: 9(511):1:256, g: 9(511):1:256}, c: {t: 9(511):0:256, g: 9(511):0:256}

    let (
        pgive,
        WrappedU64Components {
            version: cgive_vsn,
            index: cgive_idx,
            ..
        },
    ) = (
        WrappedU64::new(9, 0, 256).unwrap(),
        WrappedU64Components {
            bits: 9,
            version: 0,
            index: 256,
        },
    );

    let pgive_vsn = pgive.get_version();
    let len = 256;

    if pgive_vsn < cgive_vsn || (pgive_vsn == cgive_vsn && cgive_idx as usize >= len) {
        match pgive.bump_version_wrapping() {
            Ok(fetch) => println!("Ok({:?})", fetch),
            Err(cas) => println!("Err({:?})", cas),
        }
    } else {
        println!(
            "The following is false: pgive_vsn ({pgive_vsn}) < ({cgive_vsn}) cgive_vsn || (pgive_vsn == cgive_vsn && cgive_idx ({cgive_idx}) >= ({len}) len"
        );
    }
}
