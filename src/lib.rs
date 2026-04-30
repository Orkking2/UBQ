//! A lock-free, unbounded multi-producer/multi-consumer queue backed by a linked
//! ring of fixed-size blocks.
//!
//! # Overview
//!
//! [`UBQ<T>`] is the default **lock-free MPMC queue** with no upper bound on
//! capacity.
//!
//! [`UBQ<T>`] itself is not clonable. To share it across threads, wrap it in
//! [`Arc<UBQ<T>>`](std::sync::Arc), then clone the `Arc`.
//!
//! Neither [`UBQ::push`] nor [`UBQ::pop`] ever parks the calling thread.  Both
//! operations are *lock-free*: producers and consumers make progress independently.
//! The only spin-waits occur at block boundaries, where a consumer briefly waits
//! for in-flight producers to commit their writes before claiming a slot.
//!
//! # Quick start
//!
//! ```rust
//! use ubq::UBQ;
//! use std::sync::Arc;
//! use std::thread;
//!
//! let q: Arc<UBQ<u64>> = UBQ::new_arc();
//! let q2 = Arc::clone(&q);
//!
//! let producer = thread::spawn(move || {
//!     for i in 0..1_000_u64 {
//!         q2.push(i);
//!     }
//! });
//!
//! producer.join().unwrap();
//!
//! for i in 0..1_000_u64 {
//!     assert_eq!(q.pop(), Some(i));
//! }
//! assert_eq!(q.pop(), None); // queue is now empty
//! ```
//!
//! # Internal design
//!
//! The queue maintains two atomic head pointers — **phead** (producer head) and
//! **chead** (consumer head) — each pointing into a circular ring of blocks.
//! Within each block, packed counters track claimed and committed producer/consumer
//! slots.  A consumer spins briefly on the *stability predicate* before claiming a
//! slot to guarantee it reads only fully-committed writes.
//!
//! Ordering and invariants are documented inline near the transitions they govern.

#![warn(missing_docs)]

pub mod align;
pub mod backoff;
pub mod bench_harness;
pub(crate) mod block;
pub(crate) mod queue;
pub mod sleepq;

#[cfg(test)]
mod tests;

pub use block::BLOCK_LENGTH;
pub use queue::{ConfiguredUBQ, DEFAULT_POOL_SIZE};
pub use sleepq::{NonBlockingQueue, SleepQ};

/// Default queue type alias that preserves the crate's legacy no-feature
/// configuration.
pub type UBQ<T> = ConfiguredUBQ<T>;

#[doc(hidden)]
#[macro_export]
macro_rules! __ubq_align_for_block {
    (31) => {
        $crate::align::A64
    };
    (63) => {
        $crate::align::A128
    };
    (127) => {
        $crate::align::A256
    };
    (255) => {
        $crate::align::A512
    };
    (511) => {
        $crate::align::A1024
    };
    (1023) => {
        $crate::align::A2048
    };
    (2047) => {
        $crate::align::A4096
    };
    (4095) => {
        $crate::align::A8192
    };
    ($other:expr) => {
        compile_error!(
            "ubq! requires an explicit `align:` when `block:` is not one of 31, 63, 127, 255, 511, 1023, 2047, or 4095"
        )
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! __ubq_internal {
    (
        @parse
        type = $ty:ty,
        backoff = $backoff:path,
        pool = $pool:tt,
        block = $block:tt,
        align = [];
        $(,)?
    ) => {
        $crate::ConfiguredUBQ::<
            $ty,
            $backoff,
            { $pool },
            { $block },
            $crate::__ubq_align_for_block!($block)
        >::new()
    };
    (
        @parse
        type = $ty:ty,
        backoff = $backoff:path,
        pool = $pool:tt,
        block = $block:tt,
        align = [$align:path];
        $(,)?
    ) => {
        $crate::ConfiguredUBQ::<$ty, $backoff, { $pool }, { $block }, $align>::new()
    };
    (
        @parse
        type = $ty:ty,
        backoff = $backoff:path,
        pool = $pool:tt,
        block = $block:tt,
        align = [$($align:tt)*];
        backoff: $new_backoff:path $(, $($rest:tt)*)?
    ) => {
        $crate::__ubq_internal!(
            @parse
            type = $ty,
            backoff = $new_backoff,
            pool = $pool,
            block = $block,
            align = [$($align)*];
            $($($rest)*)?
        )
    };
    (
        @parse
        type = $ty:ty,
        backoff = $backoff:path,
        pool = $pool:tt,
        block = $block:tt,
        align = [$($align:tt)*];
        pool: $new_pool:tt $(, $($rest:tt)*)?
    ) => {
        $crate::__ubq_internal!(
            @parse
            type = $ty,
            backoff = $backoff,
            pool = $new_pool,
            block = $block,
            align = [$($align)*];
            $($($rest)*)?
        )
    };
    (
        @parse
        type = $ty:ty,
        backoff = $backoff:path,
        pool = $pool:tt,
        block = $block:tt,
        align = [$($align:tt)*];
        block: $new_block:tt $(, $($rest:tt)*)?
    ) => {
        $crate::__ubq_internal!(
            @parse
            type = $ty,
            backoff = $backoff,
            pool = $pool,
            block = $new_block,
            align = [$($align)*];
            $($($rest)*)?
        )
    };
    (
        @parse
        type = $ty:ty,
        backoff = $backoff:path,
        pool = $pool:tt,
        block = $block:tt,
        align = [$($align:tt)*];
        align: $new_align:path $(, $($rest:tt)*)?
    ) => {
        $crate::__ubq_internal!(
            @parse
            type = $ty,
            backoff = $backoff,
            pool = $pool,
            block = $block,
            align = [$new_align];
            $($($rest)*)?
        )
    };
    (
        @parse
        type = $ty:ty,
        backoff = $backoff:path,
        pool = $pool:tt,
        block = $block:tt,
        align = [$($align:tt)*];
        $unknown:ident : $value:tt $(, $($rest:tt)*)?
    ) => {
        compile_error!(concat!("unsupported ubq! option `", stringify!($unknown), "`"));
    };
}

/// Creates a [`ConfiguredUBQ`] with compile-time-selected defaults and policies.
///
/// Built-in block lengths `31`, `63`, `127`, `255`, `511`, `1023`, `2047`, and
/// `4095` automatically select the matching alignment marker. Other block
/// lengths require an explicit `align:` marker.
///
/// ```rust
/// let q = ubq::ubq!(type: u64, pool: 2, block: 127);
/// q.push(7);
/// assert_eq!(q.pop(), Some(7));
/// ```
#[macro_export]
macro_rules! ubq {
    (type: $ty:ty $(, $($rest:tt)*)?) => {
        $crate::__ubq_internal!(
            @parse
            type = $ty,
            backoff = $crate::backoff::Crossbeam,
            pool = 1,
            block = 2047,
            align = [];
            $($($rest)*)?
        )
    };
}
