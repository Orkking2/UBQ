//! A lock-free, unbounded multi-producer/multi-consumer queue backed by a linked
//! ring of fixed-size blocks.
//!
//! # Overview
//!
//! [`UBQ<T>`] is the default **lock-free MPMC queue** with no upper bound on
//! capacity.
//!
//! [`UBQ<T>`] itself is not clonable. To share it across threads, wrap it in
//! [`Arc<UBQ<T>>`](alloc::sync::Arc), then clone the `Arc`.
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
//!
//! # `no_std`
//!
//! Disable default features to use UBQ in a `no_std + alloc` environment. The
//! final application must provide a global allocator, and the target must have
//! native 8-bit and pointer-width atomic operations.
//!
//! ```toml
//! [dependencies]
//! ubq = { version = "5", default-features = false }
//! ```

#![cfg_attr(not(feature = "std"), no_std)]
#![warn(missing_docs)]

extern crate alloc;
#[cfg(test)]
extern crate std;

mod head;
mod page;
mod slot;

#[cfg(any(unix, windows, target_family = "wasm"))]
pub mod kfifo;

pub mod backoff;
#[cfg(feature = "bench_tools")]
pub mod bench_harness;
pub(crate) mod block;
#[cfg(feature = "jni")]
mod jni;
pub(crate) mod queue;

#[cfg(not(all(target_has_atomic = "8", target_has_atomic = "ptr")))]
compile_error!("ubq requires native 8-bit and pointer-width atomic operations");

#[cfg(test)]
mod tests;

pub use queue::UBQ;

/*
CARGO_TARGET_DIR=/home/nicolas/UBQ/target/vtune \
CARGO_PROFILE_RELEASE_DEBUG=2 \
RUSTFLAGS="-C force-frame-pointers=yes" \
cargo build --release \
  --features bench_registry,bench_rbbq,bench_lfqueue,bench_wcq \
  --bin bench_matrix \
  --bin bench_atomic_updates \
  --bin bench_head_reload
*/
