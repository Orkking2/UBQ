//! A lock-free, unbounded multi-producer/multi-consumer queue backed by a linked
//! ring of fixed-size blocks.
//!
//! # Overview
//!
//! [`UBQ<T>`] is a **lock-free MPMC queue** with no upper bound on capacity.
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

#[cfg(feature = "ubq_backoff_cq")]
pub(crate) mod backoff;
pub(crate) mod block;
pub(crate) mod queue;

#[cfg(test)]
mod tests;

pub use block::BLOCK_LENGTH;
pub use queue::UBQ;
