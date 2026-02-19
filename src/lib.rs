//! A lock-free, unbounded multi-producer/multi-consumer queue backed by a linked
//! ring of fixed-size blocks.
//!
//! # Overview
//!
//! [`UBQ<T>`] is a **lock-free MPMC queue** with no upper bound on capacity.
//! Elements are stored in fixed-size *blocks* that form a circular ring; blocks
//! are allocated on demand and recycled once fully consumed.
//!
//! Every [`UBQ<T>`] handle is cheaply clonable — all clones share the same
//! underlying ring and may call [`UBQ::push`] and [`UBQ::pop`] concurrently from
//! any thread.  Reference counting ensures that the ring (and any unconsumed
//! elements) is freed when the last clone is dropped.
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
//! use std::thread;
//!
//! let q: UBQ<u64> = UBQ::new();
//! let q2 = q.clone();
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
//! Detailed correctness arguments for each invariant are annotated inline in the
//! source as `[C1]`–`[C6]`.

#![warn(missing_docs)]

pub(crate) mod block;
pub(crate) mod inner;
pub(crate) mod packed;
pub(crate) mod queue;

#[cfg(test)]
mod tests;

pub use packed::L;
pub use queue::UBQ;
