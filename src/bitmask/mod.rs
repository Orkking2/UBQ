//! Experimental UBQ with 64-slot virtual blocks published through bitmasks.
//!
//! Each VBlock holds exactly [`SLOTS`] slots and two 64-bit atomic masks. A
//! producer publishes the slots it filled with one `fetch_or` on `produced`;
//! a consumer reads a slot as soon as its bit is set, in order, and completes
//! what it read with one `fetch_or` on `consumed`. Consumers never consult
//! the producer's reservation index to decide whether a slot is initialized.
//!
//! Blocks reach the chain through an attach-all freelist: the producer index
//! counts the blocks attached after the producer's block, so a reservation
//! that fits in that runway never touches the freelist, and the producer that
//! crosses a block boundary attaches everything the freelist holds.
//!
//! Every wait goes through the queue's [`BackoffPolicy`](crate::backoff::BackoffPolicy)
//! type parameter, [`Crossbeam`](crate::backoff::Crossbeam) by default, exactly
//! as for [`ubq::UBQ`](crate::UBQ).
//!
//! ```
//! use ubq::bitmask::{Config, UBQ};
//!
//! let queue = UBQ::<String>::with_config(Config {
//!     slots_per_vblock: 64,
//!     vblocks_per_block: 2,
//! }).unwrap();
//! let spare = queue.push_batch(vec![String::from("first"), String::from("second")]);
//! assert!(spare.is_empty());
//! assert_eq!(queue.pop_batch(Box::<[String]>::new_uninit_slice(8)), ["first", "second"]);
//! assert_eq!(queue.pop(), None);
//! queue.flush_freelist();
//! ```
//!
//! See `src/bitmask/DESIGN.md` for the protocol.

mod block;
mod freelist;
pub mod queue;

#[cfg(test)]
mod tests;

pub use queue::{Config, ConfigError, SLOTS, UBQ};
