//! Experimental UBQ v8: runtime block geometry, virtual-block
//! publication, contiguous batch transfers, and a queue-owned freelist.
//!
//! The reservation and reclamation protocol is still under development.
//! Correctness and lock-free progress have not yet been established.
//! The existing [`crate::UBQ`] remains the default queue.
//!
//! ```
//! use ubq::v8::{Config, UBQ};
//!
//! let queue = UBQ::with_config(Config {
//!     slots_per_vblock: 32,
//!     vblocks_per_block: 8,
//! }).unwrap();
//! queue.push_batch(vec![String::from("first"), String::from("second")]);
//! assert_eq!(queue.pop_batch(Box::<[String]>::new_uninit_slice(8)), ["first", "second"]);
//! assert_eq!(queue.pop(), None);
//! queue.flush_freelist();
//! ```
//!
//! See `src/v8/DESIGN.md` for the source layout and current protocol notes.

mod block;
mod freelist;
pub mod queue;

#[cfg(test)]
mod tests;

pub use queue::{Config, ConfigError, UBQ};
