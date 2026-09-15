//! Experimental UBQ reservations over small, aligned virtual blocks.
//!
//! Each allocation groups several fixed-capacity vblocks. Publication and
//! completion use per-vblock counters; payload slots carry no atomic state.
//! Packed heads use the alignment computed from the queue's geometry.
//!
//! Like UBQ, a reservation that crosses a block boundary temporarily holds
//! that head at an end marker while linking or traversing its successor.
//! A stalled boundary owner or unpublished reservation can delay other calls;
//! this experiment does not promise lock-free progress.
//!
//! Every wait goes through the queue's [`BackoffPolicy`](crate::backoff::BackoffPolicy)
//! type parameter, [`Crossbeam`](crate::backoff::Crossbeam) by default, exactly
//! as for [`ubq::UBQ`](crate::UBQ).
//!
//! ```
//! use ubq::dynblock::{Config, UBQ};
//!
//! let queue = UBQ::<String>::with_config(Config {
//!     slots_per_vblock: 32,
//!     vblocks_per_block: 16,
//! }).unwrap();
//! let spare = queue.push_batch(vec![String::from("first"), String::from("second")]);
//! assert!(spare.is_empty());
//! assert_eq!(queue.pop_batch(Box::<[String]>::new_uninit_slice(8)), ["first", "second"]);
//! assert_eq!(queue.pop(), None);
//! ```

mod block;
mod head;
pub mod queue;

pub use queue::{Config, ConfigError, UBQ};
