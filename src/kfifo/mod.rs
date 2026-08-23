//! Experimental linked-shard channel.
//!
//! [`channel`] creates one [`Sender`] with a private queue shard and one
//! [`Receiver`]. Cloning a sender reuses an inactive empty shard or appends a
//! permanent one; cloned receivers visit the stable shard list independently
//! in round-robin order.

mod head;
mod lubq;
#[allow(dead_code)]
mod queue;

pub use lubq::{Receiver, Sender, TryRecvError, channel, channel_with};
