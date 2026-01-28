# UBQ

[![Crates.io](https://img.shields.io/crates/v/ubq.svg)](https://crates.io/crates/ubq)
[![Docs.rs](https://docs.rs/ubq/badge.svg)](https://docs.rs/ubq)

UBQ is a lock-free queue built from linked blocks, intended for concurrent producers and consumers.

## Usage

Add UBQ to your Cargo manifest:

```toml
ubq = "0.1"
```

A minimal example that allocates a small ring of blocks and moves a value through it:

```rust
use ubq::{UBQ, UBQBounds};

fn main() {
    let mut queue = UBQ::new(2, UBQBounds { min: 2, max: 3 });
    queue.push("hello").unwrap();
    assert_eq!(queue.pop().unwrap(), "hello");
}
```

See the full API docs on [docs.rs](https://docs.rs/ubq) and the crate page on [crates.io](https://crates.io/crates/ubq).
