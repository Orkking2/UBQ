//! Public-contract acceptance tests for the in-progress v8 buffer API.
//! Queue workloads have a hard timeout under ordinary `cargo test` as well as
//! the repeated debug/release runner in scripts/test_v8_viability.py.
use ubq::v8::{Config, UBQ as Queue};

#[path = "support/v8_cases.rs"]
mod cases;
