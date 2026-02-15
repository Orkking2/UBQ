mod cursor;
mod head;

pub mod debug;

mod ubq;
pub use ubq::*;

/*
UBQ_DEBUG_DIR=ubq_logs \
UBQ_DEBUG_FILE=bench_throughput_integrity.log \
UBQ_DEBUG_TAGS=push.,pop.,reset. \
UBQ_BENCH_TEST_ITEMS=200 \
UBQ_TEST_TIMEOUT_SECS=60 \
cargo test --features ubq_debug --test ubq_bench_instrumentation \
  ubq_bench::tests::throughput_integrity_smoke_all_paths \
  -- --nocapture --test-threads=1 */