// Minimal extern "C" shim over moodycamel::ConcurrentQueue<uint64_t>, so the
// Rust side (src/bench_harness/baselines/moodycamel_cq.rs) can drive it
// through a handful of primitive-typed FFI calls over an opaque handle.
//
// Deliberately monomorphized to uint64_t only: values that don't fit (e.g.
// LogRecord) are boxed on the Rust side and passed through as a pointer cast
// to uint64_t, the same technique already used elsewhere in this benchmark
// harness (see bench_data_latency_with_queue in src/bench_harness/mod.rs).
//
// Every enqueue/dequeue call here goes through a ProducerToken or
// ConsumerToken rather than the token-free enqueue()/try_dequeue() API.
// moodycamel's own header documents this as the intended usage for known,
// persistent threads (concurrentqueue.h's usage-guidance comment: "ideally
// there should be a maximum of one token per thread (of each kind)") —
// without a token, every call from a thread that hasn't touched this queue
// before contends on a lock to register as an "implicit producer", which is
// the actual bottleneck under many concurrent threads, not a property of the
// algorithm itself. The Rust side creates exactly one token pair per
// benchmark thread, once, before that thread's timed loop starts.

#include <cstddef>
#include <cstdint>

#include "concurrentqueue.h"

using MoodycamelQueue = moodycamel::ConcurrentQueue<std::uint64_t>;
using ProducerTokenType = moodycamel::ProducerToken;
using ConsumerTokenType = moodycamel::ConsumerToken;

extern "C" {

void *mc_queue_new() { return new MoodycamelQueue(); }

void mc_queue_free(void *handle) {
  delete static_cast<MoodycamelQueue *>(handle);
}

void *mc_queue_create_producer_token(void *handle) {
  return new ProducerTokenType(*static_cast<MoodycamelQueue *>(handle));
}

void mc_queue_free_producer_token(void *token) {
  delete static_cast<ProducerTokenType *>(token);
}

bool mc_queue_enqueue_with_token(void *handle, void *token,
                                  std::uint64_t value) {
  return static_cast<MoodycamelQueue *>(handle)->enqueue(
      *static_cast<ProducerTokenType *>(token), value);
}

bool mc_queue_enqueue_bulk_with_token(void *handle, void *token,
                                       const std::uint64_t *items,
                                       std::size_t count) {
  return static_cast<MoodycamelQueue *>(handle)->enqueue_bulk(
      *static_cast<ProducerTokenType *>(token), items, count);
}

void *mc_queue_create_consumer_token(void *handle) {
  return new ConsumerTokenType(*static_cast<MoodycamelQueue *>(handle));
}

void mc_queue_free_consumer_token(void *token) {
  delete static_cast<ConsumerTokenType *>(token);
}

bool mc_queue_try_dequeue_with_token(void *handle, void *token,
                                      std::uint64_t *out) {
  return static_cast<MoodycamelQueue *>(handle)->try_dequeue(
      *static_cast<ConsumerTokenType *>(token), *out);
}

std::size_t mc_queue_try_dequeue_bulk_with_token(void *handle, void *token,
                                                  std::uint64_t *out,
                                                  std::size_t max) {
  return static_cast<MoodycamelQueue *>(handle)->try_dequeue_bulk(
      *static_cast<ConsumerTokenType *>(token), out, max);
}

} // extern "C"
