# v8 correctness milestone

The first viability milestone is **functional correctness and bounded
completion**, as agreed with the author. The gate is:

```sh
python3 scripts/test_v8_viability.py
```

It builds the actual `ubq::v8::UBQ` integration target, runs every required case
three times in debug and release, and returns nonzero for build failures,
assertion failures, crashes, timeouts, missing cases, or ignored tests. Each
case runs in a fresh process with a 20-second deadline. This includes time
inside queue operations and thread joins, where an in-thread deadline cannot
interrupt a stuck operation. Other cases still run after a runtime failure.
A build failure stops before execution; no test pass is inferred from it.

Results, commands, durations, and captured diagnostics are written to
`target/v8-viability/v8.json`. A green report is acceptance of these observed
workloads on the tested build/machine. It does not establish lock-freedom,
exhaustive concurrency correctness, memory safety, or a performance target.

## Contract under test

These tests follow the current signatures in `src/v8/queue.rs`:

- `push_batch(Vec<T>) -> Vec<T>` moves all values and returns the original
  allocation with length zero, retaining its pointer and capacity.
- `pop_batch(Box<[MaybeUninit<T>]>) -> Vec<T>` transfers ownership into the
  supplied buffer, retaining its allocation and capacity for non-ZST values.
  A quiescent queue returns `min(buffer length, available values)`, consistent
  with the earlier batch tests. Zero-length requests are no-ops.
- Scalar operations are FIFO; sequential operations match `VecDeque`.
  Concurrent consumers must receive each value exactly once. One consumer
  observing concurrent producers must see each producer's program order.
  The suite does not require a whole batch to be atomic.
- Popping transfers ownership; queue teardown drops only remaining values.
  Emptying/refilling and flushing cannot damage live values. A second flush
  with no intervening queue operations returns zero. The tests do not require
  a particular number of cached allocations or a specific reclamation policy.
- Zero-sized types with destructors, over-aligned types, and `Send + !Sync`
  values are supported.

The buffer allocation contract is intentional. The obsolete `tests/v8.rs`
was removed after its behavioral coverage was superseded by this suite; its
unique geometry checks now live in `src/v8/tests.rs`. Private freelist tests
live in `src/v8/tests/freelist.rs`. No tests depend on the removed `push_slice`,
`pop_batch_into`, or private reservation helpers.

## Coverage

| Case | Failure it should expose |
| --- | --- |
| Empty operations | Phantom items, nonempty push return, zero-size reservation side effects |
| Scalar FIFO / refill | Skipped or repeated slots and stale heads after empty periods |
| Batch boundaries | Off-by-one reservations at virtual and physical boundaries, exact-full batches, multi-block copies |
| Seeded `VecDeque` comparison | Mixed scalar/batch/flush sequences with reproducible seed and step |
| Partial drain / teardown | Premature destruction, leaked payloads, repeated destruction |
| Special payload types | ZST destructor count errors, alignment errors, unnecessarily requiring `Sync` |
| Flush with live data | Freeing active values or corrupting refill behavior |
| Concurrent delivery | Loss, duplication, corrupt IDs, ownership errors, stalled completion in 1P1C, 4P1C, 1P4C, 4P4C |
| Concurrent producer order | Reordering within one producer's sequence with a single observing consumer |

Small geometries include `1x1` and non-powers of two. Batches land immediately
before, exactly on, and immediately after virtual/physical boundaries, also
starting at nonzero offsets. Concurrent tests mix scalar and batch operations
while a separate caller flushes the freelist. Scheduling is nondeterministic;
seeded sequential workloads are deterministic. Repetition broadens observations
but does not exhaust schedules.

## Validate the framework independently

```sh
python3 -m unittest discover -s scripts -p test_v8_viability_runner.py
python3 scripts/test_v8_viability.py --reference
```

The first command injects nonzero exits, a killed child, a sleeping child,
zero-test success, ignored-test success, and missing cases. It also confirms
that a real one-test success is accepted and another case runs after timeout.

The second compiles a standalone `Mutex<VecDeque<T>>` control using `rustc` and
runs the **same Rust assertions**. It does not import or build libubq. Its
separate report, `target/v8-viability/reference.json`, is labelled reference
only and cannot turn the v8 gate green. This validates the fixtures independently
of v8's current failures. There are no new Cargo dependencies or features.

For focused development:

```sh
python3 scripts/test_v8_viability.py --profile debug --repeat 1 --timeout 10
```

Use the default command for milestone evidence. `--report PATH` retains a
particular run; `--build-timeout SECONDS` adjusts compilation's deadline.
Ordinary `cargo test` also runs the acceptance suite. Each queue workload runs
in a child process with a 20-second deadline; a panic, crash, or timeout becomes
a failing Cargo test, and sibling tests can finish. Three additional harness
tests exercise worker success, assertion reporting, and timeout/reaping. No
acceptance tests are ignored or marked `should_panic` to mask queue failures.

The Python runner executes each workload directly in its own child, avoiding
a nested worker, and uses its configurable timeout. Its build runs offline,
so missing cached dependencies correctly produce a build failure.

## Current fixes and remaining protocol review

The mechanical-fix pass repairs ownership transfer in `BlockList::add_assign`,
including an empty right-hand list. It also fixes the initial read reservation
to use the output capacity, makes empty batch operations no-ops, uses the
sentinel-inclusive token stride on both sides, and measures the producer's
current offset in slots rather than virtual-block counts.

Consumer retry paths reload the consumer head. Boundary traversal follows the
claimed block chain instead of sampling a producer pointer that could have
advanced after the producer-index load. Producers capture the successor before
publishing their segment, so publication remains their final block access.

`UBQ::drop` reclaims the remaining active chain from the consumer head. Focused
teardown tests cover unread batches (including exact-full batches), a partially
consumed chain, and an advanced head with a separately cached predecessor.

These fixes preserve the current reservation and publication algorithm. The
following need further protocol review before stronger correctness claims:

1. **Head transitions and index overflow.** Establish the coherent
   pointer/index observation rules, token-wrap behavior, and how stalled
   participants interact with address reuse.
2. **Partial publication.** Prove that the produced-counter/producer-index
   comparison cannot accept an out-of-order total as an initialized prefix.
3. **Reclamation.** Establish that every participant's final block access
   precedes reclamation, and that completed operations leave the remaining
   consumer chain valid and disjoint from cached blocks.

For each agreed change: first add a failing regression, make the smallest
implementation change, then run the full milestone. Keep the tests on the
public API where possible; the reference queue is a fixture control, not a
replacement algorithm.

Before stronger claims, add deterministic pause hooks for out-of-order
publication, readers paused before completion, split-head transitions,
preallocated unreserved successors, and address reuse. Those hooks require
agreement on the new protocol; the obsolete private reservation tests were
removed with their helpers. Follow with memory-model exploration,
Miri/sanitizers where available, index-wrap cases, and allocation/unwind
analysis. `cargo check --no-default-features --lib` checks the `no_std` build.
Full linearizability checking, lock-free progress, and comparative performance
remain separate milestones. No checks in this list are silently counted as
passed by the current gate.
