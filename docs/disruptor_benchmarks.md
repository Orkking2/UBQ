# LMAX Disruptor Benchmark Adapter

This repository includes a JNI adapter for running UBQ from the official LMAX
Disruptor performance tests. It is meant for the BBQ-paper-style comparison
against `OneToOneThroughputTest`, `ThreeToOneThroughputTest`, and
`OneToThreeThroughputTest`.

The adapter exports a small static registry of native UBQ variants:

- `balanced,0,127,crossbeam`
- `balanced,4,127,crossbeam`
- `balanced,8,63,crossbeam`
- `balanced,8,127,crossbeam`
- `balanced,8,255,crossbeam`
- `balanced,16,127,crossbeam`
- `balanced,32,127,crossbeam`
- `balanced,8,31,crossbeam`
- `balanced,8,511,crossbeam`
- `balanced,8,127,yield`

Each measured queue still uses a monomorphic Rust queue type inside the native
hot loop. The Java wrapper selects one variant when the native handle is
created, so there is no per-item variant dispatch inside the benchmark loop.

## Build UBQ JNI

```bash
cargo build --release --features jni
```

The dynamic library is emitted under `target/release` as `libubq.dylib` on
macOS or `libubq.so` on Linux.

To include the same RBBQ/FastFifo backend used by this repository's Rust
benchmark harness, build with:

```bash
cargo build --release --features jni,bench_fastfifo
```

## Java Wrapper

The Java wrapper lives under `bindings/disruptor-jni/src/main/java`:

- `ubq.jni.UbqLongQueue`: primitive `long` API for benchmark edits.
- `ubq.jni.UbqBlockingQueue`: `BlockingQueue<Long>` adapter for quick
  queue-baseline experiments.
- `ubq.jni.RbbqLongQueue`: primitive wrapper for `rbbq::FastFifo`, available
  when `libubq` is built with `bench_fastfifo`.

`UbqLongQueue` defaults to `balanced,8,127,crossbeam`. Select another supported
variant with `-Dubq.jni.ubqVariant=<label>` or by constructing
`new UbqLongQueue("<label>")`.

Compile it with:

```bash
mkdir -p target/disruptor-jni/classes
javac -d target/disruptor-jni/classes \
  bindings/disruptor-jni/src/main/java/ubq/jni/*.java \
  bindings/disruptor-jni/src/test/java/ubq/jni/UbqJniSmoke.java
```

Smoke-test it with:

```bash
java \
  -Djava.library.path=target/release \
  -cp target/disruptor-jni/classes \
  ubq.jni.UbqJniSmoke
```

## Using It In Disruptor Perf Tests

The reproducible runner builds the native library, clones a pinned Disruptor
revision, applies the selected patch, runs the three target tests, and writes raw
logs plus CSV summaries:

```bash
scripts/run_disruptor_jni_bench.sh --queue ubq,rbbq
```

The default UBQ run uses `balanced,8,127,crossbeam`. Use
`--ubq-variants sweep` to run every configured UBQ variant:

```bash
scripts/run_disruptor_jni_bench.sh --queue all --ubq-variants sweep
```

For a hand-picked subset, use semicolons because UBQ labels already contain
commas:

```bash
scripts/run_disruptor_jni_bench.sh \
  --queue ubq \
  --ubq-variants 'balanced,8,63,crossbeam;balanced,8,127,crossbeam'
```

Use `--queue all` to include the official Disruptor sequenced baseline. Results
are written under `bench_results/disruptor_jni/<timestamp>-<host>/`:

- `logs/*.log`: raw LMAX output.
- `samples.csv`: one row per LMAX run.
- `summary.csv`: min/median/mean/max per queue and scenario.
- `metadata.txt`: pinned Disruptor revision and run settings.

Plot a completed run with:

```bash
python3 scripts/plot_disruptor_jni.py \
  bench_results/disruptor_jni/<timestamp>-<host>/summary.csv
```

The plotter writes grouped throughput bars and, when the Disruptor baseline is
present, a speedup chart next to the summary CSV.

For hebrides, run from a machine where `ssh hebrides` works:

```bash
scripts/run_disruptor_jni_hebrides.sh --queue all --ubq-variants sweep
```

The hebrides wrapper syncs the files needed for this benchmark to `~/UBQ` on the
remote host, runs the local runner there, and pulls
`bench_results/disruptor_jni` back into the local workspace. Override the remote
path with `--remote-repo <dir>`, the host with `--host <name>`, the UBQ variant
set with `--ubq-variants <set>`, or the pinned upstream checkout with
`--disruptor-repo <url>` and `--disruptor-rev <rev>`.

The lower-level manual flow is:

Clone the Disruptor repository and copy the adapter into its `perftest` source
set:

```bash
git clone https://github.com/LMAX-Exchange/disruptor /tmp/lmax-disruptor
cp -R bindings/disruptor-jni/src/main/java/ubq /tmp/lmax-disruptor/src/perftest/java/
```

For the low-overhead JNI-native run, apply the native queue-adapter patch:

```bash
cd /tmp/lmax-disruptor
git apply /path/to/UBQ/bindings/disruptor-jni/lmax-native-queue-adapter.patch
./gradlew perftestClasses
java \
  -Djava.library.path=/path/to/UBQ/target/release \
  -Dubq.jni.ubqVariant=balanced,8,127,crossbeam \
  -cp build/classes/java/main:build/classes/java/test:build/classes/java/perftest \
  com.lmax.disruptor.queue.OneToOneQueueThroughputTest
java \
  -Djava.library.path=/path/to/UBQ/target/release \
  -Dubq.jni.ubqVariant=balanced,8,127,crossbeam \
  -cp build/classes/java/main:build/classes/java/test:build/classes/java/perftest \
  com.lmax.disruptor.queue.ThreeToOneQueueThroughputTest
java \
  -Djava.library.path=/path/to/UBQ/target/release \
  -Dubq.jni.ubqVariant=balanced,8,127,crossbeam \
  -cp build/classes/java/main:build/classes/java/test:build/classes/java/perftest \
  com.lmax.disruptor.queue.OneToThreeQueueThroughputTest
```

`perfJar` currently fails on the upstream Disruptor Gradle 8.10 build because
it resolves the non-resolvable `perftestCompileOnly` configuration. Running from
the compiled class directories exercises the same perf-test classes without
depending on that packaging task.

That native patch replaces the official queue processors with `UbqLongQueue`
producer/consumer loops. Each measured producer or consumer thread crosses JNI
once and then performs the full run in Rust, avoiding Java boxing and per-item
JNI transitions.

For the equivalent RBBQ/FastFifo run, build `libubq` with
`--features jni,bench_fastfifo` and apply
`bindings/disruptor-jni/lmax-native-rbbq-adapter.patch` instead. The default
RBBQ block size is 64 and can be changed with
`-Dubq.jni.rbbq.blockSize=<size>`.

For a quick "same Java `BlockingQueue` harness" run instead, apply
`bindings/disruptor-jni/lmax-queue-adapter.patch`. That patch replaces the
official queue baseline objects with `UbqBlockingQueue`; it is convenient but
includes Java boxing and one JNI transition per item.

For the closest BBQ-style JNI comparison, modify the relevant Disruptor
throughput tests to use `UbqLongQueue` and the average Disruptor batch size:

```java
try (UbqLongQueue queue = new UbqLongQueue()) {
    queue.pushRange(firstSequence, batchSize);
    int consumed = queue.popBatch(batchSize);
}
```

`pushRange` and `popBatch` intentionally do the per-event work inside Rust so
one JNI transition covers a whole batch. Use the primitive adapter for final
numbers; the `BlockingQueue<Long>` adapter is convenient but includes Java
boxing and one JNI transition per element.

The comparison is direct for consume-once shapes:

- `1P1C`
- `NP1C`, including the BBQ paper's generalized `ThreeToOne` producer counts
- `1PNC`, including the generalized `OneToThree` consumer counts

Disruptor graph shapes such as multicast, pipeline, and diamond are not direct
queue-primitive comparisons. UBQ can model them with multiple queues or
reference-counted work items, but those results should be reported as
application-structure experiments rather than one-ring-buffer replacements.
