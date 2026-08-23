# Application-Level Benchmark Notes

The application-level modes are synthetic experiments that preserve the
benchmark harness' controlled setup while exercising queue shapes that appear in
real systems. They should be read as queue-behavior experiments, not as claims
about complete production applications.

- `app_log_fan_in`: many producers enqueue boxed log/event records into one
  shared queue. Consumers dequeue, hash, and free records. The mode reports
  completed events per second and average enqueue-to-dequeue latency.
- `app_pipeline`: producers enqueue work to stage 1, worker threads transform
  records and enqueue them to stage 2, and one collector drains completions. The
  mode reports completed records per second and average end-to-end latency.
- `app_task_roundtrip`: client threads submit one in-flight request at a time to
  a worker queue and receive completions through a shared response queue. The
  mode reports completed requests per second and average round-trip latency.

These modes use the schema-v7 benchmark JSON format. Throughput is stored in
`ops_per_sec`, average latency in `avg_data_latency_ns`, and producer/consumer
elapsed timing in `push_elapsed_ns` and `pop_elapsed_ns`.

Each scheduled queue sample has a hard process timeout. The default is 300
seconds per sample and can be overridden with
`UBQ_BENCH_JOB_TIMEOUT_SECS`. If a queue hangs, the parent kills and reaps the
worker process—including all of its benchmark threads—then starts a fresh
worker for the next sample. A timed-out or failed result has no throughput
value, records `consumed_items = 0` because a trustworthy partial count is not
available after termination, and remains eligible for retry. There is no
shorter no-progress watchdog.

Run the application modes directly:

```bash
cargo run --release --features bench_registry,bench_rbbq,bench_lfqueue --bin bench_matrix -- \
  --machine-label local \
  --queues ubq,segqueue,concurrent-queue,rbbq,lfqueue \
  --ubq-label balanced,1,page,crossbeam \
  --scenarios 1p1c,4p1c,1p4c,4p4c,8p8c,16p16c \
  --modes app_log_fan_in,app_pipeline,app_task_roundtrip \
  --items-per-producer 100000 \
  --repeats 3
```
