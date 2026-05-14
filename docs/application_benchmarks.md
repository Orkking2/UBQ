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

These modes use the existing v2 JSON schema. Throughput is stored in
`ops_per_sec`, average latency in `avg_data_latency_ns`, and producer/consumer
elapsed timing in `push_elapsed_ns` and `pop_elapsed_ns`.

Run the configured fleet:

```bash
cargo run --release --bin full_bench_fleet -- \
  --config bench_fleet_app.toml \
  --machines local \
  --repeats 3
```
