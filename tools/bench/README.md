# Benchmark tool

The load generator behind the performance numbers in the top-level README. It talks the line
protocol, so the same tool measures `examples/bench_echo` and SuperSocket's `BenchEcho` sample.

```sh
# The server; BENCH_PORT picks the port (default 4040).
cargo run --release -p supersocket --example bench_echo &

# Throughput and latency: 100 connections, 10 seconds, 32-byte lines, 1 request in flight each.
cargo run --release -p supersocket-bench -- pingpong 127.0.0.1:4040 100 10 32 1 <server pid> rust

# Memory: 10,000 idle connections.
cargo run --release -p supersocket-bench -- idle 127.0.0.1:4040 10000 <server pid> rust
```

`pingpong` prints a `RESULT` line with the requests per second, the p50, p99, p99.9 and maximum
latency in microseconds, how many cores the server used, how many requests it handled per CPU
second and its memory. `idle` prints an `IDLE` line with the server's memory per connection.
`summarize.py` turns the lines of several rounds into Markdown tables.

For comparable numbers, pin the server and the tool to separate cores (`taskset` on Linux), run
each scenario several times and raise the open file limit (`ulimit -n 100000`) before the idle
measurement. The server's CPU time is read from `/proc` on Linux and from `ps` elsewhere.
