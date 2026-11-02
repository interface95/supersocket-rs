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

## Session + command + middleware comparison

`examples/bench_pipeline` and `dotnet/Program.cs` implement the same application using their
framework's actual session lifecycle, connection middleware, command dispatcher and command
filters. This is a separate benchmark from the simple echo numbers above. The initial comparison
left the library and `session_server` teaching example unchanged; the subsequently measured
parsing and vectored-reply optimizations are now applied. Historical results retain their original
source hashes; see the isolated optimization experiments below for their archived baseline.

The common line protocol is:

| Step | Request | Response |
|---|---|---|
| Session connected | — | `READY` |
| Command filter rejects an unnamed session | `ECHO hello` | `ERR set NAME first` |
| Set connection-local state | `NAME bench` | `OK name=bench` |
| Timed command, with before/after filters | `ECHO <32-byte body>` | `OK echo=<32-byte body>` |
| Verify after-filter execution | `STATS` | `OK count=<successful ECHO count>` |
| Inspect independent session state | `WHOAMI` | `OK name=bench` |
| Graceful close | `QUIT` | `BYE`, then EOF |

Every line ends in CRLF. Names contain 1–32 ASCII letters, digits or underscores. A short lock
protects each session's name; an atomic counter records successful ECHO commands. An atomic
admission counter in the connection middleware limits active sessions and releases slots when
they disconnect. NAME is a display-name operation, not authentication. Per-request logging is
disabled in both servers. The before-filter also rejects empty/control-character ECHO bodies.

Both servers explicitly use a 4096-byte maximum package, 4096-byte requested kernel send/receive
buffers, TCP_NODELAY, backlog 1024, serial command execution, a 30-second command timeout and
a 5-second send timeout. OS-adjusted buffer sizes and the frameworks' internal buffering can
still differ. Rust additionally has a 2-second close timeout; SuperSocket 2.1 has no equivalent
global setting. The C# middleware rejects before its sending loop starts, so an over-limit
connection observes EOF; Rust sends `ERR server busy`. These rejection/closing paths are
validated but are outside the timed workload.

### Build and run

Install Rust and the .NET SDK pinned in `dotnet/global.json`, then from the repository root:

```sh
cargo build --release --locked -p supersocket --example bench_pipeline
cargo build --release --locked -p supersocket-bench --bin pipeline
(cd tools/bench/dotnet && dotnet restore --locked-mode && dotnet build -c Release --no-restore)

# Full wire-contract check only, including two-session isolation, rejection and slot release.
python3 tools/bench/run_pipeline.py --validate-only --output /tmp/pipeline-validation

# Three alternating rounds, with a fresh server and sustained warmup for every sample.
# Use a new output directory each time; existing evidence is never overwritten.
python3 tools/bench/run_pipeline.py \
  --rounds 3 --warmup 15 --seconds 10 --threads 8 \
  --output tools/bench/results/my-pipeline-run
```

The runner raises the inherited file descriptor limit to support 10,000 connections. It stops
only the child servers it starts. Both Rust processes use `--threads` Tokio workers; the C#
process uses the same `DOTNET_PROCESSOR_COUNT`, with server GC and application runtime async
enabled. This is not a CPU quota or a guarantee of identical thread counts. Released SuperSocket
NuGet dependencies are pinned to 2.1.0 and are not rebuilt with runtime async.

`pipeline` can also be run directly against either server:

```sh
BENCH_THREADS=8 BENCH_WARMUP_SECONDS=15 target/release/pipeline \
  run 127.0.0.1:4040 100 10 32 1 <server-pid> <label>
target/release/pipeline idle 127.0.0.1:4040 10000 <server-pid> <label>
```

### Measurement and interpretation

- The client establishes and names every session before warmup. After all warmup requests
  drain, a barrier and shared start signal establish the measurement interval. Throughput
  counts every completed request and includes the final in-flight batches in elapsed time.
- Every response is compared byte-for-byte. After CPU/RSS snapshots, STATS checks the exact
  warmup-plus-measurement count on every session, proving that the after-filter executed.
- Latency is sampled every 16 batches, with different offsets for different connections.
  At depth 16, latency is the RTT of the entire batch, not one request. This is a closed-loop
  workload; its percentiles do not describe overload under a fixed external arrival rate.
- Server and client CPU consumption are both reported. Connections remain open until the
  memory snapshot completes. No forced GC is applied to either implementation.
- Idle memory uses a fresh process, measures RSS before opening the named sessions, waits
  five seconds, then measures RSS again. Each reading is the median of five samples. All
  sessions must subsequently answer STATS=0 and WHOAMI=bench. The RSS delta includes JIT,
  runtime pools and initialization; it is not the size of a session object alone.
- The report shows medians and full min–max throughput ranges across rounds. Its p99 column
  is the median of per-round p99 values, not a percentile of a pooled distribution.
- On macOS the two processes share the host without CPU affinity. Treat the numbers as
  complete-path throughput on that host, not isolated maximum server capacity. Check client
  thread-count and warmup sensitivity before interpreting small differences.

The runner preserves raw JSON samples, version information, source hashes and individual server
logs alongside `report.md`. Keep results from different hosts separate from the README's earlier
Linux/Ryzen echo benchmark.

## Isolated Rust optimization experiments

The parsing and vectored-reply optimizations are now applied to the workspace. To reproduce the
historical A/B comparison, use its archived **pre-optimization** source in a separate directory;
`prepare` rejects source that no longer matches that baseline before building anything:

```sh
bench_repro_dir="$(mktemp -d)"
tar -xzf tools/bench/results/2026-11-01-rust-optimizations/baseline-source.tar.gz -C "$bench_repro_dir"
cd "$bench_repro_dir/snapshot"
git init -b codex/bench-repro
git add .
git -c user.name=Benchmark -c user.email=benchmark@example.invalid commit -m "chore: 保存性能实验基线"
cargo fetch --locked
```

Run the commands below from that snapshot directory, using the Rust version recorded in the report.

`optimize_pipeline.py` snapshots the current workspace (including uncommitted example/tool files)
into a temporary directory. It builds a fixed client and five server variants without replacing
the workspace's source or binaries:

- `baseline`: unchanged Rust server, ordinary release profile.
- `parse`: borrow UTF-8 text when decoding a command instead of creating an intermediate String.
- `vectored`: assemble ECHO replies with the existing `send_vectored` API instead of `format!`.
- `parse_vectored`: both source changes.
- `parse_vectored_lto`: both source changes, plus Thin LTO and one codegen unit.

```sh
python3 tools/bench/optimize_pipeline.py prepare \
  --output tools/bench/results/my-optimization/builds
python3 tools/bench/optimize_pipeline.py run \
  --builds tools/bench/results/my-optimization/builds/builds.json \
  --output tools/bench/results/my-optimization/screen \
  --rounds 3 --warmup 5 --seconds 8
```

Preparation runs parser compatibility tests against both implementations and the common wire
validation against every executable. The build manifest contains source/executable hashes,
patches and compiler settings. Executables remain in the recorded temporary directory; if they
have been removed, run `prepare` again into a new output directory.

Each candidate has its own adjacent control sample, with alternating run order and candidate
rotation between rounds. The first three candidates compare with `baseline`; the LTO variant
compares with the same `parse_vectored` source under the ordinary profile. Thus its result measures
the combination of Thin LTO and `codegen-units=1`, not source changes or Thin LTO alone.

Use `--variants parse,vectored`, `--scenarios 1:1,100:1,1000:1,100:16` and `--rounds 6` to select
follow-up cases. `--reference baseline` explicitly compares a complete candidate with the original
implementation. A three-pair screen is exploratory: report all paired ratios and their ranges;
small or directionally inconsistent changes need more evidence before adoption. This experiment
does not test buffer pooling, Future representation, substring search, or PGO.

The [2026-11-01 hot-path report](results/2026-11-01-rust-optimizations/report.md) preserves the
three-pair screen, an independent eight-pair confirmation, idle-memory checks, source snapshot
and candidate patches. It distinguishes small observed changes from repeatable performance claims.
