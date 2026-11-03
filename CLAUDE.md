# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

supersocket-rs: a socket server framework on tokio that follows the architecture of .NET
SuperSocket (pipeline filters cut bytes into packages → packages are handled per session →
commands, middleware, TLS, WebSocket on top). Early development, not on crates.io.
MSRV is Rust 1.85, edition 2024.

## Commands

CI (`.github/workflows/ci.yml`) runs exactly these; run the relevant ones before calling work done:

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo clippy --workspace --all-targets -- -D warnings            # no features must build too
cargo test --workspace --all-features
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps
cargo hack check --workspace --each-feature --no-dev-deps       # every feature alone
cargo +1.85 check --workspace --all-features --all-targets      # MSRV
cargo deny check                                                # advisories/licenses, deny.toml
```

Tests:

- Unit tests live inside each crate: `cargo test -p supersocket-proto`,
  `cargo test -p supersocket-websocket --features deflate`.
- Integration tests live in `crates/supersocket/tests/`. Most have `required-features` in
  `crates/supersocket/Cargo.toml`; without the feature the test binary is **silently skipped**,
  so pass it:
  ```sh
  cargo test -p supersocket --features websocket --test websocket -- silent_clients
  cargo test -p supersocket --features websocket,client,tls --test websocket_client
  cargo test -p supersocket --features http --test http
  cargo test -p supersocket --features udp --test udp
  cargo test -p supersocket --features json,websocket --test json
  cargo test -p supersocket --test server -- echo_handles          # server/connection/transport need none
  ```
- Doctests of one crate: `cargo test -p supersocket-server --doc`.

Fuzzing (`fuzz/` is a separate workspace, needs nightly, run from the repo root):

```sh
cargo fuzz list
cargo fuzz run proto_filters -- -max_total_time=20 -max_len=4096
```

Examples (`crates/supersocket/examples`; the README lists which features each needs):

```sh
cargo run --example echo_server                                 # nc 127.0.0.1 4040
cargo run --example websocket_echo_server --features websocket,deflate
```

Benchmarks (`tools/bench`, workspace member `supersocket-bench`; see `tools/bench/README.md`):

```sh
cargo run --release -p supersocket --example bench_echo &
cargo run --release -p supersocket-bench -- pingpong 127.0.0.1:4040 100 10 32 1 <server pid> rust
python3 tools/bench/run_pipeline.py --validate-only --output /tmp/pipeline-validation
```

`scripts/websocket_deflate_interop.py` checks permessage-deflate against the Python
`websockets` library; its docstring has the commands.

## Workspace layout

Bottom-up dependency order. Each crate's `lib.rs` doc comment is the best short overview.

| Crate | Role |
|---|---|
| `supersocket-proto` | `PipelineFilter` trait, built-in filters, `TextPackage`/`CommandPackage`, `ProtocolError`, `head_length` (shared by the WebSocket handshake and HTTP). No tokio dependency. |
| `supersocket-connection` | Shared by server and client: `PackageReader` (reads and cuts packages, holds no buffer while idle), `SendQueue` (batched writes, back-pressure, `try_send`), `catch_panic`, `Rewind`, gzip. |
| `supersocket-server` | `Server`/`ServerBuilder`, `Session`, commands, middleware, `StreamLayer`, options and config loading, PROXY protocol, TLS. |
| `supersocket-client` | `EasyClient` with a `Connector` chain (TCP or proxy first, then TLS, gzip, custom). |
| `supersocket-websocket` | RFC 6455 server on `supersocket-server`, client on `supersocket-client`, permessage-deflate, fragmented sending. |
| `supersocket-udp` | `UdpListener`: a `Listener` that serves datagrams as sessions keyed by a `SessionKey`. A receive task of its own demultiplexes datagrams, so live sessions never depend on the server accepting. |
| `supersocket-http` | HTTP/1.1: `HttpFilter`, `HttpResponse`, keep-alive, Server-Sent Events. |
| `supersocket` | Facade: re-exports, `prelude`, feature forwarding. **All examples and integration tests live here.** |
| `tools/bench` | Load generator, `publish = false`. |

Features (`tls`, `config`, `client`, `proxy`, `websocket`, `gzip`, `codec`, `deflate`, `json`,
`udp`, `http`) are forwarded by `crates/supersocket/Cargo.toml` with `?/` syntax. A new optional
feature needs an entry there and in the feature list in `crates/supersocket/src/lib.rs`.

## Architecture

### Connection lifecycle (server)

`crates/supersocket-server/src/connection.rs` is the heart. Each kind of connection (plain TCP,
TLS, with stream layers, Unix socket, `Server::serve_connection`, custom `Listener`) has its own
`serve_*` entry so a task carries only the state it needs; all funnel into `start` →
`run_session`. Listeners from the options are `Endpoint`s (TCP or Unix) bound in `Server::start`
and driven by `accept_loop`; TCP with stream layers and Unix sockets share `serve_endpoint` for
the PROXY header, the accept hook and TLS. Custom `Listener`s run in `custom_accept_loop` and
feed `serve_stream`:

1. `accept`: the `on_accept` hook (before TLS, for cheap rejection), session id, tracing span,
   and the `handshake_timeout` deadline starts.
2. PROXY protocol header (if the listener enables it) → TLS handshake → `StreamLayer`s, all
   within that deadline. Bytes read too early are passed on as `buffered` into
   `PackageReader::with_buffered` (the client uses `Rewind` for the same purpose).
3. `start`: the filter factory and the session-state factory run under `catch_panic_sync`.
4. `run_session`: spawns the writer task (`SendQueue::run_writer`); runs middleware
   `register_session` in `order()` (a rejection, panic, timeout or closure rolls back the
   already-registered middleware in reverse); unless `require_handshake` is set, `connect` adds
   the session to the `SessionContainer` and runs `SessionState::on_connected` + `on_connected`.
5. `read_loop`: with `require_handshake`, packages are handled serially until a handler calls
   `session.complete_handshake()`, then the session connects. Otherwise `handle_package` runs
   serially or through a `JoinSet` (`PackageHandling::Concurrent`). A failing handler goes to
   `on_error`, whose `ErrorAction` decides whether the session closes.
6. Close: `PackageHandler::closing` (goodbye message; connected sessions only) →
   `close_send_queue` → wait for the writer up to `close_timeout` → remove from the container →
   middleware `unregister_session` in reverse → `SessionState::on_closed` / `on_closed`
   (connected sessions only). A `Cleanup` drop guard repeats the essential steps if the task
   unwinds. A session closed with `CloseReason::Detached` (`Session::detach`) skips the goodbye,
   keeps the transport open and hands reader, writer and read-ahead bytes to the `Detaching`
   future instead.

Activity: received data (`Session::touch` in the read loop) and every successful send update
`last_received_time` / `last_sent_time`. The idle session timeout uses the later of the two; the
WebSocket pong timeout uses only the received one.

Every application callback (handlers, commands, command filters, middleware, state hooks,
filter and state construction) runs under `catch_panic` / `catch_panic_sync`: a panic becomes
`PackageError::Panicked` or a log line, never a crashed server. Keep that invariant when adding
new callbacks.

### Trait pattern

Public extension traits use `impl Future` return types (async-fn-in-trait style):
`PackageHandler`, `Middleware`, `StreamLayer`, `Listener`, `AsyncCommand`, `CommandFilter`,
`SessionState`, `Connector`, `WebSocketCommand`, `JsonCommand`. Each one that is stored as a
trait object has a `pub(crate) Dyn*` twin returning `BoxFuture` with a blanket impl
(`DynMiddleware`, `DynStreamLayer`, `DynListener`, `DynPackageHandler`, `DynConnector`). Adding a
method to the public trait means adding it to the twin too.

### Protocol switching

`PipelineFilter::next_filter` is called after every `filter` call; returning a filter replaces
the connection's filter (`PipelineFilterExt::then_switch_to` wraps this). WebSocket relies on it:
`HeadFilter` parses the HTTP head, then switches to `FrameFilter`. Server and client share these
filters through `Role`.

### WebSocket on top of the core

`WebSocketBuilder::build` composes
`Server::builder(HeadFilter).session_with(WebSocketState::new(user_state)).require_handshake()`
and installs a `PackageHandler` whose `handle` turns `WebSocketPackage`s (handshake, message,
ping, close, violation) into handler calls, and whose `closing` hook sends the close frame with
`Session::send_last`. `WebSocketState<S>` wraps the user's state; `WebSocketSession<S>` wraps
`Session<WebSocketState<S>>` and derefs to `S`. `WebSocketCommand`s are adapted to `AsyncCommand`
so the core `Commands` dispatcher is reused. With the `deflate` feature off, `deflate.rs` defines
the same type names as empty enums so the rest of the crate needs no `cfg`s. Message sends take
the per-connection `sending` read-write lock (fragmented sending holds it for writing, plain
sends for reading), so a `FragmentWriter` is never interleaved with another message while plain
sends do not wait for each other; control frames bypass it.

### Client

`ClientBuilder` holds a filter factory and a `Connector` chain: the first connector
(`TcpConnector` or a proxy) opens the stream, later ones (`TlsConnector`, `GzipConnector`,
custom) wrap it. Proxy connectors parse replies with proto filters through `PackageReader` and
push leftover bytes back with `Rewind`. The client reuses `PackageReader` and `SendQueue`, so
package limits, batching and back-pressure behave like a server session.

### Configuration

With the `config` feature, `ServerOptions::load` reads TOML or JSON by extension, then applies
`SUPERSOCKET__FIELD__0__SUBFIELD` environment overrides. `lenient.rs` holds the serde
deserializers that accept numbers as strings and durations as `"30s"`. `TlsOptions::config`
(a rustls `ServerConfig`) is never read from files.

### Fuzzing hooks

`supersocket-server/src/fuzzing.rs` and `supersocket-websocket/src/fuzzing.rs` expose crate
internals to the targets in `fuzz/fuzz_targets/`. They compile only under `--cfg fuzzing`
(declared in `[workspace.lints.rust] unexpected_cfgs`).

## Conventions

- Workspace lints: `missing_docs = "warn"`, `unsafe_code = "forbid"`, `clippy::all = "warn"`,
  and CI runs clippy and rustdoc with `-D warnings`. Every `pub` item needs a doc comment and
  intra-doc links must resolve.
- Every feature must compile alone and with none enabled (`cargo hack`). Gate optional code
  with `#[cfg(feature = "...")]`.
- Stay within Rust 1.85 (`deny.toml` ignores a `time` advisory because the fix needs 1.88).
  Async closures (`async |session, package| { .. }`) are used throughout.
- Integration tests mostly use `#[tokio::test(flavor = "multi_thread", worker_threads = 4)]`;
  use it for anything that exercises concurrency. They share the helpers in
  `crates/supersocket/tests/common/`: `start(builder)` binds `127.0.0.1:0`,
  `Client` reads lines with a 5 s timeout, `Stall` is a stream that never drains (deterministic
  back-pressure instead of relying on OS socket buffers), `track_close_reasons` reports
  `CloseReason`s. A new test file that needs optional features gets a `[[test]]` entry with
  `required-features`.
- Unit tests with heavy scaffolding may live in a sibling file included with `#[path]`
  (`supersocket-websocket/src/client_close_tests.rs`).
- `README.md` and `README.CN.md` are translations of each other and must stay in sync.
  `CHANGELOG.md` follows Keep a Changelog with an `[Unreleased]` section.
- Commits follow Conventional Commits with a scope (`feat(websocket): …`, `perf(proto): …`);
  recent subjects are written in Chinese.
