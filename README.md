# supersocket-rs

[中文](README.CN.md)

A socket server framework for Rust on [tokio](https://tokio.rs), inspired by
[SuperSocket](https://github.com/kerryjiang/SuperSocket) for .NET.

You describe how bytes become packages and what to do with each package; supersocket-rs takes
care of accepting connections, buffering, framing, sessions, timeouts, TLS and graceful shutdown.

```rust
use supersocket::prelude::*;

#[tokio::main]
async fn main() -> Result<(), supersocket::Error> {
    Server::builder(LineFilter::default)
        .on_package(async |session, package| {
            session.send(format!("{}\r\n", package.text)).await?;
            Ok(())
        })
        .listen("0.0.0.0:4040")
        .build()?
        .run()
        .await
}
```

> **Status:** early development, not yet published to crates.io. The API may still change.
> Requires Rust 1.85 or later.

## Features

- **Pipeline filters** cut byte streams into packages: lines, terminators, fixed size, fixed
  header with a length field, begin/end marks, or your own. A filter can switch the connection
  to another filter mid-stream.
- **Sessions** are cheap handles you can clone into other tasks to send from anywhere. Attach
  typed per-session state, or arbitrary data through extensions.
- **Commands** dispatch packages by key, with filters that run before and after commands.
- **Middleware** sees sessions come and go and can reject them.
- **Serial or concurrent** package handling per session, with a handling timeout and an error
  handler that decides whether a failing session stays open. A panicking handler is caught and
  reported to the error handler; it never takes the server down.
- **Batched writes**: data queued while packages are handled goes out in one system call, with
  back-pressure when a peer reads slowly and an optional send timeout.
- **Idle sessions hold no buffers**, so many mostly idle connections stay cheap.
- **A client**, modeled on SuperSocket's `EasyClient`: it connects through a chain of
  connectors (TCP or an HTTP CONNECT, SOCKS4 or SOCKS5 proxy, then TLS) and reuses the server's
  pipeline filters and send queue.
- **TLS** through rustls, **configuration** from TOML or JSON files with environment overrides,
  **tracing** for logs, graceful shutdown on Ctrl+C or SIGTERM.

## Getting started

Until the first release, depend on the repository:

```toml
[dependencies]
supersocket = { git = "https://github.com/interface95/supersocket-rs" }  # optional features: "tls", "config", "client", "proxy"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

Run the examples in `crates/supersocket/examples`:

```sh
cargo run --example echo_server        # nc 127.0.0.1 4040
cargo run --example command_server
cargo run --example custom_protocol
cargo run --example live_chat
cargo run --example tls_echo_server --features tls
cargo run --example config_server --features config
cargo run --example echo_client --features client     # with echo_server running
cargo run --example proxy_client --features proxy -- 127.0.0.1:1080 example.com:80
```

## Guide

### Pipeline filters

| Filter | Framing | Package |
|---|---|---|
| `LineFilter` | lines ending with `\r\n` | `TextPackage` |
| `CommandLineFilter` | `KEY arg1 arg2\r\n` | `CommandPackage` |
| `TerminatorFilter` | any terminator | your type |
| `FixedSizeFilter` | fixed size | your type |
| `FixedHeaderFilter` | header carrying the body length | your type |
| `BeginEndMarkFilter` | begin and end marks | your type |
| `TransparentFilter` | none | `Bytes` |

For a binary protocol with a fixed header, implement `FixedHeaderProtocol`:

```rust
#[derive(Default)]
struct MessageProtocol;

impl FixedHeaderProtocol for MessageProtocol {
    type Package = Message;
    const HEADER_SIZE: usize = 5; // opcode, 2-byte body length, 2-byte sequence

    fn body_length(&self, header: &[u8]) -> Result<usize, ProtocolError> {
        Ok(u16::from_be_bytes([header[1], header[2]]) as usize)
    }

    fn decode(&mut self, package: Bytes) -> Result<Message, ProtocolError> {
        // the header followed by the body
    }
}

Server::builder(FixedHeaderFilter::<MessageProtocol>::default)
```

Anything else implements `PipelineFilter` directly. Returning a filter from `next_filter`
switches the connection to it, which is how a protocol upgrade works.

### Commands and filters

```rust
struct Add;

impl AsyncCommand<CommandPackage> for Add {
    fn key(&self) -> String {
        "ADD".into()
    }

    async fn execute(&self, session: &Session, package: CommandPackage) -> HandlerResult {
        let mut sum = 0i64;
        for arg in &package.args {
            sum += arg.parse::<i64>()?;
        }
        session.send(format!("{sum}\r\n")).await?;
        Ok(())
    }
}

Server::builder(CommandLineFilter::default).commands(|commands| {
    commands
        .add(Add)
        .add(Secret.with_filter(RequireLogin)) // a filter for one command
        .filter(Timing);                        // a filter for every command
    commands.on_unknown(async |session, package| {
        session.send(format!("unknown command {}\r\n", package.key)).await?;
        Ok(())
    });
})
```

A `CommandFilter` implements `on_executing` (return `false` to skip the command) and
`on_executed`, which receives the command's result.

### Sessions

A `Session` is a cheap handle: clone it into other tasks and call `send` from anywhere. Data
from concurrent sends is never interleaved. Per-session state is a type implementing
`SessionState`; its fields are reachable directly on the session:

```rust
#[derive(Default)]
struct Chatter {
    name: Mutex<String>,
}

impl SessionState for Chatter {
    async fn on_connected(session: &Session<Self>) {
        *session.name.lock().unwrap() = format!("guest{}", session.id());
    }
}

Server::builder(CommandLineFilter::default)
    .session::<Chatter>() // before handlers and commands
```

`session.server().sessions()` lists the connected sessions, for example to broadcast.
`session.extensions()` stores data by type, handy for middleware. Shared services registered
with `.service(value)` are reachable through `session.server().service::<T>()`. A task spawned
for a session can wait for `session.closed()` to stop with it.

### Package handling, errors and timeouts

```rust
Server::builder(LineFilter::default)
    .on_package(handler)
    .on_error(async |session, error| match error {
        PackageError::Timeout(_) => ErrorAction::Continue,
        _ => ErrorAction::Close,
    })
    .configure(|options| {
        options.package_handling_timeout = Duration::from_secs(10);
        options.package_handling = PackageHandling::Concurrent { max_in_flight: 16 };
        options.idle_session_timeout = Some(Duration::from_secs(300));
        options.send_timeout = Some(Duration::from_secs(30));
    })
```

By default the packages of a session are handled one at a time, in order. In concurrent mode up
to `max_in_flight` packages of a session run at the same time on the runtime's worker threads.

### Client

Enable the `client` feature. An `EasyClient` cuts what the server sends into packages with a
pipeline filter, the same ones the server uses:

```rust
let mut client = EasyClient::builder(LineFilter::default())
    .connect("127.0.0.1:4040")
    .await?;
client.send("hello\r\n").await?;
let reply = client.receive().await?; // Some(TextPackage), or None once closed
```

Or hand every package to a handler running in the background, and keep the returned sender for
sending and closing:

```rust
let sender = client.start_receive(async |sender, package| {
    let _ = sender.send(format!("got {}\r\n", package.text)).await;
});
sender.close().await; // sends what is queued first, for up to `close_timeout`
```

The background task does not keep the connection open: dropping the last sender closes it the
same way.

The connection is built by a chain of connectors, like SuperSocket's `IConnector`: the first
one opens it, later ones wrap it.

```rust
EasyClient::builder(LineFilter::default())
    .proxy(Socks5Proxy::new("127.0.0.1:1080").credentials("user", "secret")) // feature "proxy"
    .tls(TlsConnector::new()) // feature "tls"; Mozilla roots by default
    .connect("example.com:443")
    .await?;
```

`HttpProxy` (CONNECT) and `Socks4Proxy` (with SOCKS4a for host names) work the same way, and
anything implementing `Connector` can be added with `.connector(...)`.

### Middleware

```rust
struct ConnectionLimit {
    limit: usize,
    current: AtomicUsize,
}

impl<S: SessionState> Middleware<S> for ConnectionLimit {
    async fn register_session(&self, _session: &Session<S>) -> bool {
        if self.current.fetch_add(1, Ordering::SeqCst) < self.limit {
            true
        } else {
            self.current.fetch_sub(1, Ordering::SeqCst);
            false
        }
    }

    async fn unregister_session(&self, _session: &Session<S>) {
        self.current.fetch_sub(1, Ordering::SeqCst);
    }
}
```

Sessions connect concurrently, so a limit keeps its own count rather than reading
`session_count()`.

### TLS

Enable the `tls` feature and give a listener a PEM certificate chain and private key:

```rust
.configure(|options| {
    options.listeners.push(ListenOptions {
        tls: Some(TlsOptions::new("cert.pem", "key.pem")),
        ..ListenOptions::new("0.0.0.0:4443")
    });
})
```

The handshake runs in the connection's own task, so slow clients never hold up accepting.

### Configuration

With the `config` feature, `ServerOptions::load` reads a TOML or JSON file. Every field has a
default, and environment variables override the file. Values are converted to the type of
their field, so numbers may be written as strings and durations as `"30s"` or a number of
seconds:

```toml
name = "echo"
package_handling_timeout = "10s"
idle_session_timeout = "5m"
package_handling = { concurrent = { max_in_flight = 16 } }

[[listeners]]
address = "0.0.0.0:4040"

[[listeners]]
address = "0.0.0.0:4443"
tls = { cert = "cert.pem", key = "key.pem" }
```

```sh
SUPERSOCKET__LISTENERS__0__ADDRESS=127.0.0.1:4041 SUPERSOCKET__SEND_TIMEOUT=30s ./server
```

```rust
let options = ServerOptions::load("supersocket.toml")?;
Server::builder(LineFilter::default).options(options)
```

## Performance

In an echo benchmark on a Ryzen 9 9950X running Linux, with server and load generator pinned to
separate cores (4 cores, 8 threads each), the core of supersocket-rs compared with SuperSocket on
.NET 11 RC2 with runtime async and server GC as follows:

| | supersocket-rs | SuperSocket (.NET 11) |
|---|---|---|
| 100 connections, request/response | 2.02 M req/s, p99 89 µs | 1.50 M req/s, p99 211 µs |
| 1,000 connections | 2.82 M req/s, p99 0.71 ms | 1.51 M req/s, p99 1.26 ms |
| 16 pipelined requests per connection | 14.2 M req/s | 10.6 M req/s |
| Requests per CPU-second, 100 connections | 331 K | 205 K |
| Memory with 10,000 idle connections | 63 MB | 186 MB |

These numbers come from a prototype of the same core. Since then idle sessions stopped holding
read and write buffers, which halved the memory per idle connection again (6.4 KB to about
3 KB in a local measurement). Echo measures framework overhead only; real applications spend most of
their time elsewhere.

## Roadmap

- WebSocket server and client, built on pipeline filters
- Derive-style command registration
- UDP sessions, if there is demand

## License

Apache License 2.0. supersocket-rs is an independent implementation that follows the
architecture of [SuperSocket](https://github.com/kerryjiang/SuperSocket) by Kerry Jiang and
contributors; see [NOTICE](NOTICE).
