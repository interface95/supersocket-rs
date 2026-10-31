# Changelog

Notable changes to supersocket-rs are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

The first release.

### Server (`supersocket-server`, `supersocket-proto`, `supersocket-connection`)

- Pipeline filters: lines, terminators, fixed size, fixed header with a length field, begin and
  end marks, and transparent; filters can switch mid-stream. Decoders may hold state.
  `CodecFilter` runs `tokio_util::codec` decoders (feature `codec`).
- Sessions with typed state, created with `Default` or from the connection, extensions, batched
  writes with back-pressure, `try_send` for broadcasts, and no buffers while idle.
- Commands dispatched by key with command filters; middleware; serial or concurrent package
  handling with a handling timeout; an error handler; panic containment.
- `PackageHandler` for handlers that are types, with a `closing` hook; sessions that must
  complete a handshake before they count as connected.
- `StreamLayer` wraps connections, with gzip built in (feature `gzip`); connections from any
  transport through `serve_connection`.
- An accept hook before TLS, `max_connections`, one `handshake_timeout` for setting up a
  connection, `stop_accepting` for draining, and `run_until`.
- TLS listeners with client certificates and custom rustls configurations (feature `tls`);
  PROXY protocol v1 and v2; TCP keepalive and socket buffer sizes per listener.
- Configuration from TOML or JSON files with environment overrides (feature `config`).
- Little work per package: the handling timeout only sets up a timer once a handler actually
  waits, serial handling does not touch the task set of concurrent handling, and a send batch
  starts with room for several small messages instead of growing a few bytes at a time.

### Client (`supersocket-client`)

- `EasyClient` with reusable builders and connector chains: TCP, TLS, HTTP CONNECT, SOCKS4 and
  SOCKS5 proxies (feature `proxy`), and gzip; receiving by pulling or through a handler.

### WebSocket (`supersocket-websocket`)

- Server and client for RFC 6455 on the same core: subprotocols, handshake validation with
  custom response headers, pings with a pong timeout, and the closing handshake; interoperability
  tested against tungstenite.
- Per-message compression (RFC 7692 `permessage-deflate`) for the server and the client, feature
  `deflate`: pooled compressors and no compression state per connection by default.
- Commands for WebSocket messages (`WebSocketCommand`, `commands`) and handlers per subprotocol
  (`subprotocol`), like SuperSocket's `UseCommand` and `UseWebSocketMessageHandler(protocol, ..)`.
