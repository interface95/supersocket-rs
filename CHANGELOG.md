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
  Terminators and marks are searched with `memchr`.
- Unix domain socket listeners with `ListenOptions::unix` and `listen_unix`, also from config
  files, and the `Listener` trait with `ServerBuilder::listener` for transports of your own,
  served within the connection limit and stopped by `stop_accepting`. `Server::start` reports
  where every listener listens as a `ListenAddr`. A Unix socket file that another process still
  serves is not taken over.
- `Session::detach` hands a connection over to another protocol or server, closing the session
  with `CloseReason::Detached`.
- `Session::send_with`, `try_send_with` and the `send_fmt!` macro encode straight into the send
  buffer; `send_package` uses it.
- `ServerStatus` and `SessionStatus`, through `Server::status` and `Session::status`.
- The idle session timeout counts sends as activity too, so a session that only pushes data,
  such as a Server-Sent Events stream, is not idle; `Session::last_received_time` and
  `last_sent_time` tell the two apart.
- `ServerBuilder::commands_with` dispatches packages of any type to commands through a mapper,
  and with feature `json`, `JsonCommand` wrapped in `Json` runs commands whose body is JSON.

### UDP (`supersocket-udp`, feature `udp`)

- `UdpListener` serves datagrams as sessions, keyed by a `SessionKey`, by default the peer's
  address; one socket, a virtual connection per session. A receive task of its own keeps the
  sessions going while the server waits for a connection slot, replies follow a peer that moved,
  and replies larger than `max_send_size` go out as several datagrams.

### HTTP (`supersocket-http`, feature `http`)

- `HttpFilter` cuts connections into `HttpRequest`s with keep-alive and chunked bodies, and
  rejects requests with ambiguous body framing (RFC 9112); `HttpResponse` with
  `HttpSessionExt::respond`, which also tells HTTP/1.0 clients when a connection is kept; and
  Server-Sent Events through `SseWriter`, with a heartbeat while nothing else is sent.

### Client (`supersocket-client`)

- `EasyClient` with reusable builders and connector chains: TCP, TLS, HTTP CONNECT, SOCKS4 and
  SOCKS5 proxies (feature `proxy`), and gzip; receiving by pulling or through a handler.
- Proxy connectors laid out like SuperSocket's: `HttpConnector`, `Socks4Connector` and
  `Socks5Connector` implement `ProxyConnector` (SuperSocket's `ProxyConnectorBase`), which opens
  the connection to the proxy and leaves the handshake to `connect_proxy`; handshakes run on a
  `ProxyConnection` that cuts replies with pipeline filters and detaches the connection at the
  end. SOCKS5 follows the method the proxy chose, also when it skips the offered authentication.
  `validate_target` refuses a target the protocol cannot carry before the proxy is dialed, and
  `ProxyConnection::receive_step` rejects data a proxy sends ahead of the next request.
  `PackageReader::get_mut` writes to the stream between packages, `buffered` shows what is read
  ahead.
- `ConnectorChain` runs connectors one after another as one `Connector`, like SuperSocket's
  `ConnectorBase` and `BuildConnectors`; `ClientBuilder` connects the same way, and a
  `ProxyConnector` can return one from `create_default_connector` to reach a proxy through TLS.
- `ClientSender::send_with`, `try_send_with` and `send_fmt`.

### WebSocket (`supersocket-websocket`)

- Server and client for RFC 6455 on the same core: subprotocols, handshake validation with
  custom response headers, pings with a pong timeout, and the closing handshake; interoperability
  tested against tungstenite.
- Per-message compression (RFC 7692 `permessage-deflate`) for the server and the client, feature
  `deflate`: pooled compressors and no compression state per connection by default.
- Commands for WebSocket messages (`WebSocketCommand`, `commands`) and handlers per subprotocol
  (`subprotocol`), like SuperSocket's `UseCommand` and `UseWebSocketMessageHandler(protocol, ..)`;
  with feature `json`, `JsonWebSocketCommand` for messages whose body is JSON.
- Sending a message in fragments with `send_fragmented`, on the server and the client.
