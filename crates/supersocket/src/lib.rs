//! A socket server framework for Rust, inspired by [SuperSocket](https://github.com/kerryjiang/SuperSocket).
//!
//! Connections are decoded into packages by pipeline filters, packages are handled per session,
//! commands dispatch packages by key, and middleware sees sessions come and go.
//!
//! ```no_run
//! use supersocket::prelude::*;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), supersocket::Error> {
//!     Server::builder(LineFilter::default)
//!         .on_package(|session, package| async move {
//!             session.send(format!("{}\r\n", package.text)).await?;
//!             Ok(())
//!         })
//!         .listen("0.0.0.0:4040")
//!         .build()?
//!         .run()
//!         .await
//! }
//! ```
//!
//! # Features
//!
//! - `tls`: TLS through rustls, for listeners and, with `client`, for the client.
//! - `config`: loading [`ServerOptions`] from TOML or JSON files with environment overrides.
//! - `client`: the client, `EasyClient`, in the `client` module.
//! - `proxy`: HTTP CONNECT, SOCKS4 and SOCKS5 proxy connectors for the client.
//! - `websocket`: the WebSocket server in the `websocket` module, and with `client` the WebSocket
//!   client.
//! - `deflate`: WebSocket per-message compression (`permessage-deflate`), with `websocket`.
//! - `gzip`: gzip compression of connections, `GzipLayer` for the server and, with `client`,
//!   `GzipConnector` for the client.
//! - `codec`: `CodecFilter`, which runs a `tokio_util::codec::Decoder` as a pipeline filter.
//! - `json`: commands whose package body is JSON: `JsonCommand`, and with `websocket`
//!   `JsonWebSocketCommand`.
//! - `udp`: the UDP listener in the `udp` module, which serves datagrams as sessions.
//! - `http`: HTTP/1.1 in the `http` module: a request filter, responses, keep-alive and
//!   Server-Sent Events.

#[cfg(feature = "client")]
pub use supersocket_client as client;
pub use supersocket_connection as connection;
pub use supersocket_connection::send_fmt;
#[cfg(feature = "http")]
pub use supersocket_http as http;
pub use supersocket_proto as proto;
#[cfg(feature = "codec")]
pub use supersocket_proto::CodecFilter;
pub use supersocket_proto::{
    BeginEndMarkFilter, BoxFilter, CommandLineFilter, CommandPackage, Decoder, FixedHeaderFilter,
    FixedHeaderProtocol, FixedSizeFilter, LineFilter, PackageEncoder, PipelineFilter,
    PipelineFilterExt, ProtocolError, SwitchAfterPackage, TerminatorFilter, TextPackage,
    TransparentFilter, bytes,
};
pub use supersocket_server as server;
#[cfg(feature = "config")]
pub use supersocket_server::ConfigError;
#[cfg(feature = "gzip")]
pub use supersocket_server::GzipLayer;
pub use supersocket_server::{
    AsyncCommand, AsyncCommandExt, BoxError, BoxStream, CloseReason, CommandFilter, Commands,
    ConnectionInfo, Detached, Detaching, Error, ErrorAction, Extensions, Filtered, HandlerResult,
    KeyedPackage, ListenAddr, ListenOptions, Listener, Middleware, PackageError, PackageHandler,
    PackageHandling, ProxyCommand, ProxyInfo, Server, ServerBuilder, ServerInfo, ServerOptions,
    ServerStatus, Session, SessionContainer, SessionState, SessionStatus, Stream, StreamLayer,
    TlsOptions, shutdown_signal,
};
#[cfg(feature = "json")]
pub use supersocket_server::{Json, JsonCommand};
#[cfg(feature = "tls")]
pub use supersocket_server::{TlsInfo, rustls};
#[cfg(feature = "udp")]
pub use supersocket_udp as udp;
#[cfg(feature = "websocket")]
pub use supersocket_websocket as websocket;

/// The items most applications need.
pub mod prelude {
    pub use supersocket_proto::{
        CommandLineFilter, CommandPackage, LineFilter, PackageEncoder, PipelineFilter,
        ProtocolError, TextPackage,
    };
    pub use supersocket_server::{
        AsyncCommand, AsyncCommandExt, CloseReason, CommandFilter, ErrorAction, HandlerResult,
        Middleware, PackageError, Server, ServerOptions, Session, SessionState,
    };

    #[cfg(feature = "client")]
    pub use supersocket_client::{ClientSender, EasyClient};
}
