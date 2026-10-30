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

#[cfg(feature = "client")]
pub use supersocket_client as client;
pub use supersocket_connection as connection;
pub use supersocket_proto as proto;
pub use supersocket_proto::*;
pub use supersocket_server as server;
pub use supersocket_server::*;

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
