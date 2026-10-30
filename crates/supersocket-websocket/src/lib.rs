//! WebSocket (RFC 6455) for supersocket-rs, modeled on SuperSocket's WebSocket server.
//!
//! The server runs on the supersocket server. Its first pipeline filter reads the HTTP upgrade
//! request and switches to the frame filter, and sessions count as connected once the handshake
//! is complete, so broadcasts never reach a connection that is still upgrading. Listeners with
//! TLS give `wss://`. Pings, pongs and the closing handshake are handled for you: handlers see
//! text and binary messages, and fragmented messages arrive joined.
//!
//! ```no_run
//! use supersocket_websocket::WebSocketServer;
//!
//! # async fn run() -> Result<(), supersocket_server::Error> {
//! WebSocketServer::builder()
//!     .on_message(async |session, message| {
//!         session.send(&message).await?;
//!         Ok(())
//!     })
//!     .listen("0.0.0.0:4040")
//!     .build()?
//!     .run()
//!     .await
//! # }
//! ```
//!
//! With the `client` feature, [`WebSocketClient`] connects to any WebSocket server through the
//! connectors of `EasyClient`, including proxies and, with `tls`, `wss://`.

#[cfg(feature = "client")]
mod client;
mod filter;
mod frame;
#[cfg(fuzzing)]
#[doc(hidden)]
pub mod fuzzing;
mod handshake;
mod message;
mod server;
mod session;

#[cfg(feature = "client")]
pub use client::{WebSocketClient, WebSocketClientBuilder, WebSocketSender};
pub use filter::WebSocketPackage;
pub use handshake::{Accept, HandshakeRequest, Rejection};
pub use message::{CloseCode, CloseFrame, Message};
pub use server::{WebSocketBuilder, WebSocketServer, WebSocketState};
pub use session::WebSocketSession;
