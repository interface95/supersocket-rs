//! HTTP/1.1 for supersocket-rs, like SuperSocket's `SuperSocket.Http`: an [`HttpFilter`] that
//! cuts a connection into [`HttpRequest`]s, [`HttpResponse`]s to answer them, keep-alive, and
//! Server-Sent Events through [`SseWriter`].
//!
//! This is a small HTTP for services that speak it besides their own protocols, such as a health
//! endpoint, a push stream or a WebSocket upgrade, not a web framework: there is no routing beyond
//! [`HttpRequest::path`], no TLS termination beyond the server's TLS listeners, and no HTTP/2.
//!
//! ```no_run
//! use supersocket_http::{HttpFilter, HttpResponse, HttpSessionExt};
//! use supersocket_server::Server;
//!
//! # async fn run() -> Result<(), supersocket_server::Error> {
//! Server::builder(HttpFilter::default)
//!     .on_package(async |session, request| {
//!         let response = match (request.method(), request.path()) {
//!             ("GET", "/health") => HttpResponse::ok().text("ok"),
//!             _ => HttpResponse::new(404).text("not found"),
//!         };
//!         session.respond(&request, response).await?;
//!         Ok(())
//!     })
//!     .listen("0.0.0.0:8080")
//!     .build()?
//!     .run()
//!     .await
//! # }
//! ```
//!
//! A connection serves one request after the other, as HTTP/1.1 keep-alive does: the filter
//! waits for the whole request, body included, before the handler sees it, so pipelined
//! requests are handled in order. A request or response that says `Connection: close` ends the
//! connection once the response is sent. The [idle session
//! timeout](supersocket_server::ServerOptions::idle_session_timeout) is the keep-alive timeout,
//! and [`max_package_length`](supersocket_server::ServerOptions::max_package_length) limits a
//! request with its body.

mod filter;
mod request;
mod response;
mod session;

pub use filter::{HttpFilter, HttpOptions};
pub use request::{HttpRequest, HttpVersion};
pub use response::{HttpEncoder, HttpResponse};
pub use session::{HttpSessionExt, SseEvent, SseOptions, SseWriter};
