//! Server side of supersocket-rs: a socket server framework on tokio.
//!
//! A [`Server`] accepts connections, decodes each byte stream into packages with a
//! [`PipelineFilter`](supersocket_proto::PipelineFilter) and passes every package to a handler
//! together with the [`Session`] it came from. Commands dispatch packages by key, middleware sees
//! sessions come and go, and listeners can use TLS.
//!
//! ```no_run
//! use supersocket_proto::LineFilter;
//! use supersocket_server::Server;
//!
//! # async fn run() -> Result<(), supersocket_server::Error> {
//! Server::builder(LineFilter::default)
//!     .on_package(|session, package| async move {
//!         session.send(format!("{}\r\n", package.text)).await?;
//!         Ok(())
//!     })
//!     .listen("0.0.0.0:4040")
//!     .build()?
//!     .run()
//!     .await
//! # }
//! ```

mod command;
#[cfg(feature = "config")]
mod config;
mod connection;
mod error;
mod extensions;
mod layer;
mod lenient;
mod middleware;
mod options;
mod proxy_protocol;
mod server;
mod session;
#[cfg(feature = "tls")]
mod tls;

pub use command::{AsyncCommand, AsyncCommandExt, CommandFilter, Commands, Filtered, KeyedPackage};
#[cfg(feature = "config")]
pub use config::ConfigError;
pub use error::{BoxError, Error, HandlerResult};
pub use extensions::Extensions;
#[cfg(feature = "gzip")]
pub use layer::GzipLayer;
pub use layer::StreamLayer;
pub use middleware::Middleware;
pub use options::{ListenOptions, PackageHandling, ServerOptions, TlsOptions};
pub use proxy_protocol::{ProxyCommand, ProxyInfo};
pub use server::{ErrorAction, PackageError, PackageHandler, Server, ServerBuilder, ServerInfo};
pub use session::{CloseReason, ConnectionInfo, Session, SessionContainer, SessionState};
pub use supersocket_connection::{BoxStream, Stream};
#[cfg(feature = "tls")]
pub use tls::TlsInfo;
#[cfg(feature = "tls")]
pub use tokio_rustls::rustls;

type BoxFuture<'a, T> = std::pin::Pin<Box<dyn std::future::Future<Output = T> + Send + 'a>>;
