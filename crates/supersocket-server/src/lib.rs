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
mod lenient;
mod middleware;
mod options;
mod panic;
mod server;
mod session;
#[cfg(feature = "tls")]
mod tls;

pub use command::{AsyncCommand, AsyncCommandExt, CommandFilter, Commands, Filtered, KeyedPackage};
#[cfg(feature = "config")]
pub use config::ConfigError;
pub use error::{BoxError, Error, HandlerResult};
pub use extensions::Extensions;
pub use middleware::Middleware;
pub use options::{ListenOptions, PackageHandling, ServerOptions, TlsOptions};
pub use server::{ErrorAction, PackageError, Server, ServerBuilder, ServerInfo};
pub use session::{CloseReason, Session, SessionContainer, SessionState};

type BoxFuture<'a, T> = std::pin::Pin<Box<dyn std::future::Future<Output = T> + Send + 'a>>;
