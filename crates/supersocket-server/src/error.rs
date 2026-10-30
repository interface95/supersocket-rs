use std::{io, net::SocketAddr};

/// A boxed error. Any `std::error::Error + Send + Sync` converts into it with `?`.
pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// What package handlers, commands and command filters return.
pub type HandlerResult = Result<(), BoxError>;

/// Errors raised by the server itself while building or starting.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// [`Server::serve_connection`](crate::Server::serve_connection) was called before
    /// [`Server::start`](crate::Server::start) or after the server shut down.
    #[error("the server is not running")]
    NotRunning,

    /// The options contain a value that cannot work, such as a zero timeout.
    #[error("invalid server options: {0}")]
    InvalidOptions(String),

    /// A listener address could not be parsed.
    #[error("invalid listen address `{address}`: {source}")]
    InvalidAddress {
        /// The address as configured.
        address: String,
        /// Why it could not be parsed.
        source: io::Error,
    },

    /// Binding a listener failed.
    #[error("failed to bind {address}: {source}")]
    Bind {
        /// The address that could not be bound.
        address: SocketAddr,
        /// The underlying I/O error.
        source: io::Error,
    },

    /// A listener has TLS options but the crate was built without the `tls` feature.
    #[error("listener {0} has TLS options but the `tls` feature is disabled")]
    TlsDisabled(String),

    /// The TLS certificate or key could not be loaded.
    #[cfg(feature = "tls")]
    #[error("invalid TLS configuration for listener {address}: {message}")]
    Tls {
        /// The listener address.
        address: String,
        /// What went wrong.
        message: String,
    },

    /// [`Server::start`](crate::Server::start) was called more than once.
    #[error("the server has already been started")]
    AlreadyStarted,

    /// A middleware failed to start; see [`Middleware::start`](crate::Middleware::start).
    #[error("a middleware failed to start: {0}")]
    Middleware(#[source] BoxError),

    /// [`Server::serve_connection`](crate::Server::serve_connection) was called while
    /// [`max_connections`](crate::ServerOptions::max_connections) sessions are being served.
    #[error("the connection limit is reached")]
    ConnectionLimit,

    /// Any other I/O error.
    #[error(transparent)]
    Io(#[from] io::Error),
}
