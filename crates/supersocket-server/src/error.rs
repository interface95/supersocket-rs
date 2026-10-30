use std::{io, net::SocketAddr};

/// A boxed error. Any `std::error::Error + Send + Sync` converts into it with `?`.
pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// What package handlers, commands and command filters return.
pub type HandlerResult = Result<(), BoxError>;

/// Errors raised by the server itself while building or starting.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// [`ServerBuilder::build`](crate::ServerBuilder::build) was called without any listener.
    #[error("no listener is configured")]
    NoListener,

    /// A listener address could not be parsed.
    #[error("invalid listen address `{address}`: {source}")]
    InvalidAddress {
        /// The address as configured.
        address: String,
        /// Why it could not be parsed.
        source: std::net::AddrParseError,
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

    /// Any other I/O error.
    #[error(transparent)]
    Io(#[from] io::Error),
}
