use std::{path::PathBuf, time::Duration};

use serde::{Deserialize, Serialize};

use crate::{Error, lenient};

/// Options of a server. Every field has a default, so a config file only needs the fields it
/// changes. Durations are written like `"30s"` or `"5m"`, or as a number of seconds.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ServerOptions {
    /// Name of the server, used in logs. Default: `"supersocket"`.
    #[serde(deserialize_with = "lenient::string")]
    pub name: String,

    /// Addresses to listen on.
    pub listeners: Vec<ListenOptions>,

    /// Largest package a filter may buffer before it is a protocol error. Default: 1 MiB.
    #[serde(deserialize_with = "lenient::usize")]
    pub max_package_length: usize,

    /// How much to read from a socket at a time while a package is incomplete. Default: 4 KiB.
    /// Idle sessions hold no read buffer.
    #[serde(deserialize_with = "lenient::usize")]
    pub receive_buffer_size: usize,

    /// How long a handler may take for one package. Default: 30 seconds.
    #[serde(with = "lenient::duration")]
    pub package_handling_timeout: Duration,

    /// Whether the packages of a session are handled one at a time or concurrently.
    pub package_handling: PackageHandling,

    /// Close sessions that received nothing for this long. Default: never.
    #[serde(with = "lenient::option_duration")]
    pub idle_session_timeout: Option<Duration>,

    /// How often to look for idle sessions when `idle_session_timeout` is set. Default: 2 minutes.
    #[serde(with = "lenient::duration")]
    pub clear_idle_session_interval: Duration,

    /// Close a session whose peer does not accept sent data for this long. Default: never.
    #[serde(with = "lenient::option_duration")]
    pub send_timeout: Option<Duration>,

    /// Once this much data is waiting to be sent on a session, [`send`](crate::Session::send)
    /// waits for it to drain. A single send is accepted whenever nothing is waiting, so `0` sends
    /// one piece at a time. Default: 64 KiB.
    #[serde(deserialize_with = "lenient::usize")]
    pub send_buffer_limit: usize,

    /// How long a closing session may take to send data that is still queued. Default: 5 seconds.
    #[serde(with = "lenient::duration")]
    pub close_timeout: Duration,
}

impl Default for ServerOptions {
    fn default() -> Self {
        Self {
            name: "supersocket".into(),
            listeners: Vec::new(),
            max_package_length: 1024 * 1024,
            receive_buffer_size: 4 * 1024,
            package_handling_timeout: Duration::from_secs(30),
            package_handling: PackageHandling::Serial,
            idle_session_timeout: None,
            clear_idle_session_interval: Duration::from_secs(120),
            send_timeout: None,
            send_buffer_limit: 64 * 1024,
            close_timeout: Duration::from_secs(5),
        }
    }
}

impl ServerOptions {
    /// Rejects values that cannot work, such as zero sizes and zero timeouts.
    /// [`ServerBuilder::build`](crate::ServerBuilder::build) calls this.
    pub fn validate(&self) -> Result<(), Error> {
        let invalid = |message: String| Err(Error::InvalidOptions(message));
        if self.max_package_length == 0 {
            return invalid("max_package_length must be greater than zero".into());
        }
        if self.receive_buffer_size == 0 {
            return invalid("receive_buffer_size must be greater than zero".into());
        }
        if self.package_handling_timeout.is_zero() {
            return invalid("package_handling_timeout must be greater than zero".into());
        }
        if let PackageHandling::Concurrent { max_in_flight: 0 } = self.package_handling {
            return invalid("max_in_flight must be greater than zero".into());
        }
        if let Some(timeout) = self.idle_session_timeout {
            if timeout.is_zero() {
                return invalid("idle_session_timeout must be greater than zero".into());
            }
            if self.clear_idle_session_interval.is_zero() {
                return invalid("clear_idle_session_interval must be greater than zero".into());
            }
        }
        if self.send_timeout.is_some_and(|timeout| timeout.is_zero()) {
            return invalid("send_timeout must be greater than zero".into());
        }
        for listener in &self.listeners {
            if listener.backlog == 0 {
                return invalid(format!(
                    "listener {}: backlog must be greater than zero",
                    listener.address
                ));
            }
            if let Some(tls) = &listener.tls {
                if tls.handshake_timeout.is_zero() {
                    return invalid(format!(
                        "listener {}: handshake_timeout must be greater than zero",
                        listener.address
                    ));
                }
            }
        }
        Ok(())
    }
}

/// How the packages of one session are handled.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PackageHandling {
    /// One package at a time, in the order received. Reading pauses while a package is handled.
    #[default]
    Serial,
    /// Packages are handled concurrently on the runtime's worker threads, in no particular order.
    /// Reading pauses while `max_in_flight` packages of the session are being handled.
    Concurrent {
        /// How many packages of one session may be handled at the same time.
        #[serde(deserialize_with = "lenient::usize")]
        max_in_flight: usize,
    },
}

/// A listening address.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ListenOptions {
    /// Socket address to bind, such as `"0.0.0.0:4040"` or `"[::]:4040"`. Port 0 picks a free port.
    #[serde(deserialize_with = "lenient::string")]
    pub address: String,

    /// Length of the queue of connections waiting to be accepted. Default: 1024.
    #[serde(default = "default_backlog", deserialize_with = "lenient::u32")]
    pub backlog: u32,

    /// Whether to disable Nagle's algorithm on accepted connections. Default: `true`.
    #[serde(default = "default_no_delay", deserialize_with = "lenient::bool")]
    pub no_delay: bool,

    /// Accept TLS connections with this certificate. Requires the `tls` feature.
    #[serde(default)]
    pub tls: Option<TlsOptions>,
}

impl ListenOptions {
    /// Plain TCP on `address` with default settings.
    pub fn new(address: impl Into<String>) -> Self {
        Self {
            address: address.into(),
            backlog: default_backlog(),
            no_delay: default_no_delay(),
            tls: None,
        }
    }
}

fn default_backlog() -> u32 {
    1024
}

fn default_no_delay() -> bool {
    true
}

/// TLS settings of a listener. The certificate chain and the private key are PEM files.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TlsOptions {
    /// PEM file with the certificate chain, leaf certificate first.
    #[serde(deserialize_with = "lenient::path")]
    pub cert: PathBuf,

    /// PEM file with the private key.
    #[serde(deserialize_with = "lenient::path")]
    pub key: PathBuf,

    /// How long a client may take to complete the handshake. Default: 10 seconds.
    #[serde(default = "default_handshake_timeout", with = "lenient::duration")]
    pub handshake_timeout: Duration,
}

impl TlsOptions {
    /// TLS with the certificate chain and private key in the given PEM files.
    pub fn new(cert: impl Into<PathBuf>, key: impl Into<PathBuf>) -> Self {
        Self {
            cert: cert.into(),
            key: key.into(),
            handshake_timeout: default_handshake_timeout(),
        }
    }
}

fn default_handshake_timeout() -> Duration {
    Duration::from_secs(10)
}
