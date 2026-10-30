use std::{fmt, io, sync::Arc};

use tokio_rustls::rustls::{self, RootCertStore, pki_types::ServerName};

use crate::{Connection, Connector, Endpoint};

/// Wraps the connection in TLS, like SuperSocket's `SslStreamConnector`.
///
/// By default the server certificate is verified against the Mozilla root certificates, for the
/// host name of the target.
#[derive(Clone)]
pub struct TlsConnector {
    connector: tokio_rustls::TlsConnector,
    server_name: Option<String>,
}

impl Default for TlsConnector {
    fn default() -> Self {
        Self::new()
    }
}

impl TlsConnector {
    /// Verifies servers against the Mozilla root certificates.
    pub fn new() -> Self {
        Self::with_root_certificates(RootCertStore {
            roots: webpki_roots::TLS_SERVER_ROOTS.to_vec(),
        })
    }

    /// Verifies servers against `roots`, for example a private certificate authority.
    pub fn with_root_certificates(roots: RootCertStore) -> Self {
        let config = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .expect("the ring provider supports the default protocol versions")
        .with_root_certificates(roots)
        .with_no_client_auth();
        Self::with_config(Arc::new(config))
    }

    /// Uses a complete rustls configuration, for client certificates or custom verification.
    pub fn with_config(config: Arc<rustls::ClientConfig>) -> Self {
        Self {
            connector: tokio_rustls::TlsConnector::from(config),
            server_name: None,
        }
    }

    /// Verifies the certificate for `name` instead of the target's host, which is useful when
    /// connecting by IP address.
    pub fn server_name(mut self, name: impl Into<String>) -> Self {
        self.server_name = Some(name.into());
        self
    }
}

impl fmt::Debug for TlsConnector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TlsConnector")
            .field("server_name", &self.server_name)
            .finish_non_exhaustive()
    }
}

impl Connector for TlsConnector {
    async fn connect(
        &self,
        target: &Endpoint,
        previous: Option<Connection>,
    ) -> io::Result<Connection> {
        let previous = previous.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "TlsConnector needs a connection opened by an earlier connector",
            )
        })?;
        let name = self
            .server_name
            .clone()
            .unwrap_or_else(|| target.host().to_owned());
        let name = ServerName::try_from(name)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
        let (stream, local_addr, peer_addr) = previous.into_parts();
        let stream = self.connector.connect(name, stream).await?;
        Ok(Connection::from_parts(
            Box::new(stream),
            local_addr,
            peer_addr,
        ))
    }
}
