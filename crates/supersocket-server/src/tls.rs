use std::{io, path::Path, sync::Arc};

use tokio::io::{AsyncRead, AsyncWrite};
use tokio_rustls::{
    TlsAcceptor,
    rustls::{
        self, ProtocolVersion, RootCertStore, ServerConnection,
        pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject},
        server::WebPkiClientVerifier,
    },
    server::TlsStream,
};

use crate::{Error, options::TlsOptions};

/// What the TLS handshake of a session established; see
/// [`Session::tls_info`](crate::Session::tls_info).
#[derive(Debug, Clone)]
pub struct TlsInfo {
    peer_certificates: Vec<CertificateDer<'static>>,
    alpn_protocol: Option<Vec<u8>>,
    server_name: Option<String>,
    protocol_version: Option<ProtocolVersion>,
}

impl TlsInfo {
    pub(crate) fn new(connection: &ServerConnection) -> Self {
        Self {
            peer_certificates: connection
                .peer_certificates()
                .map(<[_]>::to_vec)
                .unwrap_or_default(),
            alpn_protocol: connection.alpn_protocol().map(<[u8]>::to_vec),
            server_name: connection.server_name().map(str::to_owned),
            protocol_version: connection.protocol_version(),
        }
    }

    /// The certificate chain the client presented, leaf first; empty when it sent none.
    pub fn peer_certificates(&self) -> &[CertificateDer<'static>] {
        &self.peer_certificates
    }

    /// The application protocol agreed on with ALPN.
    pub fn alpn_protocol(&self) -> Option<&[u8]> {
        self.alpn_protocol.as_deref()
    }

    /// The host name the client asked for with SNI.
    pub fn server_name(&self) -> Option<&str> {
        self.server_name.as_deref()
    }

    /// The TLS version in use.
    pub fn protocol_version(&self) -> Option<ProtocolVersion> {
        self.protocol_version
    }
}

pub(crate) struct TlsListener {
    acceptor: TlsAcceptor,
}

impl TlsListener {
    pub(crate) fn new(address: &str, options: &TlsOptions) -> Result<Self, Error> {
        let config = match &options.config {
            Some(config) => config.clone(),
            None => Arc::new(build_config(options).map_err(|message| Error::Tls {
                address: address.to_owned(),
                message,
            })?),
        };
        Ok(Self {
            acceptor: TlsAcceptor::from(config),
        })
    }

    /// Performs the handshake, giving up at `deadline`.
    pub(crate) async fn accept<IO>(
        &self,
        stream: IO,
        deadline: tokio::time::Instant,
    ) -> io::Result<TlsStream<IO>>
    where
        IO: AsyncRead + AsyncWrite + Unpin,
    {
        tokio::time::timeout_at(deadline, self.acceptor.accept(stream))
            .await
            .unwrap_or_else(|_| {
                Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "TLS handshake timed out",
                ))
            })
    }
}

/// Builds the rustls configuration the files in `options` describe.
fn build_config(options: &TlsOptions) -> Result<rustls::ServerConfig, String> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let certs = read_certificates(&options.cert)?;
    let key = PrivateKeyDer::from_pem_file(&options.key).map_err(|error| {
        format!(
            "cannot read the private key from {}: {error}",
            options.key.display()
        )
    })?;
    let builder = rustls::ServerConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()
        .map_err(|error| error.to_string())?;
    let builder = match &options.client_ca {
        None => builder.with_no_client_auth(),
        Some(path) => {
            let mut roots = RootCertStore::empty();
            for cert in read_certificates(path)? {
                roots.add(cert).map_err(|error| {
                    format!(
                        "invalid certificate authority in {}: {error}",
                        path.display()
                    )
                })?;
            }
            let verifier = WebPkiClientVerifier::builder_with_provider(Arc::new(roots), provider);
            let verifier = if options.client_auth_optional {
                verifier.allow_unauthenticated()
            } else {
                verifier
            };
            builder.with_client_cert_verifier(verifier.build().map_err(|error| error.to_string())?)
        }
    };
    builder
        .with_single_cert(certs, key)
        .map_err(|error| error.to_string())
}

fn read_certificates(path: &Path) -> Result<Vec<CertificateDer<'static>>, String> {
    let certs = CertificateDer::pem_file_iter(path)
        .and_then(|certs| certs.collect::<Result<Vec<_>, _>>())
        .map_err(|error| format!("cannot read certificates from {}: {error}", path.display()))?;
    if certs.is_empty() {
        return Err(format!("no certificate in {}", path.display()));
    }
    Ok(certs)
}
