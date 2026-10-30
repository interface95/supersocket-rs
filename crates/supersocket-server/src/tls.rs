use std::{io, sync::Arc, time::Duration};

use tokio::net::TcpStream;
use tokio_rustls::{
    TlsAcceptor,
    rustls::{
        self,
        pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject},
    },
    server::TlsStream,
};

use crate::{Error, options::TlsOptions};

pub(crate) struct TlsListener {
    acceptor: TlsAcceptor,
    handshake_timeout: Duration,
}

impl TlsListener {
    pub(crate) fn new(address: &str, options: &TlsOptions) -> Result<Self, Error> {
        let invalid = |message: String| Error::Tls {
            address: address.to_owned(),
            message,
        };
        let certs = CertificateDer::pem_file_iter(&options.cert)
            .and_then(|certs| certs.collect::<Result<Vec<_>, _>>())
            .map_err(|error| {
                invalid(format!(
                    "cannot read certificates from {}: {error}",
                    options.cert.display()
                ))
            })?;
        if certs.is_empty() {
            return Err(invalid(format!(
                "no certificate in {}",
                options.cert.display()
            )));
        }
        let key = PrivateKeyDer::from_pem_file(&options.key).map_err(|error| {
            invalid(format!(
                "cannot read the private key from {}: {error}",
                options.key.display()
            ))
        })?;
        let config = rustls::ServerConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .map_err(|error| invalid(error.to_string()))?
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|error| invalid(error.to_string()))?;
        Ok(Self {
            acceptor: TlsAcceptor::from(Arc::new(config)),
            handshake_timeout: options.handshake_timeout,
        })
    }

    pub(crate) async fn accept(&self, stream: TcpStream) -> io::Result<TlsStream<TcpStream>> {
        tokio::time::timeout(self.handshake_timeout, self.acceptor.accept(stream))
            .await
            .unwrap_or_else(|_| {
                Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "TLS handshake timed out",
                ))
            })
    }
}
