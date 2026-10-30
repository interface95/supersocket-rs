//! TLS helpers shared by the TLS tests.

use std::{net::SocketAddr, path::PathBuf, sync::Arc};

use supersocket::{ListenOptions, TlsOptions, prelude::*};
use tokio::net::TcpStream;
use tokio_rustls::{
    TlsConnector,
    client::TlsStream,
    rustls::{
        self, RootCertStore,
        pki_types::{CertificateDer, ServerName},
    },
};

/// A self-signed certificate for `localhost`, written to PEM files that are removed on drop.
pub struct TestCertificate {
    pub cert: PathBuf,
    pub key: PathBuf,
    pub der: CertificateDer<'static>,
}

impl TestCertificate {
    pub fn new(name: &str) -> Self {
        let certified = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
        let directory =
            std::env::temp_dir().join(format!("supersocket-tls-{}-{name}", std::process::id()));
        std::fs::create_dir_all(&directory).unwrap();
        let (cert, key) = (directory.join("cert.pem"), directory.join("key.pem"));
        std::fs::write(&cert, certified.cert.pem()).unwrap();
        std::fs::write(&key, certified.signing_key.serialize_pem()).unwrap();
        Self {
            cert,
            key,
            der: certified.cert.der().clone(),
        }
    }

    /// A local TLS listener on a free port.
    pub fn listener(&self) -> ListenOptions {
        ListenOptions {
            tls: Some(TlsOptions::new(&self.cert, &self.key)),
            ..ListenOptions::new("127.0.0.1:0")
        }
    }

    /// Roots that trust only this certificate.
    pub fn roots(&self) -> RootCertStore {
        let mut roots = RootCertStore::empty();
        roots.add(self.der.clone()).unwrap();
        roots
    }

    /// Opens a raw TLS connection that trusts this certificate.
    pub async fn connect(&self, address: SocketAddr) -> TlsStream<TcpStream> {
        let config = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(self.roots())
        .with_no_client_auth();
        let stream = TcpStream::connect(address).await.unwrap();
        TlsConnector::from(Arc::new(config))
            .connect(ServerName::try_from("localhost").unwrap(), stream)
            .await
            .unwrap()
    }
}

impl Drop for TestCertificate {
    fn drop(&mut self) {
        if let Some(directory) = self.cert.parent() {
            let _ = std::fs::remove_dir_all(directory);
        }
    }
}

/// Starts a TLS echo server that also reports whether the session is secure.
pub fn start_tls_echo(certificate: &TestCertificate) -> (Server<TextPackage>, SocketAddr) {
    let listener = certificate.listener();
    let server = Server::builder(LineFilter::default)
        .on_package(async |session, package| {
            session
                .send(format!(
                    "{} secure={}\r\n",
                    package.text,
                    session.is_secure()
                ))
                .await?;
            Ok(())
        })
        .configure(move |options| options.listeners.push(listener))
        .build()
        .unwrap();
    let address = server.start().unwrap()[0];
    (server, address)
}
