//! TLS helpers shared by the TLS tests.

use std::{io, net::SocketAddr, path::PathBuf, sync::Arc};

use supersocket::{ListenOptions, TlsOptions, prelude::*};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::TcpStream,
};
use tokio_rustls::{
    TlsConnector,
    client::TlsStream,
    rustls::{
        self, RootCertStore,
        pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer, ServerName},
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

impl TestCertificate {
    /// A client configuration that trusts this certificate, sends `identity` when given and
    /// offers the ALPN protocols `alpn`.
    pub fn client_config(
        &self,
        identity: Option<(CertificateDer<'static>, PrivateKeyDer<'static>)>,
        alpn: &[&[u8]],
    ) -> rustls::ClientConfig {
        let builder = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(self.roots());
        let mut config = match identity {
            Some((cert, key)) => builder.with_client_auth_cert(vec![cert], key).unwrap(),
            None => builder.with_no_client_auth(),
        };
        config.alpn_protocols = alpn.iter().map(|protocol| protocol.to_vec()).collect();
        config
    }

    /// Performs the TLS handshake with `config` over `stream`.
    pub async fn handshake<IO>(
        &self,
        config: rustls::ClientConfig,
        stream: IO,
    ) -> io::Result<TlsStream<IO>>
    where
        IO: AsyncRead + AsyncWrite + Unpin,
    {
        TlsConnector::from(Arc::new(config))
            .connect(ServerName::try_from("localhost").unwrap(), stream)
            .await
    }
}

impl Drop for TestCertificate {
    fn drop(&mut self) {
        if let Some(directory) = self.cert.parent() {
            let _ = std::fs::remove_dir_all(directory);
        }
    }
}

/// A certificate authority for client certificates, written to a PEM file that is removed on
/// drop.
pub struct TestAuthority {
    pub pem: PathBuf,
    issuer: rcgen::Issuer<'static, rcgen::KeyPair>,
}

impl TestAuthority {
    pub fn new(name: &str) -> Self {
        let key = rcgen::KeyPair::generate().unwrap();
        let mut params = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
        params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        params
            .distinguished_name
            .push(rcgen::DnType::CommonName, format!("{name} authority"));
        params.key_usages = vec![
            rcgen::KeyUsagePurpose::KeyCertSign,
            rcgen::KeyUsagePurpose::DigitalSignature,
        ];
        let cert = params.self_signed(&key).unwrap();
        let directory =
            std::env::temp_dir().join(format!("supersocket-ca-{}-{name}", std::process::id()));
        std::fs::create_dir_all(&directory).unwrap();
        let pem = directory.join("ca.pem");
        std::fs::write(&pem, cert.pem()).unwrap();
        Self {
            pem,
            issuer: rcgen::Issuer::new(params, key),
        }
    }

    /// Issues a client certificate for `name`.
    pub fn issue(&self, name: &str) -> (CertificateDer<'static>, PrivateKeyDer<'static>) {
        let key = rcgen::KeyPair::generate().unwrap();
        let mut params = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
        params
            .distinguished_name
            .push(rcgen::DnType::CommonName, name);
        params.extended_key_usages = vec![rcgen::ExtendedKeyUsagePurpose::ClientAuth];
        let cert = params.signed_by(&key, &self.issuer).unwrap();
        let key = PrivatePkcs8KeyDer::from(key.serialize_der()).into();
        (cert.der().clone(), key)
    }
}

impl Drop for TestAuthority {
    fn drop(&mut self) {
        if let Some(directory) = self.pem.parent() {
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
