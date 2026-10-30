//! Tests of TLS listeners.

mod common;

use std::{net::SocketAddr, path::PathBuf, sync::Arc};

use common::Client;
use supersocket::{ListenOptions, TlsOptions, prelude::*};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};
use tokio_rustls::{
    TlsConnector,
    client::TlsStream,
    rustls::{
        self, RootCertStore,
        pki_types::{CertificateDer, ServerName},
    },
};

/// A self-signed certificate for `localhost`, written to PEM files.
struct TestCertificate {
    cert: PathBuf,
    key: PathBuf,
    der: CertificateDer<'static>,
}

impl TestCertificate {
    fn new(name: &str) -> Self {
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

    fn listener(&self) -> ListenOptions {
        ListenOptions {
            tls: Some(TlsOptions::new(&self.cert, &self.key)),
            ..ListenOptions::new("127.0.0.1:0")
        }
    }

    async fn connect(&self, address: SocketAddr) -> TlsStream<TcpStream> {
        let mut roots = RootCertStore::empty();
        roots.add(self.der.clone()).unwrap();
        let config = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(roots)
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

fn start_tls_echo(certificate: &TestCertificate) -> (Server<TextPackage>, SocketAddr) {
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tls_listener_serves_encrypted_sessions() {
    let certificate = TestCertificate::new("echo");
    let (server, address) = start_tls_echo(&certificate);

    let (reader, writer) = tokio::io::split(certificate.connect(address).await);
    let mut client = Client::new(reader, writer);
    client.send(b"hello\r\nworld\r\n").await;
    assert_eq!(client.line().await, "hello secure=true");
    assert_eq!(client.line().await, "world secure=true");

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failed_handshakes_do_not_affect_other_clients() {
    let certificate = TestCertificate::new("bad-handshake");
    let (server, address) = start_tls_echo(&certificate);

    // A plain-text client gets disconnected.
    let mut plain = TcpStream::connect(address).await.unwrap();
    plain.write_all(b"not tls\r\n").await.unwrap();
    let mut rest = Vec::new();
    let _ = tokio::time::timeout(common::WAIT, plain.read_to_end(&mut rest))
        .await
        .expect("plain client was not disconnected");

    let (reader, writer) = tokio::io::split(certificate.connect(address).await);
    let mut client = Client::new(reader, writer);
    client.send(b"still works\r\n").await;
    assert_eq!(client.line().await, "still works secure=true");
    assert_eq!(server.info().session_count(), 1);

    server.shutdown().await;
}

#[test]
fn unreadable_certificates_fail_the_build() {
    let listener = ListenOptions {
        tls: Some(TlsOptions::new(
            "/nonexistent/cert.pem",
            "/nonexistent/key.pem",
        )),
        ..ListenOptions::new("127.0.0.1:0")
    };
    let result = Server::builder(LineFilter::default)
        .configure(move |options| options.listeners.push(listener))
        .build();
    assert!(matches!(result, Err(supersocket::Error::Tls { .. })));
}
