//! Tests of TLS listeners.

mod common;

use std::{net::SocketAddr, sync::Arc};

use common::{
    Client, WAIT,
    tls::{TestAuthority, TestCertificate, start_tls_echo},
};
use supersocket::{
    ListenOptions, TlsOptions,
    prelude::*,
    rustls::{
        self,
        pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject},
    },
};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader, BufWriter},
    net::TcpStream,
    sync::mpsc,
    time::timeout,
};

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

/// What a TLS session looked like to the server.
struct Report {
    certificates: Vec<CertificateDer<'static>>,
    alpn: Option<Vec<u8>>,
    server_name: Option<String>,
    remote_addr: SocketAddr,
}

/// Starts a server on `listener` that answers every line with `ok` and reports every session
/// that connects.
fn start_reporting(
    listener: ListenOptions,
) -> (
    Server<TextPackage>,
    SocketAddr,
    mpsc::UnboundedReceiver<Report>,
) {
    let (reports, received) = mpsc::unbounded_channel();
    let server = Server::builder(LineFilter::default)
        .on_connected(move |session| {
            let reports = reports.clone();
            async move {
                let tls = session.tls_info().expect("a TLS session");
                let _ = reports.send(Report {
                    certificates: tls.peer_certificates().to_vec(),
                    alpn: tls.alpn_protocol().map(<[u8]>::to_vec),
                    server_name: tls.server_name().map(str::to_owned),
                    remote_addr: session.remote_addr(),
                });
            }
        })
        .on_package(async |session, _package| {
            session.send("ok\r\n").await?;
            Ok(())
        })
        .configure(move |options| options.listeners.push(listener))
        .build()
        .unwrap();
    let address = server.start().unwrap()[0];
    (server, address, received)
}

fn tls_listener(tls: TlsOptions) -> ListenOptions {
    ListenOptions {
        tls: Some(tls),
        ..ListenOptions::new("127.0.0.1:0")
    }
}

/// Whether a client connecting with `config` is served: a line it sends gets an answer.
async fn served(
    certificate: &TestCertificate,
    address: SocketAddr,
    config: rustls::ClientConfig,
) -> bool {
    let stream = TcpStream::connect(address).await.unwrap();
    let Ok(stream) = certificate.handshake(config, stream).await else {
        return false;
    };
    let (reader, mut writer) = tokio::io::split(stream);
    if writer.write_all(b"hi\r\n").await.is_err() || writer.flush().await.is_err() {
        return false;
    }
    let mut line = String::new();
    let read = timeout(WAIT, BufReader::new(reader).read_line(&mut line))
        .await
        .expect("the server neither answered nor closed the connection");
    read.is_ok() && line == "ok\r\n"
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn client_certificates_are_required_and_reported() {
    let certificate = TestCertificate::new("client-auth");
    let authority = TestAuthority::new("client-auth");
    let (server, address, mut reports) = start_reporting(tls_listener(TlsOptions {
        client_ca: Some(authority.pem.clone()),
        ..TlsOptions::new(&certificate.cert, &certificate.key)
    }));

    let (cert, key) = authority.issue("alice");
    let identity = Some((cert.clone(), key));
    assert!(
        served(
            &certificate,
            address,
            certificate.client_config(identity, &[])
        )
        .await
    );
    let report = timeout(WAIT, reports.recv()).await.unwrap().unwrap();
    assert_eq!(report.certificates, [cert]);

    assert!(!served(&certificate, address, certificate.client_config(None, &[])).await);
    let stranger = TestAuthority::new("stranger").issue("mallory");
    let config = certificate.client_config(Some(stranger), &[]);
    assert!(!served(&certificate, address, config).await);
    // Refused clients never became sessions.
    assert!(reports.try_recv().is_err());
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn client_certificates_can_be_optional() {
    let certificate = TestCertificate::new("optional-auth");
    let authority = TestAuthority::new("optional-auth");
    let (server, address, mut reports) = start_reporting(tls_listener(TlsOptions {
        client_ca: Some(authority.pem.clone()),
        client_auth_optional: true,
        ..TlsOptions::new(&certificate.cert, &certificate.key)
    }));

    assert!(served(&certificate, address, certificate.client_config(None, &[])).await);
    let report = timeout(WAIT, reports.recv()).await.unwrap().unwrap();
    assert!(report.certificates.is_empty());

    let identity = Some(authority.issue("bob"));
    assert!(
        served(
            &certificate,
            address,
            certificate.client_config(identity, &[])
        )
        .await
    );
    let report = timeout(WAIT, reports.recv()).await.unwrap().unwrap();
    assert_eq!(report.certificates.len(), 1);

    // A certificate that is sent must still be valid.
    let stranger = TestAuthority::new("optional-stranger").issue("mallory");
    let config = certificate.client_config(Some(stranger), &[]);
    assert!(!served(&certificate, address, config).await);
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn custom_rustls_configurations_are_used_as_given() {
    let certificate = TestCertificate::new("custom-config");
    let key = PrivateKeyDer::from_pem_file(&certificate.key).unwrap();
    let mut config = rustls::ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_no_client_auth()
    .with_single_cert(vec![certificate.der.clone()], key)
    .unwrap();
    config.alpn_protocols = vec![b"chat/1".to_vec()];
    let (server, address, mut reports) =
        start_reporting(tls_listener(TlsOptions::with_config(Arc::new(config))));

    let config = certificate.client_config(None, &[b"other", b"chat/1"]);
    assert!(served(&certificate, address, config).await);
    let report = timeout(WAIT, reports.recv()).await.unwrap().unwrap();
    assert_eq!(report.alpn.as_deref(), Some(&b"chat/1"[..]));
    assert_eq!(report.server_name.as_deref(), Some("localhost"));
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn proxy_protocol_headers_come_before_the_tls_handshake() {
    let certificate = TestCertificate::new("proxied");
    let (server, address, mut reports) = start_reporting(ListenOptions {
        proxy_protocol: true,
        ..certificate.listener()
    });

    // Buffered, so that the header and the start of the handshake leave in one packet.
    let mut stream = BufWriter::new(TcpStream::connect(address).await.unwrap());
    stream
        .write_all(b"PROXY TCP4 203.0.113.5 192.0.2.1 50000 443\r\n")
        .await
        .unwrap();
    let mut stream = certificate
        .handshake(certificate.client_config(None, &[]), stream)
        .await
        .unwrap();
    stream.write_all(b"hi\r\n").await.unwrap();
    stream.flush().await.unwrap();
    let report = timeout(WAIT, reports.recv()).await.unwrap().unwrap();
    assert_eq!(report.remote_addr, "203.0.113.5:50000".parse().unwrap());
    server.shutdown().await;
}

/// Greets every connection it wraps.
struct Greeting;

impl supersocket::StreamLayer for Greeting {
    async fn wrap(
        &self,
        mut stream: supersocket::BoxStream,
        connection: &supersocket::ConnectionInfo,
    ) -> std::io::Result<supersocket::BoxStream> {
        let greeting = format!("welcome secure={}\r\n", connection.is_secure());
        stream.write_all(greeting.as_bytes()).await?;
        Ok(stream)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stream_layers_run_inside_tls() {
    let certificate = TestCertificate::new("layered");
    let listener = certificate.listener();
    let server = Server::builder(LineFilter::default)
        .stream_layer(Greeting)
        .on_package(async |session, package| {
            session.send(format!("{}\r\n", package.text)).await?;
            Ok(())
        })
        .configure(move |options| options.listeners.push(listener))
        .build()
        .unwrap();
    let address = server.start().unwrap()[0];

    let (reader, writer) = tokio::io::split(certificate.connect(address).await);
    let mut client = Client::new(reader, writer);
    assert_eq!(client.line().await, "welcome secure=true");
    client.send(b"hello\r\n").await;
    assert_eq!(client.line().await, "hello");
    server.shutdown().await;
}
