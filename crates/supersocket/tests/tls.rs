//! Tests of TLS listeners.

mod common;

use common::{
    Client,
    tls::{TestCertificate, start_tls_echo},
};
use supersocket::{ListenOptions, TlsOptions, prelude::*};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
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
