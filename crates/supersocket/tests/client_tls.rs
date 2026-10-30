//! Tests of the client over TLS.

mod common;

use common::{
    WAIT,
    tls::{TestCertificate, start_tls_echo},
};
use supersocket::{client::TlsConnector, prelude::*};
use tokio::time::timeout;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn client_talks_tls_to_a_tls_listener() {
    let certificate = TestCertificate::new("client");
    let (server, address) = start_tls_echo(&certificate);

    let mut client = EasyClient::builder(LineFilter::default())
        .tls(TlsConnector::with_root_certificates(certificate.roots()).server_name("localhost"))
        .connect(address)
        .await
        .unwrap();
    client.send("over tls\r\n").await.unwrap();
    let reply = timeout(WAIT, client.receive()).await.unwrap().unwrap();
    assert_eq!(reply.unwrap().text, "over tls secure=true");

    client.close().await;
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn untrusted_certificates_are_rejected() {
    let certificate = TestCertificate::new("untrusted");
    let (server, address) = start_tls_echo(&certificate);

    // The default roots do not include the self-signed test certificate.
    let result = EasyClient::builder(LineFilter::default())
        .tls(TlsConnector::new().server_name("localhost"))
        .connect(address)
        .await;
    assert!(result.is_err());

    server.shutdown().await;
}
