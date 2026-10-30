//! Tests of gzip-compressed connections.

mod common;

use common::{WAIT, start};
use supersocket::{GzipLayer, ServerBuilder, client::GzipConnector, prelude::*};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    time::timeout,
};

fn compressed_echo() -> ServerBuilder<TextPackage> {
    Server::builder(LineFilter::default)
        .stream_layer(GzipLayer)
        .on_package(async |session, package| {
            session.send(format!("{}\r\n", package.text)).await?;
            Ok(())
        })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn compressed_connections_carry_packages_both_ways() {
    let (server, address) = start(compressed_echo());
    let mut client = EasyClient::builder(LineFilter::default)
        .connector(GzipConnector)
        .connect(address)
        .await
        .unwrap();
    for text in ["hello".to_owned(), "a".repeat(100_000)] {
        client.send(format!("{text}\r\n")).await.unwrap();
        let reply = timeout(WAIT, client.receive()).await.unwrap().unwrap();
        assert_eq!(reply.map(|package| package.text), Some(text));
    }
    client.close().await;
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_bytes_on_the_wire_are_gzip() {
    let (server, address) = start(compressed_echo());
    let (mut raw_reader, writer) = TcpStream::connect(address).await.unwrap().into_split();
    // Compress the request, but read the reply as it is on the wire.
    let mut requests =
        supersocket::connection::gzip(Box::new(tokio::io::join(tokio::io::empty(), writer)));
    let line = format!("{}\r\n", "a".repeat(100_000));
    requests.write_all(line.as_bytes()).await.unwrap();
    requests.flush().await.unwrap();

    let mut reply = vec![0; 64 * 1024];
    let length = timeout(WAIT, raw_reader.read(&mut reply))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(reply[..2], [0x1f, 0x8b], "the gzip magic number");
    assert!(length < 2_000, "{length} bytes for a 100 kB line");
    server.shutdown().await;
}
