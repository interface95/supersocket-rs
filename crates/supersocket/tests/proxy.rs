//! Tests of the proxy connectors against small proxies written here.

mod common;

use std::{
    future::Future,
    io,
    net::{Ipv4Addr, Ipv6Addr, SocketAddr},
    sync::Arc,
    time::Duration,
};

use common::{WAIT, start};
use supersocket::{
    Server,
    client::{EasyClient, HttpProxy, Socks4Proxy, Socks5Proxy},
    prelude::*,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::timeout,
};

/// An echo server that greets each client first.
fn start_greeting_echo() -> (Server<TextPackage>, SocketAddr) {
    start(
        Server::builder(LineFilter::default)
            .on_connected(async |session| {
                let _ = session.send("welcome\r\n").await;
            })
            .on_package(async |session, package| {
                session.send(format!("{}\r\n", package.text)).await?;
                Ok(())
            }),
    )
}

/// Runs `serve` for every connection accepted on a free local port.
async fn spawn_proxy<F, Fut>(serve: F) -> SocketAddr
where
    F: Fn(TcpStream) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = io::Result<()>> + Send + 'static,
{
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let serve = Arc::new(serve);
    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            let serve = serve.clone();
            tokio::spawn(async move {
                let _ = serve(stream).await;
            });
        }
    });
    address
}

async fn relay(mut client: TcpStream, mut upstream: TcpStream) -> io::Result<()> {
    tokio::io::copy_bidirectional(&mut client, &mut upstream).await?;
    Ok(())
}

/// Replies with lines ending in `newline`.
async fn http_proxy(
    mut client: TcpStream,
    authorization: Option<&'static str>,
    newline: &'static str,
) -> io::Result<()> {
    let mut head = Vec::new();
    while !head.ends_with(b"\r\n\r\n") {
        head.push(client.read_u8().await?);
    }
    let head = String::from_utf8(head).unwrap();
    let target = head.split_whitespace().nth(1).unwrap().to_owned();
    if let Some(expected) = authorization {
        let header = format!("Proxy-Authorization: Basic {expected}");
        if !head.lines().any(|line| line == header) {
            let reply = format!("HTTP/1.1 407 Proxy Authentication Required{newline}{newline}");
            client.write_all(reply.as_bytes()).await?;
            return Ok(());
        }
    }
    let mut upstream = TcpStream::connect(target.as_str()).await?;
    // Send the reply together with the server's greeting, so the client receives data right
    // behind the reply in the same read.
    let mut greeting = vec![0; 64];
    let length = timeout(Duration::from_secs(1), upstream.read(&mut greeting))
        .await
        .unwrap_or(Ok(0))?;
    let mut reply =
        format!("HTTP/1.1 200 Connection established{newline}Via: test{newline}{newline}")
            .into_bytes();
    reply.extend_from_slice(&greeting[..length]);
    client.write_all(&reply).await?;
    relay(client, upstream).await
}

async fn read_until_nul(stream: &mut TcpStream) -> io::Result<String> {
    let mut bytes = Vec::new();
    loop {
        match stream.read_u8().await? {
            0 => return Ok(String::from_utf8(bytes).unwrap()),
            byte => bytes.push(byte),
        }
    }
}

async fn socks4_proxy(mut client: TcpStream) -> io::Result<()> {
    let mut header = [0; 8];
    client.read_exact(&mut header).await?;
    assert_eq!(header[..2], [4, 1]);
    let port = u16::from_be_bytes([header[2], header[3]]);
    let ip = [header[4], header[5], header[6], header[7]];
    let _user_id = read_until_nul(&mut client).await?;
    let target = if ip[..3] == [0, 0, 0] && ip[3] != 0 {
        format!("{}:{port}", read_until_nul(&mut client).await?)
    } else {
        format!("{}:{port}", Ipv4Addr::from(ip))
    };
    let upstream = TcpStream::connect(target.as_str()).await?;
    client.write_all(&[0, 0x5a, 0, 0, 0, 0, 0, 0]).await?;
    relay(client, upstream).await
}

async fn socks5_proxy(
    mut client: TcpStream,
    credentials: Option<(&'static str, &'static str)>,
) -> io::Result<()> {
    assert_eq!(client.read_u8().await?, 5);
    let mut methods = vec![0; usize::from(client.read_u8().await?)];
    client.read_exact(&mut methods).await?;
    match credentials {
        Some((username, password)) => {
            if !methods.contains(&2) {
                client.write_all(&[5, 0xff]).await?;
                return Ok(());
            }
            client.write_all(&[5, 2]).await?;
            assert_eq!(client.read_u8().await?, 1);
            let mut given_username = vec![0; usize::from(client.read_u8().await?)];
            client.read_exact(&mut given_username).await?;
            let mut given_password = vec![0; usize::from(client.read_u8().await?)];
            client.read_exact(&mut given_password).await?;
            let accepted =
                given_username == username.as_bytes() && given_password == password.as_bytes();
            client.write_all(&[1, if accepted { 0 } else { 1 }]).await?;
            if !accepted {
                return Ok(());
            }
        }
        None => client.write_all(&[5, 0]).await?,
    }
    let mut request = [0; 4];
    client.read_exact(&mut request).await?;
    let target = match request[3] {
        1 => {
            let mut ip = [0; 4];
            client.read_exact(&mut ip).await?;
            format!("{}:{}", Ipv4Addr::from(ip), client.read_u16().await?)
        }
        3 => {
            let mut host = vec![0; usize::from(client.read_u8().await?)];
            client.read_exact(&mut host).await?;
            let host = String::from_utf8(host).unwrap();
            format!("{host}:{}", client.read_u16().await?)
        }
        4 => {
            let mut ip = [0; 16];
            client.read_exact(&mut ip).await?;
            format!("[{}]:{}", Ipv6Addr::from(ip), client.read_u16().await?)
        }
        kind => panic!("unexpected address type {kind}"),
    };
    let upstream = TcpStream::connect(target.as_str()).await?;
    // Reply with the address type the client used, so domain replies are covered too.
    let mut reply = vec![5, 0, 0];
    if request[3] == 3 {
        reply.extend([3, 9]);
        reply.extend(b"localhost");
    } else {
        reply.extend([1, 127, 0, 0, 1]);
    }
    reply.extend(upstream.local_addr()?.port().to_be_bytes());
    client.write_all(&reply).await?;
    relay(client, upstream).await
}

async fn receive_text(client: &mut EasyClient<TextPackage>) -> Option<String> {
    timeout(WAIT, client.receive())
        .await
        .expect("timed out waiting for a package")
        .unwrap()
        .map(|package| package.text)
}

/// Checks the greeting and an echo through `client`.
async fn assert_greeting_and_echo(mut client: EasyClient<TextPackage>) {
    assert_eq!(receive_text(&mut client).await.as_deref(), Some("welcome"));
    client.send("through the proxy\r\n").await.unwrap();
    assert_eq!(
        receive_text(&mut client).await.as_deref(),
        Some("through the proxy")
    );
    client.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_proxy_connects_and_keeps_data_sent_with_its_reply() {
    let (server, address) = start_greeting_echo();
    // "user:pass" in base64.
    let proxy = spawn_proxy(|client| http_proxy(client, Some("dXNlcjpwYXNz"), "\r\n")).await;

    let client = EasyClient::builder(LineFilter::default)
        .proxy(HttpProxy::new(proxy.to_string()).credentials("user", "pass"))
        .connect(("localhost", address.port()))
        .await
        .unwrap();
    assert_eq!(client.peer_addr(), Some(proxy));
    assert_greeting_and_echo(client).await;
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_proxy_rejection_is_an_error() {
    let (server, address) = start_greeting_echo();
    let proxy = spawn_proxy(|client| http_proxy(client, Some("dXNlcjpwYXNz"), "\r\n")).await;

    let error = EasyClient::builder(LineFilter::default)
        .proxy(HttpProxy::new(proxy.to_string()).credentials("user", "wrong"))
        .connect(address)
        .await
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::ConnectionRefused);
    assert!(error.to_string().contains("407"), "{error}");
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_proxy_replies_may_end_lines_with_a_bare_newline() {
    let (server, address) = start_greeting_echo();
    let proxy = spawn_proxy(|client| http_proxy(client, None, "\n")).await;

    let client = EasyClient::builder(LineFilter::default)
        .proxy(HttpProxy::new(proxy.to_string()))
        .connect(address)
        .await
        .unwrap();
    assert_greeting_and_echo(client).await;
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn socks5_proxy_resolves_host_names() {
    let (server, address) = start_greeting_echo();
    let proxy = spawn_proxy(|client| socks5_proxy(client, None)).await;

    let client = EasyClient::builder(LineFilter::default)
        .proxy(Socks5Proxy::new(proxy.to_string()))
        .connect(("localhost", address.port()))
        .await
        .unwrap();
    assert_greeting_and_echo(client).await;
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn socks5_proxy_authenticates() {
    let (server, address) = start_greeting_echo();
    let proxy = spawn_proxy(|client| socks5_proxy(client, Some(("user", "secret")))).await;

    let client = EasyClient::builder(LineFilter::default)
        .proxy(Socks5Proxy::new(proxy.to_string()).credentials("user", "secret"))
        .connect(address)
        .await
        .unwrap();
    assert_greeting_and_echo(client).await;

    let error = EasyClient::builder(LineFilter::default)
        .proxy(Socks5Proxy::new(proxy.to_string()).credentials("user", "wrong"))
        .connect(address)
        .await
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::ConnectionRefused);

    let error = EasyClient::builder(LineFilter::default)
        .proxy(Socks5Proxy::new(proxy.to_string()))
        .connect(address)
        .await
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::ConnectionRefused);
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn socks4_proxy_connects_by_address_and_by_host_name() {
    let (server, address) = start_greeting_echo();
    let proxy = spawn_proxy(socks4_proxy).await;

    for target in [address.to_string(), format!("localhost:{}", address.port())] {
        let client = EasyClient::builder(LineFilter::default)
            .proxy(Socks4Proxy::new(proxy.to_string()).user_id("tester"))
            .connect(target)
            .await
            .unwrap();
        assert_greeting_and_echo(client).await;
    }
    server.shutdown().await;
}
