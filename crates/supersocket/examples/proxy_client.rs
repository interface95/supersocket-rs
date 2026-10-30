//! Fetches the status line of an HTTP page through a SOCKS5 proxy.
//!
//! ```sh
//! cargo run --example proxy_client --features proxy -- 127.0.0.1:1080 example.com:80
//! ```

use supersocket::{client::Socks5Proxy, prelude::*};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let [proxy, target] = args.as_slice() else {
        eprintln!("usage: proxy_client <proxy host:port> <target host:port>");
        std::process::exit(2);
    };
    let mut client = EasyClient::builder(LineFilter::default())
        .proxy(Socks5Proxy::new(proxy.as_str()))
        .connect(target.as_str())
        .await?;
    let host = target
        .rsplit_once(':')
        .map_or(target.as_str(), |(host, _)| host);
    client
        .send(format!(
            "GET / HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n\r\n"
        ))
        .await?;
    if let Some(status) = client.receive().await? {
        println!("{}", status.text);
    }
    client.close().await;
    Ok(())
}
