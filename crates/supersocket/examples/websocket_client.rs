//! Talks to the WebSocket echo server, then closes with the closing handshake.
//!
//! ```sh
//! cargo run --example websocket_echo_server --features websocket
//! cargo run --example websocket_client --features websocket,client [ws://127.0.0.1:4040/]
//! ```
//!
//! With the `deflate` feature as well, per-message compression is offered to the server.

use supersocket::websocket::{CloseCode, WebSocketClient};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "ws://127.0.0.1:4040/".into());
    let builder = WebSocketClient::builder(url);
    #[cfg(feature = "deflate")]
    let builder = builder.compression(supersocket::websocket::CompressionOptions::default());
    let mut client = builder.connect().await?;
    let long = "compressible ".repeat(1000);
    for text in ["hello", "supersocket", long.as_str()] {
        client.send_text(text).await?;
        if let Some(reply) = client.receive().await? {
            match reply.as_text() {
                Some(text) if text.len() <= 32 => println!("received {text:?}"),
                _ => println!(
                    "received {} bytes, {}",
                    reply.len(),
                    if reply.as_bytes() == text.as_bytes() {
                        "as sent"
                    } else {
                        "DIFFERENT"
                    }
                ),
            }
        }
    }
    client.close(CloseCode::NORMAL, "done").await?;
    Ok(())
}
