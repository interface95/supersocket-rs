//! Talks to the WebSocket echo server, then closes with the closing handshake.
//!
//! ```sh
//! cargo run --example websocket_echo_server --features websocket
//! cargo run --example websocket_client --features websocket,client
//! ```

use supersocket::websocket::{CloseCode, WebSocketClient};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = WebSocketClient::connect("ws://127.0.0.1:4040/").await?;
    for text in ["hello", "supersocket"] {
        client.send_text(text).await?;
        if let Some(reply) = client.receive().await? {
            println!("received {:?}", reply.as_text().unwrap_or("<binary>"));
        }
    }
    client.close(CloseCode::NORMAL, "done").await;
    Ok(())
}
