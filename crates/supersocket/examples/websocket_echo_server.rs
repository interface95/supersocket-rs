//! A WebSocket echo server.
//!
//! ```sh
//! cargo run --example websocket_echo_server --features websocket
//! ```
//!
//! Then, in a browser console:
//! `const ws = new WebSocket("ws://127.0.0.1:4040"); ws.onmessage = (e) => console.log(e.data);`
//! and `ws.send("hello")`.

use supersocket::websocket::WebSocketServer;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<(), supersocket::Error> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    WebSocketServer::builder()
        .on_message(async |session, message| {
            session.send(&message).await?;
            Ok(())
        })
        .listen("0.0.0.0:4040")
        .build()?
        .run()
        .await
}
