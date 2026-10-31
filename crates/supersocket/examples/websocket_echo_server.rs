//! A WebSocket echo server.
//!
//! ```sh
//! cargo run --example websocket_echo_server --features websocket
//! cargo run --example websocket_echo_server --features websocket,deflate # compressed
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

    let builder = WebSocketServer::builder().on_message(async |session, message| {
        session.send(&message).await?;
        Ok(())
    });
    // With the `deflate` feature, messages are compressed for clients that support it, as
    // browsers do.
    #[cfg(feature = "deflate")]
    let builder = builder.compression(supersocket::websocket::CompressionOptions::default());
    builder.listen("0.0.0.0:4040").build()?.run().await
}
