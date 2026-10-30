//! A chat room over WebSocket: per-session names, broadcasting to the connected sessions, and a
//! handshake that only accepts the `/chat` path.
//!
//! ```sh
//! cargo run --example websocket_chat --features websocket
//! ```
//!
//! Open `ws://127.0.0.1:4040/chat?name=alice` in two WebSocket clients and send text.

use std::sync::Mutex;

use supersocket::websocket::{Accept, Message, Rejection, WebSocketServer, WebSocketSession};
use tracing_subscriber::EnvFilter;

#[derive(Default)]
struct Chatter {
    name: Mutex<String>,
}

async fn broadcast(from: &WebSocketSession<Chatter>, text: &str) {
    for session in from.sessions() {
        let _ = session.send_text(text).await;
    }
}

#[tokio::main]
async fn main() -> Result<(), supersocket::Error> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    WebSocketServer::builder()
        .session::<Chatter>()
        .on_handshake(async |session: WebSocketSession<Chatter>| {
            let request = session.request();
            if request.path() != "/chat" {
                return Err(Rejection::new(404, "try /chat"));
            }
            let name = request
                .query()
                .and_then(|query| query.strip_prefix("name="))
                .map_or_else(|| format!("guest{}", session.id()), str::to_owned);
            *session.name.lock().unwrap() = name;
            Ok(Accept::new())
        })
        .on_connected(async |session: WebSocketSession<Chatter>| {
            let name = session.name.lock().unwrap().clone();
            broadcast(&session, &format!("{name} joined")).await;
        })
        .on_message(async |session: WebSocketSession<Chatter>, message| {
            if let Message::Text(text) = message {
                let name = session.name.lock().unwrap().clone();
                broadcast(&session, &format!("{name}: {text}")).await;
            }
            Ok(())
        })
        .on_closed(async |session: WebSocketSession<Chatter>, _reason| {
            let name = session.name.lock().unwrap().clone();
            broadcast(&session, &format!("{name} left")).await;
        })
        .listen("0.0.0.0:4040")
        .build()?
        .run()
        .await
}
