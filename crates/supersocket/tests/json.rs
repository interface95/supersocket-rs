//! Tests of commands whose package body is JSON.

mod common;

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use common::{Client, start};
use serde::Deserialize;
use supersocket::{Json, JsonCommand, prelude::*};

#[derive(Deserialize)]
struct Credentials {
    user: String,
    admin: Option<bool>,
}

struct Login;

impl JsonCommand for Login {
    type Value = Credentials;

    fn key(&self) -> String {
        "LOGIN".into()
    }

    async fn execute(&self, session: &Session, credentials: Credentials) -> HandlerResult {
        session
            .send(format!(
                "OK {} admin={}\r\n",
                credentials.user,
                credentials.admin.unwrap_or(false)
            ))
            .await?;
        Ok(())
    }
}

struct Ping;

impl JsonCommand for Ping {
    type Value = Option<u32>;

    fn key(&self) -> String {
        "PING".into()
    }

    async fn execute(&self, session: &Session, sequence: Option<u32>) -> HandlerResult {
        session
            .send(format!("PONG {}\r\n", sequence.unwrap_or(0)))
            .await?;
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn json_commands_decode_their_body() {
    let errors = Arc::new(AtomicUsize::new(0));
    let (server, address) = start(
        Server::builder(CommandLineFilter::default)
            .commands(|commands| {
                commands.add(Json(Login)).add(Json(Ping));
            })
            .on_error({
                let errors = errors.clone();
                move |_, error| {
                    let errors = errors.clone();
                    async move {
                        assert!(matches!(error, PackageError::Handler(_)), "{error}");
                        errors.fetch_add(1, Ordering::SeqCst);
                        ErrorAction::Continue
                    }
                }
            }),
    );
    let mut client = Client::connect(address).await;
    client
        .send(b"LOGIN {\"user\": \"alice\", \"admin\": true}\r\n")
        .await;
    assert_eq!(client.line().await, "OK alice admin=true");
    client.send(b"LOGIN {\"user\": \"bob\"}\r\n").await;
    assert_eq!(client.line().await, "OK bob admin=false");
    // An empty body is null, which an Option accepts; a body that is not JSON is a handler error.
    client
        .send(b"PING\r\nPING 7\r\nLOGIN not json\r\nPING 8\r\n")
        .await;
    assert_eq!(client.line().await, "PONG 0");
    assert_eq!(client.line().await, "PONG 7");
    assert_eq!(client.line().await, "PONG 8");
    assert_eq!(errors.load(Ordering::SeqCst), 1);
    server.shutdown().await;
}

#[cfg(feature = "websocket")]
mod websocket {
    use futures_util::{SinkExt, StreamExt};
    use serde::Deserialize;
    use supersocket::{
        HandlerResult,
        websocket::{Json, JsonWebSocketCommand, WebSocketServer, WebSocketSession},
    };
    use tokio_tungstenite::{connect_async, tungstenite};

    #[derive(Deserialize)]
    struct Join {
        room: String,
    }

    struct JoinRoom;

    impl JsonWebSocketCommand for JoinRoom {
        type Value = Join;

        fn key(&self) -> String {
            "JOIN".into()
        }

        async fn execute(&self, session: &WebSocketSession, join: Join) -> HandlerResult {
            session.send_text(&format!("joined {}", join.room)).await?;
            Ok(())
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn websocket_json_commands_decode_their_body() {
        let server = WebSocketServer::builder()
            .commands(|commands| {
                commands.add(Json(JoinRoom));
            })
            .listen("127.0.0.1:0")
            .build()
            .unwrap();
        let address = server.start().unwrap()[0].tcp().unwrap();
        let (mut ws, _) = connect_async(format!("ws://{address}/")).await.unwrap();
        ws.send(tungstenite::Message::text("JOIN {\"room\": \"lobby\"}"))
            .await
            .unwrap();
        let reply = ws.next().await.unwrap().unwrap();
        assert_eq!(reply.to_text().unwrap(), "joined lobby");
        server.shutdown().await;
    }
}
