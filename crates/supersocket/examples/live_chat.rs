//! A chat room: per-session state, broadcasting through the session container, and closing idle
//! sessions.
//!
//! ```sh
//! cargo run --example live_chat
//! nc 127.0.0.1 4040    # in two terminals, then: NAME alice / SAY hello
//! ```

use std::{sync::Mutex, time::Duration};

use supersocket::prelude::*;
use tracing_subscriber::EnvFilter;

#[derive(Default)]
struct Chatter {
    name: Mutex<String>,
}

impl Chatter {
    fn name(&self) -> String {
        self.name.lock().unwrap().clone()
    }
}

impl SessionState for Chatter {
    async fn on_connected(session: &Session<Self>) {
        *session.name.lock().unwrap() = format!("guest{}", session.id());
        let _ = session
            .send("welcome, use NAME <name> and SAY <text>\r\n")
            .await;
    }

    async fn on_closed(session: &Session<Self>, reason: CloseReason) {
        broadcast(session, format!("{} left ({reason})\r\n", session.name())).await;
    }
}

async fn broadcast(from: &Session<Chatter>, message: String) {
    for session in from.server().sessions().all() {
        let _ = session.send(&message).await;
    }
}

struct Name;

impl AsyncCommand<CommandPackage, Chatter> for Name {
    fn key(&self) -> String {
        "NAME".into()
    }

    async fn execute(&self, session: &Session<Chatter>, package: CommandPackage) -> HandlerResult {
        *session.name.lock().unwrap() = package.body;
        Ok(())
    }
}

struct Say;

impl AsyncCommand<CommandPackage, Chatter> for Say {
    fn key(&self) -> String {
        "SAY".into()
    }

    async fn execute(&self, session: &Session<Chatter>, package: CommandPackage) -> HandlerResult {
        broadcast(session, format!("{}: {}\r\n", session.name(), package.body)).await;
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), supersocket::Error> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    Server::builder(CommandLineFilter::default)
        .session::<Chatter>()
        .commands(|commands| {
            commands.add(Name).add(Say);
        })
        .configure(|options| {
            options.name = "live-chat".into();
            options.idle_session_timeout = Some(Duration::from_secs(600));
        })
        .listen("0.0.0.0:4040")
        .build()?
        .run()
        .await
}
