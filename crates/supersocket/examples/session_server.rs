//! Custom session state, commands and middleware working together through composition and traits.
//!
//! Run the server, then send commands from another terminal:
//!
//! ```sh
//! cargo run --example session_server
//! printf 'ECHO hello\r\nNAME alice\r\nWHOAMI\r\nECHO hello\r\nQUIT\r\n' | nc 127.0.0.1 4040
//! ```
//!
//! The replies are a READY greeting, `ERR set NAME first`, `OK name=alice`, `OK name=alice`,
//! `OK echo=hello`, and `BYE`. Commands end with CRLF; `printf` supplies those bytes. `NAME`
//! sets a display name for this connection only; it does not authenticate a user.
//!
//! Open `nc 127.0.0.1 4040` in three terminals to see the connection middleware reject the third
//! connection with `ERR server busy`. Closing either admitted connection frees its slot.
//! To use a different listening address:
//!
//! ```sh
//! cargo run --example session_server -- 127.0.0.1:4041
//! ```

use std::{
    sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use supersocket::{BoxError, ServerInfo, prelude::*};
use tracing_subscriber::EnvFilter;

const MAX_SESSIONS: usize = 2;
const MAX_NAME_LENGTH: usize = 32;

/// Each connection gets its own state. Only the short, synchronous state operations lock it;
/// no mutex guard is held across an await.
#[derive(Default)]
struct UserState {
    name: Mutex<Option<String>>,
}

impl UserState {
    fn name(&self) -> String {
        self.name
            .lock()
            .unwrap()
            .clone()
            .unwrap_or_else(|| "anonymous".into())
    }

    fn is_named(&self) -> bool {
        self.name.lock().unwrap().is_some()
    }

    fn set_name(&self, name: String) {
        *self.name.lock().unwrap() = Some(name);
    }
}

// This is an alias for the framework session with our state, not a subclass.
type UserSession = Session<UserState>;

impl SessionState for UserState {
    async fn on_connected(session: &Session<Self>) {
        tracing::info!(session = session.id(), "session connected");
        if let Err(error) = session
            .send("READY commands: NAME <name>, WHOAMI, ECHO <text>, QUIT\r\n")
            .await
        {
            tracing::warn!(session = session.id(), %error, "greeting failed");
            session.close(CloseReason::SocketError);
        }
    }

    async fn on_closed(session: &Session<Self>, reason: CloseReason) {
        tracing::info!(session = session.id(), name = session.name(), %reason, "session closed");
    }
}

/// Counts admitted connections itself: session_count() does not yet include sessions registering
/// concurrently. A rejected session never reaches UserState::on_connected.
struct ConnectionLimit {
    limit: usize,
    active: AtomicUsize,
}

impl Middleware<UserState> for ConnectionLimit {
    fn start(&self, server: &ServerInfo<UserState>) -> Result<(), BoxError> {
        tracing::info!(
            server = server.name(),
            limit = self.limit,
            "connection limit started"
        );
        Ok(())
    }

    async fn register_session(&self, session: &UserSession) -> bool {
        let admitted = self
            .active
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |active| {
                (active < self.limit).then(|| active + 1)
            })
            .is_ok();
        if !admitted {
            tracing::info!(session = session.id(), "connection rejected: limit reached");
            if let Err(error) = session.send("ERR server busy\r\n").await {
                tracing::warn!(session = session.id(), %error, "rejection response failed");
            }
            // No slot was acquired. The framework does not call unregister_session on the
            // middleware that rejected a session, so there is nothing to release here.
            return false;
        }
        tracing::info!(session = session.id(), "connection admitted");
        true
    }

    async fn unregister_session(&self, session: &UserSession) {
        let active = self.active.fetch_sub(1, Ordering::SeqCst) - 1;
        tracing::info!(session = session.id(), active, "connection slot released");
    }

    fn shutdown(&self, server: &ServerInfo<UserState>) {
        tracing::info!(
            server = server.name(),
            active = self.active.load(Ordering::SeqCst),
            "connection limit stopped"
        );
    }
}

struct Name;

impl AsyncCommand<CommandPackage, UserState> for Name {
    fn key(&self) -> String {
        "NAME".into()
    }

    async fn execute(&self, session: &UserSession, package: CommandPackage) -> HandlerResult {
        let name = package.body.trim();
        if name.is_empty()
            || name.len() > MAX_NAME_LENGTH
            || !name
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
        {
            session
                .send("ERR NAME requires 1-32 ASCII letters, digits or underscores\r\n")
                .await?;
            return Ok(());
        }
        // Session<UserState> dereferences to UserState, so its business methods are available.
        session.set_name(name.to_owned());
        session.send(format!("OK name={name}\r\n")).await?;
        Ok(())
    }
}

struct WhoAmI;

impl AsyncCommand<CommandPackage, UserState> for WhoAmI {
    fn key(&self) -> String {
        "WHOAMI".into()
    }

    async fn execute(&self, session: &UserSession, package: CommandPackage) -> HandlerResult {
        if !package.body.trim().is_empty() {
            session.send("ERR usage: WHOAMI\r\n").await?;
            return Ok(());
        }
        session
            .send(format!("OK name={}\r\n", session.name()))
            .await?;
        Ok(())
    }
}

struct Echo;

impl AsyncCommand<CommandPackage, UserState> for Echo {
    fn key(&self) -> String {
        "ECHO".into()
    }

    async fn execute(&self, session: &UserSession, package: CommandPackage) -> HandlerResult {
        if package.body.trim().is_empty() || package.body.chars().any(char::is_control) {
            session
                .send("ERR usage: ECHO <text without control characters>\r\n")
                .await?;
            return Ok(());
        }
        session
            .send_vectored(&[b"OK echo=", package.body.as_bytes(), b"\r\n"])
            .await?;
        Ok(())
    }
}

/// A command filter can stop one command while leaving the connection usable.
struct RequireName;

impl CommandFilter<CommandPackage, UserState> for RequireName {
    async fn on_executing(&self, session: &UserSession, _package: &CommandPackage) -> bool {
        if session.is_named() {
            return true;
        }
        if let Err(error) = session.send("ERR set NAME first\r\n").await {
            tracing::warn!(session = session.id(), %error, "command rejection response failed");
            session.close(CloseReason::SocketError);
        }
        false
    }
}

struct Quit;

impl AsyncCommand<CommandPackage, UserState> for Quit {
    fn key(&self) -> String {
        "QUIT".into()
    }

    async fn execute(&self, session: &UserSession, package: CommandPackage) -> HandlerResult {
        if !package.body.trim().is_empty() {
            session.send("ERR usage: QUIT\r\n").await?;
            return Ok(());
        }
        session.send("BYE\r\n").await?;
        session.close(CloseReason::LocalClosed);
        Ok(())
    }
}

/// Runs around recognized commands, including commands whose local filter refuses execution.
struct Audit;

impl CommandFilter<CommandPackage, UserState> for Audit {
    async fn on_executing(&self, session: &UserSession, package: &CommandPackage) -> bool {
        tracing::info!(session = session.id(), key = ?package.key, "command dispatching");
        true
    }

    async fn on_executed(&self, session: &UserSession, key: &String, result: &HandlerResult) {
        tracing::info!(
            session = session.id(),
            ?key,
            ok = result.is_ok(),
            "command dispatch completed"
        );
    }
}

#[tokio::main]
async fn main() -> Result<(), supersocket::Error> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();
    let address = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:4040".into());

    Server::builder(CommandLineFilter::default)
        .session::<UserState>() // Choose the state before registering typed handlers or middleware.
        .middleware(ConnectionLimit {
            limit: MAX_SESSIONS,
            active: AtomicUsize::new(0),
        })
        .commands(|commands| {
            commands
                .add(Name)
                .add(WhoAmI)
                .add(Echo.with_filter(RequireName))
                .add(Quit)
                .filter(Audit);
            commands.on_unknown(async |session, _package| {
                session.send("ERR unknown command\r\n").await?;
                Ok(())
            });
        })
        .on_error(async |session, error| {
            tracing::warn!(session = session.id(), %error, "command failed; closing session");
            ErrorAction::Close
        })
        .configure(|options| {
            options.name = "session-example".into();
            options.max_package_length = 4096;
            options.send_timeout = Some(Duration::from_secs(5));
            options.close_timeout = Duration::from_secs(2);
        })
        .listen(address)
        .build()?
        .run()
        .await
}
