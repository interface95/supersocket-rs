//! Benchmark of custom session state, middleware, command dispatch and command filters.
//!
//! Run with `cargo run --release --example bench_pipeline`. `BENCH_PORT` defaults to 4040,
//! `BENCH_THREADS` to 8, and `BENCH_MAX_SESSIONS` to 20000. The listener is loopback-only.
//! Commands use CRLF: NAME, WHOAMI, ECHO, STATS and QUIT. No per-request logging is enabled.

use std::{
    env,
    sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use supersocket::{BoxError, ListenOptions, prelude::*};

#[derive(Default)]
struct UserState {
    name: Mutex<Option<String>>,
    echo_count: AtomicUsize,
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

type UserSession = Session<UserState>;

impl SessionState for UserState {
    async fn on_connected(session: &Session<Self>) {
        if session.send("READY\r\n").await.is_err() {
            session.close(CloseReason::SocketError);
        }
    }
}

struct ConnectionLimit {
    limit: usize,
    active: AtomicUsize,
}

impl Middleware<UserState> for ConnectionLimit {
    async fn register_session(&self, session: &UserSession) -> bool {
        let admitted = self
            .active
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |active| {
                (active < self.limit).then(|| active + 1)
            })
            .is_ok();
        if !admitted && session.send("ERR server busy\r\n").await.is_err() {
            session.close(CloseReason::SocketError);
        }
        admitted
    }

    async fn unregister_session(&self, _session: &UserSession) {
        self.active.fetch_sub(1, Ordering::SeqCst);
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
            || name.len() > 32
            || !name
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
        {
            session.send("ERR invalid name\r\n").await?;
            return Ok(());
        }
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
        session
            .send_vectored(&[b"OK echo=", package.body.as_bytes(), b"\r\n"])
            .await?;
        Ok(())
    }
}

/// Rejected messages never execute the command or its after-filter, so only successful, valid
/// ECHO commands increment the count. The C# benchmark uses the same before/after filter logic.
struct RequireName;

impl CommandFilter<CommandPackage, UserState> for RequireName {
    async fn on_executing(&self, session: &UserSession, package: &CommandPackage) -> bool {
        let rejection = if !session.is_named() {
            "ERR set NAME first\r\n"
        } else if package.body.trim().is_empty() || package.body.chars().any(char::is_control) {
            "ERR invalid echo\r\n"
        } else {
            return true;
        };
        if session.send(rejection).await.is_err() {
            session.close(CloseReason::SocketError);
        }
        false
    }

    async fn on_executed(&self, session: &UserSession, _key: &String, result: &HandlerResult) {
        if result.is_ok() {
            session.echo_count.fetch_add(1, Ordering::SeqCst);
        }
    }
}

struct Stats;

impl AsyncCommand<CommandPackage, UserState> for Stats {
    fn key(&self) -> String {
        "STATS".into()
    }

    async fn execute(&self, session: &UserSession, package: CommandPackage) -> HandlerResult {
        if !package.body.trim().is_empty() {
            session.send("ERR usage: STATS\r\n").await?;
            return Ok(());
        }
        let count = session.echo_count.load(Ordering::SeqCst);
        session.send(format!("OK count={count}\r\n")).await?;
        Ok(())
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

fn env_number(name: &str, default: usize, maximum: usize) -> Result<usize, BoxError> {
    let value = match env::var(name) {
        Ok(value) => value,
        Err(env::VarError::NotPresent) => return Ok(default),
        Err(error) => return Err(format!("{name}: {error}").into()),
    };
    let number = value
        .parse::<usize>()
        .map_err(|_| format!("{name} must be an integer in 1..={maximum}"))?;
    if !(1..=maximum).contains(&number) {
        return Err(format!("{name} must be in 1..={maximum}").into());
    }
    Ok(number)
}

fn main() -> Result<(), BoxError> {
    let port = env_number("BENCH_PORT", 4040, 65535)?;
    let threads = env_number("BENCH_THREADS", 8, 1024)?;
    let max_sessions = env_number("BENCH_MAX_SESSIONS", 20000, 1_000_000)?;
    let server = Server::builder(CommandLineFilter::default)
        .session::<UserState>()
        .middleware(ConnectionLimit {
            limit: max_sessions,
            active: AtomicUsize::new(0),
        })
        .commands(|commands| {
            commands
                .add(Name)
                .add(WhoAmI)
                .add(Echo.with_filter(RequireName))
                .add(Stats)
                .add(Quit);
            commands.on_unknown(async |session, _package| {
                session.send("ERR unknown command\r\n").await?;
                Ok(())
            });
        })
        .on_error(async |_session, _error| ErrorAction::Close)
        .configure(|options| {
            options.name = "bench-pipeline".into();
            options.max_package_length = 4096;
            options.package_handling_timeout = Duration::from_secs(30);
            options.send_timeout = Some(Duration::from_secs(5));
            options.close_timeout = Duration::from_secs(2);
            options.listeners.push(ListenOptions {
                backlog: 1024,
                no_delay: true,
                socket_receive_buffer_size: Some(4096),
                socket_send_buffer_size: Some(4096),
                ..ListenOptions::new(format!("127.0.0.1:{port}"))
            });
        })
        .build()?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(threads)
        .enable_all()
        .build()?;
    runtime.block_on(server.run())?;
    Ok(())
}
