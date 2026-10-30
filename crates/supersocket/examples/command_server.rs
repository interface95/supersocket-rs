//! Dispatches command lines to commands, with a filter that logs every command and one that
//! guards a single command.
//!
//! ```sh
//! cargo run --example command_server
//! printf 'ADD 1 2 3\r\nMULT 4 5\r\nSECRET\r\nLOGIN\r\nSECRET\r\nNOPE\r\n' | nc 127.0.0.1 4040
//! ```

use std::time::Instant;

use supersocket::prelude::*;
use tracing_subscriber::EnvFilter;

struct Add;

impl AsyncCommand<CommandPackage> for Add {
    fn key(&self) -> String {
        "ADD".into()
    }

    async fn execute(&self, session: &Session, package: CommandPackage) -> HandlerResult {
        let mut sum = 0i64;
        for arg in package.args() {
            sum += arg.parse::<i64>()?;
        }
        session.send(format!("{sum}\r\n")).await?;
        Ok(())
    }
}

struct Mult;

impl AsyncCommand<CommandPackage> for Mult {
    fn key(&self) -> String {
        "MULT".into()
    }

    async fn execute(&self, session: &Session, package: CommandPackage) -> HandlerResult {
        let mut product = 1i64;
        for arg in package.args() {
            product *= arg.parse::<i64>()?;
        }
        session.send(format!("{product}\r\n")).await?;
        Ok(())
    }
}

/// Marks a session as logged in.
#[derive(Clone)]
struct LoggedIn;

struct Login;

impl AsyncCommand<CommandPackage> for Login {
    fn key(&self) -> String {
        "LOGIN".into()
    }

    async fn execute(&self, session: &Session, _package: CommandPackage) -> HandlerResult {
        session.extensions().insert(LoggedIn);
        session.send("welcome\r\n").await?;
        Ok(())
    }
}

struct Secret;

impl AsyncCommand<CommandPackage> for Secret {
    fn key(&self) -> String {
        "SECRET".into()
    }

    async fn execute(&self, session: &Session, _package: CommandPackage) -> HandlerResult {
        session.send("42\r\n").await?;
        Ok(())
    }
}

/// Runs around every command and logs how long it took.
struct Timing;

impl CommandFilter<CommandPackage> for Timing {
    async fn on_executing(&self, session: &Session, _package: &CommandPackage) -> bool {
        session.extensions().insert(Instant::now());
        true
    }

    async fn on_executed(&self, session: &Session, key: &String, result: &HandlerResult) {
        let elapsed = session
            .extensions()
            .get::<Instant>()
            .map(|started| started.elapsed());
        tracing::info!(
            session = session.id(),
            key,
            ok = result.is_ok(),
            ?elapsed,
            "command executed"
        );
    }
}

/// Only lets logged-in sessions through.
struct RequireLogin;

impl CommandFilter<CommandPackage> for RequireLogin {
    async fn on_executing(&self, session: &Session, _package: &CommandPackage) -> bool {
        let logged_in = session.extensions().contains::<LoggedIn>();
        if !logged_in {
            let _ = session.send("login first\r\n").await;
        }
        logged_in
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
        .commands(|commands| {
            commands
                .add(Add)
                .add(Mult)
                .add(Login)
                .add(Secret.with_filter(RequireLogin))
                .filter(Timing);
            commands.on_unknown(async |session, package| {
                session
                    .send(format!("unknown command {}\r\n", package.key))
                    .await?;
                Ok(())
            });
        })
        .listen("0.0.0.0:4040")
        .build()?
        .run()
        .await
}
