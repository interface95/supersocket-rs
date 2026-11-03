//! A server on standard input and output instead of a socket, like SuperSocket's console
//! connection: for tools that speak a protocol over stdio, such as a language server or an MCP
//! server, and for trying a protocol by typing. A terminal ends lines with `\n` alone, so lines
//! are cut there and mapped to command packages with `commands_with`.
//!
//! ```sh
//! cargo run --example stdio_server        # then type ADD 1 2, MUL 3 4, QUIT
//! printf 'ADD 1 2\nMUL 3 4\n' | cargo run --example stdio_server
//! ```
//!
//! Logs go to standard error, so that they do not mix with the protocol on standard output.

use std::sync::Arc;

use supersocket::{ConnectionInfo, TerminatorFilter, bytes::Bytes, prelude::*};
use tokio::sync::Notify;
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
        session.send(format!("{sum}\n")).await?;
        Ok(())
    }
}

struct Mul;

impl AsyncCommand<CommandPackage> for Mul {
    fn key(&self) -> String {
        "MUL".into()
    }

    async fn execute(&self, session: &Session, package: CommandPackage) -> HandlerResult {
        let mut product = 1i64;
        for arg in package.args() {
            product *= arg.parse::<i64>()?;
        }
        session.send(format!("{product}\n")).await?;
        Ok(())
    }
}

struct Quit;

impl AsyncCommand<CommandPackage> for Quit {
    fn key(&self) -> String {
        "QUIT".into()
    }

    async fn execute(&self, session: &Session, _package: CommandPackage) -> HandlerResult {
        session.send("BYE\n").await?;
        session.close(CloseReason::LocalClosed);
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), supersocket::Error> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn")),
        )
        .with_writer(std::io::stderr)
        .init();

    let done = Arc::new(Notify::new());
    let server = Server::builder(|| TerminatorFilter::new(&b"\n"[..], TextPackage::decode))
        .commands_with(
            |line: TextPackage| {
                // A line typed on Windows ends with "\r\n": the "\r" is not part of the command.
                let line = line.text.trim_end_matches('\r');
                if line.trim().is_empty() {
                    return Ok(None);
                }
                CommandPackage::decode(Bytes::copy_from_slice(line.as_bytes())).map(Some)
            },
            |commands| {
                commands
                    .add(Add)
                    .add(Mul)
                    .add(Quit)
                    .on_unknown(async |session, package| {
                        session
                            .send(format!("unknown command {}\n", package.key))
                            .await?;
                        Ok(())
                    });
            },
        )
        .on_error(async |session, error| {
            let _ = session.send(format!("error: {error}\n")).await;
            ErrorAction::Continue
        })
        .on_closed({
            let done = done.clone();
            move |_session, _reason| {
                let done = done.clone();
                async move { done.notify_one() }
            }
        })
        .build()?;

    // No listeners: the only connection is standard input and output.
    server.start()?;
    server.serve_connection(
        tokio::io::join(tokio::io::stdin(), tokio::io::stdout()),
        ConnectionInfo::default(),
    )?;
    tokio::select! {
        () = done.notified() => {}
        _ = supersocket::shutdown_signal() => {}
    }
    server.shutdown().await;
    // Standard input is read on a blocking thread, which a read that never returns would keep
    // the runtime waiting for after Ctrl+C; the session is over, so exit right away.
    std::process::exit(0)
}
