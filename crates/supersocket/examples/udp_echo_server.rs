//! An echo server over UDP: every peer gets a session of its own, closed after a minute of
//! silence.
//!
//! ```sh
//! cargo run --example udp_echo_server --features udp
//! nc -u 127.0.0.1 4040
//! ```

use std::time::Duration;

use supersocket::{prelude::*, udp::UdpListener};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    Server::builder(LineFilter::default)
        .on_connected(async |session| {
            tracing::info!(session = session.id(), peer = %session.remote_addr(), "new peer");
        })
        .on_package(async |session, package| {
            session.send(format!("{}\r\n", package.text)).await?;
            Ok(())
        })
        .configure(|options| options.idle_session_timeout = Some(Duration::from_secs(60)))
        .listener(UdpListener::bind("0.0.0.0:4040".parse()?)?)
        .build()?
        .run()
        .await?;
    Ok(())
}
