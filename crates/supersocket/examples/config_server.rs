//! Echo server configured by `examples/supersocket.toml`. Environment variables override it:
//!
//! ```sh
//! cargo run --example config_server --features config
//! SUPERSOCKET__LISTENERS__0__ADDRESS=127.0.0.1:4041 cargo run --example config_server --features config
//! ```

use supersocket::prelude::*;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let options = ServerOptions::load(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/examples/supersocket.toml"
    ))?;
    Server::builder(LineFilter::default)
        .on_package(async |session, package| {
            session.send(format!("{}\r\n", package.text)).await?;
            Ok(())
        })
        .options(options)
        .build()?
        .run()
        .await?;
    Ok(())
}
