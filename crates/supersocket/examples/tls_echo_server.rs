//! Echoes lines over TLS. Pass a PEM certificate and key, or run without arguments to use a
//! freshly generated self-signed certificate for `localhost`.
//!
//! ```sh
//! cargo run --example tls_echo_server --features tls [-- cert.pem key.pem]
//! openssl s_client -connect 127.0.0.1:4443 -quiet
//! ```

use std::path::PathBuf;

use supersocket::{ListenOptions, TlsOptions, prelude::*};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let args: Vec<String> = std::env::args().skip(1).collect();
    let (cert, key) = match args.as_slice() {
        [cert, key] => (PathBuf::from(cert), PathBuf::from(key)),
        _ => self_signed()?,
    };

    Server::builder(LineFilter::default)
        .on_package(async |session, package| {
            session.send(format!("{}\r\n", package.text)).await?;
            Ok(())
        })
        .configure(|options| {
            options.listeners.push(ListenOptions {
                tls: Some(TlsOptions::new(cert, key)),
                ..ListenOptions::new("0.0.0.0:4443")
            });
        })
        .build()?
        .run()
        .await?;
    Ok(())
}

fn self_signed() -> Result<(PathBuf, PathBuf), Box<dyn std::error::Error>> {
    let certified = rcgen::generate_simple_self_signed(vec!["localhost".into()])?;
    let directory = std::env::temp_dir().join("supersocket-tls-example");
    std::fs::create_dir_all(&directory)?;
    let (cert, key) = (directory.join("cert.pem"), directory.join("key.pem"));
    std::fs::write(&cert, certified.cert.pem())?;
    std::fs::write(&key, certified.signing_key.serialize_pem())?;
    tracing::info!(cert = %cert.display(), "using a self-signed certificate");
    Ok((cert, key))
}
