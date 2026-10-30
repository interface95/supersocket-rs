//! Echo server for benchmarks: no logging, port from `BENCH_PORT` (default 4040).

use supersocket::prelude::*;

#[tokio::main]
async fn main() -> Result<(), supersocket::Error> {
    let port = std::env::var("BENCH_PORT").unwrap_or_else(|_| "4040".into());
    Server::builder(LineFilter::default)
        .on_package(async |session, package| {
            session.send(format!("{}\r\n", package.text)).await?;
            Ok(())
        })
        .listen(format!("0.0.0.0:{port}"))
        .build()?
        .run()
        .await
}
