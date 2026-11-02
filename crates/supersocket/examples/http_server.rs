//! A small HTTP server: a page, an echo endpoint and a Server-Sent Events stream.
//!
//! ```sh
//! cargo run --example http_server --features http
//! curl http://127.0.0.1:8080/
//! curl -d 'hello' http://127.0.0.1:8080/echo
//! curl -N http://127.0.0.1:8080/events
//! ```

use std::time::Duration;

use supersocket::{
    http::{HttpFilter, HttpResponse, HttpSessionExt, SseEvent, SseOptions},
    prelude::*,
};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<(), supersocket::Error> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    Server::builder(HttpFilter::default)
        .on_package(async |session, request| {
            match (request.method(), request.path()) {
                ("GET", "/") => {
                    let page = "<h1>supersocket-rs</h1><p>POST /echo, or GET /events.</p>";
                    session
                        .respond(&request, HttpResponse::ok().html(page))
                        .await?;
                }
                ("POST", "/echo") => {
                    let response = HttpResponse::ok()
                        .content_type(request.content_type().unwrap_or("application/octet-stream"))
                        .body(request.body().clone());
                    session.respond(&request, response).await?;
                }
                ("GET", "/events") => {
                    // The stream lives in a task of its own; the handler returns right away.
                    let events = session
                        .start_sse(SseOptions::default().keep_alive(Duration::from_secs(15)))
                        .await?;
                    tokio::spawn(async move {
                        for tick in 1.. {
                            let event = SseEvent::new(format!("tick {tick}")).id(tick.to_string());
                            if events.send(&event).await.is_err() {
                                break; // the client is gone
                            }
                            tokio::time::sleep(Duration::from_secs(1)).await;
                        }
                    });
                }
                _ => {
                    session
                        .respond(&request, HttpResponse::new(404).text("not found"))
                        .await?;
                }
            }
            Ok(())
        })
        // Keep-alive connections that stay silent are closed after a minute.
        .configure(|options| options.idle_session_timeout = Some(Duration::from_secs(60)))
        .listen("0.0.0.0:8080")
        .build()?
        .run()
        .await
}
