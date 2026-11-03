//! Tests of the HTTP filter, responses, keep-alive and Server-Sent Events.

mod common;

use std::time::Duration;

use common::{Client, start};
use supersocket::{
    ServerBuilder,
    http::{HttpFilter, HttpRequest, HttpResponse, HttpSessionExt, SseEvent, SseOptions},
    prelude::*,
};
use tokio::io::AsyncReadExt;

fn service() -> ServerBuilder<HttpRequest> {
    Server::builder(HttpFilter::default).on_package(async |session, request| {
        match (request.method(), request.path()) {
            ("GET", "/") => {
                session
                    .respond(&request, HttpResponse::ok().text("home"))
                    .await?;
            }
            ("POST", "/echo") => {
                let response = HttpResponse::ok()
                    .content_type(request.content_type().unwrap_or("text/plain"))
                    .body(request.body().clone());
                session.respond(&request, response).await?;
            }
            ("GET", "/bye") => {
                session
                    .respond(&request, HttpResponse::ok().text("bye").keep_alive(false))
                    .await?;
            }
            ("GET", "/events") => {
                let since = request.last_event_id().map(str::to_owned);
                let events = session
                    .start_sse(
                        SseOptions::default()
                            .retry(Duration::from_secs(2))
                            .keep_alive(Duration::from_millis(50)),
                    )
                    .await?;
                tokio::spawn(async move {
                    let first = since.map_or(1, |id| id.parse::<u32>().unwrap() + 1);
                    for tick in first..first + 2 {
                        let event = SseEvent::new(format!("tick {tick}"))
                            .event("tick")
                            .id(tick.to_string());
                        events.send(&event).await.unwrap();
                    }
                    tokio::time::sleep(Duration::from_millis(120)).await;
                    events.data("done").await.unwrap();
                    events.close();
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
}

/// Reads a response: the head as lines and the body of the announced length.
async fn read_response(client: &mut Client) -> (String, Vec<String>, String) {
    let status = client.line().await;
    let mut headers = Vec::new();
    let mut length = 0;
    loop {
        let line = client.line().await;
        if line.is_empty() {
            break;
        }
        if let Some(value) = line.strip_prefix("Content-Length: ") {
            length = value.parse().unwrap();
        }
        headers.push(line);
    }
    let body = String::from_utf8(client.bytes(length).await).unwrap();
    (status, headers, body)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn requests_are_answered_on_a_kept_alive_connection() {
    let (server, address) = start(service());
    let mut client = Client::connect(address).await;

    client.send(b"GET / HTTP/1.1\r\nHost: test\r\n\r\n").await;
    let (status, headers, body) = read_response(&mut client).await;
    assert_eq!(status, "HTTP/1.1 200 OK");
    assert!(headers.contains(&"Content-Type: text/plain; charset=utf-8".to_owned()));
    assert_eq!(body, "home");

    // The connection is still open: a pipelined pair of requests on it, one chunked.
    client
        .send(
            b"POST /echo HTTP/1.1\r\nContent-Type: application/json\r\nContent-Length: 7\r\n\r\n{\"a\":1}\
              POST /echo HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n3\r\nabc\r\n0\r\n\r\n",
        )
        .await;
    let (status, headers, body) = read_response(&mut client).await;
    assert_eq!(status, "HTTP/1.1 200 OK");
    assert!(headers.contains(&"Content-Type: application/json".to_owned()));
    assert_eq!(body, "{\"a\":1}");
    let (_, _, body) = read_response(&mut client).await;
    assert_eq!(body, "abc");

    client.send(b"GET /missing HTTP/1.1\r\n\r\n").await;
    let (status, _, body) = read_response(&mut client).await;
    assert_eq!(status, "HTTP/1.1 404 Not Found");
    assert_eq!(body, "not found");
    assert_eq!(server.info().session_count(), 1);

    // A response that does not keep the connection alive ends it.
    client.send(b"GET /bye HTTP/1.1\r\n\r\n").await;
    let (_, headers, body) = read_response(&mut client).await;
    assert!(headers.contains(&"Connection: close".to_owned()));
    assert_eq!(body, "bye");
    client.expect_closed().await;
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_request_decides_about_keep_alive_too() {
    let (server, address) = start(service());

    let mut client = Client::connect(address).await;
    client
        .send(b"GET / HTTP/1.1\r\nConnection: close\r\n\r\n")
        .await;
    let (_, headers, body) = read_response(&mut client).await;
    assert_eq!(body, "home");
    assert!(headers.contains(&"Connection: close".to_owned()));
    client.expect_closed().await;

    // HTTP/1.0 closes unless the client asks to keep the connection.
    let mut client = Client::connect(address).await;
    client.send(b"GET / HTTP/1.0\r\n\r\n").await;
    let (_, _, body) = read_response(&mut client).await;
    assert_eq!(body, "home");
    client.expect_closed().await;

    let mut client = Client::connect(address).await;
    client
        .send(b"GET / HTTP/1.0\r\nConnection: keep-alive\r\n\r\nGET / HTTP/1.0\r\n\r\n")
        .await;
    // An HTTP/1.0 client is told that the connection stays open.
    let (_, headers, body) = read_response(&mut client).await;
    assert_eq!(body, "home");
    assert!(
        headers.contains(&"Connection: keep-alive".to_owned()),
        "{headers:?}"
    );
    let (_, headers, body) = read_response(&mut client).await;
    assert_eq!(body, "home");
    assert!(
        headers.contains(&"Connection: close".to_owned()),
        "{headers:?}"
    );
    client.expect_closed().await;
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn server_sent_events_stream_until_the_session_closes() {
    let (server, address) = start(service());
    let mut client = Client::connect(address).await;
    client
        .send(b"GET /events HTTP/1.1\r\nAccept: text/event-stream\r\nLast-Event-ID: 4\r\n\r\n")
        .await;
    assert_eq!(client.line().await, "HTTP/1.1 200 OK");
    let mut headers = Vec::new();
    loop {
        let line = client.line().await;
        if line.is_empty() {
            break;
        }
        headers.push(line);
    }
    assert!(headers.contains(&"Content-Type: text/event-stream".to_owned()));
    assert!(headers.contains(&"Connection: close".to_owned()));
    assert!(
        !headers
            .iter()
            .any(|header| header.starts_with("Content-Length"))
    );

    // The rest of the stream, until the server closes it.
    let mut rest = Vec::new();
    tokio::time::timeout(common::WAIT, client.reader().read_to_end(&mut rest))
        .await
        .expect("the stream did not end")
        .unwrap();
    let text = String::from_utf8(rest).unwrap();
    assert!(text.starts_with("retry: 2000\n"), "{text}");
    assert!(
        text.contains("event: tick\nid: 5\ndata: tick 5\n\n"),
        "{text}"
    );
    assert!(
        text.contains("event: tick\nid: 6\ndata: tick 6\n\n"),
        "{text}"
    );
    assert!(text.contains(": keep-alive\n\n"), "{text}");
    assert!(text.ends_with("data: done\n\n"), "{text}");
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn malformed_requests_close_the_connection() {
    let (builder, mut reasons) = common::track_close_reasons(service());
    let (server, address) = start(builder);
    let mut client = Client::connect(address).await;
    client.send(b"NOT HTTP\r\n\r\n").await;
    assert_eq!(
        common::next_reason(&mut reasons).await,
        CloseReason::ProtocolError
    );
    client.expect_closed().await;
    server.shutdown().await;
}
