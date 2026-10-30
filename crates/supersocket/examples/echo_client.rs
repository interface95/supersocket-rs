//! Talks to the echo server: first pulling replies with `receive`, then handing them to a
//! handler with `start_receive`.
//!
//! ```sh
//! cargo run --example echo_server
//! cargo run --example echo_client --features client
//! ```

use std::time::Duration;

use supersocket::prelude::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Pull: send a line, wait for its echo.
    let mut client = EasyClient::builder(LineFilter::default)
        .connect("127.0.0.1:4040")
        .await?;
    for text in ["hello", "supersocket"] {
        client.send(format!("{text}\r\n")).await?;
        if let Some(reply) = client.receive().await? {
            println!("received {}", reply.text);
        }
    }

    // Push: every reply goes to a handler running in the background, and the returned sender
    // keeps sending from here.
    let sender = client.start_receive(async |_sender, reply: TextPackage| {
        println!("handler got {}", reply.text);
    });
    for number in 1..=3 {
        sender.send(format!("message {number}\r\n")).await?;
    }
    tokio::time::sleep(Duration::from_millis(200)).await;
    sender.close().await;
    Ok(())
}
