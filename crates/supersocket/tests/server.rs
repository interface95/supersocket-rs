//! Behavior tests of the server.

mod common;

use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use common::{Client, next_reason, start, track_close_reasons};
use supersocket::{
    BeginEndMarkFilter, BoxError, BoxFilter, FixedHeaderFilter, FixedHeaderProtocol,
    PackageHandler, PackageHandling, PipelineFilterExt, ServerBuilder, ServerInfo,
    bytes::{Buf, BufMut, Bytes, BytesMut},
    prelude::*,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

fn echo_lines() -> ServerBuilder<TextPackage> {
    Server::builder(LineFilter::default).on_package(async |session, package| {
        session.send(format!("{}\r\n", package.text)).await?;
        Ok(())
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn echo_handles_merged_and_split_lines() {
    let (server, address) = start(echo_lines());
    let mut client = Client::connect(address).await;

    client.send(b"hello\r\nworld\r\n").await;
    assert_eq!(client.line().await, "hello");
    assert_eq!(client.line().await, "world");

    // The terminator is split across two writes.
    for part in [&b"split\r"[..], b"\nnext", b" line\r\n"] {
        client.send(part).await;
        tokio::time::sleep(Duration::from_millis(30)).await;
    }
    assert_eq!(client.line().await, "split");
    assert_eq!(client.line().await, "next line");

    server.shutdown().await;
}

struct Add;

impl AsyncCommand<CommandPackage> for Add {
    fn key(&self) -> String {
        "ADD".into()
    }

    async fn execute(&self, session: &Session, package: CommandPackage) -> HandlerResult {
        let sum = package
            .args()
            .map(|arg| arg.parse::<i64>())
            .sum::<Result<i64, _>>()?;
        session.send(format!("{sum}\r\n")).await?;
        Ok(())
    }
}

#[derive(Default)]
struct HitCounter(AtomicUsize);

struct Hit;

impl AsyncCommand<CommandPackage> for Hit {
    fn key(&self) -> String {
        "HIT".into()
    }

    async fn execute(&self, session: &Session, _package: CommandPackage) -> HandlerResult {
        let counter = session
            .server()
            .service::<HitCounter>()
            .ok_or("HitCounter is not registered")?;
        let hits = counter.0.fetch_add(1, Ordering::SeqCst) + 1;
        session.send(format!("{hits}\r\n")).await?;
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn commands_dispatch_by_key_and_survive_errors() {
    let (server, address) = start(
        Server::builder(CommandLineFilter::default)
            .service(HitCounter::default())
            .commands(|commands| {
                commands.add(Add).add(Hit);
                commands.on_unknown(async |session, package| {
                    session
                        .send(format!("unknown: {}\r\n", package.key))
                        .await?;
                    Ok(())
                });
            }),
    );
    let mut client = Client::connect(address).await;

    client.send(b"ADD 1 2 3\r\n").await;
    assert_eq!(client.line().await, "6");
    client.send(b"HIT\r\nHIT\r\n").await;
    assert_eq!(client.line().await, "1");
    assert_eq!(client.line().await, "2");
    client.send(b"FOO bar\r\n").await;
    assert_eq!(client.line().await, "unknown: FOO");

    // A failing command is logged and the session goes on.
    client.send(b"ADD 1 x\r\nADD 40 2\r\n").await;
    assert_eq!(client.line().await, "42");

    server.shutdown().await;
}

#[derive(Clone)]
struct Authorized;

struct Login;

impl AsyncCommand<CommandPackage> for Login {
    fn key(&self) -> String {
        "LOGIN".into()
    }

    async fn execute(&self, session: &Session, _package: CommandPackage) -> HandlerResult {
        session.extensions().insert(Authorized);
        session.send("ok\r\n").await?;
        Ok(())
    }
}

struct Secret;

impl AsyncCommand<CommandPackage> for Secret {
    fn key(&self) -> String {
        "SECRET".into()
    }

    async fn execute(&self, session: &Session, _package: CommandPackage) -> HandlerResult {
        session.send("secret\r\n").await?;
        Ok(())
    }
}

struct RequireAuthorization;

impl CommandFilter<CommandPackage> for RequireAuthorization {
    async fn on_executing(&self, session: &Session, _package: &CommandPackage) -> bool {
        let authorized = session.extensions().contains::<Authorized>();
        if !authorized {
            let _ = session.send("denied\r\n").await;
        }
        authorized
    }
}

struct Audit(Arc<Mutex<Vec<String>>>);

impl CommandFilter<CommandPackage> for Audit {
    async fn on_executing(&self, _session: &Session, package: &CommandPackage) -> bool {
        self.0
            .lock()
            .unwrap()
            .push(format!("before {}", package.key));
        true
    }

    async fn on_executed(&self, _session: &Session, key: &String, result: &HandlerResult) {
        self.0
            .lock()
            .unwrap()
            .push(format!("after {key} ok={}", result.is_ok()));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn command_filters_run_around_commands() {
    let log = Arc::new(Mutex::new(Vec::new()));
    let audit = Audit(log.clone());
    let (server, address) = start(Server::builder(CommandLineFilter::default).commands(
        move |commands| {
            commands
                .add(Login)
                .add(Secret.with_filter(RequireAuthorization))
                .filter(audit);
        },
    ));
    let mut client = Client::connect(address).await;

    client.send(b"SECRET\r\n").await;
    assert_eq!(client.line().await, "denied");
    client.send(b"LOGIN\r\nSECRET\r\n").await;
    assert_eq!(client.line().await, "ok");
    assert_eq!(client.line().await, "secret");

    assert_eq!(
        *log.lock().unwrap(),
        [
            "before SECRET",
            "after SECRET ok=true",
            "before LOGIN",
            "after LOGIN ok=true",
            "before SECRET",
            "after SECRET ok=true"
        ]
    );
    server.shutdown().await;
}

#[derive(Debug, PartialEq)]
struct Message {
    opcode: u8,
    sequence: u16,
    body: String,
}

#[derive(Default)]
struct MessageProtocol;

impl FixedHeaderProtocol for MessageProtocol {
    type Package = Message;
    const HEADER_SIZE: usize = 5;

    fn body_length(&self, header: &[u8]) -> Result<usize, ProtocolError> {
        Ok(u16::from_be_bytes([header[1], header[2]]) as usize)
    }

    fn decode(&mut self, mut package: Bytes) -> Result<Message, ProtocolError> {
        let opcode = package.get_u8();
        package.advance(2);
        let sequence = package.get_u16();
        let body = String::from_utf8(package.to_vec())
            .map_err(|_| ProtocolError::new("body is not UTF-8"))?;
        Ok(Message {
            opcode,
            sequence,
            body,
        })
    }
}

struct MessageEncoder;

impl PackageEncoder<Message> for MessageEncoder {
    fn encode(&self, message: &Message, buf: &mut BytesMut) -> Result<(), ProtocolError> {
        buf.put_u8(message.opcode);
        buf.put_u16(message.body.len() as u16);
        buf.put_u16(message.sequence);
        buf.put_slice(message.body.as_bytes());
        Ok(())
    }
}

fn encode(message: &Message) -> Vec<u8> {
    let mut buf = BytesMut::new();
    MessageEncoder.encode(message, &mut buf).unwrap();
    buf.to_vec()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fixed_header_protocol_handles_fragmented_input() {
    let (server, address) = start(
        Server::builder(FixedHeaderFilter::<MessageProtocol>::default).on_package(
            async |session, message| {
                let reply = Message {
                    body: message.body.to_uppercase(),
                    ..message
                };
                session.send_package(&MessageEncoder, &reply).await
            },
        ),
    );
    let mut client = Client::connect(address).await;

    let first = Message {
        opcode: 7,
        sequence: 1,
        body: "hello".into(),
    };
    let second = Message {
        opcode: 8,
        sequence: 2,
        body: String::new(),
    };
    let mut wire = encode(&first);
    wire.extend(encode(&second));
    // Byte by byte, so both the header and the body arrive in pieces.
    for byte in wire {
        client.send([byte]).await;
        tokio::time::sleep(Duration::from_millis(2)).await;
    }

    let mut expected = encode(&Message {
        body: "HELLO".into(),
        ..first
    });
    expected.extend(encode(&second));
    assert_eq!(client.bytes(expected.len()).await, expected);

    server.shutdown().await;
}

// The first byte selects the protocol of the next package: 'Y' for packages between "Y" and
// "\x00\xff", '*' for packages between "*" and "#". After each package it switches back.
#[derive(Default)]
struct SwitchFilter {
    next: Option<BoxFilter<TextPackage>>,
}

fn switch_filter() -> BoxFilter<TextPackage> {
    Box::new(SwitchFilter::default())
}

impl PipelineFilter for SwitchFilter {
    type Package = TextPackage;

    fn filter(&mut self, buf: &mut BytesMut) -> Result<Option<TextPackage>, ProtocolError> {
        let Some(&flag) = buf.first() else {
            return Ok(None);
        };
        self.next = Some(match flag {
            b'Y' => Box::new(
                BeginEndMarkFilter::new(&b"Y"[..], &b"\x00\xff"[..], TextPackage::decode)
                    .then_switch_to(switch_filter),
            ),
            b'*' => Box::new(
                BeginEndMarkFilter::new(&b"*"[..], &b"#"[..], TextPackage::decode)
                    .then_switch_to(switch_filter),
            ),
            flag => return Err(ProtocolError::new(format!("unknown flag {flag}"))),
        });
        Ok(None)
    }

    fn next_filter(&mut self) -> Option<BoxFilter<TextPackage>> {
        self.next.take()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn next_filter_switches_protocol_mid_stream() {
    let (builder, mut reasons) = track_close_reasons(
        Server::builder(SwitchFilter::default).on_package(async |session, package| {
            session.send(format!("{}\r\n", package.text)).await?;
            Ok(())
        }),
    );
    let (server, address) = start(builder);
    let mut client = Client::connect(address).await;

    client.send(b"Yhello\x00\xff*world#Yagain\x00\xff").await;
    assert_eq!(client.line().await, "hello");
    assert_eq!(client.line().await, "world");
    assert_eq!(client.line().await, "again");

    client.send(b"?bad").await;
    client.expect_closed().await;
    assert_eq!(next_reason(&mut reasons).await, CloseReason::ProtocolError);

    server.shutdown().await;
}

#[derive(Default)]
struct Chatter {
    name: Mutex<String>,
}

impl Chatter {
    fn name(&self) -> String {
        self.name.lock().unwrap().clone()
    }
}

impl SessionState for Chatter {
    async fn on_connected(session: &Session<Self>) {
        let _ = session.send("welcome\r\n").await;
    }

    async fn on_closed(session: &Session<Self>, _reason: CloseReason) {
        broadcast(session, format!("{} left\r\n", session.name())).await;
    }
}

async fn broadcast(from: &Session<Chatter>, message: String) {
    for session in from.server().sessions().all() {
        let _ = session.send(&message).await;
    }
}

struct Name;

impl AsyncCommand<CommandPackage, Chatter> for Name {
    fn key(&self) -> String {
        "NAME".into()
    }

    async fn execute(&self, session: &Session<Chatter>, package: CommandPackage) -> HandlerResult {
        *session.name.lock().unwrap() = package.body;
        Ok(())
    }
}

struct Say;

impl AsyncCommand<CommandPackage, Chatter> for Say {
    fn key(&self) -> String {
        "SAY".into()
    }

    async fn execute(&self, session: &Session<Chatter>, package: CommandPackage) -> HandlerResult {
        broadcast(session, format!("{}: {}\r\n", session.name(), package.body)).await;
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn session_state_and_broadcast_through_the_session_container() {
    let (server, address) = start(
        Server::builder(CommandLineFilter::default)
            .session::<Chatter>()
            .commands(|commands| {
                commands.add(Name).add(Say);
            }),
    );
    let mut alice = Client::connect(address).await;
    let mut bob = Client::connect(address).await;
    assert_eq!(alice.line().await, "welcome");
    assert_eq!(bob.line().await, "welcome");
    assert_eq!(server.info().session_count(), 2);

    alice.send(b"NAME alice\r\n").await;
    bob.send(b"NAME bob\r\n").await;
    alice.send(b"SAY hi all\r\n").await;
    assert_eq!(alice.line().await, "alice: hi all");
    assert_eq!(bob.line().await, "alice: hi all");

    drop(bob);
    assert_eq!(alice.line().await, "bob left");
    assert_eq!(server.info().session_count(), 1);

    server.shutdown().await;
}

struct ConnectionLimit {
    limit: usize,
    current: AtomicUsize,
}

impl ConnectionLimit {
    fn new(limit: usize) -> Self {
        Self {
            limit,
            current: AtomicUsize::new(0),
        }
    }
}

impl<S: SessionState> Middleware<S> for ConnectionLimit {
    async fn register_session(&self, _session: &Session<S>) -> bool {
        if self.current.fetch_add(1, Ordering::SeqCst) < self.limit {
            true
        } else {
            self.current.fetch_sub(1, Ordering::SeqCst);
            false
        }
    }

    async fn unregister_session(&self, _session: &Session<S>) {
        self.current.fetch_sub(1, Ordering::SeqCst);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn middleware_can_reject_sessions() {
    let (builder, mut reasons) =
        track_close_reasons(echo_lines().middleware(ConnectionLimit::new(1)));
    let (server, address) = start(builder);

    let mut first = Client::connect(address).await;
    first.send(b"ping\r\n").await;
    assert_eq!(first.line().await, "ping");

    let mut second = Client::connect(address).await;
    second.expect_closed().await;

    first.send(b"still here\r\n").await;
    assert_eq!(first.line().await, "still here");
    // A rejected session never connected, so it gets no closed event.
    assert!(reasons.try_recv().is_err());

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn connection_limit_holds_under_a_burst_of_connections() {
    let (server, address) = start(echo_lines().middleware(ConnectionLimit::new(5)));

    let attempts = (0..20).map(|_| {
        tokio::spawn(async move {
            let mut client = Client::connect(address).await;
            client.send(b"ping\r\n").await;
            let mut line = String::new();
            let accepted = tokio::time::timeout(
                common::WAIT,
                tokio::io::AsyncBufReadExt::read_line(client.reader(), &mut line),
            )
            .await
            .is_ok_and(|read| read.is_ok_and(|length| length > 0));
            (accepted, client)
        })
    });
    let mut accepted = 0;
    let mut clients = Vec::new();
    for attempt in attempts.collect::<Vec<_>>() {
        let (ok, client) = attempt.await.unwrap();
        accepted += usize::from(ok);
        clients.push(client);
    }
    assert_eq!(accepted, 5);
    assert_eq!(server.info().session_count(), 5);

    drop(clients);
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn idle_sessions_are_closed_with_timeout() {
    let (builder, mut reasons) = track_close_reasons(echo_lines().configure(|options| {
        options.idle_session_timeout = Some(Duration::from_millis(300));
        options.clear_idle_session_interval = Duration::from_millis(100);
    }));
    let (server, address) = start(builder);

    let mut client = Client::connect(address).await;
    client.expect_closed().await;
    assert_eq!(next_reason(&mut reasons).await, CloseReason::Timeout);

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn oversized_package_is_a_protocol_error() {
    let (builder, mut reasons) =
        track_close_reasons(echo_lines().configure(|options| options.max_package_length = 16));
    let (server, address) = start(builder);

    let mut client = Client::connect(address).await;
    client.send([b'x'; 64]).await;
    client.expect_closed().await;
    assert_eq!(next_reason(&mut reasons).await, CloseReason::ProtocolError);

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn handler_can_close_its_session_after_sending() {
    let (builder, mut reasons) = track_close_reasons(
        Server::builder(LineFilter::default).on_package(async |session, package| {
            if package.text == "quit" {
                session.send("bye\r\n").await?;
                session.close(CloseReason::LocalClosed);
            }
            Ok(())
        }),
    );
    let (server, address) = start(builder);

    let mut client = Client::connect(address).await;
    // Packages after "quit" are not handled, but "bye" is still delivered.
    client.send(b"quit\r\nignored\r\n").await;
    assert_eq!(client.line().await, "bye");
    client.expect_closed().await;
    assert_eq!(next_reason(&mut reasons).await, CloseReason::LocalClosed);

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn error_handler_decides_after_failures_and_timeouts() {
    let builder = Server::builder(LineFilter::default)
        .on_package(async |session, package| {
            match package.text.as_str() {
                "slow" => tokio::time::sleep(Duration::from_secs(60)).await,
                "fail" => return Err("fatal".into()),
                _ => {}
            }
            session.send(format!("{}\r\n", package.text)).await?;
            Ok(())
        })
        .on_error(async |session, error| match error {
            PackageError::Timeout(_) => {
                let _ = session.send("timed out\r\n").await;
                ErrorAction::Continue
            }
            _ => ErrorAction::Close,
        })
        .configure(|options| options.package_handling_timeout = Duration::from_millis(200));
    let (builder, mut reasons) = track_close_reasons(builder);
    let (server, address) = start(builder);

    let mut client = Client::connect(address).await;
    client.send(b"slow\r\nfast\r\nfail\r\nafter\r\n").await;
    assert_eq!(client.line().await, "timed out");
    assert_eq!(client.line().await, "fast");
    client.expect_closed().await;
    assert_eq!(
        next_reason(&mut reasons).await,
        CloseReason::ApplicationError
    );

    server.shutdown().await;
}

fn slow_echo(max_in_flight: usize) -> ServerBuilder<TextPackage> {
    Server::builder(LineFilter::default)
        .on_package(async |session, package| {
            tokio::time::sleep(Duration::from_millis(300)).await;
            session.send(format!("{}\r\n", package.text)).await?;
            Ok(())
        })
        .configure(move |options| {
            options.package_handling = PackageHandling::Concurrent { max_in_flight }
        })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_handling_overlaps_packages_up_to_the_limit() {
    for (max_in_flight, at_least, below) in [(4, 300, 590), (2, 590, 890)] {
        let (server, address) = start(slow_echo(max_in_flight));
        let mut client = Client::connect(address).await;
        let started = Instant::now();
        client.send(b"a\r\nb\r\nc\r\nd\r\n").await;
        let mut lines = Vec::new();
        for _ in 0..4 {
            lines.push(client.line().await);
        }
        let elapsed = started.elapsed().as_millis();
        lines.sort();
        assert_eq!(lines, ["a", "b", "c", "d"]);
        assert!(
            (at_least..below).contains(&elapsed),
            "max_in_flight {max_in_flight}: took {elapsed} ms"
        );
        server.shutdown().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sessions_can_be_used_from_other_tasks_without_interleaving() {
    let (server, address) = start(Server::builder(LineFilter::default).on_package(
        async |session, _package| {
            for task in 0..8 {
                let session = session.clone();
                tokio::spawn(async move {
                    for line in 0..200 {
                        let _ = session
                            .send(format!("task{task} line{line} {}\r\n", "x".repeat(200)))
                            .await;
                    }
                });
            }
            Ok(())
        },
    ));

    let mut client = Client::connect(address).await;
    client.send(b"go\r\n").await;
    let mut next_line = [0; 8];
    for _ in 0..8 * 200 {
        let line = client.line().await;
        let (task, rest) = line
            .strip_prefix("task")
            .unwrap()
            .split_once(" line")
            .unwrap();
        let (number, padding) = rest.split_once(' ').unwrap();
        let task: usize = task.parse().unwrap();
        assert_eq!(
            number.parse::<usize>().unwrap(),
            next_line[task],
            "lines of task {task} out of order"
        );
        assert_eq!(padding.len(), 200);
        next_line[task] += 1;
    }

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn send_timeout_closes_sessions_whose_peer_stops_reading() {
    let builder = Server::builder(LineFilter::default)
        .on_package(async |session, _package| {
            tokio::spawn(async move {
                let chunk = vec![b'x'; 64 * 1024];
                while session.send(&chunk).await.is_ok() {}
            });
            Ok(())
        })
        .configure(|options| options.send_timeout = Some(Duration::from_millis(300)));
    let (builder, mut reasons) = track_close_reasons(builder);
    let (server, address) = start(builder);

    // Ask for a flood and never read it.
    let mut client = Client::connect(address).await;
    client.send(b"flood\r\n").await;
    assert_eq!(next_reason(&mut reasons).await, CloseReason::Timeout);

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_closes_sessions_and_listeners() {
    let (builder, mut reasons) = track_close_reasons(echo_lines());
    let (server, address) = start(builder);

    let mut client = Client::connect(address).await;
    client.send(b"ping\r\n").await;
    assert_eq!(client.line().await, "ping");

    server.shutdown().await;
    client.expect_closed().await;
    assert_eq!(next_reason(&mut reasons).await, CloseReason::ServerShutdown);
    assert!(
        TcpStream::connect(address).await.is_err(),
        "the listener should be closed after shutdown"
    );
}

#[test]
fn building_with_bad_addresses_fails() {
    let bad_address = Server::builder(LineFilter::default)
        .listen("not an address")
        .build();
    assert!(matches!(
        bad_address,
        Err(supersocket::Error::InvalidAddress { .. })
    ));
}

#[test]
fn options_that_cannot_work_fail_the_build() {
    let invalid: [fn(&mut ServerOptions); 3] = [
        |options| options.package_handling = PackageHandling::Concurrent { max_in_flight: 0 },
        |options| {
            options.idle_session_timeout = Some(Duration::from_secs(60));
            options.clear_idle_session_interval = Duration::ZERO;
        },
        |options| options.max_package_length = 0,
    ];
    for configure in invalid {
        let result = Server::builder(LineFilter::default)
            .listen("127.0.0.1:0")
            .configure(configure)
            .build();
        assert!(matches!(result, Err(supersocket::Error::InvalidOptions(_))));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn zero_send_buffer_limit_still_sends() {
    let (server, address) = start(echo_lines().configure(|options| options.send_buffer_limit = 0));
    let mut client = Client::connect(address).await;
    client.send(b"one\r\ntwo\r\nthree\r\n").await;
    assert_eq!(client.line().await, "one");
    assert_eq!(client.line().await, "two");
    assert_eq!(client.line().await, "three");
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn handler_panics_reach_the_error_handler_and_shutdown_still_completes() {
    let (server, address) = start(
        Server::builder(LineFilter::default)
            .on_package(async |session, package| {
                if package.text == "boom" {
                    panic!("handler exploded");
                }
                session.send(format!("{}\r\n", package.text)).await?;
                Ok(())
            })
            .on_error(async |session, error| {
                if let PackageError::Panicked(message) = error {
                    let _ = session.send(format!("panicked: {message}\r\n")).await;
                }
                ErrorAction::Continue
            }),
    );
    let mut client = Client::connect(address).await;
    client.send(b"boom\r\nafter\r\n").await;
    assert_eq!(client.line().await, "panicked: handler exploded");
    assert_eq!(client.line().await, "after");

    tokio::time::timeout(common::WAIT, server.shutdown())
        .await
        .expect("shutdown hung after a handler panic");
    client.expect_closed().await;
}

/// Panics on `!`, otherwise cuts lines.
#[derive(Default)]
struct PanickyFilter(LineFilter);

impl PipelineFilter for PanickyFilter {
    type Package = TextPackage;

    fn filter(&mut self, buf: &mut BytesMut) -> Result<Option<TextPackage>, ProtocolError> {
        assert!(!buf.contains(&b'!'), "filter exploded");
        self.0.filter(buf)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_panicking_filter_closes_only_its_session() {
    let (builder, mut reasons) = track_close_reasons(
        Server::builder(PanickyFilter::default).on_package(async |session, package| {
            session.send(format!("{}\r\n", package.text)).await?;
            Ok(())
        }),
    );
    let (server, address) = start(builder);

    let mut healthy = Client::connect(address).await;
    let mut broken = Client::connect(address).await;
    broken.send(b"bad!\r\n").await;
    broken.expect_closed().await;
    assert_eq!(next_reason(&mut reasons).await, CloseReason::InternalError);

    healthy.send(b"fine\r\n").await;
    assert_eq!(healthy.line().await, "fine");
    assert_eq!(server.info().session_count(), 1);

    tokio::time::timeout(common::WAIT, server.shutdown())
        .await
        .expect("shutdown hung after a filter panic");
}

/// Counts the connected and closed events of a server.
#[derive(Default)]
struct Events {
    connected: AtomicUsize,
    closed: AtomicUsize,
}

/// A line protocol whose first line must be `hello`: the server answers `welcome`, then echoes.
fn greeting_server(events: Arc<Events>) -> ServerBuilder<TextPackage> {
    let closed = events.clone();
    Server::builder(LineFilter::default)
        .require_handshake()
        .on_connected(move |session| {
            let events = events.clone();
            async move {
                assert!(session.is_handshake_complete());
                events.connected.fetch_add(1, Ordering::SeqCst);
            }
        })
        .on_closed(move |_, _| {
            let events = closed.clone();
            async move {
                events.closed.fetch_add(1, Ordering::SeqCst);
            }
        })
        .on_package(async |session, package| {
            if session.is_handshake_complete() {
                session.send(format!("{}\r\n", package.text)).await?;
            } else if package.text == "hello" {
                // Until the handshake is complete, broadcasts cannot reach the session.
                assert!(session.server().sessions().get(session.id()).is_none());
                session.send("welcome\r\n").await?;
                session.complete_handshake();
            } else {
                session.close(CloseReason::Rejected);
            }
            Ok(())
        })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sessions_connect_once_their_handshake_is_complete() {
    let events = Arc::new(Events::default());
    let (server, address) = start(greeting_server(events.clone()));

    let mut client = Client::connect(address).await;
    client.send(b"hello\r\nfirst\r\n").await;
    assert_eq!(client.line().await, "welcome");
    assert_eq!(client.line().await, "first");
    assert_eq!(server.info().session_count(), 1);
    assert_eq!(events.connected.load(Ordering::SeqCst), 1);

    // A failed handshake never connects, so it has no closed event either.
    let mut rejected = Client::connect(address).await;
    rejected.send(b"wrong\r\n").await;
    rejected.expect_closed().await;

    server.shutdown().await;
    assert_eq!(events.connected.load(Ordering::SeqCst), 1);
    assert_eq!(events.closed.load(Ordering::SeqCst), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sessions_that_do_not_complete_the_handshake_in_time_are_closed() {
    let events = Arc::new(Events::default());
    let limit = Duration::from_millis(200);
    let (server, address) = start(
        greeting_server(events.clone()).configure(|options| options.handshake_timeout = limit),
    );

    // Data keeps arriving, but never a complete handshake: the deadline still holds.
    let (mut reader, mut writer) = TcpStream::connect(address).await.unwrap().into_split();
    let trickle = tokio::spawn(async move {
        while writer.write_all(b"x").await.is_ok() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    });
    let started = Instant::now();
    let mut rest = Vec::new();
    let _ = tokio::time::timeout(Duration::from_secs(5), reader.read_to_end(&mut rest))
        .await
        .expect("the server did not close the session");
    let elapsed = started.elapsed();
    assert!(
        elapsed >= limit && elapsed < Duration::from_secs(2),
        "{elapsed:?}"
    );
    trickle.abort();

    server.shutdown().await;
    assert_eq!(events.connected.load(Ordering::SeqCst), 0);
    assert_eq!(events.closed.load(Ordering::SeqCst), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn packages_before_the_handshake_are_handled_in_order() {
    let (server, address) = start(
        Server::builder(LineFilter::default)
            .require_handshake()
            .configure(|options| {
                options.package_handling = PackageHandling::Concurrent { max_in_flight: 8 };
            })
            .on_package(async |session, package| {
                if package.text == "hello" {
                    // A slow handshake: the packages behind it must wait for it.
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    session.complete_handshake();
                } else {
                    let when = if session.is_handshake_complete() {
                        "after"
                    } else {
                        "before"
                    };
                    session.send(format!("{} {when}\r\n", package.text)).await?;
                }
                Ok(())
            }),
    );
    let mut client = Client::connect(address).await;
    client.send(b"hello\r\none\r\ntwo\r\n").await;
    let mut lines = [client.line().await, client.line().await];
    lines.sort();
    assert_eq!(lines, ["one after", "two after"]);
    server.shutdown().await;
}

struct Farewell;

impl PackageHandler<TextPackage> for Farewell {
    async fn handle(&self, session: &Session, package: TextPackage) -> HandlerResult {
        session.send(format!("{}\r\n", package.text)).await?;
        Ok(())
    }

    async fn closing(&self, session: &Session, reason: CloseReason) {
        let _ = session.send(format!("bye: {reason}\r\n")).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn package_handler_types_can_say_goodbye_when_sessions_close() {
    let (server, address) = start(Server::builder(LineFilter::default).package_handler(Farewell));
    let mut client = Client::connect(address).await;
    client.send(b"hi\r\n").await;
    assert_eq!(client.line().await, "hi");

    server.shutdown().await;
    assert_eq!(client.line().await, "bye: server shutdown");
    client.expect_closed().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn handshakes_completed_by_another_task_do_not_time_out() {
    let events = Arc::new(Events::default());
    let connected = events.clone();
    let (server, address) = start(
        Server::builder(LineFilter::default)
            .require_handshake()
            .configure(|options| options.handshake_timeout = Duration::from_millis(300))
            .on_connected(move |_session| {
                let events = connected.clone();
                async move {
                    events.connected.fetch_add(1, Ordering::SeqCst);
                }
            })
            .on_package(async |session, package| {
                if session.is_handshake_complete() {
                    session.send(format!("{}\r\n", package.text)).await?;
                } else {
                    // An authorization that another task finishes.
                    let session = session.clone();
                    tokio::spawn(async move {
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        session.complete_handshake();
                    });
                }
                Ok(())
            }),
    );
    let mut client = Client::connect(address).await;
    client.send(b"hello\r\n").await;
    // Nothing more arrives before the handshake timeout passes.
    tokio::time::sleep(Duration::from_millis(500)).await;
    client.send(b"after\r\n").await;
    assert_eq!(client.line().await, "after");
    assert_eq!(events.connected.load(Ordering::SeqCst), 1);
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_handshake_timeout_also_limits_handling() {
    let (server, address) = start(
        Server::builder(LineFilter::default)
            .require_handshake()
            .configure(|options| options.handshake_timeout = Duration::from_millis(200))
            .on_package(async |_session, _package| {
                // A validation far slower than the handshake may take.
                tokio::time::sleep(Duration::from_secs(10)).await;
                Ok(())
            }),
    );
    let mut client = Client::connect(address).await;
    client.send(b"hello\r\n").await;
    client.expect_closed().await;
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_connection_limit_pauses_accepting() {
    let (server, address) =
        start(echo_lines().configure(|options| options.max_connections = Some(2)));
    let mut first = Client::connect(address).await;
    let second = Client::connect(address).await;
    first.send(b"one\r\n").await;
    assert_eq!(first.line().await, "one");

    // The third connection waits in the backlog until a session ends.
    let mut third = Client::connect(address).await;
    third.send(b"three\r\n").await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(server.info().session_count(), 2);
    drop(second);
    assert_eq!(third.line().await, "three");
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_accept_hook_refuses_connections_before_they_become_sessions() {
    let allow = Arc::new(AtomicBool::new(false));
    let gate = allow.clone();
    let (server, address) = start(echo_lines().on_accept(move |_| gate.load(Ordering::SeqCst)));

    let mut refused = Client::connect(address).await;
    refused.expect_closed().await;
    assert_eq!(server.info().session_count(), 0);

    allow.store(true, Ordering::SeqCst);
    let mut client = Client::connect(address).await;
    client.send(b"in\r\n").await;
    assert_eq!(client.line().await, "in");
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stop_accepting_closes_the_listeners_but_keeps_the_sessions() {
    let (server, address) = start(echo_lines());
    let mut client = Client::connect(address).await;
    // A round trip, so that the session exists before the listener closes; connections still in
    // the backlog at that moment are reset.
    client.send(b"hello\r\n").await;
    assert_eq!(client.line().await, "hello");
    server.stop_accepting();
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        TcpStream::connect(address).await.is_err(),
        "the listener should be closed"
    );
    client.send(b"still here\r\n").await;
    assert_eq!(client.line().await, "still here");
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn run_until_stops_on_the_given_signal() {
    let server = echo_lines().listen("127.0.0.1:0").build().unwrap();
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let running = tokio::spawn(server.run_until(async {
        let _ = stopped.await;
    }));
    tokio::time::sleep(Duration::from_millis(50)).await;
    stop.send(()).unwrap();
    tokio::time::timeout(common::WAIT, running)
        .await
        .expect("run_until did not stop")
        .unwrap()
        .unwrap();
}

struct FailsToStart;

impl Middleware for FailsToStart {
    fn start(&self, _server: &ServerInfo) -> Result<(), BoxError> {
        Err("no database".into())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_bind_can_be_retried() {
    // Hold a port, so binding it fails.
    let held = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let taken = held.local_addr().unwrap();
    let server = echo_lines().listen(taken).build().unwrap();

    let error = server.start().unwrap_err();
    assert!(matches!(error, supersocket::Error::Bind { .. }), "{error}");
    // The failed start must not leave the server marked as started: a retry binds again rather
    // than reporting AlreadyStarted.
    let error = server.start().unwrap_err();
    assert!(matches!(error, supersocket::Error::Bind { .. }), "{error}");

    // Once the port is free, the same server starts.
    drop(held);
    server.start().unwrap();
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_middleware_that_fails_to_start_stops_the_server_from_starting() {
    let server = echo_lines()
        .middleware(FailsToStart)
        .listen("127.0.0.1:0")
        .build()
        .unwrap();
    let error = server.start().unwrap_err();
    assert!(
        matches!(error, supersocket::Error::Middleware(_)),
        "{error}"
    );
    // Starting can be tried again; the failure did not leave the server half started.
    let error = server.start().unwrap_err();
    assert!(
        matches!(error, supersocket::Error::Middleware(_)),
        "{error}"
    );
}

struct Visitor {
    from: SocketAddr,
}

impl SessionState for Visitor {}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn session_state_can_be_built_from_the_connection() {
    let (server, address) = start(
        Server::builder(LineFilter::default)
            .session_with(|connection| Visitor {
                from: connection.remote_addr(),
            })
            .on_package(async |session, package| {
                session
                    .send(format!("{} from {}\r\n", package.text, session.from))
                    .await?;
                Ok(())
            }),
    );
    let mut client = Client::connect(address).await;
    client.send(b"hi\r\n").await;
    let line = client.line().await;
    assert!(line.starts_with("hi from 127.0.0.1:"), "{line}");
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn listeners_take_host_names_and_socket_addresses() {
    let server = echo_lines().listen("localhost:0").build().unwrap();
    let address = server.start().unwrap()[0];
    assert!(address.ip().is_loopback());
    server.shutdown().await;

    let bound: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let server = echo_lines().listen(bound).build().unwrap();
    assert_eq!(server.start().unwrap()[0].ip(), bound.ip());
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn broadcasts_with_try_send_skip_sessions_that_do_not_read() {
    let (server, address) = start(
        Server::builder(LineFilter::default)
            .configure(|options| {
                options.send_buffer_limit = 16;
                options.close_timeout = Duration::from_millis(100);
            })
            .on_package(async |session, package| {
                match package.text.as_str() {
                    // Far more than the socket buffers hold: the queue stays full.
                    "flood" => session.send(vec![b'x'; 4 * 1024 * 1024]).await?,
                    "broadcast" => {
                        let (mut sent, mut skipped) = (0, 0);
                        session.server().sessions().for_each(|other| {
                            match other.try_send("news\r\n") {
                                Ok(()) => sent += 1,
                                Err(_) => skipped += 1,
                            }
                        });
                        session
                            .send(format!("sent {sent} skipped {skipped}\r\n"))
                            .await?;
                    }
                    _ => {}
                }
                Ok(())
            }),
    );
    let mut reader = Client::connect(address).await;
    let mut stalled = Client::connect(address).await;
    stalled.send(b"flood\r\n").await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    reader.send(b"broadcast\r\n").await;
    assert_eq!(reader.line().await, "news");
    assert_eq!(reader.line().await, "sent 1 skipped 1");

    // Sessions compare by identity, and are kept in maps by id, for rooms and the like.
    let sessions = server.info().sessions().all();
    let members: HashMap<u64, Session> = sessions.iter().map(|s| (s.id(), s.clone())).collect();
    assert_eq!(members.len(), 2);
    assert_eq!(members[&sessions[0].id()], sessions[0]);
    assert_ne!(sessions[0], sessions[1]);
    server.shutdown().await;
}
