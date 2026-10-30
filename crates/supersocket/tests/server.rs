//! Behavior tests of the server.

mod common;

use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use common::{Client, next_reason, start, track_close_reasons};
use supersocket::{
    BeginEndMarkFilter, BoxFilter, FixedHeaderFilter, FixedHeaderProtocol, PackageHandling,
    PipelineFilterExt, ServerBuilder,
    bytes::{Buf, BufMut, Bytes, BytesMut},
    prelude::*,
};
use tokio::net::TcpStream;

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
            .args
            .iter()
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
fn building_without_listeners_or_with_bad_addresses_fails() {
    let no_listener = Server::builder(LineFilter::default).build();
    assert!(matches!(no_listener, Err(supersocket::Error::NoListener)));
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
