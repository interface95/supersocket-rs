//! UDP for supersocket-rs, like SuperSocket's `UseUdp()`.
//!
//! A [`UdpListener`] receives datagrams on one socket and turns them into sessions, so that the
//! server's pipeline filters, handlers, commands and middleware work on UDP as they do on TCP.
//! The first datagram from a peer opens a session on a virtual connection, later datagrams from
//! that peer are delivered to it, and what the session sends goes back to the peer as datagrams.
//! Which datagrams belong to one session is decided by a [`SessionKey`]: by default the peer's
//! address, like SuperSocket's `IPAddressUdpSessionIdentifierProvider`.
//!
//! A peer never says goodbye on UDP, so set
//! [`idle_session_timeout`](supersocket_server::ServerOptions::idle_session_timeout) to close
//! sessions whose peer went silent; without it they live until the server shuts down.
//!
//! ```no_run
//! use supersocket_proto::LineFilter;
//! use supersocket_server::Server;
//! use supersocket_udp::UdpListener;
//!
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! Server::builder(LineFilter::default)
//!     .on_package(async |session, package| {
//!         session.send(format!("{}\r\n", package.text)).await?;
//!         Ok(())
//!     })
//!     .configure(|options| options.idle_session_timeout = Some(std::time::Duration::from_secs(60)))
//!     .listener(UdpListener::bind("0.0.0.0:4040".parse()?)?)
//!     .build()?
//!     .run()
//!     .await?;
//! # Ok(())
//! # }
//! ```
//!
//! What a session sends goes out as datagrams of what was queued together, as a connection
//! batches writes: the peer must cut the stream into packages with the same framing. A batch
//! larger than [`max_send_size`](UdpListener::max_send_size) is sent as several datagrams. A
//! datagram that does not fit in the peer's buffers or exceeds the path's limits is lost, as on
//! any UDP socket. Empty datagrams are ignored.

use std::{
    collections::HashMap,
    fmt,
    hash::Hash,
    io,
    net::SocketAddr,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll, ready},
};

use bytes::{Buf, Bytes};
use supersocket_connection::BoxStream;
use supersocket_server::{ConnectionInfo, ListenAddr, Listener};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    net::UdpSocket,
    sync::mpsc::{self, error::TrySendError},
};

/// Decides which session a datagram belongs to, like SuperSocket's
/// `IUdpSessionIdentifierProvider`. The default, [`ByAddress`], keys sessions by the peer's
/// address; a protocol that carries a session id in every datagram can key them by that instead,
/// so a client whose address changes keeps its session: replies go to the address its latest
/// datagram came from, while [`remote_addr`](supersocket_server::Session::remote_addr) stays the
/// address of the first one.
pub trait SessionKey: Send + 'static {
    /// The key of a session.
    type Key: Eq + Hash + Clone + Send + 'static;

    /// The key of the session `datagram` from `remote` belongs to.
    fn key(&self, remote: SocketAddr, datagram: &[u8]) -> Self::Key;
}

/// Keys sessions by the address of the peer.
#[derive(Debug, Clone, Copy, Default)]
pub struct ByAddress;

impl SessionKey for ByAddress {
    type Key = SocketAddr;

    fn key(&self, remote: SocketAddr, _datagram: &[u8]) -> SocketAddr {
        remote
    }
}

/// The largest datagram the listener receives; longer ones are cut off.
const MAX_DATAGRAM_SIZE: usize = 64 * 1024;

/// The largest datagram sent; a larger batch is sent as several. Within what every platform
/// sends without special settings: macOS refuses datagrams over 9 KiB by default.
const DEFAULT_SEND_SIZE: usize = 8 * 1024;

/// How many datagrams may wait for a session that is busy; further ones are dropped.
const QUEUE_CAPACITY: usize = 64;

/// A UDP socket that serves datagrams as sessions; see the [crate] documentation. Add it
/// to a server with [`ServerBuilder::listener`](supersocket_server::ServerBuilder::listener).
pub struct UdpListener<K: SessionKey = ByAddress> {
    socket: Arc<UdpSocket>,
    local_addr: SocketAddr,
    keying: K,
    sessions: Arc<Sessions<K::Key>>,
    buf: Vec<u8>,
    queue_capacity: usize,
    send_size: usize,
    /// Tells sessions with the same key apart, so that a session that ends removes only its own
    /// entry.
    next_generation: u64,
}

/// The sessions by key, shared with their streams so that a stream can remove its own session.
struct Sessions<K> {
    map: Mutex<HashMap<K, Entry>>,
}

struct Entry {
    incoming: mpsc::Sender<Bytes>,
    /// Where the peer's latest datagram came from, which is where replies go.
    remote: Arc<Mutex<SocketAddr>>,
    generation: u64,
}

impl UdpListener {
    /// Binds a UDP socket at `address`. Must be called from within a Tokio runtime.
    pub fn bind(address: SocketAddr) -> io::Result<Self> {
        let socket = std::net::UdpSocket::bind(address)?;
        socket.set_nonblocking(true)?;
        Self::from_socket(UdpSocket::from_std(socket)?)
    }

    /// Serves the datagrams of `socket`, which is already bound.
    pub fn from_socket(socket: UdpSocket) -> io::Result<Self> {
        let local_addr = socket.local_addr()?;
        Ok(Self {
            socket: Arc::new(socket),
            local_addr,
            keying: ByAddress,
            sessions: Arc::new(Sessions {
                map: Mutex::new(HashMap::new()),
            }),
            buf: vec![0; MAX_DATAGRAM_SIZE],
            queue_capacity: QUEUE_CAPACITY,
            send_size: DEFAULT_SEND_SIZE,
            next_generation: 0,
        })
    }
}

impl<K: SessionKey> UdpListener<K> {
    /// Decides which session a datagram belongs to with `keying` instead of the peer's address.
    pub fn keyed_by<N: SessionKey>(self, keying: N) -> UdpListener<N> {
        UdpListener {
            socket: self.socket,
            local_addr: self.local_addr,
            keying,
            sessions: Arc::new(Sessions {
                map: Mutex::new(HashMap::new()),
            }),
            buf: self.buf,
            queue_capacity: self.queue_capacity,
            send_size: self.send_size,
            next_generation: 0,
        }
    }

    /// The largest datagram to receive; longer ones are cut off. Default: 64 KiB, the most UDP
    /// carries.
    pub fn max_datagram_size(mut self, size: usize) -> Self {
        self.buf = vec![0; size.max(1)];
        self
    }

    /// How many datagrams may wait for a session whose packages are still being handled; further
    /// ones are dropped, as a network would drop them. Default: 64.
    pub fn queue_capacity(mut self, datagrams: usize) -> Self {
        self.queue_capacity = datagrams.max(1);
        self
    }

    /// The largest datagram to send; a batch of queued data larger than this is sent as several
    /// datagrams. Default: 8 KiB, which every platform sends without special settings. Ethernet
    /// carries about 1472 bytes without IP fragmentation, and 65507 is the most IPv4 allows.
    pub fn max_send_size(mut self, size: usize) -> Self {
        self.send_size = size.max(1);
        self
    }

    /// The address the socket is bound to.
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Number of sessions with a datagram queue.
    pub fn session_count(&self) -> usize {
        self.sessions.map.lock().unwrap().len()
    }
}

impl<K: SessionKey> Listener for UdpListener<K> {
    fn local_addr(&self) -> ListenAddr {
        ListenAddr::Other(format!("udp://{}", self.local_addr))
    }

    async fn accept(&mut self) -> io::Result<Option<(BoxStream, ConnectionInfo)>> {
        loop {
            // Nothing is awaited after a datagram arrived, so dropping this future loses none.
            let (length, remote) = self.socket.recv_from(&mut self.buf).await?;
            let datagram = Bytes::copy_from_slice(&self.buf[..length]);
            let key = self.keying.key(remote, &datagram);
            let mut sessions = self.sessions.map.lock().unwrap();
            let datagram = match sessions.get(&key) {
                Some(entry) => {
                    // The peer may have moved: replies go to where its latest datagram came from.
                    let mut reply_to = entry.remote.lock().unwrap();
                    if *reply_to != remote {
                        *reply_to = remote;
                    }
                    drop(reply_to);
                    match entry.incoming.try_send(datagram) {
                        Ok(()) => continue,
                        Err(TrySendError::Full(_)) => {
                            tracing::debug!(%remote, "dropping a datagram, the session is busy");
                            continue;
                        }
                        // The session ended but is not removed yet; it is replaced.
                        Err(TrySendError::Closed(datagram)) => datagram,
                    }
                }
                None => datagram,
            };
            let (sender, receiver) = mpsc::channel(self.queue_capacity);
            // The first datagram of a session always fits: its queue is new.
            let _ = sender.try_send(datagram);
            let generation = self.next_generation;
            self.next_generation += 1;
            let reply_to = Arc::new(Mutex::new(remote));
            sessions.insert(
                key.clone(),
                Entry {
                    incoming: sender,
                    remote: reply_to.clone(),
                    generation,
                },
            );
            drop(sessions);
            let stream = UdpStream {
                incoming: receiver,
                current: Bytes::new(),
                socket: self.socket.clone(),
                remote: reply_to,
                send_size: self.send_size,
                sessions: self.sessions.clone(),
                key,
                generation,
            };
            return Ok(Some((
                Box::new(stream),
                ConnectionInfo::new(remote, self.local_addr),
            )));
        }
    }
}

impl<K: SessionKey> fmt::Debug for UdpListener<K> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UdpListener")
            .field("local_addr", &self.local_addr)
            .field("sessions", &self.session_count())
            .finish_non_exhaustive()
    }
}

/// The virtual connection of one session: reads the datagrams queued for it, writes datagrams to
/// its peer.
struct UdpStream<Key: Eq + Hash> {
    incoming: mpsc::Receiver<Bytes>,
    /// The rest of the datagram being read.
    current: Bytes,
    socket: Arc<UdpSocket>,
    /// Where the peer's latest datagram came from; the listener updates it.
    remote: Arc<Mutex<SocketAddr>>,
    send_size: usize,
    sessions: Arc<Sessions<Key>>,
    key: Key,
    generation: u64,
}

// Nothing in the stream is pinned: the key is plain data, whatever its type.
impl<Key: Eq + Hash> Unpin for UdpStream<Key> {}

impl<Key: Eq + Hash> AsyncRead for UdpStream<Key> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        // An empty datagram carries nothing, and an empty read would mean the end of the stream.
        while this.current.is_empty() {
            match ready!(this.incoming.poll_recv(cx)) {
                Some(datagram) => this.current = datagram,
                // The listener replaced the session: the stream has ended.
                None => return Poll::Ready(Ok(())),
            }
        }
        // One read never spans two datagrams, so a filter without framing sees their bounds.
        let length = this.current.len().min(buf.remaining());
        buf.put_slice(&this.current[..length]);
        this.current.advance(length);
        Poll::Ready(Ok(()))
    }
}

impl<Key: Eq + Hash> AsyncWrite for UdpStream<Key> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        // A batch larger than a datagram should carry goes out as several datagrams.
        let chunk = &buf[..buf.len().min(this.send_size)];
        let remote = *this.remote.lock().unwrap();
        this.socket.poll_send_to(cx, chunk, remote)
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

impl<Key: Eq + Hash> Drop for UdpStream<Key> {
    /// The session is over: later datagrams from the peer open a new one.
    fn drop(&mut self) {
        let mut sessions = self.sessions.map.lock().unwrap();
        if sessions
            .get(&self.key)
            .is_some_and(|entry| entry.generation == self.generation)
        {
            sessions.remove(&self.key);
        }
    }
}
