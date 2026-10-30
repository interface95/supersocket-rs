# supersocket-rs

[English](README.md)

基于 [tokio](https://tokio.rs) 的 Rust 套接字服务器框架，架构参照 .NET 的
[SuperSocket](https://github.com/kerryjiang/SuperSocket)。

你只需要描述字节流怎么切成包、每个包怎么处理；接受连接、缓冲、分包、会话、超时、TLS 和优雅关闭都由
supersocket-rs 负责。

```rust
use supersocket::prelude::*;

#[tokio::main]
async fn main() -> Result<(), supersocket::Error> {
    Server::builder(LineFilter::default)
        .on_package(async |session, package| {
            session.send(format!("{}\r\n", package.text)).await?;
            Ok(())
        })
        .listen("0.0.0.0:4040")
        .build()?
        .run()
        .await
}
```

> **状态：** 早期开发阶段，还没有发布到 crates.io，API 仍可能调整。需要 Rust 1.85 或更高版本。

## 功能

- **协议过滤器**把字节流切成包：按行、按结束符、定长、带长度字段的定长包头、首尾标记，也可以自己实现。
  过滤器可以在连接中途切换到另一个过滤器。
- **会话**是很轻的句柄，可以克隆到别的任务里随时发送。可以挂带类型的会话状态，也可以通过扩展表挂任意数据。
- **命令**按 key 分发包，命令过滤器在命令执行前后运行。
- **中间件**能看到会话的建立和关闭，也可以拒绝会话。
- 每个会话的包可以**串行或并发**处理，有处理超时，出错时由错误回调决定会话是否继续。处理函数 panic 会被捕获并交给错误回调，不会拖垮服务。
- **合并发送**：处理一批包期间排队的数据用一次系统调用发出；对方读得慢时 `send` 会等待，也可以设发送超时。
- **空闲会话不占缓冲区**，大量长期空闲的连接也很省内存。
- **客户端**照 SuperSocket 的 `EasyClient` 设计：通过一串连接器建立连接（先 TCP 或 HTTP CONNECT、SOCKS4、SOCKS5 代理，再套 TLS），
  复用服务端的协议过滤器和发送队列。
- **WebSocket**（RFC 6455）服务端和客户端建在同一个核心上：握手过滤器读完升级请求后切换到帧过滤器，握手完成后会话才算连上。
  分片、ping 和关闭握手都替你处理好了，并和 tungstenite 做过互通测试。
- 通过 rustls 支持 **TLS**，包括客户端证书认证；部署在负载均衡后面时支持 **PROXY protocol** v1 和 v2；
  每个监听地址可以单独设置 TCP keepalive 和 socket 收发缓冲区。
- **任意传输层**：Unix 套接字、stdin/stdout、HTTP 升级后的连接都能交给服务端处理；连接还可以套上**流包装层**，
  比如内置的 gzip 压缩。
- **配置**可以从 TOML 或 JSON 文件读取，并用环境变量覆盖；日志用 **tracing**；收到 Ctrl+C 或 SIGTERM 时优雅关闭。

## 开始使用

在第一个版本发布之前，先直接依赖仓库：

```toml
[dependencies]
supersocket = { git = "https://github.com/interface95/supersocket-rs" }  # 可选 feature："tls"、"config"、"client"、"proxy"、"websocket"、"gzip"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

`crates/supersocket/examples` 里有可以直接运行的示例：

```sh
cargo run --example echo_server        # nc 127.0.0.1 4040
cargo run --example command_server
cargo run --example custom_protocol
cargo run --example live_chat
cargo run --example tls_echo_server --features tls
cargo run --example config_server --features config
cargo run --example echo_client --features client     # 先运行 echo_server
cargo run --example proxy_client --features proxy -- 127.0.0.1:1080 example.com:80
cargo run --example websocket_echo_server --features websocket
cargo run --example websocket_chat --features websocket
cargo run --example websocket_client --features websocket,client   # 先运行 websocket_echo_server
```

## 使用说明

### 协议过滤器

| 过滤器 | 分包方式 | 包类型 |
|---|---|---|
| `LineFilter` | 以 `\r\n` 结尾的行 | `TextPackage` |
| `CommandLineFilter` | `KEY 参数1 参数2\r\n` | `CommandPackage` |
| `TerminatorFilter` | 任意结束符 | 自定义 |
| `FixedSizeFilter` | 固定长度 | 自定义 |
| `FixedHeaderFilter` | 包头里带包体长度 | 自定义 |
| `BeginEndMarkFilter` | 首尾标记 | 自定义 |
| `TransparentFilter` | 不分包 | `Bytes` |

带定长包头的二进制协议，实现 `FixedHeaderProtocol` 即可：

```rust
#[derive(Default)]
struct MessageProtocol;

impl FixedHeaderProtocol for MessageProtocol {
    type Package = Message;
    const HEADER_SIZE: usize = 5; // 操作码、2 字节包体长度、2 字节序号

    fn body_length(&self, header: &[u8]) -> Result<usize, ProtocolError> {
        Ok(u16::from_be_bytes([header[1], header[2]]) as usize)
    }

    fn decode(&mut self, package: Bytes) -> Result<Message, ProtocolError> {
        // 包头加包体的完整数据
    }
}

Server::builder(FixedHeaderFilter::<MessageProtocol>::default)
```

其他协议直接实现 `PipelineFilter`。在 `next_filter` 里返回另一个过滤器，连接就会切换过去，协议升级就是这么做的。

### 命令和命令过滤器

```rust
struct Add;

impl AsyncCommand<CommandPackage> for Add {
    fn key(&self) -> String {
        "ADD".into()
    }

    async fn execute(&self, session: &Session, package: CommandPackage) -> HandlerResult {
        let mut sum = 0i64;
        for arg in &package.args {
            sum += arg.parse::<i64>()?;
        }
        session.send(format!("{sum}\r\n")).await?;
        Ok(())
    }
}

Server::builder(CommandLineFilter::default).commands(|commands| {
    commands
        .add(Add)
        .add(Secret.with_filter(RequireLogin)) // 只作用于一个命令的过滤器
        .filter(Timing);                        // 作用于所有命令的过滤器
    commands.on_unknown(async |session, package| {
        session.send(format!("unknown command {}\r\n", package.key)).await?;
        Ok(())
    });
})
```

`CommandFilter` 实现 `on_executing`（返回 `false` 跳过命令）和 `on_executed`（拿到命令的执行结果）。

### 会话

`Session` 是很轻的句柄，可以克隆到别的任务里随时调用 `send`，并发发送的数据不会交错。会话状态是一个实现了
`SessionState` 的类型，它的字段可以直接在会话上访问：

```rust
#[derive(Default)]
struct Chatter {
    name: Mutex<String>,
}

impl SessionState for Chatter {
    async fn on_connected(session: &Session<Self>) {
        *session.name.lock().unwrap() = format!("guest{}", session.id());
    }
}

Server::builder(CommandLineFilter::default)
    .session::<Chatter>() // 要在注册处理函数和命令之前调用
```

`session.server().sessions()` 列出当前所有会话，可以用来广播。`session.extensions()` 按类型存数据，适合中间件使用。
用 `.service(value)` 注册的共享服务，可以通过 `session.server().service::<T>()` 取到。为会话 spawn 的任务可以等待
`session.closed()`，在会话关闭时一起结束。

### 包处理、错误和超时

```rust
Server::builder(LineFilter::default)
    .on_package(handler)
    .on_error(async |session, error| match error {
        PackageError::Timeout(_) => ErrorAction::Continue,
        _ => ErrorAction::Close,
    })
    .configure(|options| {
        options.package_handling_timeout = Duration::from_secs(10);
        options.package_handling = PackageHandling::Concurrent { max_in_flight: 16 };
        options.idle_session_timeout = Some(Duration::from_secs(300));
        options.send_timeout = Some(Duration::from_secs(30));
    })
```

默认每个会话的包按顺序一个一个处理。并发模式下，一个会话最多同时有 `max_in_flight` 个包在运行时的工作线程上处理。

### 客户端

开启 `client` feature。`EasyClient` 用协议过滤器把服务端发来的数据切成包，和服务端用的是同一套过滤器：

```rust
let mut client = EasyClient::builder(LineFilter::default())
    .connect("127.0.0.1:4040")
    .await?;
client.send("hello\r\n").await?;
let reply = client.receive().await?; // Some(TextPackage)，连接关闭后返回 None
```

也可以把每个包交给后台运行的处理函数，用返回的发送句柄继续发送和关闭：

```rust
let sender = client.start_receive(async |sender, package| {
    let _ = sender.send(format!("got {}\r\n", package.text)).await;
});
sender.close().await; // 先把队列里的数据发完，最多等 close_timeout
```

后台任务不会让连接一直开着：最后一个发送句柄被丢弃时，连接也会这样关闭。

连接由一串连接器建立，对应 SuperSocket 的 `IConnector`：第一个负责打开连接，后面的依次往外包一层。

```rust
EasyClient::builder(LineFilter::default())
    .proxy(Socks5Proxy::new("127.0.0.1:1080").credentials("user", "secret")) // 需要 "proxy" feature
    .tls(TlsConnector::new()) // 需要 "tls" feature，默认用 Mozilla 根证书校验
    .connect("example.com:443")
    .await?;
```

`HttpProxy`（CONNECT 方法）和 `Socks4Proxy`（主机名用 SOCKS4a）用法一样；任何实现了 `Connector` 的类型都可以通过
`.connector(...)` 加进来。

### WebSocket

启用 `websocket` feature。WebSocket 服务端跑在同一个核心上：第一个协议过滤器读 HTTP 升级请求，读完切换到帧过滤器；
握手完成后会话才算连上，所以广播不会发到还在升级的连接上。监听器配了 TLS 就是 `wss://`。

```rust
use supersocket::websocket::{Rejection, WebSocketServer};

WebSocketServer::builder()
    .subprotocols(["chat.v1"])
    .on_handshake(async |session| match session.request().query() {
        Some("token=secret") => Ok(()),
        _ => Err(Rejection::new(401, "log in first")),
    })
    .on_message(async |session, message| {
        for other in session.sessions() {
            let _ = other.send(&message).await; // 广播
        }
        Ok(())
    })
    .listen("0.0.0.0:4040")
    .build()?
    .run()
    .await
```

`session.close(CloseCode::NORMAL, "bye")` 发起关闭握手；服务端关闭时会给每个客户端发 1001（going away）。
`max_package_length` 同时限制单帧大小和分片拼接后的消息大小。

再启用 `client` feature，就可以用 `WebSocketClient`。它和 `EasyClient` 用同一套连接器，所以代理和 `wss://` 的用法也一样：

```rust
let mut client = WebSocketClient::connect("wss://example.com/chat").await?;
client.send_text("hello").await?;
let reply = client.receive().await?; // Some(Message)，连接关闭后返回 None
client.close(CloseCode::NORMAL, "done").await;
```

WebSocket 服务端用到的两个核心功能，别的协议也能用：一是 `PackageHandler` 类型，它的 `closing` 钩子可以在关闭前排一条告别消息；
二是 `ServerBuilder::require_handshake`，在处理器调用 `session.complete_handshake()` 之前，会话不进会话容器，连接回调也先不触发。

### 中间件

```rust
struct ConnectionLimit {
    limit: usize,
    current: AtomicUsize,
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
```

会话是并发建立的，所以限流时要自己计数，不能依赖 `session_count()`。

### TLS

开启 `tls` feature，给监听地址配上 PEM 格式的证书链和私钥：

```rust
.configure(|options| {
    options.listeners.push(ListenOptions {
        tls: Some(TlsOptions::new("cert.pem", "key.pem")),
        ..ListenOptions::new("0.0.0.0:4443")
    });
})
```

TLS 握手在每个连接自己的任务里进行，慢客户端不会拖住接受新连接。

要求客户端出示证书时，配上签发客户端证书的 CA；会话里可以拿到客户端发来的证书：

```rust
let tls = TlsOptions {
    client_ca: Some("clients.pem".into()),
    client_auth_optional: false, // 设为 true 时，没有证书的客户端也接受
    ..TlsOptions::new("cert.pem", "key.pem")
};
// 处理函数里：session.tls_info().unwrap().peer_certificates()
```

其他需求，比如 ALPN、按域名用不同证书，可以用 `TlsOptions::with_config(Arc::new(server_config))`
直接传入完整的 rustls 配置。

### 部署在代理后面

放在 HAProxy、nginx 或 AWS Network Load Balancer 后面、并且代理会发 PROXY protocol 头时，给监听地址打开这个选项。
这样 `session.remote_addr()` 就是客户端的地址而不是代理的地址，中间件一开始就能看到；头里的其他内容在
`session.proxy_info()` 里。没有合法头的连接会被直接关闭，所以只在只能经由代理访问的监听地址上打开。

```rust
ListenOptions {
    proxy_protocol: true,                        // 支持 v1 和 v2；同时开 TLS 时先读头再握手
    keepalive: Some(Duration::from_secs(60)),    // TCP keepalive
    socket_receive_buffer_size: Some(256 * 1024), // SO_RCVBUF；发送缓冲区用 socket_send_buffer_size
    ..ListenOptions::new("0.0.0.0:4040")
}
```

其他 socket 选项可以用 `ServerBuilder::configure_socket(|stream| ...)`，每个新连接都会调用。

### 其他传输层和流包装层

连接不一定来自服务端自己的 TCP 监听器。服务端启动后，任何字节流都可以交给它，它会和其他会话一样：经过中间件、进入会话容器、
随服务端关闭而关闭。服务端也可以完全不配置监听地址。

```rust
let listener = tokio::net::UnixListener::bind("/tmp/app.sock")?;
while let Ok((stream, _)) = listener.accept().await {
    server.serve_connection(stream, ConnectionInfo::default())?; // Unix 套接字没有 IP 地址
}
```

流包装层在 TLS 握手之后包住每个连接，对应 SuperSocket 的 `IConnectionStreamInitializer`。启用 `gzip` feature 后，
`GzipLayer` 对连接双向压缩，客户端用 `GzipConnector` 配合：

```rust
Server::builder(LineFilter::default).stream_layer(GzipLayer)
EasyClient::builder(LineFilter::default()).connector(GzipConnector)
```

自己的包装层（比如流量统计、其他加密）实现 `StreamLayer` 即可。

### 配置

开启 `config` feature 后，`ServerOptions::load` 可以读取 TOML 或 JSON 文件。每个字段都有默认值，环境变量可以覆盖文件里的值。值会按字段类型转换，数字可以写成字符串，时长可以写 `"30s"` 或秒数：

```toml
name = "echo"
package_handling_timeout = "10s"
idle_session_timeout = "5m"
package_handling = { concurrent = { max_in_flight = 16 } }

[[listeners]]
address = "0.0.0.0:4040"

[[listeners]]
address = "0.0.0.0:4443"
tls = { cert = "cert.pem", key = "key.pem" }
```

```sh
SUPERSOCKET__LISTENERS__0__ADDRESS=127.0.0.1:4041 SUPERSOCKET__SEND_TIMEOUT=30s ./server
```

```rust
let options = ServerOptions::load("supersocket.toml")?;
Server::builder(LineFilter::default).options(options)
```

## 性能

在 Linux 下的 Ryzen 9 9950X 上做回显压测，服务端和压测客户端各绑 4 个物理核（8 个线程），supersocket-rs
的核心和 .NET 11 RC2 上开了 runtime async 与服务器 GC 的 SuperSocket 对比如下：

| | supersocket-rs | SuperSocket（.NET 11） |
|---|---|---|
| 100 连接，一问一答 | 202 万/秒，p99 89 µs | 150 万/秒，p99 211 µs |
| 1000 连接 | 282 万/秒，p99 0.71 ms | 151 万/秒，p99 1.26 ms |
| 每个连接连发 16 个请求 | 1420 万/秒 | 1060 万/秒 |
| 每 CPU 秒处理的请求数（100 连接） | 33.1 万 | 20.5 万 |
| 1 万个空闲连接的内存 | 63 MB | 186 MB |

这组数据来自同一核心的原型。之后空闲会话不再保留读写缓冲区，每个空闲连接的内存又降了一半多（本机实测从 6.4 KB 降到
约 3 KB）。回显只衡量框架本身的开销，实际应用的大部分时间花在别处。

## 路线图

- WebSocket 压缩（permessage-deflate），并跑一遍 Autobahn 测试套件
- 用派生宏注册命令
- UDP 会话（有需求再做）

## 许可证

Apache License 2.0。supersocket-rs 是参照 Kerry Jiang 等人的 [SuperSocket](https://github.com/kerryjiang/SuperSocket)
架构独立实现的项目，详见 [NOTICE](NOTICE)。
