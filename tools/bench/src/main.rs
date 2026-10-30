//! Load generator for line-protocol echo servers, used for the numbers in the README. The same
//! client measures every server, such as `examples/bench_echo` and SuperSocket's BenchEcho.
//!
//! ```sh
//! bench pingpong <address> <connections> <seconds> <payload bytes> <pipeline depth> <server pid> <label>
//! bench idle     <address> <connections> <server pid> <label>
//! ```
//!
//! `pingpong` prints one `RESULT` line: throughput, latency percentiles, the server's CPU use
//! and memory. `idle` opens the connections, waits, and prints an `IDLE` line with the server's
//! memory per connection. `BENCH_THREADS` sets the number of worker threads (default: the
//! available CPUs) and `CONNECT_CONCURRENCY` how many connections are opened at a time
//! (default: 64). See `README.md` in this directory.

use std::{
    collections::BTreeMap,
    env, fs, io,
    net::SocketAddr,
    process::Command,
    sync::{Arc, Mutex, OnceLock},
    time::{Duration, Instant},
};

use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{
        TcpStream,
        tcp::{OwnedReadHalf, OwnedWriteHalf},
    },
    sync::Semaphore,
};

fn ps(pid: u32, field: &str) -> String {
    let output = Command::new("ps")
        .args(["-o", &format!("{field}="), "-p", &pid.to_string()])
        .output()
        .expect("ps");
    String::from_utf8_lossy(&output.stdout).trim().to_owned()
}

fn clock_ticks_per_sec() -> f64 {
    static TICKS: OnceLock<f64> = OnceLock::new();
    *TICKS.get_or_init(|| {
        let output = Command::new("getconf")
            .arg("CLK_TCK")
            .output()
            .expect("getconf");
        String::from_utf8_lossy(&output.stdout)
            .trim()
            .parse()
            .expect("the clock ticks per second")
    })
}

/// CPU seconds the server used so far. Linux is read from /proc, because its `ps` only has
/// whole seconds; elsewhere `ps` reports `[[hh:]mm:]ss.xx`.
fn server_cpu_secs(pid: u32) -> f64 {
    if let Ok(stat) = fs::read_to_string(format!("/proc/{pid}/stat")) {
        // The process name may hold spaces, so count from the last closing parenthesis: utime
        // and stime are the 14th and 15th fields.
        let after_name = stat.rfind(')').expect("the process name") + 2;
        let fields: Vec<&str> = stat[after_name..].split_whitespace().collect();
        let ticks: f64 = fields[11].parse::<f64>().unwrap() + fields[12].parse::<f64>().unwrap();
        return ticks / clock_ticks_per_sec();
    }
    ps(pid, "time").split(':').fold(0.0, |total, part| {
        total * 60.0 + part.parse::<f64>().unwrap()
    })
}

/// Resident memory of the server in MB.
fn server_rss_mb(pid: u32) -> f64 {
    if let Ok(status) = fs::read_to_string(format!("/proc/{pid}/status")) {
        let line = status
            .lines()
            .find_map(|line| line.strip_prefix("VmRSS:"))
            .expect("VmRSS");
        let kb: f64 = line.trim().trim_end_matches("kB").trim().parse().unwrap();
        return kb / 1024.0;
    }
    ps(pid, "rss").parse::<f64>().unwrap() / 1024.0
}

struct Connection {
    reader: BufReader<OwnedReadHalf>,
    writer: OwnedWriteHalf,
}

/// Connects and exchanges one line, so that only connections with a working session count.
async fn open(addr: SocketAddr) -> io::Result<Connection> {
    let stream = TcpStream::connect(addr).await?;
    stream.set_nodelay(true)?;
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::with_capacity(8 * 1024, reader);
    writer.write_all(b"hello\r\n").await?;
    let mut line = Vec::new();
    if reader.read_until(b'\n', &mut line).await? != 7 {
        return Err(io::Error::other(
            "connection closed before the handshake echo",
        ));
    }
    Ok(Connection { reader, writer })
}

/// Opens `count` connections, `CONNECT_CONCURRENCY` at a time. Failed attempts are retried and
/// counted by error kind.
async fn connect_all(addr: SocketAddr, count: usize) -> Vec<Connection> {
    let concurrency = env::var("CONNECT_CONCURRENCY")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(64);
    let limit = Arc::new(Semaphore::new(concurrency));
    let failures = Arc::new(Mutex::new(BTreeMap::<String, usize>::new()));
    let mut connecting = Vec::with_capacity(count);
    for _ in 0..count {
        let permit = limit.clone().acquire_owned().await.unwrap();
        let failures = failures.clone();
        connecting.push(tokio::spawn(async move {
            let _permit = permit;
            loop {
                match open(addr).await {
                    Ok(connection) => return connection,
                    Err(error) => {
                        *failures
                            .lock()
                            .unwrap()
                            .entry(format!("{:?}", error.kind()))
                            .or_default() += 1;
                        tokio::time::sleep(Duration::from_millis(20)).await;
                    }
                }
            }
        }));
    }
    let mut connections = Vec::with_capacity(count);
    for task in connecting {
        connections.push(task.await.unwrap());
    }
    let failures = failures.lock().unwrap();
    if !failures.is_empty() {
        eprintln!("CONNECT_FAILURES,{count},{concurrency},{failures:?}");
    }
    connections
}

async fn round_trip(
    reader: &mut BufReader<OwnedReadHalf>,
    writer: &mut OwnedWriteHalf,
    batch: &[u8],
    depth: usize,
    line: &mut Vec<u8>,
) {
    writer.write_all(batch).await.expect("write failed");
    for _ in 0..depth {
        line.clear();
        let read = reader.read_until(b'\n', line).await.expect("read failed");
        assert_eq!(
            read,
            batch.len() / depth,
            "unexpected echo: {:?}",
            String::from_utf8_lossy(line)
        );
    }
}

/// The `p` quantile of `sorted` nanoseconds, in microseconds; `0.0` when there are no samples.
fn percentile(sorted: &[u64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let index = ((sorted.len() as f64 * p).ceil() as usize).clamp(1, sorted.len()) - 1;
    sorted[index] as f64 / 1000.0
}

async fn pingpong(
    addr: SocketAddr,
    conns: usize,
    secs: u64,
    payload: usize,
    depth: usize,
    pid: u32,
    label: &str,
) {
    let connections = connect_all(addr, conns).await;
    let mut request = vec![b'x'; payload];
    request.extend_from_slice(b"\r\n");
    let batch: Arc<[u8]> = request.repeat(depth).into();
    let start_at = Instant::now() + Duration::from_millis(500);
    let end_at = start_at + Duration::from_secs(secs);

    let mut tasks = Vec::with_capacity(conns);
    for connection in connections {
        let batch = batch.clone();
        tasks.push(tokio::spawn(async move {
            let Connection {
                mut reader,
                mut writer,
            } = connection;
            let mut line = Vec::with_capacity(payload + 2);
            let mut latencies = Vec::with_capacity(1 << 16);
            round_trip(&mut reader, &mut writer, &batch, depth, &mut line).await;
            tokio::time::sleep_until(start_at.into()).await;
            while Instant::now() < end_at {
                let sent_at = Instant::now();
                round_trip(&mut reader, &mut writer, &batch, depth, &mut line).await;
                latencies.push(sent_at.elapsed().as_nanos() as u64);
            }
            latencies
        }));
    }

    tokio::time::sleep_until(start_at.into()).await;
    let cpu_before = server_cpu_secs(pid);
    tokio::time::sleep_until(end_at.into()).await;
    let cpu_used = server_cpu_secs(pid) - cpu_before;
    let rss = server_rss_mb(pid);

    let mut latencies = Vec::new();
    for task in tasks {
        latencies.extend(task.await.unwrap());
    }
    latencies.sort_unstable();
    let requests = (latencies.len() * depth) as f64;
    let rps = requests / secs as f64;
    // label, connections, depth, requests per second, p50, p99, p99.9 and maximum latency in
    // microseconds, server CPU cores used, requests per server CPU second, server RSS in MB
    println!(
        "RESULT,{label},{conns},{depth},{rps:.0},{:.1},{:.1},{:.1},{:.1},{:.2},{:.0},{rss:.1}",
        percentile(&latencies, 0.50),
        percentile(&latencies, 0.99),
        percentile(&latencies, 0.999),
        percentile(&latencies, 1.0),
        cpu_used / secs as f64,
        requests / cpu_used,
    );
}

async fn idle(addr: SocketAddr, conns: usize, pid: u32, label: &str) {
    let rss_before = server_rss_mb(pid);
    let connections = connect_all(addr, conns).await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    let rss_after = server_rss_mb(pid);
    // label, connections, server RSS before and after in MB, KB per connection
    println!(
        "IDLE,{label},{conns},{rss_before:.1},{rss_after:.1},{:.1}",
        (rss_after - rss_before) * 1024.0 / conns as f64
    );
    // Reset the connections instead of closing them, so that no TIME_WAIT sockets are left to
    // use up the local ports before the next run.
    for Connection { reader, writer } in connections {
        if let Ok(stream) = reader.into_inner().reunite(writer) {
            let _ = socket2::SockRef::from(&stream).set_linger(Some(Duration::ZERO));
        }
    }
}

fn main() {
    let mut runtime = tokio::runtime::Builder::new_multi_thread();
    if let Some(threads) = env::var("BENCH_THREADS")
        .ok()
        .and_then(|value| value.parse().ok())
    {
        runtime.worker_threads(threads);
    }
    runtime.enable_all().build().unwrap().block_on(run());
}

async fn run() {
    let args: Vec<String> = env::args().collect();
    let usage = "usage: bench pingpong <address> <connections> <seconds> <payload> <depth> <pid> <label>\n       bench idle <address> <connections> <pid> <label>";
    let (Some(mode), Some(addr)) = (args.get(1), args.get(2)) else {
        eprintln!("{usage}");
        std::process::exit(2);
    };
    let addr: SocketAddr = addr.parse().expect("a socket address");
    match mode.as_str() {
        "pingpong" if args.len() == 9 => {
            let seconds: u64 = args[4].parse().expect("seconds");
            let depth: usize = args[6].parse().expect("pipeline depth");
            assert!(seconds >= 1, "seconds must be at least 1");
            assert!(depth >= 1, "pipeline depth must be at least 1");
            pingpong(
                addr,
                args[3].parse().expect("connections"),
                seconds,
                args[5].parse().expect("payload bytes"),
                depth,
                args[7].parse().expect("server pid"),
                &args[8],
            )
            .await
        }
        "idle" if args.len() == 6 => {
            idle(
                addr,
                args[3].parse().expect("connections"),
                args[4].parse().expect("server pid"),
                &args[5],
            )
            .await
        }
        _ => {
            eprintln!("{usage}");
            std::process::exit(2);
        }
    }
}
