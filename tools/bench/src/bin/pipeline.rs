//! One load generator for the Rust and C# session/command/middleware benchmarks.
//! See tools/bench/README.md for the wire contract and measurement limitations.

use std::{
    env, fs, io,
    net::SocketAddr,
    process::Command,
    sync::{Arc, OnceLock},
    time::{Duration, Instant},
};

use serde_json::json;
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{TcpStream, tcp::OwnedReadHalf, tcp::OwnedWriteHalf},
    sync::{Barrier, Semaphore, watch},
    time::timeout,
};

type BenchResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
const IO_TIMEOUT: Duration = Duration::from_secs(15);
const SAMPLE_STRIDE: usize = 16;

fn process_field(pid: u32, field: &str) -> String {
    let output = Command::new("ps")
        .args(["-o", &format!("{field}="), "-p", &pid.to_string()])
        .output()
        .expect("ps must be available");
    assert!(output.status.success(), "ps failed for process {pid}");
    String::from_utf8(output.stdout).unwrap().trim().to_owned()
}

fn cpu_seconds(pid: u32) -> f64 {
    if let Ok(stat) = fs::read_to_string(format!("/proc/{pid}/stat")) {
        static TICKS: OnceLock<f64> = OnceLock::new();
        let ticks_per_sec = TICKS.get_or_init(|| {
            let output = Command::new("getconf").arg("CLK_TCK").output().unwrap();
            assert!(output.status.success());
            String::from_utf8(output.stdout)
                .unwrap()
                .trim()
                .parse()
                .unwrap()
        });
        let fields: Vec<_> = stat[stat.rfind(')').unwrap() + 2..]
            .split_whitespace()
            .collect();
        return (fields[11].parse::<f64>().unwrap() + fields[12].parse::<f64>().unwrap())
            / ticks_per_sec;
    }
    process_field(pid, "time")
        .split(':')
        .fold(0.0, |total, part| {
            total * 60.0 + part.parse::<f64>().unwrap()
        })
}

fn rss_mib(pid: u32) -> f64 {
    if let Ok(status) = fs::read_to_string(format!("/proc/{pid}/status")) {
        let value = status
            .lines()
            .find_map(|line| line.strip_prefix("VmRSS:"))
            .unwrap();
        return value
            .split_whitespace()
            .next()
            .unwrap()
            .parse::<f64>()
            .unwrap()
            / 1024.0;
    }
    process_field(pid, "rss").parse::<f64>().unwrap() / 1024.0
}

struct Connection {
    reader: BufReader<OwnedReadHalf>,
    writer: OwnedWriteHalf,
    response: Vec<u8>,
}

impl Connection {
    async fn receive(&mut self, expected: &[u8]) -> BenchResult<()> {
        self.response.clear();
        timeout(
            IO_TIMEOUT,
            self.reader.read_until(b'\n', &mut self.response),
        )
        .await??;
        if self.response != expected {
            return Err(io::Error::other(format!(
                "unexpected response: expected {expected:?}, received {:?}",
                self.response
            ))
            .into());
        }
        Ok(())
    }

    async fn exchange(&mut self, request: &[u8], response: &[u8], depth: usize) -> BenchResult<()> {
        timeout(IO_TIMEOUT, self.writer.write_all(request)).await??;
        for _ in 0..depth {
            self.receive(response).await?;
        }
        Ok(())
    }

    async fn open(addr: SocketAddr) -> BenchResult<Self> {
        let stream = timeout(IO_TIMEOUT, TcpStream::connect(addr)).await??;
        stream.set_nodelay(true)?;
        // Avoid exhausting local ephemeral ports between repeated benchmark processes.
        socket2::SockRef::from(&stream).set_linger(Some(Duration::ZERO))?;
        let (reader, writer) = stream.into_split();
        let mut connection = Self {
            reader: BufReader::with_capacity(8192, reader),
            writer,
            response: Vec::with_capacity(128),
        };
        connection.receive(b"READY\r\n").await?;
        connection
            .exchange(b"NAME bench\r\n", b"OK name=bench\r\n", 1)
            .await?;
        Ok(connection)
    }
}

async fn connections(addr: SocketAddr, count: usize) -> BenchResult<Vec<Connection>> {
    let limit = Arc::new(Semaphore::new(64));
    let mut tasks = Vec::with_capacity(count);
    for _ in 0..count {
        let permit = limit.clone().acquire_owned().await?;
        tasks.push(tokio::spawn(async move {
            let _permit = permit;
            Connection::open(addr).await
        }));
    }
    let mut connected = Vec::with_capacity(count);
    for task in tasks {
        connected.push(task.await??);
    }
    Ok(connected)
}

fn percentile(sorted: &[u64], percent: f64) -> f64 {
    let index = ((sorted.len() as f64 * percent).ceil() as usize).clamp(1, sorted.len()) - 1;
    sorted[index] as f64 / 1000.0
}

struct Options {
    address: SocketAddr,
    connections: usize,
    seconds: u64,
    payload: usize,
    depth: usize,
    pid: u32,
    label: String,
}

async fn measure(options: Options) -> BenchResult<()> {
    let connections = connections(options.address, options.connections).await?;
    let body = "x".repeat(options.payload);
    let request: Arc<[u8]> = format!("ECHO {body}\r\n")
        .repeat(options.depth)
        .into_bytes()
        .into();
    let response: Arc<[u8]> = format!("OK echo={body}\r\n").into_bytes().into();
    let warmup_seconds: u64 = env::var("BENCH_WARMUP_SECONDS")
        .unwrap_or_else(|_| "5".into())
        .parse()?;
    let warmup_end = Instant::now() + Duration::from_secs(warmup_seconds);
    let warmup_finished = Arc::new(Barrier::new(options.connections + 1));
    let (start_tx, start_rx) = watch::channel(None::<Instant>);
    let mut tasks = Vec::with_capacity(options.connections);
    for (index, mut connection) in connections.into_iter().enumerate() {
        let request = request.clone();
        let response = response.clone();
        let warmup_finished = warmup_finished.clone();
        let mut start_rx = start_rx.clone();
        let depth = options.depth;
        let duration = Duration::from_secs(options.seconds);
        tasks.push(tokio::spawn(async move {
            let mut samples = Vec::with_capacity(256);
            let mut warmup_requests = 0;
            while Instant::now() < warmup_end {
                connection.exchange(&request, &response, depth).await?;
                warmup_requests += depth;
            }
            warmup_finished.wait().await;
            start_rx.changed().await?;
            let end = start_rx.borrow().unwrap() + duration;
            let mut batches = 0;
            while Instant::now() < end {
                let sent = Instant::now();
                connection.exchange(&request, &response, depth).await?;
                if batches % SAMPLE_STRIDE == index % SAMPLE_STRIDE {
                    samples.push(sent.elapsed().as_nanos() as u64);
                }
                batches += 1;
            }
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>((
                connection,
                samples,
                warmup_requests,
                batches * depth,
            ))
        }));
    }
    // No warmup I/O remains when CPU accounting and the shared measurement window start.
    timeout(
        Duration::from_secs(warmup_seconds + 30),
        warmup_finished.wait(),
    )
    .await?;
    let client_cpu_before = cpu_seconds(std::process::id());
    let server_cpu_before = cpu_seconds(options.pid);
    let started = Instant::now();
    start_tx.send(Some(started))?;
    let mut finished = Vec::with_capacity(options.connections);
    for task in tasks {
        finished.push(task.await??);
    }
    // Include the drain of every final in-flight batch in both elapsed time and request count.
    let elapsed = started.elapsed().as_secs_f64();
    let server_cpu = cpu_seconds(options.pid) - server_cpu_before;
    let client_cpu = cpu_seconds(std::process::id()) - client_cpu_before;
    let server_rss = rss_mib(options.pid);
    let client_rss = rss_mib(std::process::id());
    let mut samples = Vec::new();
    let mut warmup_requests = 0;
    let mut requests = 0;
    for (mut connection, latencies, warmed, measured) in finished {
        let count = warmed + measured;
        let expected = format!("OK count={count}\r\n");
        connection
            .exchange(b"STATS\r\n", expected.as_bytes(), 1)
            .await?;
        warmup_requests += warmed;
        requests += measured;
        samples.extend(latencies);
    }
    if samples.is_empty() || server_cpu <= 0.0 {
        return Err(io::Error::other("empty measurement or unavailable CPU accounting").into());
    }
    samples.sort_unstable();
    println!(
        "{}",
        json!({
            "kind": "result", "label": options.label, "connections": options.connections,
            "depth": options.depth, "payload_bytes": options.payload, "seconds": elapsed,
            "warmup_seconds": warmup_seconds, "warmup_requests": warmup_requests,
            "requests": requests, "rps": requests as f64 / elapsed,
            "p50_us": percentile(&samples, 0.50), "p99_us": percentile(&samples, 0.99),
            "p999_us": percentile(&samples, 0.999), "sampled_max_us": *samples.last().unwrap() as f64 / 1000.0,
            "sample_stride": SAMPLE_STRIDE, "sample_count": samples.len(),
            "server_cpu_cores": server_cpu / elapsed, "client_cpu_cores": client_cpu / elapsed,
            "requests_per_cpu_second": requests as f64 / server_cpu,
            "server_rss_mib": server_rss, "client_rss_mib": client_rss,
            "validated_echo_count": requests + warmup_requests,
        })
    );
    Ok(())
}

async fn idle(address: SocketAddr, count: usize, pid: u32, label: &str) -> BenchResult<()> {
    let before = median_rss(pid).await;
    let mut connected = connections(address, count).await?;
    tokio::time::sleep(Duration::from_secs(5)).await;
    let after = median_rss(pid).await;
    // Sample first, then prove the measured sessions remain connected, named, and unused.
    for connection in &mut connected {
        connection
            .exchange(b"STATS\r\n", b"OK count=0\r\n", 1)
            .await?;
        connection
            .exchange(b"WHOAMI\r\n", b"OK name=bench\r\n", 1)
            .await?;
    }
    println!(
        "{}",
        json!({
            "kind": "idle", "label": label, "connections": count,
            "before_mib": before, "after_mib": after,
            "incremental_kib_per_connection": (after - before) * 1024.0 / count as f64,
        })
    );
    drop(connected);
    Ok(())
}

async fn median_rss(pid: u32) -> f64 {
    let mut readings = Vec::with_capacity(5);
    for _ in 0..5 {
        readings.push(rss_mib(pid));
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    readings.sort_by(f64::total_cmp);
    readings[2]
}

async fn run() -> BenchResult<()> {
    let args: Vec<_> = env::args().collect();
    match args.get(1).map(String::as_str) {
        Some("run") if args.len() == 9 => {
            let options = Options {
                address: args[2].parse()?, connections: args[3].parse()?, seconds: args[4].parse()?,
                payload: args[5].parse()?, depth: args[6].parse()?, pid: args[7].parse()?, label: args[8].clone(),
            };
            assert!(options.connections > 0 && options.seconds > 0 && options.depth > 0);
            assert!((1..=4000).contains(&options.payload));
            measure(options).await
        }
        Some("idle") if args.len() == 6 => {
            let count = args[3].parse()?;
            assert!(count > 0);
            idle(args[2].parse()?, count, args[4].parse()?, &args[5]).await
        }
        _ => Err(io::Error::other(
            "usage: pipeline run <address> <connections> <seconds> <payload> <depth> <server pid> <label>\n       pipeline idle <address> <connections> <server pid> <label>"
        ).into()),
    }
}

fn main() -> BenchResult<()> {
    let threads = env::var("BENCH_THREADS")
        .unwrap_or_else(|_| "8".into())
        .parse()?;
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(threads)
        .enable_all()
        .build()?
        .block_on(run())
}
