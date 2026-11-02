"""Validate and compare the two full-pipeline servers. Python 3, no third-party dependencies.

Build the binaries first; see README.md. Every sample starts a fresh server. No result is
accepted unless the common client validates all replies and every session's filter counter.
"""

import argparse
from contextlib import contextmanager
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import platform
import resource
import signal
import socket
import statistics
import subprocess
import time

ROOT = Path(__file__).resolve().parents[2]
SCENARIOS = [(1, 1), (100, 1), (1000, 1), (100, 16)]


def command_output(command, cwd=ROOT):
    return subprocess.check_output(command, cwd=cwd, text=True).strip()


class Client:
    def __init__(self, address):
        self.socket = socket.create_connection(address, timeout=5)
        self.socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.reader = self.socket.makefile("rb")

    def expect(self, response):
        actual = self.reader.readline(8192)
        assert actual == response, (response, actual)

    def exchange(self, request, response):
        self.socket.sendall(request)
        self.expect(response)

    def close(self):
        self.reader.close()
        self.socket.close()


@contextmanager
def server(kind, output, label, threads, limit=20000):
    with socket.socket() as reservation:
        reservation.bind(("127.0.0.1", 0))
        port = reservation.getsockname()[1]
    address = ("127.0.0.1", port)
    environment = os.environ.copy()
    environment.update({"BENCH_PORT": str(port), "BENCH_THREADS": str(threads),
                        "BENCH_MAX_SESSIONS": str(limit), "DOTNET_PROCESSOR_COUNT": str(threads)})
    commands = {
        "rust": [str(ROOT / "target/release/examples/bench_pipeline")],
        "csharp": ["dotnet", str(ROOT / "tools/bench/dotnet/bin/Release/net11.0/PipelineBench.dll")],
    }
    with (output / f"{label}.server.log").open("w") as log:
        process = subprocess.Popen(commands[kind], cwd=ROOT, env=environment, stdout=log,
                                   stderr=subprocess.STDOUT)
        try:
            deadline = time.monotonic() + 20
            while True:
                if process.poll() is not None:
                    raise RuntimeError(f"{label} exited during startup; see {log.name}")
                try:
                    probe = Client(address)
                except OSError:
                    if time.monotonic() >= deadline:
                        raise TimeoutError(f"{label} did not listen")
                    time.sleep(0.05)
                    continue
                try:
                    probe.expect(b"READY\r\n")
                    probe.exchange(b"QUIT\r\n", b"BYE\r\n")
                    probe.expect(b"")
                finally:
                    probe.close()
                break
            # Let the readiness session's asynchronous unregister finish before limit tests.
            time.sleep(0.1)
            yield process, address
            if process.poll() is not None:
                raise RuntimeError(f"{label} exited unexpectedly; see {log.name}")
        finally:
            if process.poll() is None:
                process.send_signal(signal.SIGINT)
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
                    raise RuntimeError(f"{label} required forced termination")


def validate(kind, output, threads):
    with server(kind, output, f"validate-{kind}", threads, limit=2) as (_, address):
        clients = []
        try:
            first = Client(address)
            clients.append(first)
            first.expect(b"READY\r\n")
            first.exchange(b"WHOAMI\r\n", b"OK name=anonymous\r\n")
            first.exchange(b"ECHO blocked\r\n", b"ERR set NAME first\r\n")
            first.exchange(b"STATS\r\n", b"OK count=0\r\n")
            first.exchange(b"NAME alice\r\n", b"OK name=alice\r\n")
            for name in [b"", b"x" * 33, b"a b", b"a\x00b", "名字".encode()]:
                first.exchange(b"NAME " + name + b"\r\n", b"ERR invalid name\r\n")
            first.exchange(b"WHOAMI\r\n", b"OK name=alice\r\n")
            first.exchange(b"ECHO hello\r\n", b"OK echo=hello\r\n")
            first.exchange(b"STATS\r\n", b"OK count=1\r\n")
            for body in [b"", b"a\x00b", b"a\tb"]:
                first.exchange(b"ECHO " + body + b"\r\n", b"ERR invalid echo\r\n")
            first.exchange(b"STATS\r\n", b"OK count=1\r\n")
            first.exchange(b"UNKNOWN\r\n", b"ERR unknown command\r\n")
            first.socket.sendall(b"ECHO one\r\nECHO two\r\nSTATS\r\n")
            for reply in [b"OK echo=one\r\n", b"OK echo=two\r\n", b"OK count=3\r\n"]:
                first.expect(reply)
            second = Client(address)
            clients.append(second)
            second.expect(b"READY\r\n")
            second.exchange(b"WHOAMI\r\n", b"OK name=anonymous\r\n")
            second.exchange(b"STATS\r\n", b"OK count=0\r\n")
            rejected = Client(address)
            clients.append(rejected)
            rejection = rejected.reader.readline(8192)
            assert rejection in (b"", b"ERR server busy\r\n"), rejection
            if rejection:
                rejected.expect(b"")
            first.exchange(b"QUIT\r\n", b"BYE\r\n")
            first.expect(b"")
            # Prove that unregister released the slot; poll the known asynchronous boundary.
            deadline = time.monotonic() + 5
            while True:
                replacement = Client(address)
                clients.append(replacement)
                greeting = replacement.reader.readline(8192)
                if greeting == b"READY\r\n":
                    replacement.exchange(b"WHOAMI\r\n", b"OK name=anonymous\r\n")
                    break
                assert greeting in (b"", b"ERR server busy\r\n"), greeting
                replacement.close()
                if time.monotonic() >= deadline:
                    raise AssertionError("middleware failed to release the connection slot")
                time.sleep(0.05)
            return {"kind": kind, "status": "passed", "limit_rejection": rejection.decode().strip() or "EOF"}
        finally:
            for client in clients:
                client.close()


def run_client(mode, process, address, label, count, depth, args):
    command = [str(ROOT / "target/release/pipeline"), mode, f"{address[0]}:{address[1]}", str(count)]
    if mode == "run":
        command.extend([str(args.seconds), "32", str(depth)])
    command.extend([str(process.pid), label])
    environment = os.environ.copy()
    environment.update({"BENCH_THREADS": str(args.threads), "BENCH_WARMUP_SECONDS": str(args.warmup)})
    result = subprocess.run(command, env=environment, cwd=ROOT, capture_output=True, text=True,
                            timeout=args.seconds + args.warmup + 90)
    if result.returncode:
        raise RuntimeError(f"{label}: client exited {result.returncode}: {result.stderr}")
    if result.stderr:
        raise RuntimeError(f"{label}: unexpected client stderr: {result.stderr}")
    return json.loads(result.stdout)


def report(output, data):
    metadata = data["metadata"]
    lines = ["# Session + 命令 + 中间件压测", "", f"- 时间：{metadata['started_utc']}（UTC）",
             f"- 主机：{metadata['machine']}；{metadata['platform']}",
             f"- Rust：{metadata['rustc']}；.NET SDK：{metadata['dotnet_sdk']}",
             f"- SuperSocket NuGet：2.1.0；Rust 基线：`{metadata['git_head']}`，具体源码校验值见 results.json。",
             f"- 每场景 {metadata['rounds']} 轮；每轮新服务端，{metadata['warmup_seconds']} 秒持续预热，"
             f"{metadata['measurement_seconds']} 秒计时加最终在途批次排空；轮次交替实现顺序。",
             f"- Rust 服务端/客户端各 {metadata['threads']} worker；.NET ProcessorCount={metadata['threads']}，"
             "Server GC、应用项目 runtime async 开启。未设置 CPU affinity 或 CPU 配额；"
             "worker 数和 ProcessorCount 不是相同的 CPU 使用限制。",
             "- 回环 TCP、32 字节 ASCII ECHO 正文，命名初始化不计时。相同响应逐字节校验，"
             "最后通过 STATS 校验所有 Session 的命令后置过滤器计数。无 TLS、压缩和逐条日志。",
             "- 下表为各轮中位数；吞吐范围为最小～最大。p99 为各轮 p99 的中位数，"
             "并非合并分布的 p99。depth=16 的延迟是整批 16 条请求的 RTT。", "",
             "| 连接数 / depth | 实现 | 次/秒中位数（最小～最大） | p50 µs | p99 µs | p99.9 µs | 服务端CPU核 | 客户端CPU核 | 次/CPU秒 | 服务端RSS MiB |",
             "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|"]
    comparisons = []
    for count, depth in SCENARIOS:
        summaries = {}
        for kind in ["rust", "csharp"]:
            rows = [r for r in data["samples"] if r["kind"] == "result" and r["implementation"] == kind
                    and r["connections"] == count and r["depth"] == depth]
            if not rows:
                continue
            summary = {key: statistics.median(row[key] for row in rows) for key in
                       ["rps", "p50_us", "p99_us", "p999_us", "server_cpu_cores", "client_cpu_cores",
                        "requests_per_cpu_second", "server_rss_mib"]}
            summaries[kind] = summary
            low, high = min(r["rps"] for r in rows), max(r["rps"] for r in rows)
            lines.append(f"| {count} / {depth} | {kind} | {summary['rps']:,.0f} ({low:,.0f}～{high:,.0f}) | "
                         f"{summary['p50_us']:.1f} | {summary['p99_us']:.1f} | {summary['p999_us']:.1f} | "
                         f"{summary['server_cpu_cores']:.2f} | {summary['client_cpu_cores']:.2f} | "
                         f"{summary['requests_per_cpu_second']:,.0f} | {summary['server_rss_mib']:.1f} |")
        if len(summaries) == 2:
            ratio = summaries['rust']['rps'] / summaries['csharp']['rps']
            comparisons.append(f"- {count} 连接、depth={depth}：Rust/C# 吞吐比 **{ratio:.2f}×**。")
    lines.extend(["", *comparisons, "", "## 命名空闲连接", "",
                  "每轮独立新进程，连接全部建立并完成 NAME 后等待 5 秒采样；每次RSS取5个样本的中位数，再取轮次中位数。",
                  "增量包括运行时池、JIT、GC 和初始化分配，不能解释为纯 Session 对象大小。", "",
                  "| 实现 | 连接数 | 建连前 RSS MiB | 建连后 RSS MiB | 增量 KiB/连接 |", "|---|---:|---:|---:|---:|"])
    for kind in ["rust", "csharp"]:
        rows = [r for r in data["samples"] if r["kind"] == "idle" and r["implementation"] == kind]
        if rows:
            values = [statistics.median(r[key] for r in rows) for key in
                      ["before_mib", "after_mib", "incremental_kib_per_connection"]]
            lines.append(f"| {kind} | {rows[0]['connections']} | {values[0]:.1f} | {values[1]:.1f} | {values[2]:.1f} |")
    lines.extend(["", "## 边界与复现", "",
                  "- 这是固定并发、收到全部响应再发下一批的闭环测试；不等于固定到达率下的排队延迟或生产容量。",
                  "- 吞吐统计全部请求；延迟每16批采样一次，各连接错开采样位置，p99.9仅作辅助观察。",
                  "- 客户端与服务端共享主机；CPU、回环内核和客户端自身均可能成为瓶颈。macOS 调度与其它进程会造成波动。",
                  "- 不与 README 中另一台 Linux/Ryzen 主机的简单回显数字直接相减或求开销比例。",
                  "- C# 使用发布的 NuGet 二进制；runtime async 编译选项只作用于本压测应用，并未重编这些依赖。",
                  "- 两边连接中间件正常注册/释放均被验证。C# 原生注册拒绝的发包行为及关闭超时能力存在差异；"
                  "计时阶段的连接数低于限额，不测试拒绝路径和关闭路径吞吐。",
                  "- 构建与复现命令见 ../../README.md；原始逐轮结果、环境和源码 SHA-256 见 results.json。", ""])
    (output / "report.md").write_text("\n".join(lines))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument("--seconds", type=int, default=10)
    parser.add_argument("--warmup", type=int, default=5)
    parser.add_argument("--threads", type=int, default=8)
    parser.add_argument("--idle-connections", type=int, default=10000)
    parser.add_argument("--validate-only", action="store_true")
    args = parser.parse_args()
    assert min(args.rounds, args.seconds, args.threads, args.idle_connections) > 0
    assert args.warmup >= 0
    args.output.mkdir(parents=True, exist_ok=False)
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    resource.setrlimit(resource.RLIMIT_NOFILE, (min(65536, hard) if hard != resource.RLIM_INFINITY else 65536, hard))
    machine = command_output(["sysctl", "-n", "machdep.cpu.brand_string"]) if platform.system() == "Darwin" else platform.processor()
    sources = sorted(set(ROOT.glob("crates/**/*.rs")) | set(ROOT.glob("crates/**/Cargo.toml"))
                     | set((ROOT / "tools/bench").glob("**/*.rs")) | set((ROOT / "tools/bench").glob("*.py"))
                     | {ROOT / "Cargo.lock", ROOT / "Cargo.toml", ROOT / "tools/bench/Cargo.toml"}
                     | {p for p in (ROOT / "tools/bench/dotnet").glob("*") if p.is_file()})
    metadata = {"started_utc": datetime.now(timezone.utc).isoformat(), "platform": platform.platform(),
                "machine": machine, "cpu_count": os.cpu_count(), "rustc": command_output(["rustc", "--version"]),
                "dotnet_sdk": command_output(["dotnet", "--version"], ROOT / "tools/bench/dotnet"),
                "git_head": command_output(["git", "rev-parse", "HEAD"]),
                "git_status": command_output(["git", "status", "--short"]), "threads": args.threads,
                "rounds": args.rounds, "warmup_seconds": args.warmup, "measurement_seconds": args.seconds,
                "command": ["python3", "tools/bench/run_pipeline.py", *os.sys.argv[1:]],
                "source_sha256": {str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest() for p in sources},
                "binary_sha256": {str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest() for p in [
                    ROOT / "target/release/examples/bench_pipeline", ROOT / "target/release/pipeline",
                    ROOT / "tools/bench/dotnet/bin/Release/net11.0/PipelineBench.dll",
                    ROOT / "tools/bench/dotnet/bin/Release/net11.0/PipelineBench.runtimeconfig.json"]}}
    data = {"metadata": metadata, "validation": [], "samples": []}
    result_file = args.output / "results.json"

    def save():
        result_file.write_text(json.dumps(data, indent=2, ensure_ascii=False) + "\n")

    save()
    for kind in ["rust", "csharp"]:
        data["validation"].append(validate(kind, args.output, args.threads))
        save()
        print(f"{kind}: protocol, session isolation, filters and middleware validated", flush=True)
    if args.validate_only:
        return
    for round_number in range(1, args.rounds + 1):
        order = ["rust", "csharp"] if round_number % 2 else ["csharp", "rust"]
        for count, depth in SCENARIOS:
            for kind in order:
                label = f"{kind}-c{count}-d{depth}-r{round_number}"
                with server(kind, args.output, label, args.threads) as (process, address):
                    sample = run_client("run", process, address, label, count, depth, args)
                sample.update({"implementation": kind, "round": round_number})
                data["samples"].append(sample)
                save()
                print(f"{label}: {sample['rps']:,.0f} req/s, p99={sample['p99_us']:.1f} us", flush=True)
        for kind in order:
            label = f"{kind}-idle-r{round_number}"
            with server(kind, args.output, label, args.threads) as (process, address):
                sample = run_client("idle", process, address, label, args.idle_connections, 1, args)
            sample.update({"implementation": kind, "round": round_number})
            data["samples"].append(sample)
            save()
            print(f"{label}: {sample['after_mib']:.1f} MiB", flush=True)
    metadata["finished_utc"] = datetime.now(timezone.utc).isoformat()
    save()
    report(args.output, data)
    print(f"Saved {result_file} and {args.output / 'report.md'}", flush=True)


if __name__ == "__main__":
    main()
