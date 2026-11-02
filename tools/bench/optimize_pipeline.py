"""Build isolated Rust hot-path variants and run paired, validated pipeline comparisons.

prepare --output <new-directory> snapshots the current workspace, builds five variants in a
temporary directory, and records their exact patches and executable hashes.
run --builds <directory>/builds.json --output <new-directory> measures adjacent baseline / variant
pairs with alternating order. Source files and existing benchmark binaries are never overwritten.
"""

import argparse
from datetime import datetime, timezone
import difflib
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import platform
import resource
import shutil
import statistics
import subprocess
import tempfile
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[2]
PARSE_FILE = "crates/supersocket-proto/src/package.rs"
ECHO_FILE = "crates/supersocket/examples/bench_pipeline.rs"
VARIANTS = ("baseline", "parse", "vectored", "parse_vectored", "parse_vectored_lto")
PARSE_BEFORE = '''        let text = TextPackage::decode(buf)?.text;
        let (key, body) = text.split_once(' ').unwrap_or((text.as_str(), ""));'''
PARSE_AFTER = '''        let text = std::str::from_utf8(&buf)
            .map_err(|_| ProtocolError::new("text is not valid UTF-8"))?;
        let (key, body) = text.split_once(' ').unwrap_or((text, ""));'''
ECHO_BEFORE = r'''            .send(format!("OK echo={}\r\n", package.body))'''
ECHO_AFTER = r'''            .send_vectored(&[b"OK echo=", package.body.as_bytes(), b"\r\n"])'''
COMPATIBILITY_TESTS = r'''

#[cfg(test)]
mod optimization_compatibility {
    use super::*;

    #[test]
    fn preserves_command_text_boundaries() {
        for (input, key, body) in [
            ("", "", ""), ("PING", "PING", ""), ("PING ", "PING", ""),
            (" ", "", ""), ("  ", "", " "), (" NAME alice", "", "NAME alice"),
            ("ECHO  你好\t世界", "ECHO", " 你好\t世界"),
            ("ECHO\u{00a0}hello", "ECHO\u{00a0}hello", ""), ("\tPING", "\tPING", ""),
        ] {
            let package = CommandPackage::decode(Bytes::copy_from_slice(input.as_bytes())).unwrap();
            assert_eq!((package.key.as_str(), package.body.as_str()), (key, body));
        }
        let package = CommandPackage::decode(Bytes::from_static("ECHO a\tb\u{2003}c".as_bytes())).unwrap();
        assert_eq!(package.args().collect::<Vec<_>>(), ["a", "b", "c"]);
    }

    #[test]
    fn preserves_invalid_utf8_errors() {
        for input in [&b"\xff"[..], &b"ECHO \xff"[..], &b"ECHO \xe4\xb8"[..]] {
            let error = CommandPackage::decode(Bytes::copy_from_slice(input)).unwrap_err();
            assert_eq!(error.to_string(), "protocol error: text is not valid UTF-8");
        }
    }
}
'''


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def write_json(path, data):
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n")


def load_runner():
    spec = importlib.util.spec_from_file_location("pipeline_runner", ROOT / "tools/bench/run_pipeline.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    return runner


def checked(command, cwd, environment, log):
    with log.open("w") as output:
        result = subprocess.run(command, cwd=cwd, env=environment, stdout=output, stderr=subprocess.STDOUT)
    if result.returncode:
        raise RuntimeError(f"{command} failed; see {log}\n{log.read_text()[-6000:]}")


def prepare(output):
    for name, pattern in ((PARSE_FILE, PARSE_BEFORE), (ECHO_FILE, ECHO_BEFORE)):
        if (ROOT / name).read_text().count(pattern) != 1:
            raise SystemExit(
                "prepare requires the original, pre-optimization source. "
                "Use baseline-source.tar.gz from tools/bench/results/"
                "2026-11-01-rust-optimizations; see tools/bench/README.md."
            )
    workspace = Path(tempfile.mkdtemp(prefix="supersocket-opt-"))
    snapshot = workspace / "source"
    snapshot.mkdir()
    listed = subprocess.check_output(["git", "ls-files", "-z", "--cached", "--others", "--exclude-standard"], cwd=ROOT)
    sources = {}
    for name in set(listed.decode().split("\0")) - {""}:
        source = ROOT / name
        if source.is_file():
            destination = snapshot / name
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source, destination)
            if source.suffix in {".rs", ".toml", ".lock"} or name == "Cargo.lock":
                sources[name] = digest(source)
    original = {name: (snapshot / name).read_text() for name in (PARSE_FILE, ECHO_FILE)}
    assert original[PARSE_FILE].count(PARSE_BEFORE) == 1
    assert original[ECHO_FILE].count(ECHO_BEFORE) == 1
    environment = os.environ.copy()
    for name in ("RUSTFLAGS", "CARGO_ENCODED_RUSTFLAGS"):
        environment.pop(name, None)
    environment.update({"CARGO_TARGET_DIR": str(workspace / "target"), "CARGO_INCREMENTAL": "0",
                        "CARGO_PROFILE_RELEASE_LTO": "false", "CARGO_PROFILE_RELEASE_CODEGEN_UNITS": "16",
                        "CARGO_PROFILE_RELEASE_OPT_LEVEL": "3", "CARGO_PROFILE_RELEASE_DEBUG": "0"})
    cargo = ["cargo", "build", "--release", "--locked", "--offline"]
    print("Building the fixed load generator", flush=True)
    checked(cargo + ["-p", "supersocket-bench", "--bin", "pipeline"], snapshot, environment, output / "client-build.log")
    client = workspace / "pipeline"
    shutil.copy2(workspace / "target/release/pipeline", client)
    metadata = {"created_utc": datetime.now(timezone.utc).isoformat(), "workspace": str(workspace),
                "git_head": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
                "rustc": subprocess.check_output(["rustc", "--version"], text=True).strip(),
                "platform": platform.platform(), "source_sha256": sources, "client_sha256": digest(client),
                "runner_sha256": digest(ROOT / "tools/bench/run_pipeline.py"),
                "experiment_sha256": digest(Path(__file__)), "variants": {}}
    for variant in VARIANTS:
        parse = "parse" in variant
        vectored = "vectored" in variant
        content = {
            PARSE_FILE: original[PARSE_FILE].replace(PARSE_BEFORE, PARSE_AFTER) if parse else original[PARSE_FILE],
            ECHO_FILE: original[ECHO_FILE].replace(ECHO_BEFORE, ECHO_AFTER) if vectored else original[ECHO_FILE],
        }
        patch = "".join("".join(difflib.unified_diff(original[name].splitlines(True), value.splitlines(True),
                                                     fromfile=f"a/{name}", tofile=f"b/{name}"))
                        for name, value in content.items())
        (output / f"{variant}.patch").write_text(patch)
        for name, value in content.items():
            # Test-only additions are identical for all variants and do not enter release builds.
            (snapshot / name).write_text(value + (COMPATIBILITY_TESTS if name == PARSE_FILE else ""))
        if variant in ("baseline", "parse"):
            print(f"Checking parser compatibility: {variant}", flush=True)
            checked(["cargo", "test", "--locked", "--offline", "-p", "supersocket-proto", "--lib"],
                    snapshot, environment, output / f"{variant}-tests.log")
        profile = environment.copy()
        if variant.endswith("_lto"):
            profile.update({"CARGO_PROFILE_RELEASE_LTO": "thin", "CARGO_PROFILE_RELEASE_CODEGEN_UNITS": "1"})
        print(f"Building {variant}", flush=True)
        checked(cargo + ["-p", "supersocket", "--example", "bench_pipeline"], snapshot, profile,
                output / f"{variant}-build.log")
        root = workspace / "artifacts" / variant
        executable = root / "target/release/examples/bench_pipeline"
        executable.parent.mkdir(parents=True)
        shutil.copy2(workspace / "target/release/examples/bench_pipeline", executable)
        (root / "target/release/pipeline").symlink_to(client)
        metadata["variants"][variant] = {"root": str(root), "server_sha256": digest(executable),
                                          "patch_sha256": digest(output / f"{variant}.patch"),
                                          "lto": profile["CARGO_PROFILE_RELEASE_LTO"],
                                          "codegen_units": int(profile["CARGO_PROFILE_RELEASE_CODEGEN_UNITS"])}
        write_json(output / "builds.json", metadata)
    runner = load_runner()
    validations = []
    for variant, settings in metadata["variants"].items():
        runner.ROOT = Path(settings["root"])
        destination = output / variant
        destination.mkdir()
        validations.append({"variant": variant, **runner.validate("rust", destination, 8)})
        print(f"Validated {variant}: sessions, commands, filters, middleware", flush=True)
    metadata["validation"] = validations
    write_json(output / "builds.json", metadata)
    print(f"Prepared variants: {output / 'builds.json'}", flush=True)


def summarize(output, data):
    lines = ["# Rust 热路径优化 A/B", "", "同机相邻配对，轮次交替先后顺序；各变体使用同一个客户端二进制。",
             f"每个进程预热 {data['warmup_seconds']} 秒，计时 {data['seconds']} 秒，加在途请求排空。",
             "每个场景单独重新测基线，不使用前一天或前一份报告的吞吐作分母。",
             "所有响应逐字节校验，STATS验证完整命令后置过滤器计数。", "",
             "| 候选 | 对照 | 连接/depth | 配对数 | 对照中位次/秒 | 候选中位次/秒 | 配对吞吐变化中位数（最小～最大） | p99变化中位数 | CPU效率变化中位数 |",
             "|---|---|---|---:|---:|---:|---:|---:|---:|"]
    for variant in data["variants"]:
        for count, depth in data["scenarios"]:
            pairs = [p for p in data["pairs"] if p["variant"] == variant and p["connections"] == count and p["depth"] == depth]
            if not pairs:
                continue
            changes = {key: [(p['candidate'][key] / p['baseline'][key] - 1) * 100 for p in pairs]
                       for key in ("rps", "p99_us", "requests_per_cpu_second")}
            baseline = statistics.median(p['baseline']['rps'] for p in pairs)
            candidate = statistics.median(p['candidate']['rps'] for p in pairs)
            gains = changes['rps']
            lines.append(f"| {variant} | {pairs[0]['reference']} | {count}/{depth} | {len(pairs)} | {baseline:,.0f} | {candidate:,.0f} | "
                         f"{statistics.median(gains):+.2f}% ({min(gains):+.2f}%～{max(gains):+.2f}%) | "
                         f"{statistics.median(changes['p99_us']):+.2f}% | "
                         f"{statistics.median(changes['requests_per_cpu_second']):+.2f}% |")
    lines += ["", "吞吐/CPU效率越高越好，p99越低越好。depth=16的延迟是整批RTT。",
              "仅有少量轮次；范围不是置信区间。macOS回环网络、客户端和其它进程均可能影响结果。",
              "小幅或方向不一致的差异不能认定为稳定收益。编译全部完成后才开始计时，不与构建并行。",
              "本轮未修改工作区框架实现，候选补丁和构建设置保存在builds目录；原始配对数据见results.json。", ""]
    (output / "report.md").write_text("\n".join(lines))


def run(args):
    builds = json.loads(args.builds.read_text())
    assert digest(ROOT / "tools/bench/run_pipeline.py") == builds["runner_sha256"]
    variants = args.variants.split(",")
    assert all(v in builds["variants"] and v != "baseline" for v in variants)
    assert args.reference is None or args.reference in builds["variants"]
    scenarios = [tuple(map(int, pair.split(":"))) for pair in args.scenarios.split(",")]
    for value in builds["variants"].values():
        root = Path(value["root"])
        assert digest(root / "target/release/examples/bench_pipeline") == value["server_sha256"]
        assert digest(root / "target/release/pipeline") == builds["client_sha256"]
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    resource.setrlimit(resource.RLIMIT_NOFILE, (min(65536, hard) if hard != resource.RLIM_INFINITY else 65536, hard))
    runner = load_runner()
    data = {"started_utc": datetime.now(timezone.utc).isoformat(), "builds": str(args.builds.resolve()),
            "builds_sha256": digest(args.builds), "variants": variants, "scenarios": scenarios,
            "rounds": args.rounds, "seconds": args.seconds, "warmup_seconds": args.warmup,
            "threads": args.threads, "reference_override": args.reference, "pairs": []}
    options = SimpleNamespace(seconds=args.seconds, warmup=args.warmup, threads=args.threads)
    write_json(args.output / "results.json", data)
    for round_index in range(args.rounds):
        rotation = round_index % len(variants)
        order = variants[rotation:] + variants[:rotation]
        for count, depth in scenarios:
            for variant in order:
                reference = args.reference or ("parse_vectored" if variant.endswith("_lto") else "baseline")
                assert variant != reference
                paired = {"variant": variant, "reference": reference, "round": round_index + 1, "connections": count, "depth": depth}
                # Adjacent candidates start in opposite order; each candidate flips next round.
                forward = (round_index + variants.index(variant)) % 2 == 0
                sequence = (reference, variant) if forward else (variant, reference)
                paired["order"] = sequence
                for implementation in sequence:
                    label = f"{variant}-c{count}-d{depth}-r{round_index + 1}-{implementation}"
                    runner.ROOT = Path(builds["variants"][implementation]["root"])
                    with runner.server("rust", args.output, label, args.threads) as (process, address):
                        sample = runner.run_client("run", process, address, label, count, depth, options)
                    paired["baseline" if implementation == reference else "candidate"] = sample
                    print(f"{label}: {sample['rps']:,.0f} req/s, p99={sample['p99_us']:.1f} us", flush=True)
                data["pairs"].append(paired)
                write_json(args.output / "results.json", data)
                summarize(args.output, data)
    data["finished_utc"] = datetime.now(timezone.utc).isoformat()
    write_json(args.output / "results.json", data)
    print(f"Saved {args.output / 'report.md'}", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("prepare", "run"))
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--builds", type=Path)
    parser.add_argument("--variants", default=",".join(VARIANTS[1:]))
    parser.add_argument("--reference", choices=VARIANTS)
    parser.add_argument("--scenarios", default="100:1,100:16")
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument("--seconds", type=int, default=8)
    parser.add_argument("--warmup", type=int, default=5)
    parser.add_argument("--threads", type=int, default=8)
    args = parser.parse_args()
    assert min(args.rounds, args.seconds, args.threads) > 0 and args.warmup >= 0
    args.output.mkdir(parents=True, exist_ok=False)
    if args.mode == "prepare":
        prepare(args.output)
    else:
        assert args.builds is not None, "run requires --builds"
        run(args)


if __name__ == "__main__":
    main()
