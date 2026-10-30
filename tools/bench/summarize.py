"""Summarizes the CSV lines the bench tool prints: averages the rounds of each configuration and
shows the spread of the throughput between rounds.

    python3 summarize.py results.csv

A label ending in `-r<n>` is round `n` of a configuration; `START,<label>,<rss kB>` lines may be
added by a driver script after the server started.
"""
import re
import sys
from collections import defaultdict

ORDER = []
SCENARIOS = [(1, 1), (100, 1), (1000, 1), (100, 16)]

start, idle, results, failures, exits = defaultdict(list), defaultdict(list), defaultdict(list), [], []
for line in open(sys.argv[1]):
    parts = line.strip().split(",")
    if not parts or not parts[0]:
        continue
    kind = parts[0]
    if kind in ("START", "IDLE", "RESULT", "EXIT"):
        config = re.sub(r"-r\d+$", "", parts[1])
        if config not in ORDER:
            ORDER.append(config)
    if kind == "START":
        start[config].append(float(parts[2]) / 1024)
    elif kind == "IDLE":
        idle[config].append((float(parts[3]), float(parts[4]), float(parts[5])))
    elif kind == "RESULT":
        conns, depth = int(parts[2]), int(parts[3])
        results[(config, conns, depth)].append([float(v) for v in parts[4:]])
    elif kind == "EXIT":
        exits.append((parts[1], parts[2]))
    elif kind == "CONNECT_FAILURES":
        failures.append(line.strip())


def mean(values):
    return sum(values) / len(values)


def spread(values):
    return (max(values) - min(values)) / mean(values) * 100 if len(values) > 1 else 0.0


print("## Memory")
print("| configuration | RSS after start | RSS before | RSS with 10,000 idle connections | per connection |")
print("|---|---|---|---|---|")
for config in ORDER:
    if config not in idle:
        continue
    before = mean([v[0] for v in idle[config]])
    after = mean([v[1] for v in idle[config]])
    per_conn = mean([v[2] for v in idle[config]])
    started = f"{mean(start[config]):.1f} MB" if config in start else "-"
    print(f"| {config} | {started} | {before:.1f} MB | {after:.1f} MB | {per_conn:.1f} KB |")

names = ["rps", "p50", "p99", "p999", "max", "cpu", "req_per_cpu", "rss"]
for conns, depth in SCENARIOS:
    print(f"\n## {conns} connections, pipeline depth {depth}")
    print("| configuration | req/s (spread between rounds) | p50 µs | p99 µs | p99.9 µs | server cores | requests per CPU second | RSS at the end |")
    print("|---|---|---|---|---|---|---|---|")
    for config in ORDER:
        rows = results.get((config, conns, depth))
        if not rows:
            continue
        columns = {name: [row[i] for row in rows] for i, name in enumerate(names)}
        print(
            f"| {config} | {mean(columns['rps']):,.0f} (±{spread(columns['rps']) / 2:.0f}%) "
            f"| {mean(columns['p50']):.0f} | {mean(columns['p99']):.0f} | {mean(columns['p999']):.0f} "
            f"| {mean(columns['cpu']):.2f} | {mean(columns['req_per_cpu']):,.0f} | {mean(columns['rss']):.1f} MB |"
        )

if failures or exits:
    print("\n## Other")
    for line in failures:
        print("-", line)
    if exits:
        print("- exit codes:", ", ".join(f"{label}={code}" for label, code in exits))
