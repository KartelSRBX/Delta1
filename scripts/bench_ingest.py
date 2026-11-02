"""Ingest benchmark: how fast does Kafka -> processor -> ClickHouse drain a backlog?

For each batch size it restarts the processor, blasts a pre-generated burst of valid events
into Kafka as fast as librdkafka allows, and measures the time until every row is visible in
ClickHouse. It also counts the parts ClickHouse had to create, which is the cost that small
batches push onto the warehouse.

Run against a stack started with ``make up`` (the generator is paused during the run):

    python scripts/bench_ingest.py --events 300000 --runs 500x1,5000x1,20000x1,20000x3

Each run is BATCH_SIZE x processor replicas. Replicas share one consumer group, so Kafka
spreads the 6 partitions across them.
"""

from __future__ import annotations

import argparse
import os
import shlex
import subprocess
import sys
import threading
import time
from collections import Counter
from datetime import UTC, datetime, timedelta

import clickhouse_connect
from confluent_kafka import Producer

from delta1.events import dump_event
from delta1.generator.simulator import Simulator

COMPOSE = shlex.split(os.environ.get("COMPOSE", "docker compose"))
APPEND_ONLY = {"session_started": "sessions", "deposit": "deposits", "casino_spin": "casino_spins"}


def compose(*args: str, env: dict | None = None) -> None:
    subprocess.run(
        [*COMPOSE, *args],
        check=True,
        env={**os.environ, **(env or {})},
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def make_events(n: int, seed: int) -> tuple[list[tuple[bytes, bytes]], Counter]:
    sim = Simulator(players_per_brand=5000, seed=seed)
    start = datetime.now(UTC) - timedelta(minutes=30)
    out: list[tuple[bytes, bytes]] = []
    kinds: Counter = Counter()
    i = 0
    while len(out) < n:
        for event in sim.step(start + timedelta(milliseconds=i * 5)):
            out.append((event.player_id.encode(), dump_event(event)))
            kinds[event.event_type] += 1
        i += 1
    return out, kinds


def counts(client) -> dict[str, int]:
    return {t: client.command(f"SELECT count() FROM {t}") for t in APPEND_ONLY.values()}


def cpu_sampler(stop: threading.Event, samples: list[float]) -> None:
    while not stop.is_set():
        out = subprocess.run(
            ["docker", "stats", "--no-stream", "--format", "{{.Name}} {{.CPUPerc}}"],
            capture_output=True,
            text=True,
        ).stdout
        total = sum(
            float(line.rsplit(" ", 1)[1].rstrip("%"))
            for line in out.splitlines()
            if "processor" in line
        )
        samples.append(total)
        stop.wait(1)


def run(
    batch_size: int, replicas: int, events, expected: dict[str, int], client, bootstrap: str
) -> dict:
    compose(
        "up",
        "-d",
        "--no-deps",
        "--force-recreate",
        "--scale",
        f"processor={replicas}",
        "processor",
        env={"BATCH_SIZE": str(batch_size)},
    )
    time.sleep(10 + 2 * replicas)  # consumers join the group and finish any leftovers
    before = counts(client)
    t0 = time.perf_counter()
    since = datetime.now(UTC).replace(microsecond=0)

    producer = Producer(
        {
            "bootstrap.servers": bootstrap,
            "linger.ms": 50,
            "batch.size": 1_000_000,
            "compression.type": "lz4",
            "queue.buffering.max.messages": 2_000_000,
        }
    )
    for key, payload in events:
        while True:
            try:
                producer.produce("igaming.events", payload, key=key)
                break
            except BufferError:
                producer.poll(0.05)
    producer.flush(60)
    produced_s = time.perf_counter() - t0

    stop, cpu = threading.Event(), []
    threading.Thread(target=cpu_sampler, args=(stop, cpu), daemon=True).start()
    deadline = time.monotonic() + 900
    while True:
        now = counts(client)
        if all(now[t] - before[t] >= expected[t] for t in now):
            break
        if time.monotonic() > deadline:
            raise SystemExit(f"timed out waiting for rows: {now} vs {before} + {expected}")
        time.sleep(0.25)
    total_s = time.perf_counter() - t0
    stop.set()

    client.command("SYSTEM FLUSH LOGS")
    parts = client.command(
        "SELECT count() FROM system.part_log WHERE event_type = 'NewPart' "
        "AND database = 'delta1' AND table IN ('sessions', 'deposits', 'casino_spins', "
        "'sport_bets') AND event_time >= {since:DateTime}",
        parameters={"since": since},
    )
    return {
        "batch_size": batch_size,
        "replicas": replicas,
        "events": len(events),
        "produce_rate": len(events) / produced_s,
        "throughput": len(events) / total_s,
        "seconds": total_s,
        "parts": parts,
        "cpu": max(cpu) if cpu else float("nan"),
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--events", type=int, default=300_000)
    ap.add_argument(
        "--runs", default="500x1,5000x1,20000x1,20000x3", help="comma-separated BATCH_SIZExREPLICAS"
    )
    ap.add_argument("--bootstrap", default="localhost:29092")
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--out", help="write the Markdown table here as well")
    args = ap.parse_args()

    client = clickhouse_connect.get_client(
        host=os.environ.get("CLICKHOUSE_HOST", "localhost"),
        username=os.environ.get("CLICKHOUSE_USER", "delta1"),
        password=os.environ.get("CLICKHOUSE_PASSWORD", "delta1"),
        database="delta1",
    )
    print(f"generating {args.events:,} events ...", flush=True)
    base, kinds = make_events(args.events, args.seed)

    compose("stop", "generator")
    results = []
    try:
        plan = [tuple(int(v) for v in r.split("x")) for r in args.runs.split(",")]
        for i, (bs, replicas) in enumerate(plan):
            # Fresh event ids per run, otherwise the processor's dedup window drops them.
            events = [
                (k, p.replace(b'"event_id":"', f'"event_id":"r{i}'.encode(), 1)) for k, p in base
            ]
            expected = {APPEND_ONLY[k]: v for k, v in kinds.items() if k in APPEND_ONLY}
            label = f"batch {bs:>6} x {replicas}"
            print(f"{label}: running ...", flush=True)
            r = run(bs, replicas, events, expected, client, args.bootstrap)
            print(
                f"{label}: {r['throughput']:,.0f} events/s, {r['parts']} parts, "
                f"processor CPU peak {r['cpu']:.0f}%",
                flush=True,
            )
            results.append(r)
    finally:
        compose("up", "-d", "--no-deps", "--force-recreate", "--scale", "processor=1", "processor")
        compose("start", "generator")

    table = [
        "| Batch size | Replicas | Events | End-to-end (ev/s) | Drain time (s) "
        "| New parts | Processor CPU peak |",
        "| ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for r in results:
        table.append(
            f"| {r['batch_size']:,} | {r['replicas']} | {r['events']:,} "
            f"| {r['throughput']:,.0f} | {r['seconds']:.1f} | {r['parts']:,} | {r['cpu']:.0f}% |"
        )
    print("\n".join(table))
    if args.out:
        with open(args.out, "w") as f:
            f.write("\n".join(table) + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
