"""Micro-benchmark of the processor's pure pipeline (parse, validate, dedup, to row).

No Kafka or ClickHouse involved, so it isolates the per-event Python cost that bounds a
single processor replica. ``--profile`` prints the hottest functions.

    python scripts/bench_pipeline.py --events 200000 --profile
"""

from __future__ import annotations

import argparse
import cProfile
import pstats
import sys
import time
from datetime import UTC, datetime, timedelta

from delta1.events import dump_event
from delta1.generator.simulator import Simulator
from delta1.processor.dedup import Deduplicator
from delta1.processor.pipeline import Pipeline


def payloads(n: int, seed: int) -> list[bytes]:
    sim = Simulator(players_per_brand=2000, seed=seed)
    start = datetime(2026, 11, 1, tzinfo=UTC)
    out: list[bytes] = []
    i = 0
    while len(out) < n:
        out.extend(dump_event(e) for e in sim.step(start + timedelta(milliseconds=i)))
        i += 1
    return out[:n]


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--events", type=int, default=200_000)
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--repeat", type=int, default=3, help="report the best of N runs")
    ap.add_argument("--profile", action="store_true")
    args = ap.parse_args()

    data = payloads(args.events, args.seed)
    best = float("inf")
    for _ in range(args.repeat):
        pipeline = Pipeline(Deduplicator(len(data)))
        t0 = time.perf_counter()
        for raw in data:
            pipeline.handle(raw)
        best = min(best, time.perf_counter() - t0)
    print(
        f"{len(data) / best:,.0f} events/s per core ({best * 1e6 / len(data):.1f} µs/event), "
        f"best of {args.repeat}"
    )

    if args.profile:
        pipeline = Pipeline(Deduplicator(len(data)))
        prof = cProfile.Profile()
        prof.runcall(lambda: [pipeline.handle(raw) for raw in data])
        pstats.Stats(prof).sort_stats("tottime").print_stats(10)
    return 0


if __name__ == "__main__":
    sys.exit(main())
