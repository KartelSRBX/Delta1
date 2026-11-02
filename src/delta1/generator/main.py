"""Generator service: runs the simulator and publishes events to Kafka at a steady rate.

It deliberately injects a small share of duplicates (at-least-once producers do that in
real life) and malformed messages, so the processor's dedup and DLQ paths are exercised.
"""

from __future__ import annotations

import logging
import random
import signal
import time
from datetime import UTC, datetime

from confluent_kafka import Producer
from prometheus_client import Counter, Gauge, start_http_server

from delta1.config import GeneratorSettings, KafkaSettings
from delta1.events import dump_event
from delta1.generator.simulator import Simulator, corrupt
from delta1.kafka_admin import ensure_topics

log = logging.getLogger("delta1.generator")

PRODUCED = Counter("delta1_generator_events_total", "Events produced", ["event_type"])
INJECTED = Counter("delta1_generator_injected_total", "Injected faults", ["kind"])
PENDING = Gauge("delta1_generator_pending_sport_bets", "Sport bets waiting for settlement")


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    kafka = KafkaSettings()
    cfg = GeneratorSettings()
    ensure_topics(
        kafka.bootstrap_servers, {kafka.events_topic: kafka.partitions, kafka.dlq_topic: 1}
    )
    start_http_server(cfg.metrics_port)

    producer = Producer(
        {
            "bootstrap.servers": kafka.bootstrap_servers,
            "linger.ms": 20,
            "compression.type": "zstd",
            "enable.idempotence": True,
            "acks": "all",
        }
    )
    sim = Simulator(players_per_brand=cfg.players_per_brand, seed=cfg.seed)
    fault_rng = random.Random(cfg.seed)

    running = True

    def stop(*_: object) -> None:
        nonlocal running
        running = False

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    log.info(
        "producing ~%.0f events/s to %s (%d players)",
        cfg.events_per_second,
        kafka.events_topic,
        len(sim.players),
    )
    interval = 1.0 / cfg.events_per_second
    next_tick = time.monotonic()
    while running:
        for event in sim.step(datetime.now(UTC)):
            payload = dump_event(event)
            key = event.player_id.encode()
            if fault_rng.random() < cfg.invalid_ratio:
                payload, kind = corrupt(payload, fault_rng)
                INJECTED.labels(kind).inc()
            producer.produce(kafka.events_topic, payload, key=key)
            PRODUCED.labels(event.event_type).inc()
            if fault_rng.random() < cfg.duplicate_ratio:
                producer.produce(kafka.events_topic, payload, key=key)
                INJECTED.labels("duplicate").inc()
        producer.poll(0)
        PENDING.set(sim.pending_bets)

        next_tick += interval
        delay = next_tick - time.monotonic()
        if delay > 0:
            time.sleep(delay)
        elif delay < -1:  # fell behind by more than a second: do not try to catch up
            next_tick = time.monotonic()

    log.info("flushing producer")
    producer.flush(10)


if __name__ == "__main__":
    main()
