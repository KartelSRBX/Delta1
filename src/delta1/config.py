"""Environment-driven settings. Every knob has a sane default for ``docker compose up``."""

from __future__ import annotations

import os
from dataclasses import dataclass, field


def _env(name: str, default: str) -> str:
    return os.environ.get(name, default)


def _env_int(name: str, default: int) -> int:
    return int(os.environ.get(name, default))


def _env_float(name: str, default: float) -> float:
    return float(os.environ.get(name, default))


@dataclass(frozen=True)
class KafkaSettings:
    bootstrap_servers: str = field(
        default_factory=lambda: _env("KAFKA_BOOTSTRAP", "localhost:9092")
    )
    events_topic: str = field(default_factory=lambda: _env("EVENTS_TOPIC", "igaming.events"))
    dlq_topic: str = field(default_factory=lambda: _env("DLQ_TOPIC", "igaming.events.dlq"))
    partitions: int = field(default_factory=lambda: _env_int("EVENTS_PARTITIONS", 6))
    group_id: str = field(default_factory=lambda: _env("CONSUMER_GROUP", "delta1-processor"))


@dataclass(frozen=True)
class ClickHouseSettings:
    host: str = field(default_factory=lambda: _env("CLICKHOUSE_HOST", "localhost"))
    port: int = field(default_factory=lambda: _env_int("CLICKHOUSE_PORT", 8123))
    user: str = field(default_factory=lambda: _env("CLICKHOUSE_USER", "delta1"))
    password: str = field(default_factory=lambda: _env("CLICKHOUSE_PASSWORD", "delta1"))
    database: str = field(default_factory=lambda: _env("CLICKHOUSE_DB", "delta1"))


@dataclass(frozen=True)
class GeneratorSettings:
    events_per_second: float = field(default_factory=lambda: _env_float("EVENTS_PER_SECOND", 200))
    players_per_brand: int = field(default_factory=lambda: _env_int("PLAYERS_PER_BRAND", 2000))
    invalid_ratio: float = field(default_factory=lambda: _env_float("INVALID_RATIO", 0.005))
    duplicate_ratio: float = field(default_factory=lambda: _env_float("DUPLICATE_RATIO", 0.01))
    seed: int | None = field(
        default_factory=lambda: int(os.environ["SEED"]) if os.environ.get("SEED") else None
    )
    metrics_port: int = field(default_factory=lambda: _env_int("METRICS_PORT", 8000))


@dataclass(frozen=True)
class ProcessorSettings:
    batch_size: int = field(default_factory=lambda: _env_int("BATCH_SIZE", 5000))
    flush_interval_s: float = field(default_factory=lambda: _env_float("FLUSH_INTERVAL_S", 2.0))
    dedup_window: int = field(default_factory=lambda: _env_int("DEDUP_WINDOW", 500_000))
    metrics_port: int = field(default_factory=lambda: _env_int("METRICS_PORT", 8001))
