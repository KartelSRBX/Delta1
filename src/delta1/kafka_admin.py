"""Small helpers around the Kafka admin API."""

from __future__ import annotations

import logging
import time

from confluent_kafka.admin import AdminClient, NewTopic

log = logging.getLogger(__name__)


def ensure_topics(bootstrap: str, topics: dict[str, int], timeout_s: float = 60.0) -> None:
    """Create topics if they are missing. Waits for the broker to come up first."""
    admin = AdminClient({"bootstrap.servers": bootstrap})
    deadline = time.monotonic() + timeout_s
    while True:
        try:
            existing = set(admin.list_topics(timeout=5).topics)
            break
        except Exception as exc:  # broker not ready yet
            if time.monotonic() > deadline:
                raise
            log.info("waiting for Kafka at %s: %s", bootstrap, exc)
            time.sleep(2)

    missing = [
        NewTopic(name, num_partitions=partitions, replication_factor=1)
        for name, partitions in topics.items()
        if name not in existing
    ]
    if not missing:
        return
    for name, future in admin.create_topics(missing).items():
        try:
            future.result()
            log.info("created topic %s", name)
        except Exception as exc:  # another service may have created it concurrently
            if "TOPIC_ALREADY_EXISTS" not in str(exc):
                raise
