"""Processor service: Kafka -> validate / dedup -> ClickHouse, with a DLQ topic.

Delivery semantics: at-least-once from Kafka, offsets committed only after the batch is in
ClickHouse; the dedup window removes replays, so the warehouse sees each event once in
normal operation.
"""

from __future__ import annotations

import logging
import signal
import time

from confluent_kafka import Consumer, KafkaError, Producer, TopicPartition
from prometheus_client import Counter, Gauge, Histogram, start_http_server

from delta1.config import ClickHouseSettings, KafkaSettings, ProcessorSettings
from delta1.kafka_admin import ensure_topics
from delta1.processor.dedup import Deduplicator
from delta1.processor.pipeline import Batch, Pipeline
from delta1.processor.sink import ClickHouseSink, connect

log = logging.getLogger("delta1.processor")

CONSUMED = Counter("delta1_processor_consumed_total", "Messages consumed from Kafka")
ACCEPTED = Counter("delta1_processor_accepted_total", "Valid unique events", ["event_type"])
DUPLICATES = Counter("delta1_processor_duplicates_total", "Duplicate events dropped")
INVALID = Counter("delta1_processor_invalid_total", "Messages sent to the DLQ", ["reason"])
ROWS = Counter("delta1_processor_rows_written_total", "Rows written to ClickHouse", ["table"])
FLUSH_SECONDS = Histogram(
    "delta1_processor_flush_seconds",
    "Time to write one batch to ClickHouse",
    buckets=(0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10),
)
BATCH_ROWS = Histogram(
    "delta1_processor_batch_rows",
    "Rows per flushed batch",
    buckets=(10, 50, 100, 500, 1000, 2500, 5000, 10000, 25000),
)
LAG = Gauge("delta1_processor_consumer_lag", "Messages behind the log end", ["partition"])
DEDUP_SIZE = Gauge("delta1_processor_dedup_window_size", "Event ids held in the dedup window")


class Processor:
    def __init__(self) -> None:
        self.kafka = KafkaSettings()
        self.cfg = ProcessorSettings()
        ensure_topics(
            self.kafka.bootstrap_servers,
            {self.kafka.events_topic: self.kafka.partitions, self.kafka.dlq_topic: 1},
        )
        self.sink = ClickHouseSink(connect(ClickHouseSettings()))
        self.pipeline = Pipeline(Deduplicator(self.cfg.dedup_window))
        self.consumer = Consumer(
            {
                "bootstrap.servers": self.kafka.bootstrap_servers,
                "group.id": self.kafka.group_id,
                "enable.auto.commit": False,
                "auto.offset.reset": "earliest",
                "fetch.min.bytes": 64 * 1024,
                "fetch.wait.max.ms": 200,
            }
        )
        self.dlq = Producer({"bootstrap.servers": self.kafka.bootstrap_servers, "acks": "all"})
        self.running = True
        self._dirty = False  # consumed messages whose offsets are not committed yet
        self._last_flush = time.monotonic()
        self._last_lag = 0.0

    # ----------------------------------------------------------------- lifecycle
    def stop(self, *_: object) -> None:
        self.running = False

    def run(self) -> None:
        self.consumer.subscribe([self.kafka.events_topic], on_revoke=self._on_revoke)
        log.info("consuming %s as group %s", self.kafka.events_topic, self.kafka.group_id)
        try:
            while self.running:
                self._poll()
                if self._should_flush():
                    self.flush()
                self._report_lag()
        finally:
            self.flush()
            self.consumer.close()
            self.dlq.flush(10)

    # ----------------------------------------------------------------- internals
    def _poll(self) -> None:
        messages = self.consumer.consume(num_messages=1000, timeout=0.5)
        for msg in messages:
            err = msg.error()
            if err is not None:
                if err.code() != KafkaError._PARTITION_EOF:
                    log.error("consumer error: %s", err)
                continue
            CONSUMED.inc()
            self._dirty = True
            self.pipeline.handle(msg.value())

    def _should_flush(self) -> bool:
        if self.pipeline.pending >= self.cfg.batch_size:
            return True
        due = time.monotonic() - self._last_flush >= self.cfg.flush_interval_s
        return due and self._dirty

    def flush(self) -> None:
        batch: Batch = self.pipeline.drain()
        self._last_flush = time.monotonic()
        if not self._dirty and not batch:
            return
        for dead in batch.dead_letters:
            self.dlq.produce(
                self.kafka.dlq_topic, dead.payload, headers={"reason": dead.reason.encode()}
            )
            INVALID.labels(dead.reason).inc()
        self.dlq.flush(10)

        started = time.perf_counter()
        written = self.sink.write(batch)
        FLUSH_SECONDS.observe(time.perf_counter() - started)
        BATCH_ROWS.observe(sum(written.values()))
        for table, n in written.items():
            ROWS.labels(table).inc(n)
        for event_type, n in batch.accepted_by_type.items():
            ACCEPTED.labels(event_type).inc(n)
        DUPLICATES.inc(batch.duplicates)
        DEDUP_SIZE.set(len(self.pipeline.dedup))

        if self._dirty:
            try:
                self.consumer.commit(asynchronous=False)
            except Exception as exc:  # nothing to commit after a rebalance
                if "NO_OFFSET" not in str(exc):
                    raise
            self._dirty = False

    def _on_revoke(self, _consumer: Consumer, _partitions: list[TopicPartition]) -> None:
        log.info("partitions revoked, flushing before giving them up")
        self.flush()

    def _report_lag(self) -> None:
        now = time.monotonic()
        if now - self._last_lag < 5:
            return
        self._last_lag = now
        try:
            assigned = self.consumer.assignment()
            if not assigned:
                return
            for tp in self.consumer.position(assigned):
                _low, high = self.consumer.get_watermark_offsets(tp, timeout=2, cached=False)
                lag = high - tp.offset if tp.offset >= 0 else high
                LAG.labels(str(tp.partition)).set(max(lag, 0))
        except Exception as exc:
            log.debug("lag probe failed: %s", exc)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    start_http_server(ProcessorSettings().metrics_port)
    processor = Processor()
    signal.signal(signal.SIGTERM, processor.stop)
    signal.signal(signal.SIGINT, processor.stop)
    processor.run()


if __name__ == "__main__":
    main()
