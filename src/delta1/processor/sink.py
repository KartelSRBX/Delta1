"""ClickHouse writer with retries."""

from __future__ import annotations

import logging
import time

import clickhouse_connect
from clickhouse_connect.driver.client import Client

from delta1.config import ClickHouseSettings
from delta1.processor.pipeline import TABLE_COLUMNS, Batch

log = logging.getLogger(__name__)


def connect(cfg: ClickHouseSettings, timeout_s: float = 120.0) -> Client:
    deadline = time.monotonic() + timeout_s
    while True:
        try:
            client = clickhouse_connect.get_client(
                host=cfg.host,
                port=cfg.port,
                username=cfg.user,
                password=cfg.password,
                database=cfg.database,
                compress="lz4",
            )
            client.command("SELECT 1")
            return client
        except Exception as exc:
            if time.monotonic() > deadline:
                raise
            log.info("waiting for ClickHouse at %s:%s: %s", cfg.host, cfg.port, exc)
            time.sleep(2)


class ClickHouseSink:
    def __init__(self, client: Client, max_backoff_s: float = 30.0) -> None:
        self.client = client
        self.max_backoff_s = max_backoff_s

    def write(self, batch: Batch) -> dict[str, int]:
        """Insert every table of the batch. Blocks and retries until ClickHouse accepts it.

        Blocking is intentional: offsets are committed only after this returns, so a slow
        or down warehouse turns into consumer lag (visible on the dashboard), not data loss.
        """
        written: dict[str, int] = {}
        tables = dict(batch.rows)
        if batch.dead_letters:
            tables["dead_letters"] = [d.row() for d in batch.dead_letters]
        for table, rows in tables.items():
            if not rows:
                continue
            self._insert_with_retry(table, rows)
            written[table] = len(rows)
        return written

    def _insert_with_retry(self, table: str, rows: list[tuple]) -> None:
        backoff = 0.5
        while True:
            try:
                self.client.insert(table, rows, column_names=list(TABLE_COLUMNS[table]))
                return
            except Exception as exc:
                log.warning("insert into %s failed (%s), retrying in %.1fs", table, exc, backoff)
                time.sleep(backoff)
                backoff = min(backoff * 2, self.max_backoff_s)
