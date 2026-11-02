"""Shared helpers for Delta1 DAGs: connections, SQL files, default task arguments."""

from __future__ import annotations

import os
from datetime import timedelta
from pathlib import Path

import clickhouse_connect
from airflow.hooks.base import BaseHook

SQL_DIR = Path(__file__).parent / "sql"

# Report window size, shared with the partner service that writes the reports.
REPORT_WINDOW_MINUTES = int(os.environ.get("REPORT_WINDOW_MINUTES", "10"))
# How often the daily marts are rebuilt (the whole day, each time).
MARTS_INTERVAL_MINUTES = int(os.environ.get("MARTS_INTERVAL_MINUTES", "15"))

DEFAULT_ARGS = {
    "owner": "data-platform",
    "retries": 3,
    "retry_delay": timedelta(seconds=20),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=5),
}


def clickhouse(conn_id: str = "clickhouse_default"):
    """ClickHouse client built from an Airflow connection (set via AIRFLOW_CONN_* env)."""
    conn = BaseHook.get_connection(conn_id)
    return clickhouse_connect.get_client(
        host=conn.host,
        port=conn.port or 8123,
        username=conn.login,
        password=conn.password,
        database=conn.schema or "delta1",
    )


def sql(name: str) -> str:
    return (SQL_DIR / name).read_text()
