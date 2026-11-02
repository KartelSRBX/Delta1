"""Reconcile game-provider reports with the platform's own records, per report window.

Each run covers exactly its data interval (one report window):

wait_for_reports (sensor, reschedule mode) -> load_reports (FTP -> ClickHouse, idempotent)
-> reconcile (FULL JOIN platform vs provider) -> summarize (fails loudly on large gaps)
"""

from __future__ import annotations

import ftplib
import io
from datetime import UTC, datetime, timedelta

from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.hooks.base import BaseHook
from airflow.sensors.base import PokeReturnValue

from delta1_common import DEFAULT_ARGS, REPORT_WINDOW_MINUTES, clickhouse, sql
from delta1_reports import parse_report

PROVIDERS = ("krakenlabs", "novaplay", "redriver", "starforge")
TOLERANCE = 0.001  # 0.1% relative difference on bets or wins counts as a mismatch
ALERT_SHARE = 0.25  # fail the run if more than a quarter of the rows do not reconcile
TS_FORMAT = "%Y%m%dT%H%M"


def _ftp() -> ftplib.FTP:
    conn = BaseHook.get_connection("partner_ftp")
    ftp = ftplib.FTP()
    ftp.connect(conn.host, conn.port or 21, timeout=15)
    ftp.login(conn.login, conn.password)
    return ftp


def _report_path(provider: str, start: datetime) -> str:
    return f"{provider}/{provider}_{start.strftime(TS_FORMAT)}.csv"


@dag(
    dag_id="delta1_provider_reconciliation",
    schedule=timedelta(minutes=REPORT_WINDOW_MINUTES),
    start_date=datetime(2026, 11, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=2,
    default_args=DEFAULT_ARGS,
    tags=["delta1", "partners", "reconciliation"],
    doc_md=__doc__,
)
def provider_reconciliation():
    @task.sensor(poke_interval=20, timeout=20 * 60, mode="reschedule")
    def wait_for_reports(data_interval_start=None) -> PokeReturnValue:
        missing = []
        ftp = _ftp()
        try:
            for provider in PROVIDERS:
                try:
                    names = {n.rsplit("/", 1)[-1] for n in ftp.nlst(provider)}
                except ftplib.error_perm:  # provider directory does not exist yet
                    names = set()
                if _report_path(provider, data_interval_start).rsplit("/", 1)[-1] not in names:
                    missing.append(provider)
        finally:
            ftp.quit()
        if missing:
            print(f"still waiting for: {missing}")
        return PokeReturnValue(is_done=not missing)

    @task
    def load_reports(data_interval_start=None) -> int:
        rows = []
        ftp = _ftp()
        try:
            for provider in PROVIDERS:
                path = _report_path(provider, data_interval_start)
                buf = io.BytesIO()
                ftp.retrbinary(f"RETR {path}", buf.write)
                for r in parse_report(buf.getvalue().decode()):
                    if r["window_start"] != data_interval_start:
                        raise AirflowFailException(f"{path} covers another window")
                    rows.append((*r.values(), path))
        finally:
            ftp.quit()
        if rows:
            clickhouse().insert(
                "provider_reports",
                rows,
                column_names=[
                    "window_start",
                    "window_end",
                    "provider",
                    "game_id",
                    "currency",
                    "rounds",
                    "bets",
                    "wins",
                    "source_file",
                ],
            )
        return len(rows)

    @task
    def reconcile(data_interval_start=None, data_interval_end=None) -> dict:
        client = clickhouse()
        params = {"start": data_interval_start, "end": data_interval_end, "tolerance": TOLERANCE}
        client.command(sql("reconcile.sql"), parameters=params)
        result = client.query(
            "SELECT status, count() FROM delta1.reconciliation FINAL "
            "WHERE window_start = {start:DateTime('UTC')} GROUP BY status",
            parameters=params,
        )
        return dict(result.result_rows)

    @task
    def summarize(counts: dict, data_interval_start=None) -> None:
        total = sum(counts.values())
        bad = total - counts.get("ok", 0)
        print(f"window {data_interval_start:%Y-%m-%d %H:%M}: {counts}")
        if total and bad / total > ALERT_SHARE:
            raise AirflowFailException(f"{bad}/{total} rows do not reconcile: {counts}")

    loaded = load_reports()
    counts = reconcile()
    wait_for_reports() >> loaded >> counts
    summarize(counts)


provider_reconciliation()
