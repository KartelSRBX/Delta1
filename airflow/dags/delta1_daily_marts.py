"""Daily player and provider marts, rebuilt idempotently with write-audit-publish.

Runs every MARTS_INTERVAL_MINUTES (15 by default) and rebuilds *the whole day* the run
belongs to, so intraday numbers stay fresh and re-running any past day gives the same result:

    build staging -> audit (row counts, totals match the raw facts) -> REPLACE PARTITION

A failed audit leaves yesterday's published numbers untouched.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

from airflow.decorators import dag, task, task_group
from airflow.exceptions import AirflowFailException

from delta1_common import DEFAULT_ARGS, MARTS_INTERVAL_MINUTES, clickhouse, sql

# Facts of one day, up to the snapshot cutoff. Parameters are bound server-side.
DAY_FILTER = "toDate(ts) = {day:Date} AND ts < {cutoff:DateTime64(3, 'UTC')}"

# mart -> (sql file, audits: (description, mart expression, raw-facts query))
MARTS = {
    "player_daily": (
        "player_daily.sql",
        [
            (
                "casino bets",
                "sum(casino_bets)",
                "SELECT sum(bet) FROM delta1.casino_spins WHERE " + DAY_FILTER,
            ),
            (
                "deposits",
                "sum(deposit_amount)",
                "SELECT sum(amount) FROM delta1.deposits WHERE " + DAY_FILTER,
            ),
        ],
    ),
    "provider_daily": (
        "provider_daily.sql",
        [
            (
                "rounds",
                "sum(rounds)",
                "SELECT count() FROM delta1.casino_spins WHERE " + DAY_FILTER,
            ),
            (
                "turnover",
                "sum(turnover)",
                "SELECT sum(bet) FROM delta1.casino_spins WHERE " + DAY_FILTER,
            ),
        ],
    ),
}


def target_day(data_interval_end: datetime) -> date:
    # A run whose interval ends exactly at midnight closes out the previous day.
    return (data_interval_end - timedelta(microseconds=1)).date()


@dag(
    dag_id="delta1_daily_marts",
    schedule=timedelta(minutes=MARTS_INTERVAL_MINUTES),
    start_date=datetime(2026, 11, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["delta1", "marts"],
    doc_md=__doc__,
)
def daily_marts():
    for mart, (sql_file, audits) in MARTS.items():

        @task_group(group_id=mart)
        def build_mart(mart=mart, sql_file=sql_file, audits=audits):
            @task
            def build(data_interval_end=None) -> dict:
                day = target_day(data_interval_end)
                # Freeze the snapshot at the end of the run's interval (or of the day), so
                # facts arriving while the run is in progress cannot fail the audit.
                end_of_day = datetime.combine(day + timedelta(days=1), datetime.min.time(), UTC)
                cutoff = min(data_interval_end, end_of_day)
                client = clickhouse()
                client.command(f"TRUNCATE TABLE delta1.{mart}_staging")
                client.command(sql(sql_file), parameters={"day": day, "cutoff": cutoff})
                return {"day": day.isoformat(), "cutoff": cutoff.isoformat()}

            @task
            def audit(snapshot: dict) -> None:
                client = clickhouse()
                day = snapshot["day"]
                params = {
                    "day": date.fromisoformat(day),
                    "cutoff": datetime.fromisoformat(snapshot["cutoff"]),
                }
                rows = client.command(f"SELECT count() FROM delta1.{mart}_staging")
                raw_rows = client.command(
                    "SELECT count() FROM delta1.casino_spins WHERE " + DAY_FILTER,
                    parameters=params,
                )
                if raw_rows and not rows:
                    raise AirflowFailException(f"{mart} staging is empty for {day}")
                for name, mart_expr, raw_sql in audits:
                    got = client.command(f"SELECT {mart_expr} FROM delta1.{mart}_staging")
                    want = client.command(raw_sql, parameters=params)
                    if str(got) != str(want):
                        raise AirflowFailException(
                            f"{mart}.{name} for {day}: mart={got} raw={want}"
                        )
                print(f"{mart} {day}: {rows} rows, audits passed")

            @task
            def publish(snapshot: dict) -> None:
                partition = date.fromisoformat(snapshot["day"]).strftime("%Y%m%d")
                clickhouse().command(
                    f"ALTER TABLE delta1.{mart} REPLACE PARTITION ID '{partition}' "
                    f"FROM delta1.{mart}_staging"
                )

            snapshot = build()
            audit(snapshot) >> publish(snapshot)

        build_mart()


daily_marts()
