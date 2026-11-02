"""Hourly FX rates from the partner REST API into ClickHouse.

fetch (retries on 5xx) -> validate (all currencies, positive, no wild jumps)
-> load history + latest -> reload the in-memory dictionary dashboards convert with.
"""

from __future__ import annotations

from datetime import UTC, datetime

import requests
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.hooks.base import BaseHook

from delta1_common import DEFAULT_ARGS, clickhouse

EXPECTED = {"EUR", "USD", "RSD"}
MAX_JUMP = 0.05  # reject a quote that moves more than 5% vs the current rate


@dag(
    dag_id="delta1_fx_rates",
    schedule="@hourly",
    start_date=datetime(2026, 11, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["delta1", "partners"],
    doc_md=__doc__,
)
def fx_rates():
    @task
    def fetch() -> dict:
        conn = BaseHook.get_connection("partner_api")
        url = f"http://{conn.host}:{conn.port}/v1/fx/latest"
        resp = requests.get(url, headers={"X-API-Key": conn.extra_dejson["api_key"]}, timeout=10)
        if resp.status_code == 401:
            raise AirflowFailException("partner rejected the API key; retrying will not help")
        resp.raise_for_status()  # 503 and friends -> task retry with backoff
        return resp.json()

    @task
    def validate(quote: dict) -> dict:
        rates = quote["rates"]
        missing = EXPECTED - rates.keys()
        if missing:
            raise AirflowFailException(f"quote is missing currencies: {sorted(missing)}")
        if any(float(v) <= 0 for v in rates.values()):
            raise AirflowFailException(f"non-positive rate in {rates}")

        client = clickhouse()
        current = dict(
            client.query(
                "SELECT currency, argMax(rate_to_eur, updated_at) FROM delta1.fx_rates "
                "GROUP BY currency"
            ).result_rows
        )
        for currency, units_per_eur in rates.items():
            new = 1 / float(units_per_eur)
            old = current.get(currency)
            if old and abs(new - old) / old > MAX_JUMP:
                raise AirflowFailException(
                    f"{currency} moved {abs(new - old) / old:.1%} in one hour; refusing to load"
                )
        return quote

    @task
    def load(quote: dict) -> int:
        as_of = datetime.fromisoformat(quote["as_of"])
        rows = [(c, 1 / float(v)) for c, v in sorted(quote["rates"].items())]
        client = clickhouse()
        client.insert(
            "fx_rates_history",
            [(as_of, c, r, "partner_api") for c, r in rows],
            column_names=["as_of", "currency", "rate_to_eur", "source"],
        )
        client.insert(
            "fx_rates",
            [(c, r, as_of) for c, r in rows],
            column_names=["currency", "rate_to_eur", "updated_at"],
        )
        return len(rows)

    @task
    def reload_dictionary() -> None:
        clickhouse().command("SYSTEM RELOAD DICTIONARY delta1.fx_dict")

    load(validate(fetch())) >> reload_dictionary()


fx_rates()
