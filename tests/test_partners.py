import sys
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from delta1.partners import app as app_module
from delta1.partners.fx import BASE_RATES, MAX_WOBBLE, history, rates_at
from delta1.partners.reports import (
    PROVIDERS,
    DriftConfig,
    ReportRow,
    ReportWriter,
    apply_drift,
    completed_windows,
    report_name,
    to_csv,
    window_start,
)

sys.path.insert(0, str(Path(__file__).parents[1] / "airflow" / "dags"))
from delta1_reports import parse_report

T = datetime(2026, 11, 2, 13, 47, 31, tzinfo=UTC)


# ---------------------------------------------------------------------- FX
def test_rates_are_deterministic_per_hour_and_close_to_base():
    a, b = rates_at(T), rates_at(T.replace(minute=5))
    assert a == b
    assert a["EUR"] == 1.0
    for currency, base in BASE_RATES.items():
        assert abs(a[currency] - base) <= base * MAX_WOBBLE + 1e-9
    assert rates_at(T + timedelta(hours=1)) != a


def test_history_has_one_quote_per_hour():
    quotes = history(date(2026, 11, 2))
    assert len(quotes) == 24
    assert quotes[13]["rates"] == rates_at(T)


@pytest.fixture
def api(monkeypatch):
    monkeypatch.setattr(app_module, "FAILURE_RATE", 0.0)
    return TestClient(app_module.app)


def test_api_requires_key(api):
    assert api.get("/v1/fx/latest").status_code == 401
    assert api.get("/v1/fx/latest", headers={"X-API-Key": "wrong"}).status_code == 401


def test_api_returns_rates(api):
    resp = api.get("/v1/fx/latest", headers={"X-API-Key": app_module.API_KEY})
    assert resp.status_code == 200
    body = resp.json()
    assert body["base"] == "EUR"
    assert set(body["rates"]) == set(BASE_RATES)


def test_api_fails_like_a_real_vendor(api, monkeypatch):
    monkeypatch.setattr(app_module, "FAILURE_RATE", 1.0)
    resp = api.get("/v1/fx/latest", headers={"X-API-Key": app_module.API_KEY})
    assert resp.status_code == 503


# ---------------------------------------------------------------------- reports
def test_windows_are_aligned_and_completed():
    assert window_start(T, 10) == datetime(2026, 11, 2, 13, 40, tzinfo=UTC)
    starts = completed_windows(T, 10, lag_s=60, lookback=3)
    assert starts == [
        datetime(2026, 11, 2, 13, 30, tzinfo=UTC),
        datetime(2026, 11, 2, 13, 20, tzinfo=UTC),
        datetime(2026, 11, 2, 13, 10, tzinfo=UTC),
    ]
    # Within the lag after a window closes, that window is not "completed" yet.
    just_closed = datetime(2026, 11, 2, 13, 40, 30, tzinfo=UTC)
    assert completed_windows(just_closed, 10, lag_s=60, lookback=1)[0].minute == 20


def rows_for(provider: str, start: datetime, n: int = 5) -> list[ReportRow]:
    end = start + timedelta(minutes=10)
    return [
        ReportRow(
            start, end, provider, f"g{i}", "EUR", 100 + i, Decimal("250.00"), Decimal("240.10")
        )
        for i in range(n)
    ]


def test_drift_is_deterministic_and_bounded():
    start = window_start(T, 10)
    rows = rows_for("redriver", start)
    cfg = DriftConfig(drift_probability=1.0, missing_probability=1.0, max_drift=0.03)
    out1, injected1 = apply_drift(rows, "redriver", start, cfg)
    out2, injected2 = apply_drift(rows, "redriver", start, cfg)
    assert out1 == out2 and injected1 == injected2
    assert len(out1) == len(rows) - 1
    changed = [r for r in out1 if r not in rows]
    assert len(changed) == 1
    ratio = changed[0].bets / Decimal("250.00")
    assert Decimal("0.97") <= ratio <= Decimal("1.03") and ratio != 1


def test_no_drift_when_disabled():
    start = window_start(T, 10)
    rows = rows_for("novaplay", start)
    out, injected = apply_drift(rows, "novaplay", start, DriftConfig(0.0, 0.0))
    assert out == rows and injected == []


def test_csv_contract_between_writer_and_dag_parser():
    start = window_start(T, 10)
    rows = rows_for("starforge", start, 3)
    parsed = parse_report(to_csv(rows))
    assert [tuple(p.values()) for p in parsed] == [
        (r.window_start, r.window_end, r.provider, r.game_id, r.currency, r.rounds, r.bets, r.wins)
        for r in rows
    ]


def test_dag_parser_rejects_unknown_layout():
    with pytest.raises(ValueError):
        parse_report("provider,game,amount\nx,y,1\n")


class FakeClickHouse:
    def __init__(self, rows):
        self.rows = rows
        self.queries = 0

    def query(self, _sql, parameters):
        self.queries += 1

        class Result:
            result_rows = self.rows

        return Result()


def test_writer_writes_one_file_per_provider_once(tmp_path):
    start = window_start(T, 10)
    data = [("redriver", "rr-wild-west", "EUR", 10, Decimal("50.00"), Decimal("45.00"))]
    client = FakeClickHouse(data)
    writer = ReportWriter(client, tmp_path, window_minutes=10, drift=DriftConfig(0.0, 0.0))

    assert writer.write_window(start) == len(PROVIDERS)
    for provider in PROVIDERS:
        text = (tmp_path / report_name(provider, start)).read_text()
        rows = parse_report(text)
        assert all(r["provider"] == provider for r in rows)
        assert (len(rows) == 1) == (provider == "redriver")
    assert not list(tmp_path.rglob("*.tmp"))

    assert writer.write_window(start) == 0  # already written: no second query
    assert client.queries == 1
