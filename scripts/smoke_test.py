"""End-to-end smoke test against a running stack (``make up`` first).

Waits until data has flowed through Kafka -> processor -> ClickHouse and checks the
invariants that matter: every table receives rows, aggregates match raw facts, duplicates
never reach the warehouse, and malformed messages end up in the dead-letter table.
"""

from __future__ import annotations

import argparse
import os
import sys
import time

import clickhouse_connect

TABLES = ("sessions", "deposits", "casino_spins", "sport_bets", "dead_letters")


def client_from_env():
    return clickhouse_connect.get_client(
        host=os.environ.get("CLICKHOUSE_HOST", "localhost"),
        port=int(os.environ.get("CLICKHOUSE_PORT", 8123)),
        username=os.environ.get("CLICKHOUSE_USER", "delta1"),
        password=os.environ.get("CLICKHOUSE_PASSWORD", "delta1"),
        database="delta1",
    )


def report(checks: list[tuple[str, bool, object]]) -> int:
    ok = True
    for name, passed, value in checks:
        ok &= passed
        print(f"{'PASS' if passed else 'FAIL'}  {name}  ({value})")
    return 0 if ok else 1


def wait_for(client, queries: dict[str, str], timeout_s: float) -> dict[str, int]:
    deadline = time.monotonic() + timeout_s
    counts: dict[str, int] = {}
    while time.monotonic() < deadline:
        counts = {name: client.command(q) for name, q in queries.items()}
        if all(counts.values()):
            return counts
        print("waiting for data:", counts, flush=True)
        time.sleep(5)
    raise SystemExit(f"FAIL: still empty after {timeout_s:.0f}s: {counts}")


def streaming(client) -> int:
    deadline = time.monotonic() + float(os.environ.get("SMOKE_TIMEOUT_S", 180))
    counts: dict[str, int] = {}
    while time.monotonic() < deadline:
        counts = {t: client.command(f"SELECT count() FROM {t}") for t in TABLES}
        settled = client.command("SELECT count() FROM sport_bets WHERE status != 'open'")
        if all(counts.values()) and settled:
            break
        print("waiting for data:", counts, flush=True)
        time.sleep(5)
    else:
        print("FAIL: tables still empty after timeout:", counts)
        return 1

    checks: list[tuple[str, bool, object]] = []

    dupes = client.command("SELECT count() - uniqExact(event_id) FROM casino_spins")
    checks.append(("no duplicate spins in the warehouse", dupes == 0, dupes))

    raw, agg = client.query(
        """SELECT
            (SELECT sum(bet) FROM casino_spins),
            (SELECT sum(turnover) FROM casino_ggr_1m)"""
    ).result_rows[0]
    checks.append(("casino MV turnover equals raw turnover", raw == agg, (raw, agg)))

    bad_settle = client.command(
        "SELECT count() FROM sport_bets WHERE status = 'lost' AND payout != 0"
    )
    checks.append(("lost bets pay nothing", bad_settle == 0, bad_settle))

    reasons = client.command("SELECT uniqExact(reason) FROM dead_letters")
    checks.append(("dead letters are classified", reasons >= 2, reasons))

    fx = client.command("SELECT dictGet('delta1.fx_dict', 'rate_to_eur', tuple('RSD'))")
    checks.append(("FX dictionary is loaded", 0 < float(fx) < 1, fx))

    print("rows:", counts)
    return report(checks)


def batch(client) -> int:
    """Checks for the Airflow side: FX loads, reconciliation and daily marts."""
    counts = wait_for(
        client,
        {
            "fx_rates_history": "SELECT count() FROM fx_rates_history",
            "provider_reports": "SELECT count() FROM provider_reports",
            "reconciliation": "SELECT count() FROM reconciliation",
            "player_daily": "SELECT count() FROM player_daily",
            "provider_daily": "SELECT count() FROM provider_daily",
        },
        float(os.environ.get("SMOKE_TIMEOUT_S", 420)),
    )
    print("rows:", counts)
    checks: list[tuple[str, bool, object]] = []

    usd = float(client.command("SELECT dictGet('delta1.fx_dict', 'rate_to_eur', tuple('USD'))"))
    checks.append(("FX dictionary serves the API rate, not the seed", usd != 0.92, usd))

    statuses = {
        r[0] for r in client.query("SELECT DISTINCT status FROM reconciliation").result_rows
    }
    allowed = {"ok", "mismatch", "missing_in_report", "missing_on_platform"}
    checks.append(("reconciliation statuses are known", statuses <= allowed, statuses))

    match = float(
        client.command("SELECT countIf(status = 'ok') / count() FROM reconciliation FINAL")
    )
    checks.append(("most rows reconcile (drift is injected rarely)", match >= 0.8, match))

    mart, raw = client.query(
        """SELECT
            (SELECT sum(casino_bets) FROM player_daily WHERE day = today()),
            (SELECT sum(bet) FROM casino_spins WHERE toDate(ts) = today())"""
    ).result_rows[0]
    checks.append(("player mart is a snapshot no larger than raw", 0 < mart <= raw, (mart, raw)))
    return report(checks)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch", action="store_true", help="check the Airflow side too")
    args = parser.parse_args()
    client = client_from_env()
    code = streaming(client)
    if args.batch:
        code |= batch(client)
    return code


if __name__ == "__main__":
    sys.exit(main())
