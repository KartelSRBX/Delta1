"""End-to-end smoke test against a running stack (``make up`` first).

Waits until data has flowed through Kafka -> processor -> ClickHouse and checks the
invariants that matter: every table receives rows, aggregates match raw facts, duplicates
never reach the warehouse, and malformed messages end up in the dead-letter table.
"""

from __future__ import annotations

import os
import sys
import time

import clickhouse_connect

TABLES = ("sessions", "deposits", "casino_spins", "sport_bets", "dead_letters")


def main() -> int:
    client = clickhouse_connect.get_client(
        host=os.environ.get("CLICKHOUSE_HOST", "localhost"),
        port=int(os.environ.get("CLICKHOUSE_PORT", 8123)),
        username=os.environ.get("CLICKHOUSE_USER", "delta1"),
        password=os.environ.get("CLICKHOUSE_PASSWORD", "delta1"),
        database="delta1",
    )
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

    ok = True
    print("rows:", counts)
    for name, passed, value in checks:
        ok &= passed
        print(f"{'PASS' if passed else 'FAIL'}  {name}  ({value})")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
