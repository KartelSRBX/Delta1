"""Parser for provider report CSVs.

The Airflow image does not install the delta1 package, so the DAGs carry their own parser.
tests/test_partners.py checks it against the writer in delta1.partners.reports, so the two
sides of this file contract cannot drift apart.
"""

from __future__ import annotations

import csv
import io
from datetime import datetime
from decimal import Decimal

COLUMNS = (
    "window_start",
    "window_end",
    "provider",
    "game_id",
    "currency",
    "rounds",
    "bets",
    "wins",
)


def parse_report(text: str) -> list[dict]:
    reader = csv.DictReader(io.StringIO(text))
    if tuple(reader.fieldnames or ()) != COLUMNS:
        raise ValueError(f"unexpected header: {reader.fieldnames}")
    rows = []
    for r in reader:
        rows.append(
            {
                "window_start": datetime.fromisoformat(r["window_start"]),
                "window_end": datetime.fromisoformat(r["window_end"]),
                "provider": r["provider"],
                "game_id": r["game_id"],
                "currency": r["currency"],
                "rounds": int(r["rounds"]),
                "bets": Decimal(r["bets"]),
                "wins": Decimal(r["wins"]),
            }
        )
    return rows
