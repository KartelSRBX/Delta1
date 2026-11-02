"""Deterministic FX quotes: a small hourly wobble around a base rate."""

from __future__ import annotations

import random
import zlib
from datetime import UTC, date, datetime, timedelta

# Units of currency per 1 EUR.
BASE_RATES: dict[str, float] = {"EUR": 1.0, "USD": 1.087, "RSD": 117.2}
MAX_WOBBLE = 0.01  # +-1% around the base


def hour_floor(ts: datetime) -> datetime:
    return ts.astimezone(UTC).replace(minute=0, second=0, microsecond=0)


def rates_at(ts: datetime) -> dict[str, float]:
    """Rates valid for the hour containing ``ts``. Same input, same output, in any process."""
    hour = hour_floor(ts)
    out: dict[str, float] = {}
    for currency, base in BASE_RATES.items():
        if currency == "EUR":
            out[currency] = 1.0
            continue
        seed = zlib.crc32(f"{currency}:{hour.isoformat()}".encode())
        wobble = (random.Random(seed).random() * 2 - 1) * MAX_WOBBLE
        out[currency] = round(base * (1 + wobble), 6)
    return out


def history(day: date) -> list[dict]:
    start = datetime(day.year, day.month, day.day, tzinfo=UTC)
    return [
        {
            "as_of": (start + timedelta(hours=h)).isoformat(),
            "rates": rates_at(start + timedelta(hours=h)),
        }
        for h in range(24)
    ]
