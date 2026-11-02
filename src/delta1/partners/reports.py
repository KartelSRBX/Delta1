"""Game-provider reports.

Every provider sends one CSV per reporting window with what *its* servers recorded per game
and currency: rounds, bets and wins. The writer derives those numbers from the platform's
own data and then perturbs a few of them, because in real life provider reports drift:
rounding, late rounds, timezone cut-offs, plain bugs. Finding those mismatches is the job of
the reconciliation DAG.
"""

from __future__ import annotations

import csv
import io
import logging
import random
import time
import zlib
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

from delta1.generator.catalog import GAMES

log = logging.getLogger(__name__)

PROVIDERS: tuple[str, ...] = tuple(sorted({g.provider for g in GAMES}))
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
TS_FORMAT = "%Y%m%dT%H%M"


@dataclass(frozen=True)
class ReportRow:
    window_start: datetime
    window_end: datetime
    provider: str
    game_id: str
    currency: str
    rounds: int
    bets: Decimal
    wins: Decimal


@dataclass(frozen=True)
class DriftConfig:
    drift_probability: float = 0.15  # chance a provider's window has one drifted row
    missing_probability: float = 0.05  # chance a provider's window omits one row
    max_drift: float = 0.03


def window_start(ts: datetime, minutes: int) -> datetime:
    ts = ts.astimezone(UTC).replace(second=0, microsecond=0)
    return ts - timedelta(minutes=ts.minute % minutes)


def completed_windows(now: datetime, minutes: int, lag_s: int, lookback: int) -> list[datetime]:
    """Starts of the last ``lookback`` windows that closed at least ``lag_s`` seconds ago."""
    latest = window_start(now - timedelta(seconds=lag_s), minutes) - timedelta(minutes=minutes)
    return [latest - timedelta(minutes=minutes * i) for i in range(lookback)]


def report_name(provider: str, start: datetime) -> str:
    return f"{provider}/{provider}_{start.strftime(TS_FORMAT)}.csv"


def apply_drift(rows: list[ReportRow], provider: str, start: datetime, cfg: DriftConfig):
    """Perturb a provider's rows for one window. Deterministic per (provider, window)."""
    rng = random.Random(zlib.crc32(f"{provider}:{start.isoformat()}".encode()))
    rows = list(rows)
    injected: list[str] = []
    if rows and rng.random() < cfg.drift_probability:
        i = rng.randrange(len(rows))
        factor = Decimal(str(round(1 + rng.choice((-1, 1)) * rng.uniform(0.005, cfg.max_drift), 4)))
        row = rows[i]
        rows[i] = replace(
            row,
            bets=(row.bets * factor).quantize(Decimal("0.01")),
            wins=(row.wins * factor).quantize(Decimal("0.01")),
        )
        injected.append(f"drift {row.game_id}/{row.currency} x{factor}")
    if len(rows) > 1 and rng.random() < cfg.missing_probability:
        dropped = rows.pop(rng.randrange(len(rows)))
        injected.append(f"missing {dropped.game_id}/{dropped.currency}")
    return rows, injected


def to_csv(rows: list[ReportRow]) -> str:
    buf = io.StringIO()
    writer = csv.writer(buf, lineterminator="\n")
    writer.writerow(COLUMNS)
    for r in rows:
        writer.writerow(
            (
                r.window_start.isoformat(),
                r.window_end.isoformat(),
                r.provider,
                r.game_id,
                r.currency,
                r.rounds,
                f"{r.bets:.2f}",
                f"{r.wins:.2f}",
            )
        )
    return buf.getvalue()


class ReportWriter:
    """Writes one report per provider for every completed window, atomically."""

    def __init__(
        self,
        client,
        root: Path,
        window_minutes: int,
        lag_s: int = 60,
        drift: DriftConfig | None = None,
    ) -> None:
        self.client = client
        self.root = root
        self.window_minutes = window_minutes
        self.lag_s = lag_s
        self.drift = drift or DriftConfig()

    def platform_rows(self, start: datetime, end: datetime) -> list[ReportRow]:
        result = self.client.query(
            """
            SELECT provider, game_id, currency,
                   count() AS rounds, sum(bet) AS bets, sum(win) AS wins
            FROM delta1.casino_spins
            WHERE ts >= {start:DateTime64(3, 'UTC')} AND ts < {end:DateTime64(3, 'UTC')}
            GROUP BY provider, game_id, currency
            ORDER BY provider, game_id, currency
            """,
            parameters={"start": start, "end": end},
        )
        return [
            ReportRow(start, end, provider, game_id, currency, int(rounds), bets, wins)
            for provider, game_id, currency, rounds, bets, wins in result.result_rows
        ]

    def write_window(self, start: datetime) -> int:
        end = start + timedelta(minutes=self.window_minutes)
        pending = [p for p in PROVIDERS if not (self.root / report_name(p, start)).exists()]
        if not pending:
            return 0
        rows = self.platform_rows(start, end)
        for provider in pending:
            own = [r for r in rows if r.provider == provider]
            own, injected = apply_drift(own, provider, start, self.drift)
            path = self.root / report_name(provider, start)
            path.parent.mkdir(parents=True, exist_ok=True)
            tmp = path.with_suffix(".tmp")
            tmp.write_text(to_csv(own))
            tmp.rename(path)  # readers never see a half-written file
            log.info(
                "wrote %s (%d rows%s)",
                path.name,
                len(own),
                f"; injected {', '.join(injected)}" if injected else "",
            )
        return len(pending)

    def run_forever(self, poll_s: float = 15, lookback: int = 6) -> None:
        while True:
            try:
                now = datetime.now(UTC)
                for start in reversed(
                    completed_windows(now, self.window_minutes, self.lag_s, lookback)
                ):
                    self.write_window(start)
            except Exception:
                log.exception("report writer failed; retrying")
            time.sleep(poll_s)
