"""Pure processing core: raw bytes in, warehouse rows and dead letters out.

No Kafka or ClickHouse here, which keeps the logic unit-testable and lets the I/O layer
(``main.py``) decide about batching, retries and offset commits.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum

from pydantic import ValidationError

from delta1.events import (
    CasinoSpin,
    Deposit,
    SessionStarted,
    SportBetPlaced,
    SportBetSettled,
    parse_event,
)
from delta1.processor.dedup import Deduplicator

COMMON = ("event_id", "ts", "brand", "player_id", "currency")

# Column order for every target table. Must match clickhouse/init/01_schema.sql.
TABLE_COLUMNS: dict[str, tuple[str, ...]] = {
    "sessions": (*COMMON, "session_id", "device", "country"),
    "deposits": (*COMMON, "payment_id", "amount", "method", "is_first"),
    "casino_spins": (
        *COMMON,
        "session_id",
        "round_id",
        "provider",
        "game_id",
        "bet",
        "win",
    ),
    "sport_bets": (
        "bet_id",
        "brand",
        "player_id",
        "currency",
        "sport",
        "match_id",
        "market",
        "odds",
        "stake",
        "status",
        "payout",
        "placed_ts",
        "settled_ts",
        "updated_ts",
    ),
    "dead_letters": ("received_ts", "reason", "error", "payload"),
}


class Outcome(StrEnum):
    ACCEPTED = "accepted"
    DUPLICATE = "duplicate"
    INVALID = "invalid"


@dataclass(frozen=True)
class DeadLetter:
    received_ts: datetime
    reason: str
    error: str
    payload: bytes

    def row(self) -> tuple:
        return (
            self.received_ts,
            self.reason,
            self.error[:2000],
            self.payload[:10_000].decode("utf-8", errors="replace"),
        )


@dataclass
class Batch:
    rows: dict[str, list[tuple]] = field(default_factory=lambda: defaultdict(list))
    dead_letters: list[DeadLetter] = field(default_factory=list)
    accepted_by_type: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    duplicates: int = 0

    @property
    def size(self) -> int:
        return sum(len(r) for r in self.rows.values()) + len(self.dead_letters)

    def __bool__(self) -> bool:
        return self.size > 0 or self.duplicates > 0


def _classify(exc: ValidationError) -> str:
    errors = exc.errors()
    if not errors:
        return "invalid"
    kind = errors[0]["type"]
    if kind == "json_invalid":
        return "malformed_json"
    if kind in ("union_tag_invalid", "union_tag_not_found"):
        return "unknown_event_type"
    if kind == "missing":
        return "missing_field"
    return "schema_violation"


def to_row(event) -> tuple[str, tuple]:
    """Map a validated event to (table, row)."""
    common = (event.event_id, event.ts, event.brand, event.player_id, event.currency)
    match event:
        case SessionStarted():
            return "sessions", (*common, event.session_id, event.device, event.country)
        case Deposit():
            return "deposits", (
                *common,
                event.payment_id,
                event.amount,
                event.method,
                event.is_first,
            )
        case CasinoSpin():
            return "casino_spins", (
                *common,
                event.session_id,
                event.round_id,
                event.provider,
                event.game_id,
                event.bet,
                event.win,
            )
        case SportBetPlaced():
            return "sport_bets", (
                event.bet_id,
                event.brand,
                event.player_id,
                event.currency,
                event.sport,
                event.match_id,
                event.market,
                event.odds,
                event.stake,
                "open",
                0,
                event.ts,
                None,
                event.ts,
            )
        case SportBetSettled():
            return "sport_bets", (
                event.bet_id,
                event.brand,
                event.player_id,
                event.currency,
                event.sport,
                event.match_id,
                event.market,
                event.odds,
                event.stake,
                event.result,
                event.payout,
                event.placed_ts,
                event.ts,
                event.ts,
            )
    raise TypeError(f"unsupported event {type(event).__name__}")


class Pipeline:
    def __init__(self, dedup: Deduplicator) -> None:
        self.dedup = dedup
        self._batch = Batch()

    @property
    def pending(self) -> int:
        return self._batch.size

    def handle(self, raw: bytes, now: datetime | None = None) -> Outcome:
        try:
            event = parse_event(raw)
        except ValidationError as exc:
            self._batch.dead_letters.append(
                DeadLetter(now or datetime.now(UTC), _classify(exc), str(exc), raw)
            )
            return Outcome.INVALID

        if self.dedup.seen(event.event_id):
            self._batch.duplicates += 1
            return Outcome.DUPLICATE

        table, row = to_row(event)
        self._batch.rows[table].append(row)
        self._batch.accepted_by_type[event.event_type] += 1
        return Outcome.ACCEPTED

    def drain(self) -> Batch:
        batch, self._batch = self._batch, Batch()
        return batch
