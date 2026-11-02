"""Event contract shared by the generator and the stream processor.

Every message on the ``igaming.events`` topic is a JSON object with an ``event_type``
discriminator. The processor validates each message against this contract; anything
that does not match goes to the dead-letter topic instead of the warehouse.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter

Money = Annotated[Decimal, Field(ge=0, max_digits=18, decimal_places=2)]
Currency = Annotated[str, Field(pattern=r"^[A-Z]{3}$")]


class BaseEvent(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    event_id: Annotated[str, Field(min_length=8, max_length=64)]
    ts: datetime
    brand: Annotated[str, Field(min_length=1, max_length=64)]
    player_id: Annotated[str, Field(min_length=1, max_length=64)]
    currency: Currency


class SessionStarted(BaseEvent):
    event_type: Literal["session_started"] = "session_started"
    session_id: str
    device: Literal["desktop", "mobile", "tablet"]
    country: Annotated[str, Field(pattern=r"^[A-Z]{2}$")]


class Deposit(BaseEvent):
    event_type: Literal["deposit"] = "deposit"
    payment_id: str
    amount: Annotated[Decimal, Field(gt=0, max_digits=18, decimal_places=2)]
    method: Literal["card", "bank_transfer", "e_wallet", "crypto"]
    is_first: bool


class CasinoSpin(BaseEvent):
    event_type: Literal["casino_spin"] = "casino_spin"
    session_id: str
    round_id: str
    provider: str
    game_id: str
    bet: Annotated[Decimal, Field(gt=0, max_digits=18, decimal_places=2)]
    win: Money


class SportBetPlaced(BaseEvent):
    event_type: Literal["sport_bet_placed"] = "sport_bet_placed"
    bet_id: str
    sport: str
    match_id: str
    market: str
    odds: Annotated[Decimal, Field(gt=1, max_digits=8, decimal_places=2)]
    stake: Annotated[Decimal, Field(gt=0, max_digits=18, decimal_places=2)]


class SportBetSettled(BaseEvent):
    event_type: Literal["sport_bet_settled"] = "sport_bet_settled"
    bet_id: str
    sport: str
    match_id: str
    market: str
    odds: Annotated[Decimal, Field(gt=1, max_digits=8, decimal_places=2)]
    stake: Annotated[Decimal, Field(gt=0, max_digits=18, decimal_places=2)]
    result: Literal["won", "lost", "void"]
    payout: Money
    placed_ts: datetime


Event = Annotated[
    SessionStarted | Deposit | CasinoSpin | SportBetPlaced | SportBetSettled,
    Field(discriminator="event_type"),
]

EVENT_ADAPTER: TypeAdapter[Event] = TypeAdapter(Event)

EVENT_TYPES: tuple[str, ...] = (
    "session_started",
    "deposit",
    "casino_spin",
    "sport_bet_placed",
    "sport_bet_settled",
)


def parse_event(raw: bytes | str) -> Event:
    """Parse and validate one raw message. Raises ``pydantic.ValidationError``."""
    return EVENT_ADAPTER.validate_json(raw)


def dump_event(event: BaseEvent) -> bytes:
    return event.model_dump_json().encode()
