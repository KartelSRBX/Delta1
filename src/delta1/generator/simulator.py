"""Player-behaviour simulator.

Produces a realistic-looking stream of platform events:

* players belong to a brand and have a persona (casual / regular / high roller) that sets
  their stake size and how often they act;
* casino spins pay out according to each game's theoretical RTP and volatility, so the
  long-run actual RTP converges to the catalog value;
* sportsbook bets are placed now and settled later with a bookmaker margin, which is what
  creates the sportsbook GGR;
* deposits and sessions give the warehouse payments and activity data.

The simulator is pure (no I/O) and fully deterministic for a given seed.
"""

from __future__ import annotations

import heapq
import json
import math
import random
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from decimal import ROUND_HALF_UP, Decimal

from delta1.events import (
    BaseEvent,
    CasinoSpin,
    Deposit,
    SessionStarted,
    SportBetPlaced,
    SportBetSettled,
)
from delta1.generator.catalog import (
    BRANDS,
    CURRENCY_SCALE,
    GAMES,
    HIT_RATE,
    SPORTS,
    Brand,
    Game,
)

CENT = Decimal("0.01")
SPORT_MARGIN = 0.06
VOID_RATE = 0.02
NEW_SESSION_RATE = 0.03  # share of actions that are a new login
SESSION_END_RATE = 0.02  # chance an action is the last one of a session (~50 actions)
MIN_ONLINE = 20
FIRST_DEPOSIT_RATE = 0.6  # chance a not-yet-depositing player deposits on login


@dataclass(frozen=True)
class Persona:
    name: str
    weight: float  # share of the player base
    activity: float  # relative chance of acting on a tick
    stake_mean_eur: float


PERSONAS: tuple[Persona, ...] = (
    Persona("casual", 0.70, 1.0, 0.6),
    Persona("regular", 0.25, 3.0, 2.5),
    Persona("high_roller", 0.05, 6.0, 30.0),
)


@dataclass
class Player:
    player_id: str
    brand: Brand
    persona: Persona
    country: str
    device: str
    session_id: str | None = None
    has_deposited: bool = False


@dataclass(order=True)
class _PendingBet:
    settle_at: datetime
    placed: SportBetPlaced = field(compare=False)


def _money(value: float) -> Decimal:
    return Decimal(str(value)).quantize(CENT, rounding=ROUND_HALF_UP)


class Simulator:
    def __init__(self, players_per_brand: int = 2000, seed: int | None = None) -> None:
        self.rng = random.Random(seed)
        self.players: list[Player] = []
        for brand in BRANDS:
            for i in range(players_per_brand):
                persona = self.rng.choices(PERSONAS, weights=[p.weight for p in PERSONAS])[0]
                self.players.append(
                    Player(
                        player_id=f"{brand.code}-{i:06d}",
                        brand=brand,
                        persona=persona,
                        country=self.rng.choice(brand.countries),
                        device=self.rng.choices(
                            ("mobile", "desktop", "tablet"), weights=(65, 30, 5)
                        )[0],
                    )
                )
        self._activity = [p.persona.activity for p in self.players]
        self._game_weights = [g.popularity for g in GAMES]
        self._sport_weights = [s.popularity for s in SPORTS]
        self._pending: list[_PendingBet] = []
        self._online: list[Player] = []

    # ------------------------------------------------------------------ helpers
    def _id(self) -> str:
        return f"{self.rng.getrandbits(128):032x}"

    def _stake(self, player: Player) -> Decimal:
        scale = CURRENCY_SCALE[player.brand.currency]
        # Lognormal with the persona mean: many small bets, a long tail of large ones.
        sigma = 0.8
        mu = math.log(player.persona.stake_mean_eur) - sigma**2 / 2
        stake = max(0.1, self.rng.lognormvariate(mu, sigma)) * scale
        return _money(stake)

    def _base(self, player: Player, now: datetime) -> dict:
        return {
            "event_id": self._id(),
            "ts": now,
            "brand": player.brand.code,
            "player_id": player.player_id,
            "currency": player.brand.currency,
        }

    # ------------------------------------------------------------------ events
    def _start_session(self, player: Player, now: datetime) -> SessionStarted:
        player.session_id = self._id()
        return SessionStarted(
            **self._base(player, now),
            session_id=player.session_id,
            device=player.device,
            country=player.country,
        )

    def _deposit(self, player: Player, now: datetime) -> Deposit:
        amount = _money(float(self._stake(player)) * self.rng.uniform(10, 40))
        is_first = not player.has_deposited
        player.has_deposited = True
        return Deposit(
            **self._base(player, now),
            payment_id=self._id(),
            amount=amount,
            method=self.rng.choices(
                ("card", "bank_transfer", "e_wallet", "crypto"), weights=(55, 15, 25, 5)
            )[0],
            is_first=is_first,
        )

    def spin(self, player: Player, game: Game, now: datetime) -> CasinoSpin:
        bet = self._stake(player)
        hit_rate = HIT_RATE[game.volatility]
        win = Decimal("0.00")
        if self.rng.random() < hit_rate:
            # Exponential multiplier with mean rtp / hit_rate keeps E[win] = rtp * bet.
            multiplier = self.rng.expovariate(hit_rate / game.rtp)
            win = _money(float(bet) * multiplier)
        assert player.session_id is not None
        return CasinoSpin(
            **self._base(player, now),
            session_id=player.session_id,
            round_id=self._id(),
            provider=game.provider,
            game_id=game.game_id,
            bet=bet,
            win=win,
        )

    def _place_bet(self, player: Player, now: datetime) -> SportBetPlaced:
        sport = self.rng.choices(SPORTS, weights=self._sport_weights)[0]
        odds = min(15.0, max(1.05, 1 + self.rng.lognormvariate(-0.2, 0.6)))
        placed = SportBetPlaced(
            **self._base(player, now),
            bet_id=self._id(),
            sport=sport.code,
            match_id=f"{sport.code}-{self.rng.randint(1, 400):04d}",
            market=self.rng.choice(sport.markets),
            odds=_money(odds),
            stake=self._stake(player),
        )
        settle_at = now + timedelta(seconds=self.rng.uniform(5, 90))
        heapq.heappush(self._pending, _PendingBet(settle_at, placed))
        return placed

    def _settle(self, placed: SportBetPlaced, now: datetime) -> SportBetSettled:
        roll = self.rng.random()
        win_probability = (1 / float(placed.odds)) * (1 - SPORT_MARGIN)
        if roll < VOID_RATE:
            result, payout = "void", placed.stake
        elif roll < VOID_RATE + win_probability:
            result, payout = "won", _money(float(placed.stake) * float(placed.odds))
        else:
            result, payout = "lost", Decimal("0.00")
        return SportBetSettled(
            event_id=self._id(),
            ts=now,
            brand=placed.brand,
            player_id=placed.player_id,
            currency=placed.currency,
            bet_id=placed.bet_id,
            sport=placed.sport,
            match_id=placed.match_id,
            market=placed.market,
            odds=placed.odds,
            stake=placed.stake,
            result=result,
            payout=payout,
            placed_ts=placed.ts,
        )

    # ------------------------------------------------------------------ public
    def step(self, now: datetime) -> list[BaseEvent]:
        """One player action plus every sportsbook settlement that became due."""
        events: list[BaseEvent] = []
        while self._pending and self._pending[0].settle_at <= now:
            events.append(self._settle(heapq.heappop(self._pending).placed, now))

        # Most actions come from players who are already online; now and then someone new
        # logs in. Frequent players (by persona) are more likely to be the ones logging in.
        if len(self._online) < MIN_ONLINE or self.rng.random() < NEW_SESSION_RATE:
            player = self.rng.choices(self.players, weights=self._activity)[0]
            if player.session_id is None:
                events.append(self._start_session(player, now))
                self._online.append(player)
                deposit_chance = 0.15 if player.has_deposited else FIRST_DEPOSIT_RATE
                if self.rng.random() < deposit_chance:
                    events.append(self._deposit(player, now))
                return events
        else:
            idx = self.rng.randrange(len(self._online))
            player = self._online[idx]

        roll = self.rng.random()
        if roll < SESSION_END_RATE:
            player.session_id = None  # session ends silently
            self._online.remove(player)
        elif roll < SESSION_END_RATE + 0.01:
            events.append(self._deposit(player, now))
        elif self.rng.random() < player.brand.sport_share:
            events.append(self._place_bet(player, now))
        else:
            game = self.rng.choices(GAMES, weights=self._game_weights)[0]
            events.append(self.spin(player, game, now))
        return events

    @property
    def pending_bets(self) -> int:
        return len(self._pending)


# ---------------------------------------------------------------------- faults
CORRUPTIONS = ("truncated_json", "negative_amount", "unknown_type", "missing_field")


def corrupt(payload: bytes, rng: random.Random) -> tuple[bytes, str]:
    """Break a valid payload in one of a few realistic ways. Returns (payload, kind)."""
    kind = rng.choice(CORRUPTIONS)
    if kind == "truncated_json":
        return payload[: max(1, len(payload) // 2)], kind
    data = json.loads(payload)
    if kind == "negative_amount":
        for key in ("bet", "stake", "amount"):
            if key in data:
                data[key] = "-" + str(data[key])
                break
        else:
            data["currency"] = "euro"
    elif kind == "unknown_type":
        data["event_type"] = "jackpot_hit_v0"
    else:
        data.pop("player_id", None)
    return json.dumps(data).encode(), kind
