"""Static catalog of a fictional white-label platform: brands, game providers, games, sports.

All names are made up. RTP and volatility drive the win distribution in the simulator, so
the warehouse can later compare *actual* RTP against the *theoretical* one per game.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal


@dataclass(frozen=True)
class Brand:
    code: str
    currency: str
    countries: tuple[str, ...]
    sport_share: float  # share of wagering activity that goes to the sportsbook


@dataclass(frozen=True)
class Game:
    game_id: str
    provider: str
    title: str
    rtp: float  # theoretical return to player, e.g. 0.96
    volatility: str  # low | medium | high
    popularity: float  # relative weight when a player picks a game


@dataclass(frozen=True)
class Sport:
    code: str
    markets: tuple[str, ...]
    popularity: float


BRANDS: tuple[Brand, ...] = (
    Brand("luckyace", "EUR", ("DE", "AT", "NL"), sport_share=0.25),
    Brand("balkanbet", "RSD", ("RS", "BA", "ME"), sport_share=0.55),
    Brand("nordspin", "EUR", ("FI", "EE", "LV"), sport_share=0.15),
    Brand("atlasplay", "USD", ("CA", "BR", "MX"), sport_share=0.35),
)

GAMES: tuple[Game, ...] = (
    Game("sf-dragon-gold", "starforge", "Dragon Gold", 0.965, "high", 9),
    Game("sf-fruit-rush", "starforge", "Fruit Rush", 0.960, "low", 12),
    Game("sf-viking-saga", "starforge", "Viking Saga", 0.955, "medium", 6),
    Game("rr-wild-west", "redriver", "Wild West Bounty", 0.962, "high", 7),
    Game("rr-book-of-nile", "redriver", "Book of Nile", 0.961, "high", 14),
    Game("rr-lucky-sevens", "redriver", "Lucky Sevens", 0.950, "low", 5),
    Game("np-starburst-x", "novaplay", "Starburst X", 0.968, "low", 11),
    Game("np-crash-rocket", "novaplay", "Crash Rocket", 0.970, "high", 10),
    Game("np-blackjack-live", "novaplay", "Blackjack Live", 0.994, "low", 4),
    Game("kl-kraken-deep", "krakenlabs", "Kraken Deep", 0.958, "medium", 6),
    Game("kl-roulette-eu", "krakenlabs", "European Roulette", 0.973, "medium", 8),
    Game("kl-mega-wheel", "krakenlabs", "Mega Wheel", 0.952, "high", 3),
)

SPORTS: tuple[Sport, ...] = (
    Sport("football", ("1x2", "total_2.5", "btts", "handicap"), 50),
    Sport("basketball", ("moneyline", "total_points", "handicap"), 18),
    Sport("tennis", ("match_winner", "set_handicap"), 14),
    Sport("esports", ("match_winner", "map_handicap"), 10),
    Sport("hockey", ("1x2", "total_5.5"), 8),
)

# Hit rate per volatility class: high-volatility games pay rarely but big.
HIT_RATE: dict[str, float] = {"low": 0.45, "medium": 0.30, "high": 0.15}

# Static FX snapshot used for seeding ClickHouse. PR 2 replaces it with a daily API pull.
FX_TO_EUR: dict[str, Decimal] = {
    "EUR": Decimal("1"),
    "USD": Decimal("0.92"),
    "RSD": Decimal("0.0085"),
}

# Stake scale per brand currency, so that an RSD brand bets in RSD-sized numbers.
CURRENCY_SCALE: dict[str, float] = {"EUR": 1.0, "USD": 1.1, "RSD": 117.0}
