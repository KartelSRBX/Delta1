import json
import random
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from pydantic import ValidationError

from delta1.events import dump_event, parse_event
from delta1.generator.catalog import GAMES
from delta1.generator.simulator import CORRUPTIONS, Simulator, corrupt

T0 = datetime(2026, 11, 1, 12, 0, tzinfo=UTC)


def run(sim: Simulator, steps: int, dt_ms: int = 50):
    events = []
    for i in range(steps):
        events.extend(sim.step(T0 + timedelta(milliseconds=i * dt_ms)))
    return events


def test_is_deterministic_for_a_seed():
    a = [dump_event(e) for e in run(Simulator(players_per_brand=50, seed=7), 500)]
    b = [dump_event(e) for e in run(Simulator(players_per_brand=50, seed=7), 500)]
    assert a == b


def test_emits_every_event_type_and_all_round_trip():
    events = run(Simulator(players_per_brand=50, seed=1), 5000)
    kinds = {e.event_type for e in events}
    assert kinds == {
        "session_started",
        "deposit",
        "casino_spin",
        "sport_bet_placed",
        "sport_bet_settled",
    }
    for event in events:
        assert parse_event(dump_event(event)) == event


def test_settlements_reference_placed_bets():
    events = run(Simulator(players_per_brand=50, seed=3), 6000)
    placed = {e.bet_id: e for e in events if e.event_type == "sport_bet_placed"}
    settled = [e for e in events if e.event_type == "sport_bet_settled"]
    assert settled
    for s in settled:
        assert s.bet_id in placed
        assert s.ts >= placed[s.bet_id].ts
        assert s.stake == placed[s.bet_id].stake
        if s.result == "lost":
            assert s.payout == 0
        if s.result == "void":
            assert s.payout == s.stake


@pytest.mark.parametrize("game", [g for g in GAMES if g.volatility != "high"][:3])
def test_casino_rtp_converges_to_theoretical(game):
    sim = Simulator(players_per_brand=10, seed=42)
    player = sim.players[0]
    player.session_id = "s"
    bets = wins = Decimal(0)
    for _ in range(60_000):
        spin = sim.spin(player, game, T0)
        bets += spin.bet
        wins += spin.win
    assert float(wins / bets) == pytest.approx(game.rtp, abs=0.03)


@pytest.mark.parametrize("seed", range(20))
def test_corrupted_payloads_are_rejected(seed):
    sim = Simulator(players_per_brand=5, seed=seed)
    event = next(e for e in run(sim, 200) if e.event_type == "casino_spin")
    payload, kind = corrupt(dump_event(event), random.Random(seed))
    assert kind in CORRUPTIONS
    with pytest.raises(ValidationError):
        parse_event(payload)


def test_events_reject_unknown_fields():
    sim = Simulator(players_per_brand=5, seed=0)
    data = json.loads(dump_event(run(sim, 1)[0]))
    data["surprise"] = 1
    with pytest.raises(ValidationError):
        parse_event(json.dumps(data))
