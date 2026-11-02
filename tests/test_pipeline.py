from datetime import UTC, datetime, timedelta

import pytest

from delta1.events import dump_event
from delta1.generator.simulator import Simulator
from delta1.processor.dedup import Deduplicator
from delta1.processor.pipeline import TABLE_COLUMNS, Outcome, Pipeline

T0 = datetime(2026, 11, 1, 12, 0, tzinfo=UTC)


def sample_events(n: int = 3000):
    sim = Simulator(players_per_brand=50, seed=11)
    out = []
    for i in range(n):
        out.extend(sim.step(T0 + timedelta(milliseconds=i * 50)))
    return out


def test_dedup_window_is_bounded():
    d = Deduplicator(capacity=3)
    assert [d.seen(x) for x in "abca"] == [False, False, False, True]
    d.seen("d")  # evicts the least recently seen id ("b")
    assert len(d) == 3
    assert d.seen("b") is False


def test_dedup_rejects_bad_capacity():
    with pytest.raises(ValueError):
        Deduplicator(0)


def test_rows_match_table_columns():
    pipeline = Pipeline(Deduplicator(10_000))
    for event in sample_events():
        assert pipeline.handle(dump_event(event)) is Outcome.ACCEPTED
    batch = pipeline.drain()
    assert set(batch.rows) == {"sessions", "deposits", "casino_spins", "sport_bets"}
    for table, rows in batch.rows.items():
        for row in rows:
            assert len(row) == len(TABLE_COLUMNS[table]), table


def test_duplicates_are_dropped_once_seen():
    pipeline = Pipeline(Deduplicator(10_000))
    payload = dump_event(sample_events(5)[0])
    assert pipeline.handle(payload) is Outcome.ACCEPTED
    assert pipeline.handle(payload) is Outcome.DUPLICATE
    batch = pipeline.drain()
    assert batch.duplicates == 1
    assert sum(len(r) for r in batch.rows.values()) == 1


@pytest.mark.parametrize(
    ("payload", "reason"),
    [
        (b'{"event_type": "casino_sp', "malformed_json"),
        (b'{"event_type": "jackpot_v0", "event_id": "12345678"}', "unknown_event_type"),
        (b'{"event_type": "deposit", "event_id": "12345678"}', "missing_field"),
    ],
)
def test_invalid_messages_become_dead_letters(payload, reason):
    pipeline = Pipeline(Deduplicator(10))
    assert pipeline.handle(payload) is Outcome.INVALID
    batch = pipeline.drain()
    assert batch.dead_letters[0].reason == reason
    assert batch.dead_letters[0].payload == payload
    assert len(batch.dead_letters[0].row()) == len(TABLE_COLUMNS["dead_letters"])


def test_bet_lifecycle_maps_to_two_versions_of_one_row():
    events = sample_events(6000)
    settled = next(e for e in events if e.event_type == "sport_bet_settled")
    placed = next(
        e for e in events if e.event_type == "sport_bet_placed" and e.bet_id == settled.bet_id
    )
    pipeline = Pipeline(Deduplicator(10))
    pipeline.handle(dump_event(placed))
    pipeline.handle(dump_event(settled))
    rows = pipeline.drain().rows["sport_bets"]
    cols = TABLE_COLUMNS["sport_bets"]
    first, second = (dict(zip(cols, r, strict=True)) for r in rows)
    assert first["bet_id"] == second["bet_id"]
    assert first["status"] == "open" and first["settled_ts"] is None
    assert second["status"] == settled.result
    assert second["updated_ts"] > first["updated_ts"]
    assert second["placed_ts"] == first["placed_ts"]


def test_drain_resets_the_batch():
    pipeline = Pipeline(Deduplicator(10))
    pipeline.handle(dump_event(sample_events(1)[0]))
    assert pipeline.pending == 1
    pipeline.drain()
    assert pipeline.pending == 0
    assert not pipeline.drain()
