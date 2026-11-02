"""Static checks that the ClickHouse DDL and the Python code agree."""

import re
from pathlib import Path

from delta1.generator.catalog import FX_TO_EUR, GAMES
from delta1.processor.pipeline import TABLE_COLUMNS

SCHEMA = (Path(__file__).parents[1] / "clickhouse" / "init" / "01_schema.sql").read_text()


def table_columns(table: str) -> list[str]:
    match = re.search(rf"CREATE TABLE IF NOT EXISTS delta1\.{table}\s*\((.*?)\n\)", SCHEMA, re.S)
    assert match, f"table {table} not found in schema"
    cols = []
    for line in match.group(1).splitlines():
        line = line.strip()
        if line and not line.startswith("--"):
            cols.append(line.split()[0])
    return cols


def test_processor_columns_exist_in_schema_in_order():
    for table, columns in TABLE_COLUMNS.items():
        schema_cols = [c for c in table_columns(table) if c != "ingested_at"]
        assert schema_cols == list(columns), table


def test_game_catalog_matches_seed():
    for game in GAMES:
        expected = (
            f"('{game.game_id}', '{game.provider}', '{game.title}', {game.rtp}, "
            f"'{game.volatility}')"
        )
        assert expected in SCHEMA, game.game_id


def test_fx_seed_covers_every_currency():
    for currency in FX_TO_EUR:
        assert f"('{currency}'," in SCHEMA
