"""Query benchmark: what the warehouse design buys, measured on synthetic data.

It builds an isolated ``bench`` database with the same tables as ``delta1`` (spins, the
per-minute rollup, sportsbook bets), fills it with tens of millions of rows generated
inside ClickHouse, and then runs pairs of queries that answer the same question in two
ways: rollup vs raw scan, primary key vs full scan, projection vs none, FINAL vs argMax,
uniq vs uniqExact.

Each query runs ``--repeat`` times. Duration, rows read and memory come from
``system.query_log`` (medians), and granule counts come from ``EXPLAIN indexes = 1``.
Queries that are meant to be equivalent are checked to return the same result, so the
script doubles as a correctness test for the rollup and the deduplication strategies.

    python scripts/bench_queries.py --rows 30000000 --bets 3000000 --out results.md
"""

from __future__ import annotations

import argparse
import os
import re
import statistics
import sys
import time
import uuid
from dataclasses import dataclass, field

import clickhouse_connect

BRANDS = ("luckyace", "balkanbet", "nordspin", "atlasplay")
CURRENCIES = ("EUR", "RSD", "EUR", "USD")
GAMES = (
    ("sf-dragon-gold", "starforge"),
    ("sf-fruit-rush", "starforge"),
    ("sf-viking-saga", "starforge"),
    ("rr-wild-west", "redriver"),
    ("rr-book-of-nile", "redriver"),
    ("rr-lucky-sevens", "redriver"),
    ("np-starburst-x", "novaplay"),
    ("np-crash-rocket", "novaplay"),
    ("np-blackjack-live", "novaplay"),
    ("kl-kraken-deep", "krakenlabs"),
    ("kl-roulette-eu", "krakenlabs"),
    ("kl-mega-wheel", "krakenlabs"),
)
SPORTS = ("football", "tennis", "basketball", "hockey", "esports")
START = "2026-10-01 00:00:00"
DAYS = 30
PLAYERS_PER_BRAND = 50_000


def arr(values) -> str:
    return "[" + ", ".join(f"'{v}'" for v in values) + "]"


def player_id(brand: str, salt: int) -> str:
    n = f"cityHash64(number, {salt}) % {PLAYERS_PER_BRAND}"
    return f"concat({brand}, '-', leftPad(toString({n}), 6, '0'))"


SCHEMA = [
    "DROP DATABASE IF EXISTS bench SYNC",
    "CREATE DATABASE bench",
    "CREATE TABLE bench.casino_spins AS delta1.casino_spins",
    # The data is pinned to October 2026 for reproducibility; the 90-day TTL would eat it.
    "ALTER TABLE bench.casino_spins REMOVE TTL",
    # Same columns, but a player-ordered copy of the hot columns for player lookups.
    """ALTER TABLE bench.casino_spins ADD PROJECTION by_player
       (SELECT player_id, ts, game_id, bet, win ORDER BY player_id, ts)""",
    "CREATE TABLE bench.casino_ggr_1m AS delta1.casino_ggr_1m",
    "CREATE TABLE bench.sport_bets AS delta1.sport_bets",
]


def fill_spins(rows: int) -> str:
    # Deterministic: every column is a hash of the row number, so reruns give the same data.
    span_ms = DAYS * 86_400_000
    return f"""
    INSERT INTO bench.casino_spins
        (event_id, ts, brand, player_id, currency, session_id, round_id, provider, game_id,
         bet, win)
    SELECT
        hex(cityHash64(number, 1)),
        fromUnixTimestamp64Milli(toInt64(toUnixTimestamp('{START}', 'UTC') * 1000
                                 + intDiv(number * {span_ms}, {rows})), 'UTC'),
        {arr(BRANDS)}[b],
        {player_id(f"{arr(BRANDS)}[b]", 2)},
        {arr(CURRENCIES)}[b],
        hex(cityHash64(number, 3)),
        hex(cityHash64(number, 4)),
        {arr(p for _, p in GAMES)}[g],
        {arr(gid for gid, _ in GAMES)}[g],
        bet,
        if(cityHash64(number, 5) % 100 < 30,
           toDecimal64(bet * (cityHash64(number, 6) % 600) / 100, 2), toDecimal64(0, 2))
    FROM
    (
        SELECT number,
               1 + cityHash64(number, 7) % {len(BRANDS)} AS b,
               1 + cityHash64(number, 8) % {len(GAMES)} AS g,
               toDecimal64(0.2 + (cityHash64(number, 9) % 500) / 100, 2) AS bet
        FROM numbers({rows})
    )
    SETTINGS max_partitions_per_insert_block = 1000
    """


# Same SELECT as delta1.casino_ggr_1m_mv: populating the rollup this way is what the MV does
# on every insert.
FILL_ROLLUP = """
INSERT INTO bench.casino_ggr_1m
SELECT toStartOfMinute(ts) AS minute, brand, currency, provider, game_id,
       count() AS rounds, sum(bet) AS turnover, sum(win) AS payout,
       uniqState(player_id) AS players
FROM bench.casino_spins
GROUP BY minute, brand, currency, provider, game_id
"""


def fill_bets(bets: int, settled: bool) -> str:
    # Two inserts, as in production: the placed version first, then the settled one.
    span_ms = DAYS * 86_400_000
    status = (
        "['won', 'lost', 'lost', 'void'][1 + cityHash64(number, 13) % 4]" if settled else "'open'"
    )
    payout = (
        "if(status = 'won', toDecimal64(stake * odds, 2),"
        " if(status = 'void', stake, toDecimal64(0, 2)))"
        if settled
        else "toDecimal64(0, 2)"
    )
    settled_ts = "placed_ts + toIntervalMinute(90)" if settled else "NULL"
    return f"""
    INSERT INTO bench.sport_bets
    SELECT bet_id, brand, player_id, currency, sport, match_id, market, odds, stake,
           status, {payout}, placed_ts, {settled_ts}, {settled_ts if settled else "placed_ts"}
    FROM
    (
        SELECT
            hex(cityHash64(number, 10)) AS bet_id,
            {arr(BRANDS)}[1 + number % {len(BRANDS)}] AS brand,
            {player_id("brand", 11)} AS player_id,
            {arr(CURRENCIES)}[1 + number % {len(BRANDS)}] AS currency,
            {arr(SPORTS)}[1 + cityHash64(number, 12) % {len(SPORTS)}] AS sport,
            toString(cityHash64(number, 14) % 20000) AS match_id,
            '1x2' AS market,
            toDecimal32(1.2 + (cityHash64(number, 15) % 300) / 100, 2) AS odds,
            toDecimal64(1 + cityHash64(number, 16) % 100, 2) AS stake,
            {status} AS status,
            fromUnixTimestamp64Milli(toInt64(toUnixTimestamp('{START}', 'UTC') * 1000
                                     + intDiv(number * {span_ms}, {bets})), 'UTC') AS placed_ts
        FROM numbers({bets})
    )
    """


@dataclass
class Query:
    key: str
    group: str
    label: str
    sql: str
    settings: dict = field(default_factory=dict)
    explain: bool = False
    exact: bool = True  # must return the same result as the group's first query


PLAYER = "luckyace-000042"
LAST_WEEK = "'2026-10-24 00:00:00'"

QUERIES = [
    Query(
        "ggr_raw",
        "GGR per brand, last 7 days",
        "raw `casino_spins`",
        f"""SELECT brand, sum(bet) - sum(win) AS ggr FROM bench.casino_spins
            WHERE ts >= {LAST_WEEK} GROUP BY brand ORDER BY brand""",
        explain=True,
    ),
    Query(
        "ggr_rollup",
        "GGR per brand, last 7 days",
        "rollup `casino_ggr_1m`",
        f"""SELECT brand, sum(turnover) - sum(payout) AS ggr FROM bench.casino_ggr_1m
            WHERE minute >= {LAST_WEEK} GROUP BY brand ORDER BY brand""",
        explain=True,
    ),
    Query(
        "game_day_raw",
        "Hourly rounds of one game on one brand, one day",
        "primary key (brand, game_id, ts)",
        """SELECT toStartOfHour(ts) AS h, count() AS rounds, sum(bet) AS turnover
           FROM bench.casino_spins
           WHERE brand = 'luckyace' AND game_id = 'sf-dragon-gold'
             AND ts >= '2026-10-15 00:00:00' AND ts < '2026-10-16 00:00:00'
           GROUP BY h ORDER BY h""",
        explain=True,
    ),
    Query(
        "player_scan",
        "One player's history (support / risk lookup)",
        "full scan, projection off",
        f"""SELECT ts, game_id, bet, win FROM bench.casino_spins
            WHERE player_id = '{PLAYER}' ORDER BY ts""",
        settings={"optimize_use_projections": 0},
        explain=True,
    ),
    Query(
        "player_projection",
        "One player's history (support / risk lookup)",
        "projection `by_player`",
        f"""SELECT ts, game_id, bet, win FROM bench.casino_spins
            WHERE player_id = '{PLAYER}' ORDER BY ts""",
        explain=True,
    ),
    Query(
        "uniq_exact",
        "Unique players, 30 days",
        "`uniqExact`",
        "SELECT uniqExact(player_id) FROM bench.casino_spins",
    ),
    Query(
        "uniq",
        "Unique players, 30 days",
        "`uniq` (HyperLogLog-like)",
        "SELECT uniq(player_id) FROM bench.casino_spins",
        exact=False,
    ),
    Query(
        "uniq_rollup",
        "Unique players, 30 days",
        "`uniqMerge` over the rollup",
        "SELECT uniqMerge(players) FROM bench.casino_ggr_1m",
        exact=False,
    ),
    Query(
        "bets_final",
        "Sportsbook bets by current status (two versions per bet, unmerged)",
        "`FINAL`",
        """SELECT status, count() AS bets, sum(stake) AS stake FROM bench.sport_bets FINAL
           GROUP BY status ORDER BY status""",
    ),
    Query(
        "bets_argmax",
        "Sportsbook bets by current status (two versions per bet, unmerged)",
        "`argMax` per bet_id",
        """SELECT status, count() AS bets, sum(stake) AS stake FROM
           (SELECT bet_id, argMax(status, updated_ts) AS status, any(stake) AS stake
            FROM bench.sport_bets GROUP BY bet_id)
           GROUP BY status ORDER BY status""",
    ),
    Query(
        "bets_naive",
        "Sportsbook bets by current status (two versions per bet, unmerged)",
        "plain `GROUP BY` (wrong: counts both versions)",
        """SELECT status, count() AS bets, sum(stake) AS stake FROM bench.sport_bets
           GROUP BY status ORDER BY status""",
        exact=False,
    ),
]


def granules(explain: str) -> str:
    """Granules left after the last index in an ``EXPLAIN indexes = 1`` plan, e.g. '12/3663'."""
    found = re.findall(r"Granules: (\d+/\d+)", explain)
    return found[-1] if found else "n/a"


def fmt_bytes(n: float) -> str:
    for unit in ("B", "KiB", "MiB", "GiB"):
        if n < 1024:
            return f"{n:,.0f} {unit}" if unit == "B" else f"{n:,.1f} {unit}"
        n /= 1024
    return f"{n:,.1f} TiB"


def build(client, rows: int, bets: int) -> dict[str, float]:
    timings = {}
    for stmt in SCHEMA:
        client.command(stmt)
    t = time.perf_counter()
    client.command(fill_spins(rows), settings={"max_insert_threads": 4})
    timings["spins"] = time.perf_counter() - t
    t = time.perf_counter()
    client.command(FILL_ROLLUP)
    timings["rollup"] = time.perf_counter() - t
    # Merge raw and rollup to one part per partition so every run reads the same layout.
    client.command("OPTIMIZE TABLE bench.casino_spins FINAL")
    client.command("OPTIMIZE TABLE bench.casino_ggr_1m FINAL")
    # Sportsbook: keep both versions of every bet in separate parts, i.e. the state right
    # after settlement and before ReplacingMergeTree has merged anything.
    client.command("SYSTEM STOP MERGES bench.sport_bets")
    client.command(fill_bets(bets, settled=False))
    client.command(fill_bets(bets, settled=True))
    return timings


def storage(client) -> list[tuple[str, int, int, int]]:
    tables = client.query(
        """SELECT table, sum(rows), sum(data_compressed_bytes), sum(data_uncompressed_bytes)
           FROM system.parts WHERE database = 'bench' AND active GROUP BY table ORDER BY table"""
    ).result_rows
    proj = client.query(
        """SELECT concat(table, '.', name), sum(rows), sum(data_compressed_bytes),
                  sum(data_uncompressed_bytes)
           FROM system.projection_parts WHERE database = 'bench' AND active
           GROUP BY table, name"""
    ).result_rows
    return [*tables, *proj]


def run(client, q: Query, repeat: int, tag: str):
    settings = {"use_query_cache": 0, **q.settings}
    result = None
    for i in range(repeat):
        settings["log_comment"] = f"{tag}:{q.key}:{i}"
        result = client.query(q.sql, settings=settings).result_rows
    plan = ""
    if q.explain:
        plan = "\n".join(
            r[0]
            for r in client.query(f"EXPLAIN indexes = 1 {q.sql}", settings=q.settings).result_rows
        )
    return result, plan


def metrics(client, tag: str) -> dict[str, dict]:
    client.command("SYSTEM FLUSH LOGS")
    rows = client.query(
        """SELECT splitByChar(':', log_comment)[2] AS key,
                  groupArray(query_duration_ms), max(read_rows), max(read_bytes),
                  groupArray(memory_usage), groupUniqArrayArray(projections)
           FROM system.query_log
           WHERE type = 'QueryFinish' AND startsWith(log_comment, {tag:String})
           GROUP BY key""",
        parameters={"tag": tag + ":"},
    ).result_rows
    return {
        key: {
            "ms": statistics.median(durations),
            "rows": read_rows,
            "bytes": read_bytes,
            "mem": statistics.median(mem),
            "projections": projections,
        }
        for key, durations, read_rows, read_bytes, mem, projections in rows
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--rows", type=int, default=30_000_000, help="casino spins to generate")
    ap.add_argument("--bets", type=int, default=3_000_000, help="sportsbook bets to generate")
    ap.add_argument("--repeat", type=int, default=5)
    ap.add_argument("--keep", action="store_true", help="keep the bench database afterwards")
    ap.add_argument("--out", help="write the Markdown report here as well")
    args = ap.parse_args()

    client = clickhouse_connect.get_client(
        host=os.environ.get("CLICKHOUSE_HOST", "localhost"),
        username=os.environ.get("CLICKHOUSE_USER", "delta1"),
        password=os.environ.get("CLICKHOUSE_PASSWORD", "delta1"),
        send_receive_timeout=1800,
    )
    version = client.command("SELECT version()")
    print(f"ClickHouse {version}: generating {args.rows:,} spins and {args.bets:,} bets ...")
    timings = build(client, args.rows, args.bets)
    print(f"  spins in {timings['spins']:.1f}s, rollup in {timings['rollup']:.1f}s", flush=True)

    tag = f"bench-{uuid.uuid4().hex[:8]}"
    results, plans = {}, {}
    for q in QUERIES:
        results[q.key], plans[q.key] = run(client, q, args.repeat, tag)
        print(f"  ran {q.key}", flush=True)
    m = metrics(client, tag)

    failures = []
    first_in_group: dict[str, str] = {}
    for q in QUERIES:
        ref = first_in_group.setdefault(q.group, q.key)
        if q.exact and results[q.key] != results[ref]:
            failures.append(f"{q.key} differs from {ref}: {results[q.key]} vs {results[ref]}")

    lines = [
        f"ClickHouse {version}, {args.rows:,} spins over {DAYS} days, {args.bets:,} sportsbook "
        f"bets (two versions each), median of {args.repeat} warm runs.",
        "",
        "| Question | Strategy | Median time | Rows read | Data read | Memory | Granules |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: |",
    ]
    for q in QUERIES:
        r = m[q.key]
        gran = granules(plans[q.key]) if q.explain else ""
        if r["projections"]:
            gran += f" (projection {', '.join(p.split('.')[-1] for p in r['projections'])})"
        lines.append(
            f"| {q.group} | {q.label} | {r['ms']:,.0f} ms | {r['rows']:,} | "
            f"{fmt_bytes(r['bytes'])} | {fmt_bytes(r['mem'])} | {gran} |"
        )
    exact = results["uniq_exact"][0][0]
    lines += ["", "Approximate answers vs exact:", ""]
    for key in ("uniq", "uniq_rollup"):
        approx = results[key][0][0]
        lines.append(f"- `{key}`: {approx:,} vs {exact:,} ({(approx - exact) / exact:+.2%})")
    naive = sum(r[1] for r in results["bets_naive"])
    final = sum(r[1] for r in results["bets_final"])
    lines.append(f"- plain `GROUP BY` on `sport_bets`: {naive:,} bets vs {final:,} real ones")
    lines += [
        "",
        "Storage:",
        "",
        "| Table | Rows | Compressed | Uncompressed |",
        "| --- | ---: | ---: | ---: |",
    ]
    for name, rows, comp, uncomp in storage(client):
        lines.append(f"| `{name}` | {rows:,} | {fmt_bytes(comp)} | {fmt_bytes(uncomp)} |")
    for key in ("game_day_raw", "player_scan", "player_projection"):
        lines += ["", f"`EXPLAIN indexes = 1` for `{key}`:", "", "```", plans[key], "```"]
    report = "\n".join(lines)
    print(report)
    if args.out:
        with open(args.out, "w") as f:
            f.write(report + "\n")

    if not args.keep:
        client.command("DROP DATABASE bench SYNC")
    if failures:
        print("\nEquivalent queries disagree:\n" + "\n".join(failures), file=sys.stderr)
        return 1
    print("\nAll equivalent strategies returned identical results.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
