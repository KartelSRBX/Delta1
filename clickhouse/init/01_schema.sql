-- Delta1 warehouse schema.
-- Raw facts land in MergeTree tables partitioned by day; materialized views keep
-- per-minute aggregates up to date on every insert, so dashboards never scan raw data.

CREATE DATABASE IF NOT EXISTS delta1;

-- ------------------------------------------------------------------ reference data
-- FX rates to EUR. Seeded statically here; the Airflow DAG in PR 2 refreshes it daily.
CREATE TABLE IF NOT EXISTS delta1.fx_rates
(
    currency    LowCardinality(String),
    rate_to_eur Float64,
    updated_at  DateTime('UTC') DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY currency;

-- Seed rows are dated in the past so any rate loaded later wins argMax(rate, updated_at).
INSERT INTO delta1.fx_rates (currency, rate_to_eur, updated_at) VALUES
    ('EUR', 1, '2000-01-01 00:00:00'), ('USD', 0.92, '2000-01-01 00:00:00'), ('RSD', 0.0085, '2000-01-01 00:00:00');

-- Dictionary keeps rates in memory so dashboards convert with dictGet instead of a JOIN.
-- Demo credentials only: in production this would use a dedicated read-only user.
CREATE DICTIONARY IF NOT EXISTS delta1.fx_dict
(
    currency    String,
    rate_to_eur Float64 DEFAULT 1
)
PRIMARY KEY currency
SOURCE(CLICKHOUSE(
    QUERY 'SELECT currency, argMax(rate_to_eur, updated_at) AS rate_to_eur FROM delta1.fx_rates GROUP BY currency'
    USER 'delta1' PASSWORD 'delta1'
))
LIFETIME(MIN 60 MAX 300)
LAYOUT(COMPLEX_KEY_HASHED());

-- Game catalog with theoretical RTP, to compare against the RTP observed in production.
-- Kept in sync with src/delta1/generator/catalog.py (enforced by tests/test_schema.py).
CREATE TABLE IF NOT EXISTS delta1.games
(
    game_id    LowCardinality(String),
    provider   LowCardinality(String),
    title      String,
    rtp        Float64,
    volatility LowCardinality(String)
)
ENGINE = ReplacingMergeTree
ORDER BY game_id;

INSERT INTO delta1.games VALUES
    ('sf-dragon-gold', 'starforge', 'Dragon Gold', 0.965, 'high'),
    ('sf-fruit-rush', 'starforge', 'Fruit Rush', 0.96, 'low'),
    ('sf-viking-saga', 'starforge', 'Viking Saga', 0.955, 'medium'),
    ('rr-wild-west', 'redriver', 'Wild West Bounty', 0.962, 'high'),
    ('rr-book-of-nile', 'redriver', 'Book of Nile', 0.961, 'high'),
    ('rr-lucky-sevens', 'redriver', 'Lucky Sevens', 0.95, 'low'),
    ('np-starburst-x', 'novaplay', 'Starburst X', 0.968, 'low'),
    ('np-crash-rocket', 'novaplay', 'Crash Rocket', 0.97, 'high'),
    ('np-blackjack-live', 'novaplay', 'Blackjack Live', 0.994, 'low'),
    ('kl-kraken-deep', 'krakenlabs', 'Kraken Deep', 0.958, 'medium'),
    ('kl-roulette-eu', 'krakenlabs', 'European Roulette', 0.973, 'medium'),
    ('kl-mega-wheel', 'krakenlabs', 'Mega Wheel', 0.952, 'high');

-- ------------------------------------------------------------------ raw facts
CREATE TABLE IF NOT EXISTS delta1.sessions
(
    event_id    String,
    ts          DateTime64(3, 'UTC'),
    brand       LowCardinality(String),
    player_id   String,
    currency    LowCardinality(String),
    session_id  String,
    device      LowCardinality(String),
    country     LowCardinality(String),
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(ts)
ORDER BY (brand, ts)
TTL toDateTime(ts) + INTERVAL 90 DAY;

CREATE TABLE IF NOT EXISTS delta1.deposits
(
    event_id    String,
    ts          DateTime64(3, 'UTC'),
    brand       LowCardinality(String),
    player_id   String,
    currency    LowCardinality(String),
    payment_id  String,
    amount      Decimal(18, 2),
    method      LowCardinality(String),
    is_first    Bool,
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(ts)
ORDER BY (brand, ts)
TTL toDateTime(ts) + INTERVAL 365 DAY;

CREATE TABLE IF NOT EXISTS delta1.casino_spins
(
    event_id    String,
    ts          DateTime64(3, 'UTC'),
    brand       LowCardinality(String),
    player_id   String,
    currency    LowCardinality(String),
    session_id  String,
    round_id    String,
    provider    LowCardinality(String),
    game_id     LowCardinality(String),
    bet         Decimal(18, 2),
    win         Decimal(18, 2),
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(ts)
ORDER BY (brand, game_id, ts)
TTL toDateTime(ts) + INTERVAL 90 DAY;

-- A sportsbook bet is written twice: once when placed (status = open) and once when
-- settled. ReplacingMergeTree keeps the latest version per bet_id; queries that need the
-- exact current state use FINAL or argMax.
CREATE TABLE IF NOT EXISTS delta1.sport_bets
(
    bet_id      String,
    brand       LowCardinality(String),
    player_id   String,
    currency    LowCardinality(String),
    sport       LowCardinality(String),
    match_id    String,
    market      LowCardinality(String),
    odds        Decimal(8, 2),
    stake       Decimal(18, 2),
    status      LowCardinality(String),  -- open | won | lost | void
    payout      Decimal(18, 2),
    placed_ts   DateTime64(3, 'UTC'),
    settled_ts  Nullable(DateTime64(3, 'UTC')),
    updated_ts  DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(updated_ts)
PARTITION BY toYYYYMM(placed_ts)
ORDER BY (brand, bet_id);

CREATE TABLE IF NOT EXISTS delta1.dead_letters
(
    received_ts DateTime64(3, 'UTC'),
    reason      LowCardinality(String),
    error       String,
    payload     String
)
ENGINE = MergeTree
ORDER BY received_ts
TTL toDateTime(received_ts) + INTERVAL 14 DAY;

-- ------------------------------------------------------------------ real-time aggregates
CREATE TABLE IF NOT EXISTS delta1.casino_ggr_1m
(
    minute   DateTime('UTC'),
    brand    LowCardinality(String),
    currency LowCardinality(String),
    provider LowCardinality(String),
    game_id  LowCardinality(String),
    rounds   SimpleAggregateFunction(sum, UInt64),
    turnover SimpleAggregateFunction(sum, Decimal(38, 2)),
    payout   SimpleAggregateFunction(sum, Decimal(38, 2)),
    players  AggregateFunction(uniq, String)
)
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(minute)
ORDER BY (minute, brand, provider, game_id, currency);

CREATE MATERIALIZED VIEW IF NOT EXISTS delta1.casino_ggr_1m_mv TO delta1.casino_ggr_1m AS
SELECT
    toStartOfMinute(ts) AS minute,
    brand,
    currency,
    provider,
    game_id,
    count()             AS rounds,
    sum(bet)            AS turnover,
    sum(win)            AS payout,
    uniqState(player_id) AS players
FROM delta1.casino_spins
GROUP BY minute, brand, currency, provider, game_id;

CREATE TABLE IF NOT EXISTS delta1.sport_ggr_1m
(
    minute   DateTime('UTC'),
    brand    LowCardinality(String),
    currency LowCardinality(String),
    sport    LowCardinality(String),
    status   LowCardinality(String),
    bets     SimpleAggregateFunction(sum, UInt64),
    stake    SimpleAggregateFunction(sum, Decimal(38, 2)),
    payout   SimpleAggregateFunction(sum, Decimal(38, 2))
)
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(minute)
ORDER BY (minute, brand, sport, status, currency);

-- Sportsbook GGR is booked at settlement time, so only settled rows feed the aggregate.
CREATE MATERIALIZED VIEW IF NOT EXISTS delta1.sport_ggr_1m_mv TO delta1.sport_ggr_1m AS
SELECT
    toStartOfMinute(assumeNotNull(settled_ts)) AS minute,
    brand,
    currency,
    sport,
    status,
    count()     AS bets,
    sum(stake)  AS stake,
    sum(payout) AS payout
FROM delta1.sport_bets
WHERE status != 'open'
GROUP BY minute, brand, currency, sport, status;
