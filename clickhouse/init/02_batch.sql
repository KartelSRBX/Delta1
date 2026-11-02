-- Batch layer: partner data loaded by Airflow, reconciliation results and daily marts.

-- ------------------------------------------------------------------ FX history
-- Every hourly quote the FX DAG pulls. delta1.fx_rates keeps only the latest rate per
-- currency (it feeds the dictionary); this table keeps the history.
CREATE TABLE IF NOT EXISTS delta1.fx_rates_history
(
    as_of       DateTime('UTC'),
    currency    LowCardinality(String),
    rate_to_eur Float64,
    source      LowCardinality(String),
    loaded_at   DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(loaded_at)
ORDER BY (currency, as_of);

-- ------------------------------------------------------------------ provider reports
-- Raw provider CSVs as received over FTP. Re-loading a file replaces its rows.
CREATE TABLE IF NOT EXISTS delta1.provider_reports
(
    window_start DateTime('UTC'),
    window_end   DateTime('UTC'),
    provider     LowCardinality(String),
    game_id      LowCardinality(String),
    currency     LowCardinality(String),
    rounds       UInt64,
    bets         Decimal(18, 2),
    wins         Decimal(18, 2),
    source_file  String,
    loaded_at    DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(loaded_at)
PARTITION BY toYYYYMMDD(window_start)
ORDER BY (window_start, provider, game_id, currency);

-- One row per (window, provider, game, currency): what we recorded vs what they reported.
CREATE TABLE IF NOT EXISTS delta1.reconciliation
(
    window_start    DateTime('UTC'),
    window_end      DateTime('UTC'),
    provider        LowCardinality(String),
    game_id         LowCardinality(String),
    currency        LowCardinality(String),
    platform_rounds UInt64,
    provider_rounds UInt64,
    platform_bets   Decimal(38, 2),
    provider_bets   Decimal(38, 2),
    platform_wins   Decimal(38, 2),
    provider_wins   Decimal(38, 2),
    bets_diff_pct   Float64,
    status          LowCardinality(String),  -- ok | mismatch | missing_in_report | missing_on_platform
    checked_at      DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(checked_at)
PARTITION BY toYYYYMMDD(window_start)
ORDER BY (window_start, provider, game_id, currency);

-- ------------------------------------------------------------------ daily marts
-- Built by Airflow with write-audit-publish: fill the staging table, run data-quality
-- checks on it, then swap the day in with ALTER TABLE ... REPLACE PARTITION.
CREATE TABLE IF NOT EXISTS delta1.player_daily
(
    day                 Date,
    brand               LowCardinality(String),
    player_id           String,
    currency            LowCardinality(String),
    sessions            UInt64,
    deposits            UInt64,
    deposit_amount      Decimal(38, 2),
    first_deposit       UInt8,
    casino_rounds       UInt64,
    casino_bets         Decimal(38, 2),
    casino_wins         Decimal(38, 2),
    sport_bets_placed   UInt64,
    sport_stake_settled Decimal(38, 2),
    sport_payout        Decimal(38, 2),
    ggr                 Decimal(38, 2),
    ggr_eur             Float64,
    built_at            DateTime('UTC') DEFAULT now()
)
ENGINE = MergeTree
PARTITION BY day
ORDER BY (brand, player_id);

CREATE TABLE IF NOT EXISTS delta1.player_daily_staging AS delta1.player_daily;

CREATE TABLE IF NOT EXISTS delta1.provider_daily
(
    day       Date,
    provider  LowCardinality(String),
    game_id   LowCardinality(String),
    currency  LowCardinality(String),
    rounds    UInt64,
    players   UInt64,
    turnover  Decimal(38, 2),
    payout    Decimal(38, 2),
    turnover_eur Float64,
    ggr_eur   Float64,
    rtp       Float64,
    built_at  DateTime('UTC') DEFAULT now()
)
ENGINE = MergeTree
PARTITION BY day
ORDER BY (provider, game_id, currency);

CREATE TABLE IF NOT EXISTS delta1.provider_daily_staging AS delta1.provider_daily;
