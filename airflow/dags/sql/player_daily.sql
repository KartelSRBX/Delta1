-- Player-level daily mart for one day ({day:Date}), using facts before {cutoff} only.
-- Every source is reduced to the same shape (kind, a, b, flag) and pivoted once, which
-- reads each fact table a single time.
INSERT INTO delta1.player_daily_staging
(
    day, brand, player_id, currency, sessions, deposits, deposit_amount, first_deposit,
    casino_rounds, casino_bets, casino_wins, sport_bets_placed, sport_stake_settled,
    sport_payout, ggr, ggr_eur
)
SELECT
    {day:Date},
    brand,
    player_id,
    currency,
    countIf(kind = 'session'),
    countIf(kind = 'deposit'),
    sumIf(a, kind = 'deposit'),
    maxIf(flag, kind = 'deposit'),
    countIf(kind = 'spin'),
    sumIf(a, kind = 'spin'),
    sumIf(b, kind = 'spin'),
    countIf(kind = 'sport_placed'),
    sumIf(a, kind = 'sport_settled'),
    sumIf(b, kind = 'sport_settled'),
    sumIf(a - b, kind IN ('spin', 'sport_settled')) AS player_ggr,
    toFloat64(player_ggr) * dictGet('delta1.fx_dict', 'rate_to_eur', tuple(currency))
FROM
(
    SELECT brand, player_id, currency, 'session' AS kind,
           toDecimal64(0, 2) AS a, toDecimal64(0, 2) AS b, toUInt8(0) AS flag
    FROM delta1.sessions WHERE toDate(ts) = {day:Date} AND ts < {cutoff:DateTime64(3, 'UTC')}
    UNION ALL
    SELECT brand, player_id, currency, 'deposit', toDecimal64(amount, 2), toDecimal64(0, 2),
           toUInt8(is_first)
    FROM delta1.deposits WHERE toDate(ts) = {day:Date} AND ts < {cutoff:DateTime64(3, 'UTC')}
    UNION ALL
    SELECT brand, player_id, currency, 'spin', toDecimal64(bet, 2), toDecimal64(win, 2), 0
    FROM delta1.casino_spins WHERE toDate(ts) = {day:Date} AND ts < {cutoff:DateTime64(3, 'UTC')}
    UNION ALL
    SELECT brand, player_id, currency, 'sport_placed', toDecimal64(0, 2), toDecimal64(0, 2), 0
    FROM delta1.sport_bets FINAL
    WHERE toDate(placed_ts) = {day:Date} AND placed_ts < {cutoff:DateTime64(3, 'UTC')}
    UNION ALL
    SELECT brand, player_id, currency, 'sport_settled', toDecimal64(stake, 2),
           toDecimal64(payout, 2), 0
    FROM delta1.sport_bets FINAL
    WHERE status != 'open' AND toDate(assumeNotNull(settled_ts)) = {day:Date}
      AND assumeNotNull(settled_ts) < {cutoff:DateTime64(3, 'UTC')}
)
GROUP BY brand, player_id, currency
