-- Example analytics on the Delta1 warehouse. Run with `make ch` and paste, or open
-- http://localhost:8123/play (user delta1 / delta1).

-- 1. GGR in EUR per brand and vertical for the last hour, from the minute aggregates.
SELECT brand, vertical, round(sum(ggr_eur)) AS ggr_eur
FROM
(
    SELECT brand, 'casino' AS vertical,
           toFloat64(turnover - payout) * dictGet('delta1.fx_dict', 'rate_to_eur', tuple(currency)) AS ggr_eur
    FROM delta1.casino_ggr_1m WHERE minute >= now() - INTERVAL 1 HOUR
    UNION ALL
    SELECT brand, 'sportsbook',
           toFloat64(stake - payout) * dictGet('delta1.fx_dict', 'rate_to_eur', tuple(currency))
    FROM delta1.sport_ggr_1m WHERE minute >= now() - INTERVAL 1 HOUR
)
GROUP BY brand, vertical
ORDER BY brand, vertical;

-- 2. Games whose actual RTP drifts from the math model, with enough rounds to matter.
SELECT g.title, a.provider, a.rounds,
       round(a.rtp_actual, 4) AS rtp_actual, g.rtp AS rtp_theoretical,
       round(a.rtp_actual - g.rtp, 4) AS delta
FROM
(
    SELECT game_id, provider, sum(rounds) AS rounds, sum(payout) / sum(turnover) AS rtp_actual
    FROM delta1.casino_ggr_1m
    GROUP BY game_id, provider
    HAVING rounds > 1000
) AS a
INNER JOIN delta1.games AS g USING (game_id)
ORDER BY abs(delta) DESC;

-- 3. Top players by net loss (player GGR) in EUR, casino + sportsbook, from raw facts.
SELECT player_id, brand, round(sum(net_eur), 2) AS player_ggr_eur, sum(wagers) AS wagers
FROM
(
    SELECT player_id, brand,
           toFloat64(bet - win) * dictGet('delta1.fx_dict', 'rate_to_eur', tuple(currency)) AS net_eur,
           1 AS wagers
    FROM delta1.casino_spins
    UNION ALL
    SELECT player_id, brand,
           toFloat64(stake - payout) * dictGet('delta1.fx_dict', 'rate_to_eur', tuple(currency)), 1
    FROM delta1.sport_bets FINAL WHERE status != 'open'
)
GROUP BY player_id, brand
ORDER BY player_ggr_eur DESC
LIMIT 10;

-- 4. Time from placement to settlement, p50 / p95 per sport.
SELECT sport,
       count() AS settled,
       round(quantile(0.5)(dateDiff('second', placed_ts, assumeNotNull(settled_ts)))) AS p50_s,
       round(quantile(0.95)(dateDiff('second', placed_ts, assumeNotNull(settled_ts)))) AS p95_s
FROM delta1.sport_bets FINAL
WHERE status != 'open'
GROUP BY sport
ORDER BY settled DESC;

-- 5. Deposit conversion: share of players with a session who deposited, per brand.
SELECT s.brand,
       uniqExact(s.player_id) AS players,
       uniqExactIf(s.player_id, d.player_id != '') AS depositors,
       round(depositors / players, 3) AS conversion
FROM delta1.sessions AS s
LEFT JOIN (SELECT DISTINCT player_id FROM delta1.deposits) AS d USING (player_id)
GROUP BY s.brand
ORDER BY s.brand;

-- 6. Why the sorting key matters: this filter uses the primary index of casino_spins
--    (ORDER BY brand, game_id, ts), so ClickHouse reads only the granules it needs.
EXPLAIN indexes = 1
SELECT sum(bet) FROM delta1.casino_spins
WHERE brand = 'balkanbet' AND game_id = 'rr-book-of-nile' AND ts >= now() - INTERVAL 10 MINUTE;
