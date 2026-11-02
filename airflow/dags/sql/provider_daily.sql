-- Game/provider daily mart for one day ({day:Date}), using facts before {cutoff} only.
INSERT INTO delta1.provider_daily_staging
    (day, provider, game_id, currency, rounds, players, turnover, payout, turnover_eur, ggr_eur, rtp)
SELECT
    {day:Date},
    provider,
    game_id,
    currency,
    count(),
    uniqExact(player_id),
    sum(bet) AS t,
    sum(win) AS p,
    toFloat64(t) * dictGet('delta1.fx_dict', 'rate_to_eur', tuple(currency)),
    toFloat64(t - p) * dictGet('delta1.fx_dict', 'rate_to_eur', tuple(currency)),
    toFloat64(p) / toFloat64(t)
FROM delta1.casino_spins
WHERE toDate(ts) = {day:Date} AND ts < {cutoff:DateTime64(3, 'UTC')}
GROUP BY provider, game_id, currency
