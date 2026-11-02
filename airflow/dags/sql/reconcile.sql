-- Compare what the platform recorded in a window with what providers reported for it.
-- A FULL JOIN catches rows missing on either side; tolerance is relative, on bets and wins.
INSERT INTO delta1.reconciliation
(
    window_start, window_end, provider, game_id, currency,
    platform_rounds, provider_rounds, platform_bets, provider_bets,
    platform_wins, provider_wins, bets_diff_pct, status
)
SELECT
    {start:DateTime('UTC')},
    {end:DateTime('UTC')},
    provider,
    game_id,
    currency,
    coalesce(p_rounds, 0),
    coalesce(r_rounds, 0),
    coalesce(p_bets, 0),
    coalesce(r_bets, 0),
    coalesce(p_wins, 0),
    coalesce(r_wins, 0),
    if(coalesce(p_bets, 0) = 0, 0,
       (toFloat64(coalesce(r_bets, 0)) - toFloat64(p_bets)) / toFloat64(p_bets)),
    multiIf(
        r_rounds IS NULL, 'missing_in_report',
        p_rounds IS NULL, 'missing_on_platform',
        p_rounds != r_rounds
            OR abs(toFloat64(r_bets) - toFloat64(p_bets)) > {tolerance:Float64} * toFloat64(p_bets)
            OR abs(toFloat64(r_wins) - toFloat64(p_wins)) > {tolerance:Float64} * greatest(toFloat64(p_wins), 1),
            'mismatch',
        'ok'
    )
FROM
(
    SELECT provider, game_id, currency,
           count() AS p_rounds, sum(bet) AS p_bets, sum(win) AS p_wins
    FROM delta1.casino_spins
    WHERE ts >= {start:DateTime('UTC')} AND ts < {end:DateTime('UTC')}
    GROUP BY provider, game_id, currency
) AS p
FULL OUTER JOIN
(
    SELECT provider, game_id, currency,
           rounds AS r_rounds, bets AS r_bets, wins AS r_wins
    FROM delta1.provider_reports FINAL
    WHERE window_start = {start:DateTime('UTC')}
) AS r
USING (provider, game_id, currency)
SETTINGS join_use_nulls = 1
