{{ config(materialized='table') }}

WITH momentum_inputs AS (
    SELECT
        u.rebalance_date,
        u.ticker,
        u.sector,
        m.momentum_zscore
    FROM {{ ref('int_rebalance_universe') }} u
    LEFT JOIN {{ ref('int_momentum_ratio') }} m
        ON m.ticker = u.ticker
        AND m.rebalance_date = u.rebalance_date
)

SELECT
    rebalance_date,
    ticker,
    sector,
    momentum_zscore,
    momentum_zscore AS momentum_score
FROM momentum_inputs
