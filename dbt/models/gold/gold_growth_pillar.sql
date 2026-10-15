{{ config(materialized='table') }}

WITH growth_pillar AS (
    SELECT
        u.rebalance_date,
        u.ticker,
        u.sector,
        m.growth_zscore
    FROM {{ ref('int_rebalance_universe') }} u
    LEFT JOIN {{ ref('int_growth_ratio') }} m
        ON m.ticker = u.ticker
        AND m.rebalance_date = u.rebalance_date
)

SELECT
    rebalance_date,
    ticker,
    sector,
    growth_zscore,
    growth_zscore AS growth_score
FROM growth_pillar