{{ config(materialized='table') }}

-- MVP definition: quarterly net-income YoY growth, sector-normalized and clipped.
-- The historical earnings_surprise_* names are retained for compatibility.
-- This is not analyst-consensus surprise, standardized SUE, or a profitability factor.

WITH quality_pillar AS (
    SELECT
        u.rebalance_date,
        u.ticker,
        u.sector,
        m.earnings_surprise_zscore
    FROM {{ ref('int_rebalance_universe') }} u
    LEFT JOIN {{ ref('int_earnings_surprise') }} m
        ON m.ticker = u.ticker
        AND m.rebalance_date = u.rebalance_date
)

SELECT
    rebalance_date,
    ticker,
    sector,
    earnings_surprise_zscore,
    earnings_surprise_zscore AS quality_score
FROM quality_pillar
