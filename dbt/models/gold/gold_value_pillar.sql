{{ config(materialized='table') }}

WITH value_inputs AS (
    SELECT
        u.rebalance_date,
        u.ticker,
        u.sector,
        m.fcf_yield_zscore,
        q.sbc_zscore
    FROM {{ ref('int_rebalance_universe') }} u
    LEFT JOIN {{ ref('int_fcf_yield') }} m
        ON m.ticker = u.ticker
        AND m.rebalance_date = u.rebalance_date
    LEFT JOIN {{ ref('int_sbc_ratio') }} q
        ON q.ticker = u.ticker
        AND q.rebalance_date = u.rebalance_date

)

SELECT
    rebalance_date,
    ticker,
    sector,
    fcf_yield_zscore,
    sbc_zscore,
    CASE
        WHEN fcf_yield_zscore IS NOT NULL
            AND sbc_zscore IS NOT NULL
        THEN 0.5 * fcf_yield_zscore + 0.5 * sbc_zscore
        ELSE NULL
    END AS value_score
FROM value_inputs
