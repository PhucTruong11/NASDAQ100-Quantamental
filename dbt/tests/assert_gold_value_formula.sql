{{ config(tags=['gold'], severity='error') }}

WITH expected AS (
    SELECT
        u.ticker,
        u.rebalance_date,
        g.value_score AS actual_score,
        CASE
            WHEN f.fcf_yield_zscore IS NOT NULL AND s.sbc_zscore IS NOT NULL
            THEN 0.5 * f.fcf_yield_zscore + 0.5 * s.sbc_zscore
            ELSE NULL
        END AS expected_score
    FROM {{ ref('int_rebalance_universe') }} u
    LEFT JOIN {{ ref('gold_value_pillar') }} g
        ON g.ticker = u.ticker AND g.rebalance_date = u.rebalance_date
    LEFT JOIN {{ ref('int_fcf_yield') }} f
        ON f.ticker = u.ticker AND f.rebalance_date = u.rebalance_date
    LEFT JOIN {{ ref('int_sbc_ratio') }} s
        ON s.ticker = u.ticker AND s.rebalance_date = u.rebalance_date
)
SELECT * FROM expected
WHERE CASE
    WHEN expected_score IS NULL THEN actual_score IS NOT NULL
    WHEN actual_score IS NULL THEN TRUE
    ELSE ABS(actual_score - expected_score) > 1e-9
END
