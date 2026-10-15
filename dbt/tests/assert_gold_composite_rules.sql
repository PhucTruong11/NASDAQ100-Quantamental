{{ config(tags=['gold'], severity='error') }}

WITH expected AS (
    SELECT
        u.ticker,
        u.rebalance_date,
        c.available_pillar_count AS actual_count,
        c.is_score_eligible AS actual_eligible,
        c.composite_score AS actual_score,
        c.value_score IS DISTINCT FROM v.value_score
        OR c.growth_score IS DISTINCT FROM g.growth_score
        OR c.momentum_score IS DISTINCT FROM m.momentum_score
        OR c.quality_score IS DISTINCT FROM q.quality_score AS mismatched_pillar_columns,
        CASE WHEN v.value_score IS NOT NULL THEN 1 ELSE 0 END
        + CASE WHEN g.growth_score IS NOT NULL THEN 1 ELSE 0 END
        + CASE WHEN m.momentum_score IS NOT NULL THEN 1 ELSE 0 END
        + CASE WHEN q.quality_score IS NOT NULL THEN 1 ELSE 0 END AS expected_count,
        0.25 * v.value_score + 0.25 * g.growth_score
        + 0.25 * m.momentum_score + 0.25 * q.quality_score AS expected_score
    FROM {{ ref('int_rebalance_universe') }} u
    LEFT JOIN {{ ref('gold_compose_score') }} c
        ON c.ticker = u.ticker AND c.rebalance_date = u.rebalance_date
    LEFT JOIN {{ ref('gold_value_pillar') }} v
        ON v.ticker = u.ticker AND v.rebalance_date = u.rebalance_date
    LEFT JOIN {{ ref('gold_growth_pillar') }} g
        ON g.ticker = u.ticker AND g.rebalance_date = u.rebalance_date
    LEFT JOIN {{ ref('gold_momentum_pillar') }} m
        ON m.ticker = u.ticker AND m.rebalance_date = u.rebalance_date
    LEFT JOIN {{ ref('gold_quality_pillar') }} q
        ON q.ticker = u.ticker AND q.rebalance_date = u.rebalance_date
)
SELECT * FROM expected
WHERE actual_count IS DISTINCT FROM expected_count
   OR mismatched_pillar_columns
   OR actual_eligible IS DISTINCT FROM (expected_count = 4)
   OR CASE
        WHEN expected_score IS NULL THEN actual_score IS NOT NULL
        WHEN actual_score IS NULL THEN TRUE
        ELSE ABS(actual_score - expected_score) > 1e-9
      END
