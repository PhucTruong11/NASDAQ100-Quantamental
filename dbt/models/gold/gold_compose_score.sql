{{ config(materialized='table') }}

WITH joined_pillars AS (
    SELECT
        u.rebalance_date,
        u.ticker,
        u.cik,
        u.sector,
        u.membership_quality,
        u.identifier_quality,
        v.value_score,
        g.growth_score,
        m.momentum_score,
        q.quality_score
    FROM {{ ref('int_rebalance_universe') }} u
    LEFT JOIN {{ ref('gold_value_pillar') }} v
        ON v.ticker = u.ticker
        AND v.rebalance_date = u.rebalance_date
    LEFT JOIN {{ ref('gold_growth_pillar') }} g
        ON g.ticker = u.ticker
        AND g.rebalance_date = u.rebalance_date
    LEFT JOIN {{ ref('gold_momentum_pillar') }} m
        ON m.ticker = u.ticker
        AND m.rebalance_date = u.rebalance_date
    LEFT JOIN {{ ref('gold_quality_pillar') }} q
        ON q.ticker = u.ticker
        AND q.rebalance_date = u.rebalance_date
),

coverage AS (
    SELECT
        rebalance_date,
        ticker,
        cik,
        sector,
        membership_quality,
        identifier_quality,
        value_score,
        growth_score,
        momentum_score,
        quality_score,
        CASE WHEN value_score IS NOT NULL THEN 1 ELSE 0 END
        + CASE WHEN growth_score IS NOT NULL THEN 1 ELSE 0 END
        + CASE WHEN momentum_score IS NOT NULL THEN 1 ELSE 0 END
        + CASE WHEN quality_score IS NOT NULL THEN 1 ELSE 0 END
            AS available_pillar_count
    FROM joined_pillars
)

SELECT
    rebalance_date,
    ticker,
    cik,
    sector,
    membership_quality,
    identifier_quality,
    value_score,
    growth_score,
    momentum_score,
    quality_score,
    available_pillar_count,
    CASE
        WHEN available_pillar_count = 4
        THEN 0.25 * value_score
           + 0.25 * growth_score
           + 0.25 * momentum_score
           + 0.25 * quality_score
        ELSE NULL
    END AS composite_score,
    available_pillar_count = 4 AS is_score_eligible
FROM coverage
