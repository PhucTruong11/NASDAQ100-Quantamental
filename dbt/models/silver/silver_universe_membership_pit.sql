{{ config(materialized='table') }}

-- History has explicit provenance and half-open dates; snapshots are observations
-- from capture onward, never evidence that today's members existed in 2010.
WITH history AS (
    SELECT ticker, NULLIF(CAST(cik AS VARCHAR), '') AS cik, company,
        CAST(valid_from AS TIMESTAMP) AS valid_from,
        CAST(valid_to AS TIMESTAMP) AS valid_to,
        membership_quality, identifier_quality, source_url
    FROM {{ ref('universe_membership_history') }}
), cutoff AS (
    SELECT MAX(valid_to) AS history_end FROM history
)
SELECT * FROM history
UNION ALL
SELECT s."Ticker", s."CIK", s."Company",
    GREATEST(CAST(CAST(s.dbt_valid_from AS DATE) + INTERVAL 1 DAY AS TIMESTAMP),
        COALESCE(c.history_end, s.dbt_valid_from)) AS valid_from,
    CAST(CAST(s.dbt_valid_to AS DATE) + INTERVAL 1 DAY AS TIMESTAMP) AS valid_to,
    'live_observation' AS membership_quality,
    'current_mapping' AS identifier_quality,
    'https://yfiua.github.io/index-constituents/constituents-nasdaq100.csv' AS source_url
FROM {{ ref('silver_universe_membership') }} s
CROSS JOIN cutoff c
WHERE s.dbt_valid_to IS NULL
   OR CAST(CAST(s.dbt_valid_to AS DATE) + INTERVAL 1 DAY AS TIMESTAMP) > GREATEST(CAST(CAST(s.dbt_valid_from AS DATE) + INTERVAL 1 DAY AS TIMESTAMP),
        COALESCE(c.history_end, s.dbt_valid_from))
