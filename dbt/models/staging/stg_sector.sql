{{ config(materialized='view') }}

SELECT
    "Ticker"   AS ticker,
    "Sector"   AS sector,
    "Industry" AS industry
FROM {{ ref('bronze_sector') }}
WHERE "Ticker" IS NOT NULL
  AND "Sector" IS NOT NULL
