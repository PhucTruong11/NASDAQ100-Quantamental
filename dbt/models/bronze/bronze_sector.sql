{{ config(materialized='table') }}
SELECT
    {{ clean_string('ticker') }}   AS "Ticker",
    {{ clean_string('sector') }}   AS "Sector",
    {{ clean_string('industry') }} AS "Industry"
FROM {{ source('raw', 'sector') }}
