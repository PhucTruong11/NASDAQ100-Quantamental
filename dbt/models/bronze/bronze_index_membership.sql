{{ config(materialized='table')}}
SELECT
    {{ clean_string('Company')}} as "Company",
    {{ clean_string('Ticker')}} as "Ticker",
    {{ clean_string('CIK')}} as "CIK"
FROM {{ source('raw', 'universe') }}
