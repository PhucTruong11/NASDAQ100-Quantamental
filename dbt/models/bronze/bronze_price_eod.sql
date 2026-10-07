{{ config(materialized='table')}}
SELECT
    {{ safe_cast('"Date"', 'date') }}           as "Date",
    {{ safe_cast('"Open"', 'double') }}         as "Open",
    {{ safe_cast('"High"', 'double') }}         as "High",
    {{ safe_cast('"Low"', 'double') }}          as "Low",
    {{ safe_cast('"Close"', 'double') }}        as "Close",
    {{ safe_cast('"Adj Close"', 'double') }}    as "Adj Close",
    {{ safe_cast('"Volume"', 'bigint') }}       as "Volume",
    {{ safe_cast('"Dividends"', 'double') }}    as "Dividends",
    {{ safe_cast('"Stock Splits"', 'double') }} as "Stock Splits",
    {{ clean_string('"Ticker"') }}              as "Ticker"
FROM {{ source('raw', 'prices') }}