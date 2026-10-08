{{ config(materialized='view') }}

SELECT
    "Date"          AS trade_date,
    "Ticker"        AS ticker,
    "Open"          AS open_price,
    "High"          AS high_price,
    "Low"           AS low_price,
    "Close"         AS close_price,
    "Adj Close"     AS adj_close_price,
    "Volume"        AS volume,
    "Dividends"     AS dividend_amount,
    "Stock Splits"  AS split_ratio
FROM {{ ref('bronze_price_eod') }}
WHERE "Close" IS NOT NULL
  AND "Volume" >= 0