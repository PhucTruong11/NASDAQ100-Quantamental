SELECT ticker, trade_date, adjusted_close_price
FROM {{ ref('silver_price_adjusted') }}
WHERE adjusted_close_price <= 0
