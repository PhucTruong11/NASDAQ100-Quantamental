SELECT ticker, trade_date, COUNT(*) AS n
FROM {{ ref('stg_price_eod') }}
GROUP BY ticker, trade_date
HAVING COUNT(*) > 1
