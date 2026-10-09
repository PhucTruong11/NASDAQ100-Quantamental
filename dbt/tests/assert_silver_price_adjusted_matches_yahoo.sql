{{ config(severity='error', warn_if='>0', error_if='>500') }}

SELECT
    s.ticker,
    s.trade_date,
    s.adjusted_close_price,
    stg.adj_close_price,
    ABS(s.adjusted_close_price - stg.adj_close_price) / NULLIF(stg.adj_close_price, 0) as pct_diff
FROM {{ ref('silver_price_adjusted') }} s
JOIN {{ ref('stg_price_eod')}} stg
    ON s.ticker = stg.ticker and s.trade_date = stg.trade_date
WHERE ABS(s.adjusted_close_price - stg.adj_close_price) / NULLIF (stg.adj_close_price, 0) > 0.005