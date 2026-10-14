{{ config(severity='warn') }}

-- Do not shrink index membership to hide missing issuers or delisted prices.
-- These rows must be audited before interpreting a factor backtest.
SELECT u.ticker, u.rebalance_date,
    u.cik IS NULL AS missing_cik,
    p.ticker IS NULL AS missing_price,
    s.sector IS NULL AS missing_sector
FROM {{ ref('int_rebalance_universe') }} u
LEFT JOIN {{ ref('silver_price_adjusted') }} p
    ON p.ticker = u.ticker AND p.trade_date = u.rebalance_date
LEFT JOIN {{ ref('stg_sector') }} s ON s.ticker = u.ticker
WHERE u.cik IS NULL OR p.ticker IS NULL OR s.sector IS NULL
