{{ config(materialized='table') }}

-- Use full price history and calendar offsets across membership exits/reentries.
WITH monthly_prices AS (
    SELECT ticker, DATE_TRUNC('month', trade_date) AS price_month,
        ARG_MAX(adjusted_close_price, trade_date) AS month_close
    FROM {{ ref('silver_price_adjusted') }}
    GROUP BY 1, 2
), ratios AS (
    SELECT u.rebalance_date, u.ticker, u.sector,
        previous.month_close AS price_t_minus_1,
        year_ago.month_close AS price_t_minus_12,
        previous.month_close / NULLIF(year_ago.month_close, 0) - 1.0 AS momentum_12_1_raw
    FROM {{ ref('int_rebalance_universe') }} u
    LEFT JOIN monthly_prices previous ON previous.ticker = u.ticker
        AND previous.price_month = DATE_TRUNC('month', u.rebalance_date) - INTERVAL 1 MONTH
    LEFT JOIN monthly_prices year_ago ON year_ago.ticker = u.ticker
        AND year_ago.price_month = DATE_TRUNC('month', u.rebalance_date) - INTERVAL 12 MONTH
)
SELECT *, {{ zscore_sector_neutral('momentum_12_1_raw') }} AS momentum_zscore
FROM ratios
