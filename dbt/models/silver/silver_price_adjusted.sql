{{ config(materialized='table') }}

WITH base AS (
    SELECT
        *,
        LAG(close_price) OVER (PARTITION BY ticker ORDER BY trade_date) AS prev_close
    FROM {{ ref('stg_price_eod') }}
),
factors AS (
    SELECT 
        ticker, 
        trade_date, 
        close_price, 
        volume,
        split_ratio,
        -- Dùng giá đóng cửa phiên TRƯỚC ngày ex-div; chỉ áp khi hệ số hợp lệ (0,1)
        CASE 
            WHEN dividend_amount > 0 AND prev_close > dividend_amount
            THEN 1 - (dividend_amount / prev_close)
            ELSE 1.0
         END AS dividend_factor_raw 
    FROM base
),
cumulative AS (
    SELECT
        ticker, 
        trade_date, 
        close_price, 
        volume,
        split_ratio,
        -- DuckDB không có PRODUCT() window -> dùng mẹo EXP(SUM(LN(x)))
        -- Nhân dồn hệ số cổ tức của MỌI ngày SAU ngày này (càng về quá khứ càng bị điều chỉnh nhiều)
        EXP(SUM(ln(dividend_factor_raw)) OVER (
            PARTITION BY ticker
            ORDER BY trade_date DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        )) AS cum_dividend_factor
    FROM factors
)

SELECT
    ticker,
    trade_date,
    close_price AS raw_close_price,
    split_ratio,
    -- Ngày gần nhất có window frame rỗng -> NULL -> coalesce về 1.0 (không điều chỉnh)
    COALESCE(cum_dividend_factor, 1.0) AS dividend_adjustment_factor,
    close_price * COALESCE(cum_dividend_factor, 1.0) AS adjusted_close_price,
    volume
FROM cumulative