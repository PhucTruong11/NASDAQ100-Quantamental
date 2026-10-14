{{ config(materialized='table') }}

{#
    SILVER LAYER: Tính toán giá đóng cửa điều chỉnh cổ tức (Adjusted Close Price).
    - Phương pháp: Chiết khấu ngược từ hiện tại về quá khứ qua hệ số cổ tức (1 - div / prev_close).
    - Giải pháp kỹ thuật: DuckDB thiếu PRODUCT() window -> dùng đồng nhất thức EXP(SUM(LN(factor)))
      trên khung cửa sổ ORDER BY trade_date DESC để nhân dồn lũy kế.
    - Đảm bảo khớp số liệu Yahoo Finance với dung sai < 0.5%.
#}
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
    -- Undo only future splits in Yahoo's split-adjusted Close to recover
    -- the nominal price traded on this date. Return prices stay unchanged.
    close_price * COALESCE(EXP(SUM(LN(CASE WHEN split_ratio > 0 THEN split_ratio ELSE 1.0 END)) OVER (
        PARTITION BY ticker ORDER BY trade_date DESC
        ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
    )), 1.0) AS nominal_close_price,
    -- Ngày gần nhất có window frame rỗng -> NULL -> coalesce về 1.0 (không điều chỉnh)
    COALESCE(cum_dividend_factor, 1.0) AS dividend_adjustment_factor,
    close_price * COALESCE(cum_dividend_factor, 1.0) AS adjusted_close_price,
    volume
FROM cumulative
