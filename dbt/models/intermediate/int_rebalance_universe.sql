{{ config(materialized='table')}}

WITH rebalance_dates AS (
    -- Tìm ngày giao dịch cuối cùng của mỗi tháng từ bảng giá
    SELECT
        date_trunc('month', trade_date) AS rebalance_month, MAX(trade_date) AS rebalance_date
    FROM {{ ref('silver_price_adjusted') }}
    GROUP BY 1
)

SELECT
     rd.rebalance_date,
    u."Ticker" AS ticker,      -- Chuẩn hóa về snake_case
    u."CIK"    AS cik,         -- Chuẩn hóa về snake_case
    s.sector                   -- Gộp sector sẵn cho các bước z-score sau này
FROM rebalance_dates rd
JOIN {{ ref('silver_universe_membership') }} u
    ON (u.dbt_valid_to > rd.rebalance_date OR u.dbt_valid_to IS NULL)
LEFT JOIN {{ ref('stg_sector') }} s
    ON s.ticker = u."Ticker"