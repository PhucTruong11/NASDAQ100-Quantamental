{{ config(materialized='table')}}

{#
    INTERMEDIATE LAYER: Xương sống không - thời gian (Spatiotemporal Anchor).
    - Grain: 1 ticker x 1 rebalance_date (ngày giao dịch cuối cùng của mỗi tháng).
    - Cửa khẩu chuẩn hóa: Alias "Ticker"/"CIK" từ snapshot Silver thành snake_case (ticker, cik).
    - Tích hợp sẵn nhóm ngành (Sector) cho toàn bộ các model Z-Score phía sau.
#}
WITH rebalance_dates AS (
    -- Lấy phiên giao dịch thực tế cuối cùng mỗi tháng (tự động né ngày nghỉ lễ/cuối tuần)
    SELECT
        date_trunc('month', trade_date) AS rebalance_month, 
        MAX(trade_date)                 AS rebalance_date
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