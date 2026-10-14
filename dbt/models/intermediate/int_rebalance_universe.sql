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
    -- Do not label an unfinished download month as its month-end rebalance.
    WHERE trade_date < DATE_TRUNC('month', CURRENT_DATE)
    GROUP BY 1
)

SELECT
    rd.rebalance_date,
    u.ticker,
    u.cik,
    u.membership_quality,
    u.identifier_quality,
    u.valid_from AS membership_valid_from,
    u.valid_to AS membership_valid_to,
    s.sector
FROM rebalance_dates rd
JOIN {{ ref('silver_universe_membership_pit') }} u
    ON u.valid_from < CAST(rd.rebalance_date + INTERVAL 1 DAY AS TIMESTAMP)
   AND (u.valid_to > rd.rebalance_date OR u.valid_to IS NULL)
   AND (u.membership_quality = 'effective'
        {% if var('allow_observed_universe', false) %}
        OR u.membership_quality IN ('monthly_observation', 'live_observation')
        {% endif %})
LEFT JOIN {{ ref('stg_sector') }} s
    ON s.ticker = u.ticker
