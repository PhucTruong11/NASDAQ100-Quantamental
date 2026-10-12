{{ config(materialized='table') }}

{#
    INTERMEDIATE LAYER: Factor Momentum 12M - 1M (Đà giá).
    - Logic tài chính: Bỏ qua tháng gần nhất (t-1) để triệt tiêu nhiễu Short-term Reversal.
      Công thức: (Price_{t-1} / Price_{t-12}) - 1.
    - Chuẩn hóa: Áp macro zscore_sector_neutral để trung hòa ngành và kẹp ngoại lai [-3, 3].
    - Burn-in: 12 tháng đầu tiên của dataset (2010) sẽ có giá trị NULL vì chưa đủ lịch sử.
#}
WITH base_prices AS (
    SELECT
        u.rebalance_date,
        u.ticker,
        u.sector,
        p.adjusted_close_price AS current_close_price
    FROM {{ ref('int_rebalance_universe') }} u
    LEFT JOIN {{ ref('silver_price_adjusted') }} p
        ON u.ticker = p.ticker
        AND u.rebalance_date = p.trade_date
),

lagged_prices AS (
    SELECT
        rebalance_date,
        ticker,
        sector,
        current_close_price,
        -- Giá cuối tháng trước (t - 1 tháng)
        LAG(current_close_price, 1) OVER (
            PARTITION BY ticker
            ORDER BY rebalance_date
        ) AS price_t_minus_1,
        -- Giá cuối tháng cách đây 1 năm (t - 12 tháng)
        LAG(current_close_price, 12) OVER (
            PARTITION BY ticker
            ORDER BY rebalance_date
        ) AS price_t_minus_12
    FROM base_prices
)


SELECT
    rebalance_date,
    ticker,
    sector,
    price_t_minus_1,
    price_t_minus_12,
    -- Công thức: (P_{t-1} / P_{t-12}) - 1
    -- Phòng thủ chia cho 0 bằng NULLIF
    (price_t_minus_1 / NULLIF(price_t_minus_12, 0)) - 1.0 AS momentum_12_1_raw,
    {{ zscore_sector_neutral('momentum_12_1_raw') }} AS momentum_zscore
FROM lagged_prices