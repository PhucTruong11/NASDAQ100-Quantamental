{{ config(materialized='table') }}

{#
    INTERMEDIATE LAYER: Factor Bất ngờ Lợi nhuận - Earnings Surprise (Momentum / Quality Pillar).
    - Công thức: SUE Drift = (Net Income Quý t - Net Income Quý t-1Y) / |Net Income Quý t-1Y|.
    - Chống Stock Split: Dùng Net Income (USD) thay vì EPS thô để tránh sai lệch khi chia tách cổ phiếu.
    - As-of Point-in-Time: Chỉ lấy BCTC quý (10-Q/10-K) đã công bố trước rebalance_date.
    - Chuẩn hóa: Áp macro zscore_sector_neutral để trung hòa ngành và kẹp [-3, 3].
#}

WITH current_quarter AS (
    SELECT
        u.rebalance_date,
        u.ticker,
        u.cik,
        u.sector,
        ni.metric_value    AS ni_curr,
        ni.period_end_date AS period_end_curr
    FROM {{ ref('int_rebalance_universe') }} u

    -- Lấy Lợi nhuận ròng của quý gần nhất (độ dài kỳ từ 65 đến 115 ngày ~ 1 quý)
    LEFT JOIN LATERAL (
        SELECT f.metric_value, f.period_end_date
        FROM {{ ref('silver_fundamentals_pit') }} f
        WHERE f.cik = u.cik
          AND f.metric_tag = 'NetIncomeLoss'
          AND f.available_date <= u.rebalance_date
          AND DATEDIFF('day', f.period_start_date, f.period_end_date) BETWEEN 65 AND 115
        ORDER BY f.period_end_date DESC, f.available_date DESC
        LIMIT 1
    ) ni ON TRUE
),

with_previous_year_quarter AS (
    SELECT
        c.*,
        prev.metric_value AS ni_prev
    FROM current_quarter c

    -- Lấy Lợi nhuận ròng của quý cùng kỳ 1 năm trước (cách từ 335 đến 395 ngày)
    LEFT JOIN LATERAL (
        SELECT f.metric_value
        FROM {{ ref('silver_fundamentals_pit') }} f
        WHERE f.cik = c.cik
          AND f.metric_tag = 'NetIncomeLoss'
          AND f.available_date <= c.rebalance_date
          AND DATEDIFF('day', f.period_start_date, f.period_end_date) BETWEEN 65 AND 115
          AND f.period_end_date BETWEEN c.period_end_curr - INTERVAL 395 DAY 
                                    AND c.period_end_curr - INTERVAL 335 DAY
        ORDER BY f.period_end_date DESC, f.available_date DESC
        LIMIT 1
    ) prev ON TRUE
),

calculated_ratios AS (
    SELECT
        rebalance_date,
        ticker,
        sector,
        ni_curr,
        ni_prev,
        -- Tăng trưởng Lợi nhuận ròng quý YoY thô
        (ni_curr - ni_prev) / NULLIF(ABS(ni_prev), 0) AS earnings_surprise_raw
    FROM with_previous_year_quarter
)

SELECT
    rebalance_date,
    ticker,
    sector,
    ni_curr,
    ni_prev,
    earnings_surprise_raw,
    {{ zscore_sector_neutral('earnings_surprise_raw') }} AS earnings_surprise_zscore
FROM calculated_ratios
