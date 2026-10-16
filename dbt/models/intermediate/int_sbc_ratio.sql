{{ config(materialized='table') }}

{#
    INTERMEDIATE LAYER: Factor Pha loãng - SBC Ratio (Value / Quality Pillar).
    - Công thức: SBC Ratio = Share-Based Compensation / Revenue.
    - As-of Point-in-time: Dùng annual USD flows có duration 330-400 ngày đã công bố trước rebalance_date.
    - Fallback Doanh thu: Revenues -> RevenueFromContract... -> SalesRevenueNet.
    - Đảo dấu Z-Score: Nhân -1.0 vì SBC càng cao càng pha loãng cổ đông (xấu).
#}

WITH sbc_components AS (
    SELECT
        u.rebalance_date,
        u.ticker,
        u.sector,
        sbc.metric_value AS sbc,
        rev.metric_value AS revenue,
        rev.period_start_date, rev.period_end_date,
        rev.available_date AS revenue_available_date,
        sbc.available_date AS sbc_available_date
    FROM {{ ref('int_rebalance_universe') }} u

    -- Lấy Doanh thu năm gần nhất kèm Fallback chain 3 cấp
    LEFT JOIN LATERAL (
        SELECT f.*
        FROM {{ ref('silver_fundamentals_annual') }} f
        WHERE f.cik = u.cik
          AND f.metric_tag IN (
              'Revenues',
              'RevenueFromContractWithCustomerExcludingAssessedTax',
              'SalesRevenueNet'
          )
          AND f.available_date <= u.rebalance_date
          AND DATE_DIFF('day', f.period_end_date, u.rebalance_date) BETWEEN 0 AND {{ var('max_annual_age_days', 550) | int }}
        ORDER BY 
            f.period_end_date DESC,
            CASE f.metric_tag
                WHEN 'Revenues' THEN 1
                WHEN 'RevenueFromContractWithCustomerExcludingAssessedTax' THEN 2
                WHEN 'SalesRevenueNet' THEN 3
            END ASC,
            f.available_date DESC, f.period_start_date DESC
        LIMIT 1
    ) rev ON TRUE
    -- Lấy Chi phí trả thưởng bằng cổ phiếu (SBC) năm gần nhất
    LEFT JOIN LATERAL (
        SELECT f.*
        FROM {{ ref('silver_fundamentals_annual') }} f
        WHERE f.cik = u.cik
          AND f.metric_tag = 'ShareBasedCompensation'
          AND f.period_start_date = rev.period_start_date
          AND f.period_end_date = rev.period_end_date
          AND f.available_date <= u.rebalance_date
        ORDER BY f.period_end_date DESC, f.available_date DESC, f.period_start_date DESC
        LIMIT 1
    ) sbc ON TRUE
),

calculated_ratios AS (
    SELECT
        rebalance_date,
        ticker,
        sector,
        period_start_date, period_end_date, revenue_available_date, sbc_available_date,
        sbc,
        revenue,
        -- Tỷ lệ SBC / Revenue thô
        sbc / NULLIF(revenue, 0) AS sbc_ratio_raw
    FROM sbc_components
)

SELECT
    rebalance_date,
    ticker,
    sector,
    period_start_date, period_end_date, revenue_available_date, sbc_available_date,
    sbc,
    revenue,
    sbc_ratio_raw,
    -- Đảo dấu: Nhân -1.0 để SBC thấp (ít pha loãng) có điểm Z-Score dương cao (tốt)
    (-1.0 * {{ zscore_sector_neutral('sbc_ratio_raw') }}) AS sbc_zscore
FROM calculated_ratios
