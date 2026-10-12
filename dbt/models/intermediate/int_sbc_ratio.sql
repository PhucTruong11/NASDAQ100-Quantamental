{{ config(materialized='table') }}

{#
    INTERMEDIATE LAYER: Factor Pha loãng - SBC Ratio (Value / Quality Pillar).
    - Công thức: SBC Ratio = Share-Based Compensation / Revenue.
    - As-of Point-in-time: Chỉ dùng BCTC năm (10-K) đã công bố trước rebalance_date.
    - Fallback Doanh thu: Revenues -> RevenueFromContract... -> SalesRevenueNet.
    - Đảo dấu Z-Score: Nhân -1.0 vì SBC càng cao càng pha loãng cổ đông (xấu).
#}

WITH sbc_components AS (
    SELECT
        u.rebalance_date,
        u.ticker,
        u.sector,
        sbc.metric_value AS sbc,
        rev.metric_value AS revenue
    FROM {{ ref('int_rebalance_universe') }} u

    -- Lấy Chi phí trả thưởng bằng cổ phiếu (SBC) năm gần nhất
    LEFT JOIN LATERAL (
        SELECT f.metric_value
        FROM {{ ref('silver_fundamentals_pit') }} f
        WHERE f.cik = u.cik
          AND f.metric_tag = 'ShareBasedCompensation'
          AND f.fiscal_period = 'FY'
          AND f.available_date <= u.rebalance_date
        ORDER BY f.period_end_date DESC, f.available_date DESC
        LIMIT 1
    ) sbc ON TRUE

    -- Lấy Doanh thu năm gần nhất kèm Fallback chain 3 cấp
    LEFT JOIN LATERAL (
        SELECT f.metric_value
        FROM {{ ref('silver_fundamentals_pit') }} f
        WHERE f.cik = u.cik
          AND f.metric_tag IN (
              'Revenues',
              'RevenueFromContractWithCustomerExcludingAssessedTax',
              'SalesRevenueNet'
          )
          AND f.fiscal_period = 'FY'
          AND f.available_date <= u.rebalance_date
        ORDER BY 
            f.period_end_date DESC,
            CASE f.metric_tag
                WHEN 'Revenues' THEN 1
                WHEN 'RevenueFromContractWithCustomerExcludingAssessedTax' THEN 2
                WHEN 'SalesRevenueNet' THEN 3
            END ASC,
            f.available_date DESC
        LIMIT 1
    ) rev ON TRUE
),

calculated_ratios AS (
    SELECT
        rebalance_date,
        ticker,
        sector,
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
    sbc,
    revenue,
    sbc_ratio_raw,
    -- Đảo dấu: Nhân -1.0 để SBC thấp (ít pha loãng) có điểm Z-Score dương cao (tốt)
    (-1.0 * {{ zscore_sector_neutral('sbc_ratio_raw') }}) AS sbc_zscore
FROM calculated_ratios
