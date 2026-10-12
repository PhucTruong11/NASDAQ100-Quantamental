{{ config(materialized='table') }}

{#
    INTERMEDIATE LAYER: Factor Tăng trưởng - Rule of 40 (Growth Pillar).
    - Công thức: Rule of 40 = Revenue Growth YoY + Net Margin.
    - As-of Point-in-time: Chỉ dùng BCTC năm (10-K) đã công bố trước rebalance_date.
    - Fallback Doanh thu: Revenues -> RevenueFromContract... -> SalesRevenueNet.
    - Chuẩn hóa: Áp macro zscore_sector_neutral để trung hòa ngành và kẹp [-3, 3].
#}

WITH current_fundamentals AS (
    SELECT
        u.rebalance_date,
        u.ticker,
        u.cik,
        u.sector,
        rev.metric_value    AS rev_curr,
        rev.period_end_date AS period_end_curr,
        ni.metric_value     AS net_income_curr
    FROM {{ ref('int_rebalance_universe') }} u

    -- Lấy Doanh thu năm gần nhất kèm Fallback chain
    LEFT JOIN LATERAL (
        SELECT f.metric_value, f.period_end_date
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

    -- Lấy Lợi nhuận ròng năm gần nhất
    LEFT JOIN LATERAL (
        SELECT f.metric_value
        FROM {{ ref('silver_fundamentals_pit') }} f
        WHERE f.cik = u.cik
          AND f.metric_tag = 'NetIncomeLoss'
          AND f.fiscal_period = 'FY'
          AND f.available_date <= u.rebalance_date
        ORDER BY 
            f.period_end_date DESC,
            f.available_date DESC
        LIMIT 1
    ) ni ON TRUE
),

with_previous_year AS (
    SELECT
        c.*,
        prev.metric_value AS rev_prev
    FROM current_fundamentals c
    LEFT JOIN LATERAL (
        SELECT f.metric_value
        FROM {{ ref('silver_fundamentals_pit') }} f
        WHERE f.cik = c.cik
          AND f.metric_tag IN (
              'Revenues',
              'RevenueFromContractWithCustomerExcludingAssessedTax',
              'SalesRevenueNet'
          )
          AND f.fiscal_period = 'FY'
          AND f.period_end_date < c.period_end_curr  -- Lùi lại niên độ liền trước
        ORDER BY 
            f.period_end_date DESC,
            CASE f.metric_tag
                WHEN 'Revenues' THEN 1
                WHEN 'RevenueFromContractWithCustomerExcludingAssessedTax' THEN 2
                WHEN 'SalesRevenueNet' THEN 3
            END ASC,
            f.available_date DESC
        LIMIT 1
    ) prev ON TRUE
),

calculated_ratios AS (
    SELECT
        rebalance_date,
        ticker,
        sector,
        rev_curr,
        rev_prev,
        net_income_curr,
        -- Tăng trưởng doanh thu YoY
        (rev_curr - rev_prev) / NULLIF(ABS(rev_prev), 0) AS rev_growth_yoy,
        -- Biên lợi nhuận ròng
        net_income_curr / NULLIF(rev_curr, 0) AS net_margin,
        -- Rule of 40 = Growth YoY + Margin
        ((rev_curr - rev_prev) / NULLIF(ABS(rev_prev), 0)) 
        + (net_income_curr / NULLIF(rev_curr, 0)) AS rule_of_40_raw
    FROM with_previous_year
)

SELECT
    rebalance_date,
    ticker,
    sector,
    rev_growth_yoy,
    net_margin,
    rule_of_40_raw,
    {{ zscore_sector_neutral('rule_of_40_raw') }} AS growth_zscore
FROM calculated_ratios
