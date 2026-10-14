{{ config(materialized='table') }}

{#
    INTERMEDIATE LAYER: Factor Tăng trưởng - Rule of 40 (Growth Pillar).
    - Công thức: Rule of 40 = Revenue Growth YoY + Net Margin.
    - As-of Point-in-time: Dùng annual USD flows có duration 330-400 ngày đã công bố trước rebalance_date.
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
        rev.period_start_date AS period_start_curr,
        rev.available_date AS revenue_available_date,
        ni.available_date AS income_available_date,
        ni.metric_value     AS net_income_curr
    FROM {{ ref('int_rebalance_universe') }} u

    -- Lấy Doanh thu năm gần nhất kèm Fallback chain
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

    -- Lấy Lợi nhuận ròng năm gần nhất
    LEFT JOIN LATERAL (
        SELECT f.*
        FROM {{ ref('silver_fundamentals_annual') }} f
        WHERE f.cik = u.cik
          AND f.metric_tag = 'NetIncomeLoss'
          AND f.period_start_date = rev.period_start_date
          AND f.period_end_date = rev.period_end_date
          AND f.available_date <= u.rebalance_date
        ORDER BY 
            f.period_end_date DESC,
            f.available_date DESC, f.period_start_date DESC
        LIMIT 1
    ) ni ON TRUE
),

with_previous_year AS (
    SELECT
        c.*,
        prev.metric_value AS rev_prev,
        prev.available_date AS previous_revenue_available_date,
        prev.period_end_date AS previous_period_end_date
    FROM current_fundamentals c
    LEFT JOIN LATERAL (
        SELECT f.*
        FROM {{ ref('silver_fundamentals_annual') }} f
        WHERE f.cik = c.cik
          AND f.metric_tag IN (
              'Revenues',
              'RevenueFromContractWithCustomerExcludingAssessedTax',
              'SalesRevenueNet'
          )
          AND f.available_date <= c.rebalance_date
          AND DATE_DIFF('day', f.period_end_date, c.period_end_curr) BETWEEN 330 AND 400
          AND DATE_DIFF('day', f.period_start_date, c.period_start_curr) BETWEEN 330 AND 400
        ORDER BY 
            f.period_end_date DESC,
            CASE f.metric_tag
                WHEN 'Revenues' THEN 1
                WHEN 'RevenueFromContractWithCustomerExcludingAssessedTax' THEN 2
                WHEN 'SalesRevenueNet' THEN 3
            END ASC,
            f.available_date DESC, f.period_start_date DESC
        LIMIT 1
    ) prev ON TRUE
),

calculated_ratios AS (
    SELECT
        rebalance_date,
        ticker,
        sector,
        period_start_curr, period_end_curr, previous_period_end_date,
        revenue_available_date, income_available_date, previous_revenue_available_date,
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
    period_start_curr, period_end_curr, previous_period_end_date,
    revenue_available_date, income_available_date, previous_revenue_available_date,
    rev_growth_yoy,
    net_margin,
    rule_of_40_raw,
    {{ zscore_sector_neutral('rule_of_40_raw') }} AS growth_zscore
FROM calculated_ratios
