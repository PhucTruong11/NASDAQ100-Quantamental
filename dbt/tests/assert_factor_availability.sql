SELECT ticker, rebalance_date, 'growth' AS factor
FROM {{ ref('int_growth_ratio') }}
WHERE revenue_available_date > rebalance_date
   OR income_available_date > rebalance_date
   OR previous_revenue_available_date > rebalance_date
   OR DATE_DIFF('day', period_start_curr, period_end_curr) NOT BETWEEN 330 AND 400
   OR DATE_DIFF('day', previous_period_end_date, period_end_curr) NOT BETWEEN 330 AND 400
   OR DATE_DIFF('day', period_end_curr, rebalance_date) > {{ var('max_annual_age_days', 550) | int }}
UNION ALL
SELECT ticker, rebalance_date, 'fcf'
FROM {{ ref('int_fcf_yield') }}
WHERE ocf_available_date > rebalance_date OR capex_available_date > rebalance_date
   OR shares_available_date > rebalance_date OR shares_measurement_date > rebalance_date
   OR DATE_DIFF('day', period_start_date, period_end_date) NOT BETWEEN 330 AND 400
   OR (capex IS NULL AND free_cash_flow IS NOT NULL)
   OR (issuer_ticker_count != 1 AND market_cap IS NOT NULL)
   OR DATE_DIFF('day', period_end_date, rebalance_date) > {{ var('max_annual_age_days', 550) | int }}
   OR DATE_DIFF('day', shares_measurement_date, rebalance_date) > {{ var('max_shares_age_days', 400) | int }}
UNION ALL
SELECT ticker, rebalance_date, 'sbc'
FROM {{ ref('int_sbc_ratio') }}
WHERE revenue_available_date > rebalance_date OR sbc_available_date > rebalance_date
   OR DATE_DIFF('day', period_start_date, period_end_date) NOT BETWEEN 330 AND 400
   OR DATE_DIFF('day', period_end_date, rebalance_date) > {{ var('max_annual_age_days', 550) | int }}
UNION ALL
SELECT ticker, rebalance_date, 'quality_proxy'
FROM {{ ref('int_earnings_surprise') }}
WHERE current_income_available_date > rebalance_date
   OR previous_income_available_date > rebalance_date
   OR DATE_DIFF('day', period_start_curr, period_end_curr) NOT BETWEEN 65 AND 115
   OR DATE_DIFF('day', period_end_prev, period_end_curr) NOT BETWEEN 335 AND 395
   OR DATE_DIFF('day', period_end_curr, rebalance_date) > {{ var('max_quarter_age_days', 200) | int }}
