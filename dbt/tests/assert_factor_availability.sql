SELECT ticker, rebalance_date, 'growth' AS factor
FROM {{ ref('int_growth_ratio') }}
WHERE revenue_available_date > rebalance_date
   OR income_available_date > rebalance_date
   OR previous_revenue_available_date > rebalance_date
   OR DATE_DIFF('day', period_start_curr, period_end_curr) NOT BETWEEN 330 AND 400
   OR DATE_DIFF('day', previous_period_end_date, period_end_curr) NOT BETWEEN 330 AND 400
UNION ALL
SELECT ticker, rebalance_date, 'fcf'
FROM {{ ref('int_fcf_yield') }}
WHERE ocf_available_date > rebalance_date OR capex_available_date > rebalance_date
   OR shares_available_date > rebalance_date OR shares_measurement_date > rebalance_date
   OR DATE_DIFF('day', period_start_date, period_end_date) NOT BETWEEN 330 AND 400
   OR (capex IS NULL AND free_cash_flow IS NOT NULL)
   OR (issuer_ticker_count != 1 AND market_cap IS NOT NULL)
UNION ALL
SELECT ticker, rebalance_date, 'sbc'
FROM {{ ref('int_sbc_ratio') }}
WHERE revenue_available_date > rebalance_date OR sbc_available_date > rebalance_date
   OR DATE_DIFF('day', period_start_date, period_end_date) NOT BETWEEN 330 AND 400
