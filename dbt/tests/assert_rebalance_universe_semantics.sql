SELECT ticker, rebalance_date, 'outside_membership_window' AS problem
FROM {{ ref('int_rebalance_universe') }}
WHERE membership_valid_from >= rebalance_date + INTERVAL 1 DAY
   OR membership_valid_to <= rebalance_date
   {% if not var('allow_observed_universe', false) %}
   OR membership_quality != 'effective'
   {% endif %}
UNION ALL
SELECT ticker, rebalance_date, 'duplicate_grain'
FROM {{ ref('int_rebalance_universe') }}
GROUP BY 1, 2 HAVING COUNT(*) > 1
UNION ALL
-- Empty strict coverage is a failure, not a successful 0-row backtest.
SELECT NULL, NULL, 'no_verified_universe_coverage_import_effective_history_or_explicitly_enable_observation_mode'
WHERE NOT EXISTS (SELECT 1 FROM {{ ref('int_rebalance_universe') }})
