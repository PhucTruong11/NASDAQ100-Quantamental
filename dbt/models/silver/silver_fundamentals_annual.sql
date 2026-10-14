{{ config(materialized='view') }}

-- FY describes the filing, not this fact's duration. V1 uses annual USD
-- flows (including 52/53-week years), not rolling TTM or transition periods.
SELECT * FROM {{ ref('silver_fundamentals_pit') }}
WHERE unit_of_measure = 'USD'
  AND period_duration_days BETWEEN 330 AND 400
  AND period_start_date IS NOT NULL
  AND period_end_date >= period_start_date
