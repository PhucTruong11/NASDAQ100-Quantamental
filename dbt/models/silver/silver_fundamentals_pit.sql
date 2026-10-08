{{ config(materializes= 'table') }}

WITH ranked AS (
    SELECT 
            *, 
            ROW_NUMBER() OVER (
                PARTITION BY cik, metric_tag, unit_of_measure, period_start_date, period_end_date
                ORDER BY filing_date ASC, filing_type DESC
            ) AS rn 
    FROM {{ ref('stg_fundamentals') }}
)

SELECT
    cik, metric_tag, metric_value, unit_of_measure,
    filing_type, fiscal_yaer, fiscal_period,
    period_start_date, period_end_date, filing_date
FROM ranked
WHERE rn = 1 