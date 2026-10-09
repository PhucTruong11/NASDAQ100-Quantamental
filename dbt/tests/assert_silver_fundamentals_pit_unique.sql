SELECT cik, metric_tag, unit_of_measure, period_start_date, period_end_date, count(*) AS n
FROM {{ ref('silver_fundamentals_pit') }}
GROUP BY cik, metric_tag, unit_of_measure, period_start_date, period_end_date
HAVING count(*) > 1