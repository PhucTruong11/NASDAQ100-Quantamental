SELECT * FROM {{ ref('silver_fundamentals_annual') }}
WHERE period_duration_days NOT BETWEEN 330 AND 400
   OR unit_of_measure != 'USD'
   OR period_start_date IS NULL
   OR available_date <= filing_date
