{{ config(materialized='view') }}

SELECT
    cik,
    {{ trim_string('tag') }}     AS metric_tag,
    value                        AS metric_value,
    {{ clean_string('unit') }}   AS unit_of_measure,
    {{ clean_string('form') }}   AS filing_type,
    fy                           AS fiscal_year,
    fp                           AS fiscal_period,
    filing_date
FROM {{ ref('bronze_company_facts') }}
WHERE form IN ('10-K', '10-Q')
