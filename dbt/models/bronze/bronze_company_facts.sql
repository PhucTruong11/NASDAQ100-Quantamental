{{ config(materialized='table')}}
SELECT
    {{ clean_string('cik')}} as cik,
    {{ safe_cast('"tag"', 'varchar')}} as tag,
    {{ safe_cast('"value"', 'double')}} as value,
    {{ safe_cast('"unit"', 'varchar')}} as unit,
    {{ safe_cast('"form"', 'varchar')}} as form,
    {{ safe_cast('"fy"', 'integer')}} as fy,
    {{ safe_cast('"fp"', 'varchar')}} as fp,
    {{ safe_cast('"filing_date"', 'date')}} as filing_date
FROM {{ source('raw', 'edgar') }}