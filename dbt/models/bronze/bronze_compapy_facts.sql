{{ config(materialized='table')}}
SELECT
    {{ clean_string('cik')}} as cik,
    {{ safe_cast('"tag"', 'varchar')}} as tag,
    {{ safe_cast('"value"', 'double')}} as value,
    {{ safe_cast('"unit"', 'varcher')}} as unit,
    {{ safe_cast('"form"', 'varchar')}} as form,
    {{ safe_cast('"fy"', 'integer')}} as fy,
    {{ safe_cast('"fp"', 'varcher')}} as fp,
    {{ safe_cast('"filing date"', 'date')}} as filing date
FROM read_parquet('{{ var("raw_edgar_glob", "../data/raw/edgar/edgar_*.parquet") }}')