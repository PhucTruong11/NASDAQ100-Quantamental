{{ config(materialized='table')}}
SELECT
    {{ clean_string('Company')}} as "Company",
    {{ clean_string('Ticker')}} as "Ticker",
    {{ clean_string('CIK')}} as "CIK"
FROM read_parquet('{{ var("raw_universe_glob", "../data/raw/universe/universe.parquet")}}')

