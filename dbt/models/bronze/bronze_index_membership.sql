{{ config(materialized='table')}}

{#
    BRONZE LAYER: Nạp danh sách thành viên chỉ số NASDAQ-100 từ Parquet.
    - Cung cấp cặp mã cốt lõi: Ticker <-> CIK.
    - Được snapshot ở tầng Silver để theo dõi biến động thành viên theo thời gian (SCD Type 2).
#}
SELECT
    {{ clean_string('Company')}} as "Company",
    {{ clean_string('Ticker')}} as "Ticker",
    {{ clean_string('CIK')}} as "CIK"
FROM {{ source('raw', 'universe') }}
