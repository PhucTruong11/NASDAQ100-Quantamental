{{ config(materialized='table') }}

{#
    BRONZE LAYER: Nạp bảng ánh xạ ngành GICS từ CSV/Parquet.
    - Cung cấp cặp mã: Ticker -> Sector -> Industry.
    - Đầu vào thiết yếu cho macro trung hòa ngành (sector neutral).
#}
SELECT
    {{ clean_string('ticker') }}   AS "Ticker",
    {{ clean_string('sector') }}   AS "Sector",
    {{ clean_string('industry') }} AS "Industry"
FROM {{ source('raw', 'sector') }}
