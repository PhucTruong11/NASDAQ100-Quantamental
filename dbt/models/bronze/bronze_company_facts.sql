{{ config(materialized='table')}}

{#
    BRONZE LAYER: Nạp dữ liệu BCTC thô từ SEC EDGAR API (Parquet).
    - Nhiệm vụ: Ép kiểu an toàn (safe_cast), làm sạch chuỗi (clean_string).
    - Nguyên tắc: Giữ nguyên 100% số dòng và hạt (grain) gốc từ SEC, chưa lọc rác.
#}
SELECT
    {{ clean_string('cik')}} as cik,
    {{ safe_cast('"tag"', 'varchar')}} as tag,
    {{ safe_cast('"value"', 'double')}} as value,
    {{ safe_cast('"unit"', 'varchar')}} as unit,
    {{ safe_cast('"form"', 'varchar')}} as form,
    {{ safe_cast('"fy"', 'integer')}} as fy,
    {{ safe_cast('"fp"', 'varchar')}} as fp,
    {{ safe_cast('"period_start"', 'date')}} as period_start,
    {{ safe_cast('"period_end"', 'date')}} as period_end,
    {{ safe_cast('"filing_date"', 'date')}} as filing_date
FROM {{ source('raw', 'edgar') }}