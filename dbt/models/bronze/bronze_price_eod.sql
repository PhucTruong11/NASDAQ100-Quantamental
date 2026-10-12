{{ config(materialized='table')}}

{#
    BRONZE LAYER: Nạp dữ liệu giá EOD hàng ngày từ Parquet.
    - Nhiệm vụ: Đọc trực tiếp các file Parquet giá, chuẩn hóa kiểu dữ liệu.
    - Lưu ý: Cột Close đã tự điều chỉnh split từ Yahoo, nhưng CHƯA điều chỉnh cổ tức.
#}
SELECT
    {{ safe_cast('"Date"', 'date') }}           as "Date",
    {{ safe_cast('"Open"', 'double') }}         as "Open",
    {{ safe_cast('"High"', 'double') }}         as "High",
    {{ safe_cast('"Low"', 'double') }}          as "Low",
    {{ safe_cast('"Close"', 'double') }}        as "Close",
    {{ safe_cast('"Adj Close"', 'double') }}    as "Adj Close",
    {{ safe_cast('"Volume"', 'bigint') }}       as "Volume",
    {{ safe_cast('"Dividends"', 'double') }}    as "Dividends",
    {{ safe_cast('"Stock Splits"', 'double') }} as "Stock Splits",
    {{ clean_string('"Ticker"') }}              as "Ticker"
FROM {{ source('raw', 'prices') }}