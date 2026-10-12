{{ config(materialized='view') }}

{#
    STAGING LAYER: Lọc và chuẩn hóa dữ liệu nhóm ngành GICS.
    - Loại bỏ các dòng rác (Ticker IS NULL hoặc Sector IS NULL).
    - Đảm bảo 100/100 mã cổ phiếu có nhóm ngành hợp lệ.
#}
SELECT
    "Ticker"   AS ticker,
    "Sector"   AS sector,
    "Industry" AS industry
FROM {{ ref('bronze_sector') }}
WHERE "Ticker" IS NOT NULL
  AND "Sector" IS NOT NULL
