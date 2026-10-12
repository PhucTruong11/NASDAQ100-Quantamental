{{ config(materialized='view') }}

{#
    STAGING LAYER: Làm sạch và chuẩn hóa BCTC từ Bronze.
    - Lọc scope: Chỉ giữ lại báo cáo năm (10-K) và báo cáo quý (10-Q), loại bỏ form sửa đổi/khác.
    - Chuẩn hóa tên cột: Chuyển sang snake_case chuẩn (period_start_date, available_date/filing_date).
    - Giữ nguyên số dòng (chưa deduplicate; việc dedup PIT được thực hiện ở tầng Silver).
#}
SELECT
    cik,
    {{ trim_string('tag') }}     AS metric_tag,
    value                        AS metric_value,
    {{ clean_string('unit') }}   AS unit_of_measure,
    {{ clean_string('form') }}   AS filing_type,
    fy                           AS fiscal_year,
    fp                           AS fiscal_period,
    period_start                 AS period_start_date,
    period_end                   AS period_end_date,
    filing_date
FROM {{ ref('bronze_company_facts') }}
WHERE form IN ('10-K', '10-Q')
