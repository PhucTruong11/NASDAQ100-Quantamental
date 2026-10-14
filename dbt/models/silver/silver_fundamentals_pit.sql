{{ config(materialized= 'table') }}

{#
    SILVER LAYER: Điểm thời gian (Point-In-Time - PIT) BCTC đã khử trùng lặp.
    1. Khoá Dedup 5 thành phần: (cik, metric_tag, unit_of_measure, period_start_date, period_end_date)
       = Cùng 1 sự thật tài chính được báo cáo.
    2. Chống Look-Ahead Bias: Sắp xếp theo filing_date ASC để giữ lại con số CÔNG BỐ ĐẦU TIÊN
       mà thị trường thực sự biết tại thời điểm đó (ưu tiên bản 10-K).
#}
WITH ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY cik, metric_tag, unit_of_measure, period_start_date, period_end_date
            ORDER BY filing_date ASC,
                     CASE filing_type WHEN '10-K' THEN 0 ELSE 1 END ASC
        ) AS rn
    FROM {{ ref('stg_fundamentals') }}
)

SELECT
    cik,
    metric_tag,
    metric_value,
    unit_of_measure,
    filing_type,
    fiscal_year,
    fiscal_period,
    period_start_date,
    period_end_date,
    filing_date,
    -- A filing day alone cannot establish availability at that day's close.
    CAST(filing_date + INTERVAL 1 DAY AS DATE) AS available_date,
    DATE_DIFF('day', period_start_date, period_end_date) AS period_duration_days
FROM ranked
WHERE rn = 1
