{{ config(materialized= 'table') }}

-- Khoá dedup suy ra từ câu hỏi "cái gì làm 2 dòng thực ra là cùng 1 thứ":
-- cùng công ty + cùng chỉ tiêu + cùng đơn vị + cùng kỳ (period_start/end) = cùng 1 sự thật,
-- dù có thể bị report lại nhiều lần qua các filing khác nhau.
WITH ranked AS (
    SELECT 
        *, 
        ROW_NUMBER() OVER (
            PARTITION BY cik, metric_tag, unit_of_measure, period_start_date, period_end_date                
            ORDER BY filing_date ASC, 
                     CASE filing_type WHEN '10-K' THEN 0 ELSE 1 END ASC -- 10-K ưu tiên (hoặc đảo lại)
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
    filing_date AS available_date
FROM ranked
WHERE rn = 1 