{% macro zscore_sector_neutral(column_name, sector_col='sector', partition_date='rebalance_date') %}
{#
    Macro tính Z-score trung hòa ngành (Sector-Neutral Z-Score) với cơ chế:
    1. Phòng thủ NULL: Nếu giá trị gốc là NULL -> Z-score trả về NULL.
    2. Ngưỡng n >= 5: Chỉ tính z-score theo ngành nếu ngành có >= 5 mã hợp lệ.
       Nếu ngành < 5 mã -> Fallback tính z-score toàn Universe tại ngày rebalance đó.
    3. Winsorize: Kẹp Z-score trong khoảng [-3.0, +3.0] để triệt tiêu ngoại lai.
#}
CASE 
    -- 1. Nếu giá trị thô bị NULL (ví dụ: FPI hoặc chưa đủ 12 tháng lịch sử) -> giữ nguyên NULL
    WHEN {{ column_name }} IS NULL THEN NULL

    ELSE 
        -- 2. Winsorization kẹp giá trị trong khoảng [-3.0, +3.0]
        LEAST(
            GREATEST(
                CASE 
                    -- TRƯỜNG HỢP A: Ngành có >= 5 mã hợp lệ và độ lệch chuẩn > 0 -> Tính theo NGÀNH
                    WHEN COUNT({{ column_name }}) OVER (PARTITION BY {{ partition_date }}, {{ sector_col }}) >= 5 
                         AND STDDEV_SAMP({{ column_name }}) OVER (PARTITION BY {{ partition_date }}, {{ sector_col }}) > 0
                    THEN (
                        {{ column_name }} - AVG({{ column_name }}) OVER (PARTITION BY {{ partition_date }}, {{ sector_col }})
                    ) / STDDEV_SAMP({{ column_name }}) OVER (PARTITION BY {{ partition_date }}, {{ sector_col }})

                    -- TRƯỜNG HỢP B: Ngành < 5 mã (Utilities, Energy...) -> FALLBACK toàn UNIVERSE
                    ELSE (
                        {{ column_name }} - AVG({{ column_name }}) OVER (PARTITION BY {{ partition_date }})
                    ) / NULLIF(STDDEV_SAMP({{ column_name }}) OVER (PARTITION BY {{ partition_date }}), 0)
                END
            , -3.0)
        , 3.0)
END
{% endmacro %}
