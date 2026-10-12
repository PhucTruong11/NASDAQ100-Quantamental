{{ config(materialized='table') }}

{#
    INTERMEDIATE LAYER: Factor Định giá - FCF Yield (Value Pillar).
    - Công thức: FCF Yield = (Operating Cash Flow - Capex) / Market Capitalization.
    - Market Cap: Price tại rebalance_date * Shares Outstanding gần nhất.
    - Chuẩn hóa: Áp macro zscore_sector_neutral để trung hòa ngành và kẹp [-3, 3].
#}

WITH fcf_components AS (
    SELECT
        u.rebalance_date,
        u.ticker,
        u.sector,
        p.adjusted_close_price AS price,
        ocf.metric_value       AS ocf,
        capex.metric_value     AS capex,
        shares.metric_value    AS shares_outstanding
    FROM {{ ref('int_rebalance_universe') }} u

    -- Lấy giá tại ngày Rebalance
    LEFT JOIN {{ ref('silver_price_adjusted') }} p
        ON u.ticker = p.ticker
       AND u.rebalance_date = p.trade_date

    -- Lấy Dòng tiền từ hoạt động kinh doanh (OCF)
    LEFT JOIN LATERAL (
        SELECT f.metric_value
        FROM {{ ref('silver_fundamentals_pit') }} f
        WHERE f.cik = u.cik
          AND f.metric_tag = 'NetCashProvidedByUsedInOperatingActivities'
          AND f.fiscal_period = 'FY'
          AND f.available_date <= u.rebalance_date
        ORDER BY f.period_end_date DESC, f.available_date DESC
        LIMIT 1
    ) ocf ON TRUE

    -- Lấy Chi tiêu vốn (Capex)
    LEFT JOIN LATERAL (
        SELECT f.metric_value
        FROM {{ ref('silver_fundamentals_pit') }} f
        WHERE f.cik = u.cik
          AND f.metric_tag = 'PaymentsToAcquirePropertyPlantAndEquipment'
          AND f.fiscal_period = 'FY'
          AND f.available_date <= u.rebalance_date
        ORDER BY f.period_end_date DESC, f.available_date DESC
        LIMIT 1
    ) capex ON TRUE

    -- Lấy Số lượng cổ phiếu lưu hành để tính Vốn hóa
    LEFT JOIN LATERAL (
        SELECT f.metric_value
        FROM {{ ref('silver_fundamentals_pit') }} f
        WHERE f.cik = u.cik
          AND f.metric_tag = 'CommonStockSharesOutstanding'
          AND f.available_date <= u.rebalance_date
        ORDER BY f.period_end_date DESC, f.available_date DESC
        LIMIT 1
    ) shares ON TRUE
),

calculated_yield AS (
    SELECT
        rebalance_date,
        ticker,
        sector,
        price,
        shares_outstanding,
        -- Dòng tiền tự do FCF = OCF - Capex
        (ocf - ABS(COALESCE(capex, 0))) AS free_cash_flow,
        -- Vốn hóa thị trường = Giá * Số cổ phiếu lưu hành
        (price * shares_outstanding)    AS market_cap,
        -- FCF Yield thô
        (ocf - ABS(COALESCE(capex, 0))) / NULLIF(price * shares_outstanding, 0) AS fcf_yield_raw
    FROM fcf_components
)

SELECT
    rebalance_date,
    ticker,
    sector,
    free_cash_flow,
    market_cap,
    fcf_yield_raw,
    {{ zscore_sector_neutral('fcf_yield_raw') }} AS fcf_yield_zscore
FROM calculated_yield
