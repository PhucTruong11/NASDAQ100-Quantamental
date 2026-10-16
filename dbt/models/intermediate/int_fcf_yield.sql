{{ config(materialized='table') }}

-- Annual first-reported USD cash flows, matched by both period boundaries.
WITH components AS (
    SELECT u.rebalance_date, u.ticker, u.sector,
        p.nominal_close_price AS price,
        ocf.metric_value AS ocf, capex.metric_value AS capex,
        ocf.period_start_date, ocf.period_end_date,
        ocf.available_date AS ocf_available_date,
        capex.available_date AS capex_available_date,
        shares.metric_value AS reported_shares_outstanding,
        shares.period_end_date AS shares_measurement_date,
        shares.available_date AS shares_available_date,
        shares.metric_tag AS shares_source_tag,
        COALESCE(splits.split_factor, 1.0) AS shares_split_factor,
        -- CIK-level shares cannot value an individual class (e.g. GOOG/GOOGL).
        (SELECT COUNT(DISTINCT other.ticker)
         FROM {{ ref('int_rebalance_universe') }} other
         WHERE other.rebalance_date = u.rebalance_date AND other.cik = u.cik) AS issuer_ticker_count
    FROM {{ ref('int_rebalance_universe') }} u
    LEFT JOIN {{ ref('silver_price_adjusted') }} p
      ON p.ticker = u.ticker AND p.trade_date = u.rebalance_date
    LEFT JOIN LATERAL (
        SELECT f.* FROM {{ ref('silver_fundamentals_annual') }} f
        WHERE f.cik = u.cik AND f.metric_tag = 'NetCashProvidedByUsedInOperatingActivities'
          AND f.available_date <= u.rebalance_date
          AND f.period_end_date <= u.rebalance_date
          AND DATE_DIFF('day', f.period_end_date, u.rebalance_date) <= {{ var('max_annual_age_days', 550) | int }}
        ORDER BY f.period_end_date DESC, f.available_date DESC, f.period_start_date DESC LIMIT 1
    ) ocf ON TRUE
    LEFT JOIN LATERAL (
        SELECT f.* FROM {{ ref('silver_fundamentals_annual') }} f
        WHERE f.cik = u.cik AND f.metric_tag = 'PaymentsToAcquirePropertyPlantAndEquipment'
          AND f.period_start_date = ocf.period_start_date
          AND f.period_end_date = ocf.period_end_date
          AND f.available_date <= u.rebalance_date
        ORDER BY f.available_date DESC LIMIT 1
    ) capex ON TRUE
    LEFT JOIN LATERAL (
        SELECT f.* FROM {{ ref('silver_fundamentals_pit') }} f
        WHERE f.cik = u.cik
          AND f.metric_tag IN ('CommonStockSharesOutstanding', 'dei:EntityCommonStockSharesOutstanding')
          AND f.unit_of_measure = 'SHARES' AND f.period_start_date IS NULL
          AND f.metric_value > 0
          AND f.available_date <= u.rebalance_date
          AND f.period_end_date <= u.rebalance_date
          AND DATE_DIFF('day', f.period_end_date, u.rebalance_date) <= {{ var('max_shares_age_days', 400) | int }}
        ORDER BY f.period_end_date DESC, f.available_date DESC,
                 CASE WHEN f.metric_tag = 'CommonStockSharesOutstanding' THEN 0 ELSE 1 END
        LIMIT 1
    ) shares ON TRUE
    LEFT JOIN LATERAL (
        SELECT EXP(SUM(LN(h.split_ratio))) AS split_factor
        FROM {{ ref('silver_price_adjusted') }} h
        WHERE h.ticker = u.ticker AND h.split_ratio > 0
          AND h.trade_date > shares.period_end_date
          AND h.trade_date <= u.rebalance_date
    ) splits ON TRUE
), amounts AS (
    SELECT *,
        -- Missing Capex stays NULL; reported zero remains zero.
        ocf - ABS(capex) AS free_cash_flow,
        reported_shares_outstanding * shares_split_factor AS shares_outstanding
    FROM components
), ratios AS (
    SELECT *,
        CASE WHEN issuer_ticker_count = 1 AND price > 0
             THEN price * shares_outstanding END AS market_cap,
        free_cash_flow / NULLIF(CASE WHEN issuer_ticker_count = 1 AND price > 0
             THEN price * shares_outstanding END, 0) AS fcf_yield_raw
    FROM amounts
)
SELECT *, {{ zscore_sector_neutral('fcf_yield_raw') }} AS fcf_yield_zscore
FROM ratios
