WITH inputs AS (
    SELECT c.*,
        f.shares_source_tag,
        DATE_DIFF('day', f.period_end_date, c.rebalance_date) AS fcf_age_days,
        DATE_DIFF('day', s.period_end_date, c.rebalance_date) AS sbc_age_days,
        DATE_DIFF('day', g.period_end_curr, c.rebalance_date) AS growth_age_days,
        DATE_DIFF('day', f.shares_measurement_date, c.rebalance_date) AS shares_age_days,
        DATE_DIFF('day', q.period_end_curr, c.rebalance_date) AS quarter_age_days,
        p.nominal_close_price AS signal_price,
        CASE
            WHEN f.fcf_yield_zscore IS NOT NULL THEN 'available'
            WHEN f.issuer_ticker_count > 1 THEN 'multi_class_market_cap_unsupported'
            WHEN p.ticker IS NULL THEN 'missing_signal_price'
            WHEN f.ocf IS NULL AND EXISTS (
                SELECT 1 FROM silver_fundamentals_annual a
                WHERE a.cik=c.cik AND a.metric_tag='NetCashProvidedByUsedInOperatingActivitiesContinuingOperations'
                  AND a.available_date<=c.rebalance_date
                  AND DATE_DIFF('day', a.period_end_date, c.rebalance_date) BETWEEN 0 AND $build_annual_age
            ) THEN 'continuing_operations_ocf_only'
            WHEN f.ocf IS NULL THEN 'missing_annual_ocf'
            WHEN f.capex IS NULL AND EXISTS (
                SELECT 1 FROM silver_fundamentals_annual a
                WHERE a.cik=c.cik AND a.metric_tag='PaymentsToAcquireProductiveAssets'
                  AND a.available_date<=c.rebalance_date
                  AND a.period_start_date=f.period_start_date AND a.period_end_date=f.period_end_date
            ) THEN 'broader_productive_assets_only'
            WHEN f.capex IS NULL THEN 'missing_same_period_ppe_capex'
            WHEN f.reported_shares_outstanding IS NULL AND EXISTS (
                SELECT 1 FROM silver_fundamentals_pit a
                WHERE a.cik=c.cik
                  AND a.metric_tag IN ('CommonStockSharesOutstanding','dei:EntityCommonStockSharesOutstanding')
                  AND a.unit_of_measure='SHARES' AND a.period_start_date IS NULL
                  AND a.metric_value>0 AND a.available_date<=c.rebalance_date
                  AND DATE_DIFF('day', a.period_end_date, c.rebalance_date)>$build_shares_age
            ) THEN 'stale_instant_shares'
            WHEN f.reported_shares_outstanding IS NULL THEN 'missing_instant_shares'
            WHEN c.sector IS NULL THEN 'missing_sector'
            ELSE 'invalid_ratio_or_normalization'
        END AS fcf_status,
        CASE
            WHEN s.sbc_zscore IS NOT NULL THEN 'available'
            WHEN s.revenue IS NULL AND EXISTS (
                SELECT 1 FROM silver_fundamentals_annual a
                WHERE a.cik=c.cik AND a.metric_tag='RevenueFromContractWithCustomerIncludingAssessedTax'
                  AND a.available_date<=c.rebalance_date
                  AND DATE_DIFF('day', a.period_end_date, c.rebalance_date) BETWEEN 0 AND $build_annual_age
            ) THEN 'gross_revenue_including_tax_only'
            WHEN s.revenue IS NULL THEN 'missing_annual_revenue'
            WHEN s.sbc IS NULL AND EXISTS (
                SELECT 1 FROM silver_fundamentals_annual a
                WHERE a.cik=c.cik AND a.metric_tag='AllocatedShareBasedCompensationExpense'
                  AND a.available_date<=c.rebalance_date
                  AND a.period_start_date=s.period_start_date AND a.period_end_date=s.period_end_date
            ) THEN 'allocated_sbc_not_equivalent'
            WHEN s.sbc IS NULL THEN 'missing_same_period_reported_sbc'
            WHEN c.sector IS NULL THEN 'missing_sector'
            ELSE 'invalid_ratio_or_normalization'
        END AS sbc_status,
        CASE
            WHEN c.growth_score IS NOT NULL THEN 'available'
            WHEN c.sector IS NULL THEN 'missing_sector'
            WHEN g.period_end_curr IS NULL THEN 'missing_supported_annual_revenue'
            WHEN g.rev_growth_yoy IS NULL THEN 'missing_comparable_revenue_pair'
            WHEN g.net_margin IS NULL THEN 'missing_same_period_income'
            ELSE 'invalid_ratio_or_normalization'
        END AS growth_status,
        CASE
            WHEN c.quality_score IS NOT NULL THEN 'available'
            WHEN q.ni_curr IS NULL AND EXISTS (
                SELECT 1 FROM silver_fundamentals_pit a
                WHERE a.cik=c.cik AND a.metric_tag='NetIncomeLoss' AND a.unit_of_measure='USD'
                  AND a.available_date<=c.rebalance_date
                  AND DATE_DIFF('day',a.period_start_date,a.period_end_date) BETWEEN 65 AND 115
                  AND DATE_DIFF('day',a.period_end_date,c.rebalance_date)>$build_quarter_age
            ) THEN 'stale_or_missing_current_quarter_income'
            WHEN q.ni_curr IS NULL THEN 'missing_supported_current_quarter_income'
            WHEN q.ni_prev IS NULL THEN 'missing_prior_year_quarter_income'
            WHEN q.ni_prev=0 THEN 'zero_prior_year_income_denominator'
            WHEN c.sector IS NULL THEN 'missing_sector'
            ELSE 'invalid_ratio_or_normalization'
        END AS quality_status
    FROM gold_compose_score c
    LEFT JOIN int_fcf_yield f USING (ticker, rebalance_date)
    LEFT JOIN int_sbc_ratio s USING (ticker, rebalance_date)
    LEFT JOIN int_growth_ratio g USING (ticker, rebalance_date)
    LEFT JOIN int_earnings_surprise q USING (ticker, rebalance_date)
    LEFT JOIN silver_price_adjusted p
        ON p.ticker=c.ticker AND p.trade_date=c.rebalance_date
)
SELECT *,
    COALESCE(is_score_eligible AND cik IS NOT NULL AND sector IS NOT NULL
        AND signal_price > 0
        AND fcf_age_days BETWEEN 0 AND $annual_age
        AND sbc_age_days BETWEEN 0 AND $annual_age
        AND growth_age_days BETWEEN 0 AND $annual_age
        AND shares_age_days BETWEEN 0 AND $shares_age
        AND quarter_age_days BETWEEN 0 AND $quarter_age, FALSE) AS is_research_signal_eligible
FROM inputs
ORDER BY rebalance_date, ticker
