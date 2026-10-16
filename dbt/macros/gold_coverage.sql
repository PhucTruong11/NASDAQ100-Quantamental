{% macro gold_coverage_inputs() %}
SELECT c.*,
    COALESCE(c.sector, '__MISSING_SECTOR__') AS sector_label,
    f.fcf_yield_zscore, s.sbc_zscore,
    c.cik IS NULL AS missing_cik,
    p.ticker IS NULL AS missing_price,
    c.sector IS NULL AS missing_sector,
    f.ocf IS NULL AS missing_ocf,
    f.capex IS NULL AS missing_capex,
    f.reported_shares_outstanding IS NULL AS missing_shares,
    (f.market_cap IS NULL OR f.market_cap <= 0) AS unavailable_market_cap,
    f.issuer_ticker_count > 1 AS multi_class_issuer,
    s.revenue IS NULL OR s.revenue = 0 AS unavailable_sbc_revenue,
    s.sbc IS NULL AS missing_sbc,
    f.fcf_yield_zscore IS NULL AS missing_fcf_score,
    s.sbc_zscore IS NULL AS missing_sbc_score,
    c.growth_score IS NULL AS missing_growth_score,
    c.momentum_score IS NULL AS missing_momentum_score,
    c.quality_score IS NULL AS missing_quality_proxy_score
FROM {{ ref('gold_compose_score') }} c
LEFT JOIN {{ ref('int_fcf_yield') }} f USING (ticker, rebalance_date)
LEFT JOIN {{ ref('int_sbc_ratio') }} s USING (ticker, rebalance_date)
LEFT JOIN {{ ref('silver_price_adjusted') }} p
    ON p.ticker = c.ticker AND p.trade_date = c.rebalance_date
{% endmacro %}

{% macro gold_coverage_summary(by_sector=false) %}
WITH inputs AS ({{ gold_coverage_inputs() }})
SELECT rebalance_date,
    {% if by_sector %}sector_label AS sector,{% endif %}
    COUNT(*) AS universe_count,
    COUNT(growth_score) AS growth_count,
    COUNT(momentum_score) AS momentum_count,
    COUNT(value_score) AS value_count,
    COUNT(quality_score) AS quality_proxy_count,
    COUNT(composite_score) AS composite_count,
    ROUND(100.0 * COUNT(composite_score) / NULLIF(COUNT(*), 0), 2) AS composite_coverage_pct,
    {% for reason in ['missing_cik', 'missing_price', 'missing_sector', 'missing_ocf',
                      'missing_capex', 'missing_shares', 'unavailable_market_cap',
                      'multi_class_issuer', 'unavailable_sbc_revenue', 'missing_sbc',
                      'missing_fcf_score', 'missing_sbc_score', 'missing_growth_score',
                      'missing_momentum_score', 'missing_quality_proxy_score'] %}
    COUNT(*) FILTER (WHERE {{ reason }}) AS {{ reason }}_count{% if not loop.last %},{% endif %}
    {% endfor %}
FROM inputs
GROUP BY rebalance_date{% if by_sector %}, sector_label{% endif %}
{% endmacro %}

{% macro export_gold_coverage(results) %}
-- depends_on: {{ ref('gold_compose_score') }}
-- depends_on: {{ ref('int_fcf_yield') }}
-- depends_on: {{ ref('int_sbc_ratio') }}
-- depends_on: {{ ref('silver_price_adjusted') }}
{% if execute %}
    {% set gold_models = ['gold_growth_pillar', 'gold_momentum_pillar', 'gold_value_pillar',
                          'gold_quality_pillar', 'gold_compose_score'] %}
    {% set state = namespace(built=[], failed=false, warnings=0) %}
    {% for result in results %}
        {% if result.node.resource_type == 'model' and result.node.name in gold_models
              and result.status == 'success' %}
            {% do state.built.append(result.node.name) %}
        {% endif %}
        {% if result.status not in ['success', 'pass', 'warn'] %}
            {% set state.failed = true %}
        {% endif %}
        {% if result.status == 'warn' %}{% set state.warnings = state.warnings + 1 %}{% endif %}
    {% endfor %}
    {# Never export stale/partially rebuilt Gold as a new complete build. #}
    {% if state.built | length == gold_models | length %}
        {% set prefix = var('gold_coverage_dir', '../data/reports/gold') ~ '/' ~ invocation_id %}
        {% set safe_prefix = prefix | replace("'", "''") %}
        {% set outputs = [
            ('by_period', gold_coverage_summary(false)),
            ('by_sector', gold_coverage_summary(true)),
            ('input_diagnostics', gold_coverage_inputs())
        ] %}
        {% for label, query in outputs %}
            {% do run_query("COPY (" ~ query ~ " ORDER BY rebalance_date, "
                ~ ('ticker' if label == 'input_diagnostics' else ('sector' if label == 'by_sector' else 'universe_count'))
                ~ ") TO '" ~ safe_prefix ~ '-' ~ label ~ ".csv' (HEADER, DELIMITER ',')") %}
        {% endfor %}
        {% set manifest %}
        SELECT '{{ invocation_id }}' AS invocation_id,
            CURRENT_TIMESTAMP AS generated_at,
            {{ 'true' if var('allow_observed_universe', false) else 'false' }} AS allow_observed_universe,
            '{{ "failed" if state.failed else "passed" }}' AS selected_build_validation,
            {{ state.warnings }} AS warning_test_count,
            'quarterly_net_income_yoy_growth_proxy_v1' AS quality_definition,
            STRUCT_PACK(
                annual_flows := {{ var('max_annual_age_days', 550) | int }},
                instant_shares := {{ var('max_shares_age_days', 400) | int }},
                quarterly_income := {{ var('max_quarter_age_days', 200) | int }}
            ) AS input_age_limits_days,
            'coverage_diagnostic_not_backtest_results' AS report_kind,
            (SELECT STRING_AGG(DISTINCT membership_quality, ',') FROM {{ ref('gold_compose_score') }}) AS membership_qualities,
            latest.*
        FROM ({{ gold_coverage_summary(false) }} ORDER BY rebalance_date DESC LIMIT 1) latest
        {% endset %}
        {% do run_query("COPY (" ~ manifest ~ ") TO '" ~ safe_prefix ~ "-run.json' (FORMAT JSON, ARRAY true)") %}
        {% set latest = run_query(gold_coverage_summary(false) ~ ' ORDER BY rebalance_date DESC LIMIT 1') %}
        {% if latest.rows | length %}
            {% do log('Gold coverage ' ~ latest.rows[0][0] ~ ': composite '
                ~ latest.rows[0][6] ~ '/' ~ latest.rows[0][1]
                ~ '; reports: ' ~ prefix ~ '-*.csv / -run.json', info=true) %}
        {% endif %}
    {% endif %}
{% endif %}
{% endmacro %}
