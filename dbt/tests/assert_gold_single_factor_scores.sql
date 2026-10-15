{{ config(tags=['gold'], severity='error') }}

{% set mappings = [
    ('gold_growth_pillar', 'growth_score', 'int_growth_ratio', 'growth_zscore'),
    ('gold_momentum_pillar', 'momentum_score', 'int_momentum_ratio', 'momentum_zscore'),
    ('gold_quality_pillar', 'quality_score', 'int_earnings_surprise', 'earnings_surprise_zscore')
] %}

{% for model, score, source, raw_score in mappings %}
SELECT '{{ model }}' AS model_name, g.ticker, g.rebalance_date
FROM {{ ref(model) }} g
LEFT JOIN {{ ref(source) }} f
    ON f.ticker = g.ticker
   AND f.rebalance_date = g.rebalance_date
WHERE CASE
    WHEN f.{{ raw_score }} IS NULL THEN g.{{ score }} IS NOT NULL
    WHEN g.{{ score }} IS NULL THEN TRUE
    ELSE ABS(g.{{ score }} - f.{{ raw_score }}) > 1e-9
END
{% if not loop.last %} UNION ALL {% endif %}
{% endfor %}
