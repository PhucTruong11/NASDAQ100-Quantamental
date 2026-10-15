{{ config(tags=['gold'], severity='error') }}

{% set scores = [
    ('gold_growth_pillar', 'growth_score'),
    ('gold_momentum_pillar', 'momentum_score'),
    ('gold_value_pillar', 'value_score'),
    ('gold_quality_pillar', 'quality_score'),
    ('gold_compose_score', 'composite_score')
] %}

{% for model, score in scores %}
SELECT '{{ model }}' AS model_name, ticker, rebalance_date,
       {{ score }} AS bad_score
FROM {{ ref(model) }}
WHERE {{ score }} IS NOT NULL
  AND (NOT ISFINITE({{ score }}) OR ABS({{ score }}) > 3.0 + 1e-9)
{% if not loop.last %} UNION ALL {% endif %}
{% endfor %}
