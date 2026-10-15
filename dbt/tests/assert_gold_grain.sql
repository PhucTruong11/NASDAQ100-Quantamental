{{ config(tags=['gold'], severity='error') }}

{% set models = [
    'gold_growth_pillar',
    'gold_momentum_pillar',
    'gold_value_pillar',
    'gold_quality_pillar',
    'gold_compose_score'
] %}

{% for model in models %}
SELECT
    '{{ model }}' AS model_name,
    ticker,
    rebalance_date,
    COUNT(*) AS row_count
FROM {{ ref(model) }}
GROUP BY 1, 2, 3
HAVING COUNT(*) > 1
{% if not loop.last %} UNION ALL {% endif %}
{% endfor %}
