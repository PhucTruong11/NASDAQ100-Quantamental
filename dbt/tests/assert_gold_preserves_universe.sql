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
    COALESCE(u.ticker, g.ticker) AS ticker,
    COALESCE(u.rebalance_date, g.rebalance_date) AS rebalance_date,
    CASE WHEN u.ticker IS NULL THEN 'extra_gold_key'
         ELSE 'missing_gold_key' END AS problem
FROM {{ ref('int_rebalance_universe') }} u
FULL OUTER JOIN {{ ref(model) }} g
    ON g.ticker = u.ticker
   AND g.rebalance_date = u.rebalance_date
WHERE u.ticker IS NULL OR g.ticker IS NULL
{% if not loop.last %} UNION ALL {% endif %}
{% endfor %}
