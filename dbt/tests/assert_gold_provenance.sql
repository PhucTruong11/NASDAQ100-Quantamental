{{ config(tags=['gold'], severity='error') }}

{% set models = ['gold_growth_pillar', 'gold_momentum_pillar', 'gold_value_pillar',
                 'gold_quality_pillar', 'gold_compose_score'] %}
{% for model in models %}
SELECT '{{ model }}' AS model_name, g.ticker, g.rebalance_date
FROM {{ ref(model) }} g
LEFT JOIN {{ ref('int_rebalance_universe') }} u
    ON u.ticker = g.ticker AND u.rebalance_date = g.rebalance_date
WHERE g.sector IS DISTINCT FROM u.sector
{% if model == 'gold_compose_score' %}
   OR g.cik IS DISTINCT FROM u.cik
   OR g.membership_quality IS DISTINCT FROM u.membership_quality
   OR g.identifier_quality IS DISTINCT FROM u.identifier_quality
{% endif %}
{% if not loop.last %} UNION ALL {% endif %}
{% endfor %}
