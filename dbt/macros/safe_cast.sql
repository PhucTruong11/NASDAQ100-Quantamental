{%% macro safe_cast(column_name, type) %}
    try_cast({{ column }}) as ({{type}})
{% endmacro %}