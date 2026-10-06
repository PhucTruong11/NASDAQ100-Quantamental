{% macro safe_cast(column_name, type) %}
    try_cast({{ column_name }} as {{ type }})
{% endmacro %}