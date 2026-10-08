{% macro trim_string(column_name) %}
    trim({{ column_name }})
{% endmacro %}