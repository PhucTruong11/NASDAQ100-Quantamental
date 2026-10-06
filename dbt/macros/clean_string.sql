{% macro clean_string(column_name) %}
    trim(upper({{ column_name}}))
{% endmacro%}