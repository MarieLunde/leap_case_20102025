{% macro add_unknown_row(model_relation, unknown_key_name, unknown_values) %}
    SELECT
        {{ unknown_values | join(', ') }}
    UNION ALL
    SELECT *
    FROM {{ model_relation }}
{% endmacro %}
