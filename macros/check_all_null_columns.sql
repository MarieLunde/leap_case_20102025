{% macro check_all_null_columns(model_name) %}
    {% set relation = ref(model_name) %}
    {% set columns = adapter.get_columns_in_relation(relation) %}

    {% set all_null_cols = [] %}
    {% for col in columns %}
        {% set query %}
            SELECT
                COUNT(*) AS total_rows,
                COUNT({{ col.name }}) AS non_nulls
            FROM {{ relation }}
        {% endset %}

        {% set result = run_query(query) %}
        {% if execute %}
            {% set total_rows = result.columns[0].values()[0] %}
            {% set non_nulls = result.columns[1].values()[0] %}
        {% else %}
            {% set total_rows = 1 %}
            {% set non_nulls = 1 %}
        {% endif %}

        {% if non_nulls == 0 %}
            {% do all_null_cols.append(col.name) %}
        {% endif %}
    {% endfor %}

    {% if all_null_cols | length > 0 %}
        {{ log("Columns that are entirely NULL in " ~ model_name ~ ": " ~ all_null_cols | join(', '), info=True) }}
    {% else %}
        {{ log("No all-null columns found in " ~ model_name, info=True) }}
    {% endif %}
{% endmacro %}
