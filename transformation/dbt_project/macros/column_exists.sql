{# True when the relation already has the column. A macro so a dbt unit test
   can override it; parse time (execute = false) reports false. #}
{% macro column_exists(relation, column_name) %}
    {%- if execute -%}
        {%- set names = adapter.get_columns_in_relation(relation)
            | map(attribute='name') | map('lower') | list -%}
        {{ return(column_name | lower in names) }}
    {%- else -%}
        {{ return(false) }}
    {%- endif -%}
{% endmacro %}
