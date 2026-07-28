{# Use the configured custom schema verbatim instead of dbt's default
   "<target_schema>_<custom_schema>" concatenation. This makes models land in
   the bare schemas (staging, intermediate, finance, marketing, summary,
   serving) that the hand-written Redshift DDL, foreign keys, and serving
   views reference. #}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}

{# Adapter-aware datediff: Redshift takes bare unit keyword, DuckDB takes quoted string. #}
{% macro datediff_unit(unit, start_expr, end_expr) %}
    {% if target.type == 'duckdb' -%}
    datediff('{{ unit }}', {{ start_expr }}, {{ end_expr }})
    {%- else -%}
    datediff({{ unit }}, {{ start_expr }}, {{ end_expr }})
    {%- endif %}
{% endmacro %}

{# Adapter-aware dateadd: Redshift takes bare unit keyword, DuckDB takes quoted string. #}
{% macro dateadd_unit(unit, interval_expr, base_expr) %}
    {% if target.type == 'duckdb' -%}
    date_add({{ base_expr }}, ({{ interval_expr }}) * INTERVAL 1 {{ unit | upper }})
    {%- else -%}
    dateadd({{ unit }}, {{ interval_expr }}, {{ base_expr }})
    {%- endif %}
{% endmacro %}
