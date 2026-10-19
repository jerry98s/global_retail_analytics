{# YYYYMMDD integer date_key from a date/timestamp expression. #}
{% macro date_key_from_date(date_expr) %}
    {% if target.type == 'duckdb' -%}
    cast(strftime(cast({{ date_expr }} as date), '%Y%m%d') as integer)
    {%- else -%}
    cast(to_char(cast({{ date_expr }} as date), 'YYYYMMDD') as integer)
    {%- endif %}
{% endmacro %}

{# Parse YYYYMMDD integer date_key back to a date. #}
{% macro date_from_date_key(date_key_expr) %}
    {% if target.type == 'duckdb' -%}
    cast(strptime(cast({{ date_key_expr }} as varchar), '%Y%m%d') as date)
    {%- else -%}
    to_date(cast({{ date_key_expr }} as varchar), 'YYYYMMDD')
    {%- endif %}
{% endmacro %}
