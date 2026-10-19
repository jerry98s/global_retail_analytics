{# Deterministic product_key for an SCD2 version (product_id + effective_from). #}
{% macro generate_product_key(product_id_expression, effective_from_expression) %}
    {% if target.type == 'duckdb' -%}
    abs(mod(cast('0x' || substring(md5(coalesce(cast({{ product_id_expression }} as varchar), '') || '|' || coalesce(cast({{ effective_from_expression }} as varchar), '')), 1, 8) as bigint), 100000000)) + 1
    {%- else -%}
    abs(mod(strtol(substring(md5(coalesce(cast({{ product_id_expression }} as varchar), '') || '|' || coalesce(cast({{ effective_from_expression }} as varchar), '')), 1, 8), 16), 100000000)) + 1
    {%- endif %}
{% endmacro %}
