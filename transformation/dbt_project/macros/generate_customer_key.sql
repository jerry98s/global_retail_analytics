{# Deterministic customer_key from a canonical string (include type prefix in the expression).
   Redshift: strtol(hex, 16) parses a hex substring to bigint.
   DuckDB : cast('0x' || hex as bigint) parses hex with 0x prefix.
   Both yield a deterministic int in [1, 10^8) for the same input string. #}
{% macro generate_customer_key(canonical_expression) %}
    {% if target.type == 'duckdb' -%}
    abs(mod(cast('0x' || substring(md5({{ canonical_expression }}), 1, 8) as bigint), 100000000)) + 1
    {%- else -%}
    abs(mod(strtol(substring(md5({{ canonical_expression }}), 1, 8), 16), 100000000)) + 1
    {%- endif %}
{% endmacro %}
