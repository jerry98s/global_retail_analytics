{# SHA-256 hex digest for SCD2 record_hash (Redshift sha2 vs DuckDB sha256). #}
{% macro record_hash_sha256(expression) %}
    {% if target.type == 'duckdb' -%}
    lower(hex(sha256({{ expression }})))
    {%- else -%}
    sha2({{ expression }}, 256)
    {%- endif %}
{% endmacro %}
