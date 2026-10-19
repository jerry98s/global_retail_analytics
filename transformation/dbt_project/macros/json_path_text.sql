{# Adapter-aware JSON path text extract.
   Redshift: json_extract_path_text(col, 'key')
   DuckDB:   json_extract_string(cast(col as json), 'key')
#}
{% macro json_path_text(column_expr, key) %}
    {% if target.type == 'duckdb' -%}
    json_extract_string(cast({{ column_expr }} as json), '{{ key }}')
    {%- else -%}
    json_extract_path_text({{ column_expr }}, '{{ key }}')
    {%- endif %}
{% endmacro %}
