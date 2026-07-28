{# Deterministic customer_key from a canonical string (include type prefix in the expression). #}
{% macro generate_customer_key(canonical_expression) %}
    abs(mod(strtol(substring(md5({{ canonical_expression }}), 1, 8), 16), 100000000)) + 1
{% endmacro %}
