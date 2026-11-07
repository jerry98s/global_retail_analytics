{# Session key for a clickstream event: the producer's session_instance_id
   (envelope 1.3+), else one legacy session per (client_id, session_id). #}
{% macro session_key(instance_col='session_instance_id', client_col='client_id', session_col='session_id') -%}
    case
        when {{ instance_col }} is not null then {{ instance_col }}
        else 'legacy:' || coalesce({{ client_col }}, '') || ':' || coalesce({{ session_col }}, '')
    end
{%- endmacro %}
