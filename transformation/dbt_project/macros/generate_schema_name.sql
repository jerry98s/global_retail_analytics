{# Use the configured custom schema verbatim instead of dbt's default
   "<target_schema>_<custom_schema>" concatenation. This makes models land in
   the bare schemas (staging, intermediate, finance, marketing, summary,
   serving) that the hand-written Redshift DDL, foreign keys, and serving
   views reference.

   WAP (ADR-009): when var('wap_phase') == 'pending', Gold marts
   (finance / marketing / summary) are routed to the *_pending schemas so a
   failing audit never touches live tables. staging / intermediate / serving
   / bronze are never redirected. #}
{% set wap_gold_schemas = ['finance', 'marketing', 'summary'] %}

{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {%- set base = custom_schema_name | trim -%}
        {%- if var('wap_phase', 'live') == 'pending' and base in wap_gold_schemas -%}
            {{ base }}_pending
        {%- else -%}
            {{ base }}
        {%- endif -%}
    {%- endif -%}
{%- endmacro %}

{# Returns the relation a model's incremental anchor should read from.
   During a WAP pending build, `this` points at the (possibly empty) pending
   relation, so the lookback must read the last committed LIVE table instead.
   When wap_phase == 'live' this is just `this`. #}
{% macro wap_prior_state(node_relation=none) -%}
    {%- set rel = node_relation if node_relation is not none else this -%}
    {%- if var('wap_phase', 'live') == 'pending' and rel.schema.endswith('_pending') -%}
        {{ api.Relation.create(database=rel.database, schema=rel.schema[:-8], identifier=rel.identifier) }}
    {%- else -%}
        {{ rel }}
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
