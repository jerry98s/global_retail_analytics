{# Use the configured custom schema verbatim instead of dbt's default
   "<target_schema>_<custom_schema>" concatenation. This makes models land in
   the bare schemas (staging, intermediate, finance, marketing, summary,
   serving) that the hand-written Redshift DDL, foreign keys, and serving
   views reference.

   WAP (ADR-009): when var('wap_phase') == 'pending', Gold marts
   (finance / marketing / summary) are routed to the *_pending schemas so a
   failing audit never touches live tables. staging / intermediate / serving
   / bronze are never redirected.

   The pending tables are clones of live, created by the DAG's clone task before
   dbt runs (orchestration/airflow/plugins/wap_publish.py). Because the relation
   already exists and holds prior state, is_incremental() is true and `this` is
   the correct incremental anchor — no special-casing needed in models.

   Views in Gold schemas (customer_360_view) are NOT redirected: only tables are
   published, so a view built into *_pending would never be promoted and the live
   view would go stale. Views always build in the live schema over live tables,
   which is why the DAGs build them after the publish step.

   Keep the Gold schema list inside the macro — a top-level `{% set %}` in a
   macros file is ignored (UnexpectedJinjaBlockDeprecation) and would silently
   skip pending routing. #}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set wap_gold_schemas = ['finance', 'marketing', 'summary'] -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {%- set base = custom_schema_name | trim -%}
        {%- set is_view = node is not none and node.config.materialized == 'view' -%}
        {%- if var('wap_phase', 'live') == 'pending' and base in wap_gold_schemas and not is_view -%}
            {{ base }}_pending
        {%- else -%}
            {{ base }}
        {%- endif -%}
    {%- endif -%}
{%- endmacro %}

{# Resolves a ref() to its LIVE relation even during a pending build.

   Use this for Gold tables owned by a *different* DAG. Every Gold table has one
   owning DAG (ADR-009); a DAG must never write another DAG's pending table, or
   two concurrent runs clobber each other. fact_sales / fact_inventory_snapshot
   join marketing.dim_product, which catalog_bihourly_product_scd2_refresh owns,
   so they read the last published live version instead of a pending copy.

   ref() is still called, so dbt's lineage and build ordering are unchanged —
   only the schema is rewritten. #}
{% macro wap_live_ref(model_name) -%}
    {%- set rel = ref(model_name) -%}
    {%- if rel.schema.endswith('_pending') -%}
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
