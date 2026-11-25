{# dim_product versions as half-open validity ranges for an as-of join
   (ADR-016). The first version of a product also covers every date before its
   effective_from, so activity older than the catalog's first record keeps the
   earliest attributes instead of the unknown key. Reads the live table: the
   catalog DAG owns dim_product (ADR-009). #}
{% macro product_versions() %}
(
    select
        product_key,
        product_id,
        case
            when row_number() over (partition by product_id order by effective_from) = 1
                then cast('1900-01-01' as date)
            else effective_from
        end as valid_from,
        effective_from,
        effective_to as valid_to
    from {{ wap_live_ref('dim_product') }}
)
{% endmacro %}

{# Join condition: the version of `versions` valid on `as_of_date`. #}
{% macro product_as_of(versions, as_of_date) -%}
{{ versions }}.valid_from <= {{ as_of_date }}
and ({{ versions }}.valid_to is null or {{ as_of_date }} < {{ versions }}.valid_to)
{%- endmacro %}
