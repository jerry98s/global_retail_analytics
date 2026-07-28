{% test no_scd2_overlaps(model, natural_key, effective_from, effective_to) %}
with bounds as (
    select
        {{ natural_key }} as nk,
        {{ effective_from }} as ef,
        coalesce({{ effective_to }}, cast('2099-12-31' as date)) as et
    from {{ model }}
),
violations as (
    select
        a.nk
    from bounds a
    join bounds b
      on a.nk = b.nk
     and (a.ef, a.et) != (b.ef, b.et)
     and a.ef < b.et
     and b.ef < a.et
)
select * from violations
{% endtest %}
