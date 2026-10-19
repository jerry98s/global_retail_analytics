{% test no_scd2_gaps(model, natural_key, effective_from, effective_to) %}
with ordered as (
    select
        {{ natural_key }} as nk,
        {{ effective_from }} as ef,
        {{ effective_to }} as et,
        lead({{ effective_from }}) over (
            partition by {{ natural_key }}
            order by {{ effective_from }}
        ) as next_ef
    from {{ model }}
),
violations as (
    select nk
    from ordered
    where et is not null
      and next_ef is not null
      and et != next_ef
)
select * from violations
{% endtest %}
