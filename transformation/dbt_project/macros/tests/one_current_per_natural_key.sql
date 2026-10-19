{% test one_current_per_natural_key(model, natural_key, is_current_col) %}
with checks as (
    select
        {{ natural_key }} as nk,
        sum(case when {{ is_current_col }} then 1 else 0 end) as current_count
    from {{ model }}
    group by {{ natural_key }}
)
select *
from checks
where current_count != 1
{% endtest %}
