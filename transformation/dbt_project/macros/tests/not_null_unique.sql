{% test not_null_unique(model, column_name) %}
select *
from {{ model }}
where {{ column_name }} is null
union all
select a.*
from {{ model }} a
join (
    select {{ column_name }} as k
    from {{ model }}
    where {{ column_name }} is not null
    group by {{ column_name }}
    having count(*) > 1
) d on a.{{ column_name }} = d.k
{% endtest %}
