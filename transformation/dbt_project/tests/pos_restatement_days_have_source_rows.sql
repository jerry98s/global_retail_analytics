-- Blocking completeness check for an explicit POS restatement: every day in
-- pos_restate_from..pos_restate_to must have source lines, unless it is listed
-- in pos_restate_empty_dates. A listed day must have none. Without this, a
-- missing delivery would reconcile as zero source lines against zero fact
-- lines once its partition reads empty. Outside a restatement it returns
-- no rows.

{% set restate = pos_restate_range() %}
{% if restate %}

with restate_days as (
    {% for day in restate.dates %}
    select cast('{{ day }}' as date) as restate_date
    {% if not loop.last %}union all{% endif %}
    {% endfor %}
),

source_days as (
    select transaction_date as restate_date, count(*) as source_line_count
    from {{ ref('stg_pos_transactions') }}
    where transaction_date between cast('{{ restate.from_date }}' as date)
                               and cast('{{ restate.to_date }}' as date)
      and dt between cast('{{ restate.from_date }}' as date)
                 and cast('{{ restate.to_date }}' as date)
    group by transaction_date
),

checked as (
    select
        d.restate_date,
        coalesce(s.source_line_count, 0) as source_line_count,
        {% if restate.empty_dates %}
        d.restate_date in (
            {%- for day in restate.empty_dates -%}
            cast('{{ day }}' as date){% if not loop.last %}, {% endif %}
            {%- endfor -%}
        )
        {% else %}
        false
        {% endif %} as is_authorized_empty
    from restate_days d
    left join source_days s
      on d.restate_date = s.restate_date
)

select
    restate_date,
    source_line_count,
    case
        when is_authorized_empty then 'authorized_empty_day_has_source_lines'
        else 'missing_source_day'
    end as problem
from checked
where (source_line_count = 0 and not is_authorized_empty)
   or (source_line_count > 0 and is_authorized_empty)

{% else %}

select
    cast(null as date) as restate_date,
    cast(null as bigint) as source_line_count,
    cast(null as varchar) as problem
from {{ ref('stg_pos_transactions') }}
where 1 = 0

{% endif %}
