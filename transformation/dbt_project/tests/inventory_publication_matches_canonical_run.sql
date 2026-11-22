/*
  The cutoff about to be published is the canonical run that built the fact,
  and it does not move backwards. Reads the source run id, not an aggregate.
*/

with pending as (
    select inventory_run_id, closed_hour_exclusive
    from {{ ref('inventory_publication') }}
),

canonical as (
    select distinct inventory_run_id
    from {{ source('silver', 'inventory_cutoff') }}
),

live as (
    select closed_hour_exclusive
    from {{ source('gold_control', 'inventory_publication') }}
)

select 'run_mismatch' as failure
from pending
where inventory_run_id not in (select inventory_run_id from canonical)
   or (select count(*) from canonical) <> 1

{% if execute and relation_exists(source('gold_control', 'inventory_publication')) %}
union all

select 'cutoff_regressed' as failure
from pending p
where exists (
    select 1
    from live l
    where p.closed_hour_exclusive is null
       or l.closed_hour_exclusive > p.closed_hour_exclusive
)
{% endif %}
