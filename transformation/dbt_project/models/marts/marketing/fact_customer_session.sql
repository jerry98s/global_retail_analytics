{{
  config(
    materialized='incremental',
    unique_key='session_id',
    incremental_strategy='delete+insert',
    on_schema_change='append_new_columns',
    dist='customer_key',
    sort='session_date_key',
    post_hook="delete from {{ this }} where session_id not in (select session_id from {{ ref('int_session_reconstruction') }})"
  )
}}

/*
  Session-grain customer behavior fact.

  Grain: one row per session_id (int_session_reconstruction): the producer's
  session_instance_id, or 'legacy:<client>:<session>' for pre-1.3 events.
  customer_key is NULL for anonymous and ambiguous sessions
  (attribution_status says which).

  Incremental strategy: int_session_reconstruction is rebuilt from full
  Silver history each run, so this model compares against {{ this }} (the WAP
  pending clone of live) and delete+inserts every session that is new or whose
  session_fingerprint changed: a late event, or a customer key that moved
  after an identity change. The post_hook deletes sessions that no longer
  exist upstream, which relies on Silver holding the full clickstream history.
*/

with sessions as (
    select s.*
    from {{ ref('int_session_reconstruction') }} s
    {% if is_incremental() %}
    left join {{ this }} t
      on t.session_id = s.session_id
    {# The cloned table predates session_fingerprint. Until the column exists,
       restate every session; the post_hook drops ids that no longer exist. #}
    where t.session_id is null
       {% if column_exists(this, 'session_fingerprint') %}
       or t.session_fingerprint is null
       or t.session_fingerprint <> s.session_fingerprint
       {% else %}
       or true
       {% endif %}
    {% endif %}
)

select
    s.session_id,
    s.session_date_key,
    s.session_start_time,
    s.customer_key,
    s.client_id,
    s.session_duration_seconds,
    s.page_view_count,
    s.product_view_count,
    s.add_to_cart_count,
    s.converted,
    s.order_id,
    s.funnel_depth,
    s.platform,
    s.producer_session_id,
    s.attribution_status,
    s.is_legacy,
    s.session_fingerprint
from sessions s
