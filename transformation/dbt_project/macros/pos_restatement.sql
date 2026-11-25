{#
  Explicit POS restatement range from vars pos_restate_from / pos_restate_to
  (YYYY-MM-DD, inclusive). Returns none when neither is set, so models fall
  back to the pos_restatement_days lookback. Returns a dict with the two dates,
  their YYYYMMDD date_keys, every date in the range, and the authorized empty
  days otherwise. returns_to_date / returns_to_key extend the range end by
  pos_return_window_days: the last day a return can reference a restated sale.

  pos_restate_empty_dates lists days inside the range whose corrected
  delivery legitimately has no lines. Only those days are cleared when the
  source has nothing for them; any other day without source lines fails
  tests/pos_restatement_days_have_source_rows.sql.
#}
{% macro pos_restate_range() %}
    {%- set start = var('pos_restate_from', none) -%}
    {%- set end = var('pos_restate_to', none) -%}
    {%- set empty_dates = var('pos_restate_empty_dates', []) -%}
    {%- if start is none and end is none -%}
        {%- if empty_dates -%}
            {{ exceptions.raise_compiler_error(
                "pos_restate_empty_dates needs pos_restate_from and pos_restate_to"
            ) }}
        {%- endif -%}
        {{ return(none) }}
    {%- endif -%}
    {%- if start is none or end is none -%}
        {{ exceptions.raise_compiler_error(
            "pos_restate_from and pos_restate_to must be set together"
        ) }}
    {%- endif -%}
    {%- if empty_dates is string -%}
        {{ exceptions.raise_compiler_error(
            "pos_restate_empty_dates must be a list of YYYY-MM-DD dates"
        ) }}
    {%- endif -%}
    {%- set start = start | string -%}
    {%- set end = end | string -%}
    {%- set empty_dates = empty_dates | map('string') | list -%}
    {%- for value in [start, end] + empty_dates -%}
        {%- if not modules.re.match('^\d{4}-\d{2}-\d{2}$', value) -%}
            {{ exceptions.raise_compiler_error(
                "POS restatement dates must be YYYY-MM-DD, got '" ~ value ~ "'"
            ) }}
        {%- endif -%}
        {%- do modules.datetime.datetime.strptime(value, '%Y-%m-%d') -%}
    {%- endfor -%}
    {%- if start > end -%}
        {{ exceptions.raise_compiler_error(
            "pos_restate_from " ~ start ~ " is after pos_restate_to " ~ end
        ) }}
    {%- endif -%}
    {%- for value in empty_dates -%}
        {%- if value < start or value > end -%}
            {{ exceptions.raise_compiler_error(
                "pos_restate_empty_dates " ~ value ~ " is outside " ~ start ~ ".." ~ end
            ) }}
        {%- endif -%}
    {%- endfor -%}
    {%- set first_day = modules.datetime.datetime.strptime(start, '%Y-%m-%d') -%}
    {%- set last_day = modules.datetime.datetime.strptime(end, '%Y-%m-%d') -%}
    {%- set dates = [] -%}
    {%- for n in range((last_day - first_day).days + 1) -%}
        {%- do dates.append((first_day + modules.datetime.timedelta(days=n)).strftime('%Y-%m-%d')) -%}
    {%- endfor -%}
    {%- set returns_end = (last_day + modules.datetime.timedelta(
        days=var('pos_return_window_days', 90) | int
    )).strftime('%Y-%m-%d') -%}
    {{ return({
        'from_date': start,
        'to_date': end,
        'from_key': start | replace('-', '') | int,
        'to_key': end | replace('-', '') | int,
        'returns_to_date': returns_end,
        'returns_to_key': returns_end | replace('-', '') | int,
        'dates': dates,
        'empty_dates': empty_dates | unique | sort | list,
        'empty_keys': empty_dates | unique | sort | map('replace', '-', '') | map('int') | list,
    }) }}
{% endmacro %}

{#
  Pre-hook for fact_sales and sales_daily_store. delete+insert only replaces
  date_keys that arrive, so an authorized empty day would otherwise keep its
  old rows. Runs against the pending clone during a WAP build.
#}
{% macro pos_clear_empty_days() %}
    {%- set restate = pos_restate_range() -%}
    {%- if restate and restate.empty_keys and is_incremental() -%}
        delete from {{ this }} where date_key in ({{ restate.empty_keys | join(', ') }})
    {%- endif -%}
{% endmacro %}

{#
  Pre-hook for fact_returns. A returns file is a full delivery for its date
  and a day without returns is a zero-row file, so an explicit restatement
  clears the whole range and reloads it; a return deleted from a corrected
  file disappears even when it was the day's last one. The loader (local)
  and the DAG (cloud) fail before dbt when a requested date has no returns
  file, so a missing file is never cleared as an empty day.
#}
{% macro pos_clear_restated_days() %}
    {%- set restate = pos_restate_range() -%}
    {%- if restate and is_incremental() -%}
        delete from {{ this }} where date_key between {{ restate.from_key }} and {{ restate.to_key }}
    {%- endif -%}
{% endmacro %}

{# Later whole return days affected by a corrected or removed original sale. #}
{% macro pos_dependent_return_days(restate, prior_returns) %}
    {% set restate_from = "cast('" ~ restate.from_date ~ "' as date)" %}
    {% set restate_to = "cast('" ~ restate.to_date ~ "' as date)" %}
    {% set returns_to = "cast('" ~ restate.returns_to_date ~ "' as date)" %}
    select r.return_date
    from {{ ref('stg_pos_returns') }} r
    join {{ ref('stg_pos_transactions') }} p
      on p.transaction_id = r.original_transaction_id
     and p.line_item_number = r.original_line_item_number
    where p.transaction_date between {{ restate_from }} and {{ restate_to }}
      and p.dt between {{ restate_from }} and {{ restate_to }}
      and r.return_date between {{ restate_from }} and {{ returns_to }}
      and r.dt between {{ restate_from }} and {{ returns_to }}
    union
    select {{ date_from_date_key('date_key') }}
    from {{ prior_returns }}
    where original_date_key between {{ restate.from_key }} and {{ restate.to_key }}
{% endmacro %}
