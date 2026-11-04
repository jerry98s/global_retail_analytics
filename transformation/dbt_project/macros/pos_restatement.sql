{#
  Explicit POS restatement range from vars pos_restate_from / pos_restate_to
  (YYYY-MM-DD, inclusive). Returns none when neither is set, so models fall
  back to the pos_restatement_days lookback. Returns a dict with the two dates
  and their YYYYMMDD date_keys otherwise.
#}
{% macro pos_restate_range() %}
    {%- set start = var('pos_restate_from', none) -%}
    {%- set end = var('pos_restate_to', none) -%}
    {%- if start is none and end is none -%}
        {{ return(none) }}
    {%- endif -%}
    {%- if start is none or end is none -%}
        {{ exceptions.raise_compiler_error(
            "pos_restate_from and pos_restate_to must be set together"
        ) }}
    {%- endif -%}
    {%- set start = start | string -%}
    {%- set end = end | string -%}
    {%- for value in [start, end] -%}
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
    {{ return({
        'from_date': start,
        'to_date': end,
        'from_key': start | replace('-', '') | int,
        'to_key': end | replace('-', '') | int,
    }) }}
{% endmacro %}
