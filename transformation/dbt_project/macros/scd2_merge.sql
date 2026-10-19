{% macro scd2_merge(target_relation, source_relation, natural_key, tracked_columns) %}
  merge into {{ target_relation }} as tgt
  using {{ source_relation }} as src
    on tgt.{{ natural_key }} = src.{{ natural_key }}
   and tgt.is_current = true
  when matched and tgt.record_hash != src.record_hash then
    update set
      tgt.effective_to = src.effective_from,
      tgt.is_current = false
  when not matched then
    insert (
      {{ natural_key }},
      {{ tracked_columns | join(', ') }},
      effective_from,
      effective_to,
      is_current,
      record_hash
    )
    values (
      src.{{ natural_key }},
      {% for column in tracked_columns -%}
      src.{{ column }}{% if not loop.last %}, {% endif %}
      {%- endfor %},
      src.effective_from,
      null,
      true,
      src.record_hash
    );
{% endmacro %}
