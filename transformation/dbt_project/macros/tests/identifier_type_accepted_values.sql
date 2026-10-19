{% test identifier_type_accepted_values(model, column_name) %}
select *
from {{ model }}
where {{ column_name }} is null
   or {{ column_name }} not in ('loyalty_id', 'customer_id', 'client_id')
{% endtest %}
