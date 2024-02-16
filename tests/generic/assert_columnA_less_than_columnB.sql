{% test assert_columnA_less_than_columnB(model, column_A, column_B, group_by = [], where=None ) %}

{{ config(severity="warn") }}

{% if group_by|length() > 0 %}
  {% set select_group_by = group_by|join(' ,') + ', ' %}
  {% set group_by_clause = 'group by ' + group_by|join(',') %}
{% endif %}


with base as (
select 
{{ select_group_by }}
{{ column_A }} as column_A,
{{column_B}} as column_B
from {{ model }}
{% if where is none %}
{% else %}
where {{where}}
{% endif %}
{{ group_by_clause }}
)

select * from base
where column_A > column_B



{% endtest %}
