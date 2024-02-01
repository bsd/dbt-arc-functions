{% test assert_columnA_less_than_columnB(model, column_A, column_B, group_by ) %}

{% set group_by = [group_by]%}


with base as (
select 
{{group_by | join(" as {{group_by}}, ")}},  
{{ column_A }} as column_A,
{{column_B}} as column_B
from {{ model }}
group by {{ group_by | join(", ") }}
)

select * from base
where column_A > column_B



{% endtest %}
