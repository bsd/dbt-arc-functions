{% test assert_no_exploding_joins_by_day (model, date_column='date_day')%}

with base_table_grouped as (
  select
    {{date_column}},
    count(*) as row_count
  from {{model}}
  group by 1
  order by 1
)

select *
from base_table_grouped
where row_count > lag(row_count) over (order by {{date_column}})
having count(*) > 0

{% endtest %}