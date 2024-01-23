{% test assert_no_exploding_joins_by_day (model, date_column='date_day')%}

select *
from (
  select
    {{date_column}},
    count(*) as row_count,
    lag(row_count) over (order by {{date_column}}) as previous_row_count
  from {{model}}
  group by 1
  order by date_day asc
) as base_table_grouped
where row_count > previous_row_count


{% endtest %}