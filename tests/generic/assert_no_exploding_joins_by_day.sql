{% test assert_no_exploding_joins_by_day(model, date_column="date_day") %}

with
    base_table_grouped as (
        select {{ date_column }}, count(*) as row_count from {{ model }} group by 1
    ),

    add_lag as (

        select
            {{ date_column }},
            row_count,
            lag(row_count) over (order by {{ date_column }}) as previous_row_count
        from base_table_grouped
        order by date_day asc
    )
select count(*)
from add_lag
where row_count > previous_row_count
having count(*) > 0

{% endtest %}
