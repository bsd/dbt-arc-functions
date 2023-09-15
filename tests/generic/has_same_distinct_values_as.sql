{% test has_same_distinct_values_as(model, column_name, other_model, other_column_name) %}

with this_model as (

    select distinct
        {{ column_name }} as test_column

    from {{ model }}

),

other_model as (

    select distinct
        {{ other_column_name }} as test_column

    from {{ other_model }}

)

select *
from this_model
full outer join other_model using (test_column)
where this_model.test_column is null or other_model.test_column is null

{% endtest %}