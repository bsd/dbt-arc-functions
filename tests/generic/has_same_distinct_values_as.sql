{% test has_same_distinct_values_as(
    model, column_name, other_model, other_column_name
) %}

    with
        this_model as (

            select distinct {{ column_name }} as test_column from {{ model }}

        ),

        other_model as (

            select distinct {{ other_column_name }} as test_column

            from {{ other_model }}

        ),

        counts as (

            select count(*) as totals
            from this_model
            full outer join
                other_model on this_model.test_column = other_model.test_column
            where
                (this_model.test_column is null or other_model.test_column is null)
                and coalesce(this_model.test_column, other_model.test_column)
                is not null
        )

    select *
    from counts
    where totals > 0

{% endtest %}
