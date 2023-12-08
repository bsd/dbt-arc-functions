{% test cumulative_column_values(model, cumulative_column, partition_by, order_by) %}

{{ config(severity = 'warn') }}

    with validating as (
        select
            *,
            lag({{ cumulative_column }}) over (partition by {{ partition_by }} order by {{ order_by }}) as prev_value
        from
            {{ model }}
    )

    select
        *
    from
        validating
    where
        {{ cumulative_column }} < prev_value
        or ({{ cumulative_column }} is null and prev_value is not null)

{% endtest %}
