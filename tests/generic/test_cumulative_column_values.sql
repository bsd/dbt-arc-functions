{% test cumulative_column_values(model, cumulative_column, partition_by, order_by) %}

{{ config(severity="warn") }}

{% if partition_by is string %} {% set partition_by = [partition_by] %} {% endif %}
{% if order_by is string %} {% set order_by = [order_by] %} {% endif %}

with
    validating as (
        select
            *,
            lag({{ cumulative_column }}) over (
                partition by {{ partition_by | join(", ") }}
                order by {{ order_by | join(", ") }}
            ) as prev_value
        from {{ model }}
    )

select *
from validating
where
    {{ cumulative_column }} < prev_value
    or ({{ cumulative_column }} is null and prev_value is not null)

{% endtest %}
