{% test cumulative_column_values(model, cumulative_column, partition_by = [], order_by = []) %}

{{ config(severity="warn") }}

{% if partition_by|length() > 0 %}
  {% set partition_by_clause = group_by|join(',') %}
{% endif %}

{% if order_by|length() > 0 %}
  {% set order_by_clause = group_by|join(',') %}
{% endif %}


with
    validating as (
        select
            *,
            lag({{ cumulative_column }}) over (
                partition by {{ partition_by_clause }}
                order by {{ order_by_clause }}
            ) as prev_value
        from {{ model }}
    )

select *
from validating
where
    {{ cumulative_column }} < prev_value
    or ({{ cumulative_column }} is null and prev_value is not null)

{% endtest %}
