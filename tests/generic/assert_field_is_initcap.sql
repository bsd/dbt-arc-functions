{% test assert_field_is_initcap(model, column_name) %}

{{ config(severity="warn") }}

with distinct_values as (select distinct {{column}} as actual_value from {{model}})

select * from distinct_values 
where actual_value != initcap(actual_value)

{% endtest %}
