{% test assert_field_is_initcap(model, column_name) %}

{{ config(severity="warn") }}

with actual_values as (select distinct {{column_name}} as actual_value from {{model}}),

expected_values as (select distinct initcap({{column_name}} as expected_value from {{model}}))

select * from actual_values 
where actual_value not in expected_values

{% endtest %}
