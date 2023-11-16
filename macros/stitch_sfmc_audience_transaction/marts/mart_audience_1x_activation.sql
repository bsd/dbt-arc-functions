{% macro create_mart_audience_1x_activation() %}

{{ dbt_arc_functions.util_mart_audience_retention_activation(recur_status='onetime') }}
{% endmacro %}


