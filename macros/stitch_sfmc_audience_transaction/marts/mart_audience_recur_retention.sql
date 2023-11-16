{% macro create_mart_audience_recur_retention() %}

{{ dbt_arc_functions.util_mart_audience_retention_activation(recur_status='recurring') }}

{% endmacro %}


