{% macro create_mart_arc_revenue_recur_actuals_by_day() %}

{{ util_mart_arc_revenue_actuals_by_day(recur_status='recurring') }}

{% endmacro %}
