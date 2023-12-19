{% macro create_mart_arc_revenue_1x_actuals_by_day() %}

{{ util_mart_arc_revenue_actuals_by_day(recur_status='onetime') }}

{% endmacro %}
