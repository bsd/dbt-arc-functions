{% macro create_mart_paidmedia_monthly_goals_and_actuals(
    reference_name='stg_paidmedia_actuals_goals_unioned_transform') %}
SELECT DISTINCT * FROM {{ ref(reference_name)}}


{% endmacro %}