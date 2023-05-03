{% macro create_mart_paidmedia_monthly_goals_and_actuals(
    reference_name="stg_paidmedia_actuals_goals_unioned_transform"
) %}
    select distinct * from {{ ref(reference_name) }}

{% endmacro %}
