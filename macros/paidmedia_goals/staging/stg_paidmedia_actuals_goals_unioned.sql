
{% macro create_stg_paidmedia_actuals_goals_unioned() %}
{{ dbt_utils.union_relations(
relations=[ref('stg_adhoc_google_spreadsheets_paidmedia_monthly_goals'),
 ref('create_stg_paidmedia_goals_monthly_actuals_rollup')]
) }}
{% endmacro %}