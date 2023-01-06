{% macro create_stg_paidmedia_actuals_budget_by_day_and_campaign_join(
    actuals='stg_paidmedia_pacing_actuals_campaign_and_date_rollup',
    budget='stg_paidmedia_pacing_budget_datespine_details_join'
) %}

SELECT
COALESCE(actuals.date_day, budget.date_day) as date_day,
COALESCE(actuals.campaign_name, budget.campaign_name) as campaign_name,
actuals.daily_spend,
budget.daily_budget,
budget.total_budget - actuals.daily_spend as remaining_budget,
budget.total_budget,
budget.start_date,
budget.end_date,
budget.descriptions

FROM {{ref(actuals)}} actuals
FULL OUTER JOIN {{ref(budget)}} budget
ON actuals.date_day = budget.date_day
and actuals.campaign_name = budget.campaign_name


{% endmacro %}