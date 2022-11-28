{% macro create_stg_paidmedia_goals_monthly_actuals_rollup_transform(
    actuals='stg_paidmedia_goals_monthly_actuals_rollup',
    goals='stg_adhoc_google_spreadsheets_paidmedia_monthly_goals') %}
SELECT
     coalesce(actuals.month_year, goals.month_year) as month_year,
     coalesce(actuals.channel, goals.channel) as channel,
      coalesce(actuals.objective, goals.objective) as objective,
      coalesce(actuals.platform, goals.platform) as platform,
      sum(actuals.actual_spend) as actual_spend,
      sum(actuals.actual_revenue) as actual_revenue,
      sum(actuals.actual_donations) as actual_donations,
      sum(goals.projected_spend) as projected_spend,
      sum(goals.projected_revenue) as projected_revenue
FROM {{ ref(actuals) }} actuals
FULL OUTER JOIN {{ ref(goals)}} goals 
ON actuals.month_year = goals.month_year
and actuals.channel = goals.channel
and actuals.objective = goals.objective 
and actuals.platform = goals.platform
GROUP BY 1, 2, 3, 4
{% endmacro %}