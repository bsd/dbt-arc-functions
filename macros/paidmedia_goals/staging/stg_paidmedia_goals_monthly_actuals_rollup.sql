{% macro create_stg_paidmedia_goals_monthly_actuals_rollup(
    reference_name='mart_paidmedia_daily_revenue_performance') %}
SELECT
     FORMAT_DATE('%B %Y', CAST(date_timestamp AS date)) as month_year,
     channel_category as channel,
      objective,
      case when channel_type is not null then channel || ' ' || lower(channel_type) 
      else channel end
      as platform,
      SUM(spend_amount) as actual_spend,
      SUM(total_revenue) as actual_revenue,
      SUM(total_gifts) as actual_donations
FROM {{ ref(reference_name) }} 
GROUP BY 1, 2, 3, 4
{% endmacro %}