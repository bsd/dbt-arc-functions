{% macro create_mart_paidmedia_pacing_budget_actuals_by_day(
    jobs='stg_paidmedia_actuals_campaign_jobs',
    daily='stg_paidmedia_actuals_budget_by_day_and_campaign_join'
) %}

SELECT
daily.date_day,
daily.start_date,
daily.end_date,
daily.campaign_name,
daily.daily_spend,
daily.daily_budget,
daily.remaining_budget,
daily.total_budget,
daily.description,
jobs.channel,
jobs.channel_category,
jobs.channel_type,
jobs.objective

FROM {{ref(daily)}} daily
LEFT JOIN {{ref(jobs)}} jobs
ON daily.campaign_name = jobs.campaign_name


{% endmacro %}