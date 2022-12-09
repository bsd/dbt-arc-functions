{% macro create_mart_paidmedia_adhoc_pacing_actuals(
    reference_name='stg_paidmedia_adhoc_pacing_actuals_rollup') %}

SELECT 
date_day,
date_trunc(date_day, WEEK) as date_week_sunday,
date_trunc(date_day, WEEK(MONDAY)) as date_week_monday,
date_trunc(date_day, MONTH) as date_month,
date_trunc(date_day, QUARTER) as date_quarter,
date_truc(date_day, YEAR) as date_year,
best_guess_entity,
objective,
channel,
channel_type,
platform,
campaign_name,
audience
actual_spend,
actual_revenue

 FROM {{ref('reference_name')}}


{% endmacro %}