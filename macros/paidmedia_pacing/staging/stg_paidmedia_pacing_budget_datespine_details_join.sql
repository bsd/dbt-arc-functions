{% macro create_stg_paidmedia_pacing_budget_datespine_details_join(
    datespine='stg_paidmedia_pacing_budget_with_datespine',
    details='stg_adhoc_google_spreadsheets_paidmedia_pacing_budget'
) %}

SELECT 
datespine.date_day,
details.campaign_start_date,
details.campaign_end_date,
datespine.campaign_name,
datespine.daily_budget,
sum(datespine.daily_budget) over (partition by datespine.campaign_name order by datespine.date_day) as cumulative_budget,
details.descriptions,
details.budget as total_budget

FROM {{ref(datespine)}} datespine
LEFT JOIN 
{{ref(details)}} details 
ON datespine.campaign_name = details.campaign_name

{% endmacro %} 
