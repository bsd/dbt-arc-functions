{% macro create_stg_paidmedia_pacing_budget_datespine_details_join(
    datespine='stg_paidmedia_pacing_budget_with_datespine',
    details='stg_adhoc_google_spreadsheets_paidmedia_pacing_budget'
) %}

SELECT 
datespine.date_day,
details.start_date,
details.end_date,
datespine.campaign_name,
datespine.daily_budget,
details.description,
details.budget as total_budget

FROM {{ref(datespine)}} datespine
LEFT JOIN 
{{ref(details)}} details 
ON datespine.campaign_name = details.campaign_name

{% endmacro %} 
