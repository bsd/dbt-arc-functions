{% macro create_stg_paidmedia_pacing_budget_with_datespine() %}

with base as (

SELECT 
campaign_start_date,
campaign_end_date,
generate_date_array(campaign_start_date, campaign_end_date) as date_day,
campaign_name,
SAFE_DIVIDE(budget , (date_diff(campaign_end_date, campaign_start_date, day)+1)) as daily_budget,
descriptions 
FROM {{ ref('stg_adhoc_google_spreadsheets_paidmedia_pacing_budget') }}

)


SELECT 
date_day, 
campaign_name,
daily_budget
 FROM base
-- cross join with unnested date_day array to create a row for each day
CROSS JOIN UNNEST(date_day) as date_day
-- filter out dates that are outside of the campaign start and end dates
WHERE date_day >= campaign_start_date AND date_day <= campaign_end_date


{% endmacro %} 

