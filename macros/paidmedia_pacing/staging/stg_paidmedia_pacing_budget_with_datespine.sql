{% macro create_stg_adhoc_pacing_budget_with_datespine() %}

with base as (

SELECT 
-- create date_day field that generates date array from start_date to end_date
generate_date_array(start_date, end_date) as date_day,
start_date,
end_date,
campaign_name,
budget as total_budget, 
-- divide budget by days_in_campaign to get daily budget
budget/datediff(end_date, start_date) as daily_budget,
description -- duplicate description for each day
 FROM {{ source('adhoc_google_spreadsheets_paidmedia_pacing','spreadsheet_paidmedia_pacing_budget') }}


{% endmacro %} 
)



SELECT * FROM base
-- cross join with date_day to create a row for each day in the campaign
CROSS JOIN UNNEST(date_day) as date_day 
-- filter out dates that are outside of the campaign start and end dates
WHERE date_day >= start_date AND date_day <= end_date


