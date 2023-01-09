{% macro create_stg_adhoc_google_spreadsheets_paidmedia_pacing_budget() %}
SELECT 
SAFE_CAST(start_date as date) as campaign_start_date,
SAFE_CAST(end_date as date) as campaign_end_date,
SAFE_CAST(lower(campaign_name) as STRING) as campaign_name,
SAFE_CAST(budget AS float64) as budget,
SAFE_CAST(description as string) as descriptions
 FROM {{ source('adhoc_google_spreadsheets_paidmedia_pacing','spreadsheet_paidmedia_pacing_budget') }}
{% endmacro %}
