{% macro create_stg_adhoc_google_spreadsheets_paidmedia_monthly_goals() %}
SELECT 
SAFE_CAST(INITCAP(month) || ' ' || year as STRING) as month_year,
SAFE_CAST(lower(objective) as STRING) as objective,
SAFE_CAST(lower(channel) as STRING) as channel,
SAFE_CAST(lower(platform) as STRING) as platform,
SAFE_CAST(projected_spend AS float64) as projected_spend,
SAFE_CAST(projected_revenue as float64) as projected_revenue
 FROM {{ source('adhoc_google_spreadsheets_paidmedia','spreadsheet_paidmedia_monthly_goals') }}
{% endmacro %}
