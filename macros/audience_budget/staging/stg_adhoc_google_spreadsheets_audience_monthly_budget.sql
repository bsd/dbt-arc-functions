{% macro create_stg_adhoc_google_spreadsheets_audience_monthly_budget() %}
    select
        SAFE_CAST(start_date AS TIMESTAMP) as start_date,
        SAFE_CAST(end_date AS TIMESTAMP) as end_date,
        SAFE_CAST(platform AS STRING) as platform,
        SAFE_CAST(donor_audience AS STRING) as donor_audience,
        SAFE_CAST(total_revenue_budget AS FLOAT64) as total_revenue_budget,
        SAFE_CAST(loyalty_new_donor_targets AS FLOAT64) as loyalty_new_donor_targets,
        SAFE_CAST(loyalty_unknown_donor_targets AS FLOAT64) as loyalty_unknown_donor_targets,
        SAFE_CAST(loyalty_retained_donor_targets AS FLOAT64) AS loyalty_retained_donor_targets,
        SAFE_CAST(loyalty_retained_three_donor_targets AS FLOAT64) as loyalty_retained_three_donor_targets,
        SAFE_CAST(loyalty_reinstated_donor_targets AS FLOAT64) as loyalty_reinstated_donor_targets,
    from {{ source("audience_budget", "spreadsheet_audience_monthly_budget") }}

{% endmacro %}
