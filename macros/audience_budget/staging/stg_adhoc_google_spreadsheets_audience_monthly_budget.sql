{% macro create_stg_adhoc_google_spreadsheets_audience_monthly_budget() %}
    select
        safe_cast(start_date as datetime) as start_date,
        safe_cast(end_date as datetime) as end_date,
        safe_cast(platform as string) as platform,
        safe_cast(donor_audience as string) as donor_audience,
        safe_cast(total_revenue_budget as float64) as total_revenue_budget,
        safe_cast(loyalty_new_donor_targets as float64) as loyalty_new_donor_targets,
        safe_cast(
            loyalty_unknown_donor_targets as float64
        ) as loyalty_unknown_donor_targets,
        safe_cast(
            loyalty_retained_donor_targets as float64
        ) as loyalty_retained_donor_targets,
        safe_cast(
            loyalty_retained_three_donor_targets as float64
        ) as loyalty_retained_three_donor_targets,
        safe_cast(
            loyalty_reinstated_donor_targets as float64
        ) as loyalty_reinstated_donor_targets,
    from {{ source("audience_budget", "spreadsheet_audience_monthly_budget") }}

{% endmacro %}
