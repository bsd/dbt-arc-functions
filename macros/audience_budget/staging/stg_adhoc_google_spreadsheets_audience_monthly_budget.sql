{% macro create_stg_adhoc_google_spreadsheets_audience_monthly_budget() %}
select
    start_date,
    end_date,
    platform,
    donor_audience,
    total_revenue_budget,
    loyalty_new_donor_targets,
    loyalty_unknown_donor_targets,
    loyalty_retained_donor_targets,
    loyalty_retained_three_donor_targets,
    loyalty_reinstated_donor_targets,
from {{ source("audience_budget", "spreadsheet_audience_monthly_budget") }}

{% endmacro %}