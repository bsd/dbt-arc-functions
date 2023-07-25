{% macro create_stg_audience_budget_by_day() %}
{% set number_of_days_in_budget = (
    "date_diff(budget.start_date, budget.end_date, day)"
) %}
with
    dailies as (
        select
            platform,
            donor_audience,
            date_spine.date_day,
            extract(year from date_spine.date_day) as date_spine_year,
            extract(month from date_spine.date_day) as date_spine_month,
            extract(day from date_spine.date_day) as date_spine_day,
            total_revenue_budget
            / {{ number_of_days_in_budget }} as total_revenue_budget_by_day,
            loyalty_new_donor_targets
            / {{ number_of_days_in_budget }} as loyalty_new_donor_targets_by_day,
            loyalty_unknown_donor_targets
            / {{ number_of_days_in_budget }} as loyalty_unknown_donor_targets_by_day,
            loyalty_retained_donor_targets
            / {{ number_of_days_in_budget }} as loyalty_retained_donor_targets_by_day,
            loyalty_retained_three_donor_targets
            / {{ number_of_days_in_budget }}
            as loyalty_retained_three_donor_targets_by_day,
            loyalty_reinstated_donor_targets
            / {{ number_of_days_in_budget }} as loyalty_reinstated_donor_targets_by_day

        from
            {{ ref("stg_adhoc_google_spreadsheets_audience_monthly_budget") }} as budget
        inner join
            {{
                ref(
                    "stg_audience_budget_date_spine"
                )
            }} as date_spine
            on budget.start_date <= date_spine.date_day
            and budget.end_date >= date_spine.date_day
        order by 1, 2, 3
    )

select
    platform,
    donor_audience,
    date_day,
    date_spine_year,
    date_spine_month,
    date_spine_day,
    total_revenue_budget_by_day,
    loyalty_new_donor_targets_by_day,
    loyalty_unknown_donor_targets_by_day,
    loyalty_retained_donor_targets_by_day,
    loyalty_retained_three_donor_targets_by_day,
    loyalty_reinstated_donor_targets_by_day,
    loyalty_new_donor_targets_by_day
    + loyalty_unknown_donor_targets_by_day
    + loyalty_retained_donor_targets_by_day
    + loyalty_retained_three_donor_targets_by_day
    + loyalty_reinstated_donor_targets_by_day as total_donors_by_day
from dailies

{% endmacro %}