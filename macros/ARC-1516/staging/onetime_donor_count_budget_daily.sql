select
    date_day,
    'daily' as interval_type,
    donor_audience,
    platform as join_source,
    sum(total_revenue_budget_by_day) as onetime_donor_count_budget,
    sum(loyalty_new_donor_targets_by_day) as onetime_new_donor_count_budget,
    sum(total_revenue_budget_by_days) over (
        partition by
            donor_audience,
            platform,
            {{
                dbt_arc_functions.get_fiscal_year(
                    "date_day", var("fiscal_year_start")
                )
            }}
        order by date_day
    ) as onetime_donor_count_budget_cumulative,
    sum(loyalty_new_donor_targets_by_day) over (
        partition by
            donor_audience,
            platform,
            {{
                dbt_arc_functions.get_fiscal_year(
                    "date_day", var("fiscal_year_start")
                )
            }}
        order by date_day
    ) as onetime_new_donor_count_cumulative,
from {{ ref("stg_audience_budget_by_day") }}
where donor_audience != 'recurring'
