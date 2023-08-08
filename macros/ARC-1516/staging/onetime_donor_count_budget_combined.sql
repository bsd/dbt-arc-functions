select
    date_day,
    interval_type,
    donor_audience,
    join_source,
    1 x_donor_count_budget,
    1 x_new_donor_count_budget,
    1 x_donor_count_budget_cumulative,
    1 x_new_donor_count_cumulative,
from {{ ref("onetime_donor_count_budget_daily") }}
union all
select
    date_day,
    interval_type,
    donor_audience,
    join_source,
    1 x_donor_count_budget,
    1 x_new_donor_count_budget,
    1 x_donor_count_budget_cumulative,
    1 x_new_donor_count_cumulative,
from {{ ref("onetime_donor_count_budget_monthly") }}
union all
select
    date_day,
    interval_type,
    donor_audience,
    join_source,
    1 x_donor_count_budget,
    1 x_new_donor_count_budget,
    1 x_donor_count_budget_cumulative,
    1 x_new_donor_count_cumulative,
from {{ ref("onetime_donor_count_budget_yearly") }}
