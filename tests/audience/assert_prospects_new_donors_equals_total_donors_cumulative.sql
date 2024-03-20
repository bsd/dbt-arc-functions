{{ config(severity="warn") }}

with
    base as (

        select
            date_day,
            sum(
                total_onetime_donor_counts_cumulative
            ) as total_onetime_donor_counts_cumulative,
            sum(
                new_onetime_donor_counts_cumulative
            ) as new_onetime_donor_counts_cumulative
        from {{ ref("mart_audience_budget_with_audience_transaction") }}
        where
            lower(interval_type) = 'daily'
            and donor_audience in ('Prospect New', 'Prospect Existing')
            and date_day <= date_sub(current_date(), interval 1 year)
        group by 1
    )

select
    base.*,
    abs(
        total_onetime_donor_counts_cumulative - new_onetime_donor_counts_cumulative
    ) as difference
from base
where total_onetime_donor_counts_cumulative != new_onetime_donor_counts_cumulative
order by date_day desc
