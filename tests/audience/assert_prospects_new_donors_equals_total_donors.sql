{{ config(severity="warn") }}


with base as (

select 
date_day,
sum(unique_totalFY_onetime_donor_counts) unique_total_onetime_donor_counts,
sum(new_onetime_donor_counts) as new_onetime_donor_counts
from {{ref('stg_audience_transaction_onetime_donor_counts_daily_rollup')}}
where donor_audience in ('Prospect New', 'Prospect Existing')
and date_day <= date_sub(current_date(), interval 1 year)
group by 1
)

select 
base.*,
abs(unique_total_onetime_donor_counts-new_onetime_donor_counts) as difference
 from base 
where unique_total_onetime_donor_counts != new_onetime_donor_counts
order by date_day desc