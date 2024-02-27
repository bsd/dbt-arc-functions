{{ config(severity="warn") }}

with activation_mart as (
    select distinct 
    join_month_year_date as join_date,
    donors_in_cohort as new_donors
    from {{ref("mart_audience_1x_activation")}}
),

onetime_donors as (
    select
    date_day as join_date
    sum(new_onetime_donor_counts) as new_donors
    from {{ref("mart_audience_budget_with_audience_transaction")}}
    where lower(interval_type) = 'monthly'
    and new_onetime_donor_counts > 0
)

select 
join_date, count(*)
from activation_mart 
full join onetime_donors using (join_date)
where activation_mart.new_donors != onetime_donors.new_donors
group by 1


