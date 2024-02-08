

with a as (
    select 
 transaction_date_day as date_day,
 sum(donors) as all_donors_a
from {{ref('mart_arc_revenue_and_donor_count_by_lifetime_gifts')}}
group by 1 
),

b as (
    select 
 date_day as date_day,
 sum(donors) as recur_donors_b
from {{ref('mart_arc_revenue_recur_donor_counts_by_gift_size')}}
where interval_type = 'daily'
group by 1 

),

c as (
    select 
 date_day as date_day,
 sum(total_recur_donor_counts) as recur_donors_c
from {{ref('mart_audience_budget_with_audience_transaction_recur')}}
where interval_type = 'daily'
group by 1 

)

d as (
    select 
 date_day as date_day,
 sum(total_recur_donor_counts) as onetime_donors_d
from {{ref('mart_audience_budget_with_audience_transaction')}}
where interval_type = 'daily'
group by 1 

)
    select 
        date_day,
        onetime_donors_d,
        recur_donors_c,
        recur_donors_b
        all_donors_a
    from a 
    full join b using date_day
    full join c using date_day
    where 
        recur_donors_c != recur_donors_b 
        or onetime_donors_d + recur_donors_c > all_donors_a 
        or onetime_donors_d + recur_donors_b > all_donors_a




