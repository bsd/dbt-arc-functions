
{{config(severity='warn')}}

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
 sum(donor_counts) as recur_donors_b
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

),

d as (
    select 
 date_day as date_day,
 sum(total_onetime_donor_counts) as onetime_donors_d
from {{ref('mart_audience_budget_with_audience_transaction')}}
where interval_type = 'daily'
group by 1 

),

full_join as (
    select 
        coalesce(a.date_day, b.date_day, c.date_day, d.date_day) as date_day,
        d.onetime_donors_d,
        c.recur_donors_c,
        b.recur_donors_b,
        a.all_donors_a
    from a 
    full join b using (date_day)
    full join c using (date_day)
    full join d using (date_day)

),

issues as (
select 
    date_day, 
    sum(case
            when recur_donors_c !=recur_donors_b 
            then 1 else 0 
        end) as c_not_equal_b,
    sum(case 
            when (onetime_donors_d + recur_donors_c > all_donors_a)
            then 1 else 0
        end) as d_and_c_greater_than_a,
    sum(case 
            when (onetime_donors_d + recur_donors_b > all_donors_a)
            then 1 else 0
        end) as d_and_b_greater_than_a
    from full_join
)

select * from full_join 
where c_not_equal_b > 0
or d_and_c_greater_than_a > 0
or d_and_b_greater_than_a > 0




