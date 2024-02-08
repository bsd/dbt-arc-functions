
{{config(severity='warn')}} 
with a as (
    select 
 transaction_date_day as date_day,
 sum(total_revenue) as onetime_revenue_a
from {{ref('mart_arc_revenue_1x_actuals_by_day')}}
group by 1 
),

b as (select 
 transaction_date_day as date_day,
 sum(total_revenue) as recur_revenue_b
from {{ref('mart_arc_revenue_recur_actuals_by_day')}}
group by 1 ),

c as (select 
 date_day as date_day,
 sum(total_revenue_actuals) as all_revenue_c
from {{ref('mart_cashflow_actuals_and_budget')}}
group by 1
),

d as (select 
 transaction_date_day as date_day,
 sum(amount) as all_revenue_d
  from  {{ref('mart_arc_revenue_and_donor_count_by_lifetime_gifts')}}
  group by 1
),

full_join as (
    select 
        coalesce(a.date_day, b.date_day, c.date_day, d.date_day) as date_day,
        a.onetime_revenue_a,
        b.recur_revenue_b,
        c.all_revenue_c,
        d.all_revenue_d
    from a 
    full join b using (date_day)
    full join c using (date_day)
    full join d using (date_day)
),

issues as (
select 
    date_day, 
    sum(case
            when all_revenue_c != all_revenue_d
            then 1 else 0 
        end) as c_not_equal_d,
    sum(case 
            when (recur_revenue_b + onetime_revenue_a > all_revenue_d)
            then 1 else 0
        end) as b_and_a_greater_than_d,
    sum(case 
            when (recur_revenue_b + onetime_revenue_a > all_revenue_c)
            then 1 else 0
        end) as b_and_a_greater_than_c
    from full_join
)

select * from full_join
where c_not_equal_d > 0 or b_and_a_greater_than_d > 0 or b_and_a_greater_than_c >0




