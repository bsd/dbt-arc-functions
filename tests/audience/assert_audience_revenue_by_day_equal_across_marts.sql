

with a as (
    select 
 transaction_date_day as date_day,
 sum(total_revenue) as onetime_revenue
from {{ref('mart_arc_revenue_1x_actuals_by_day')}}
group by 1 
),

b as (select 
 transaction_date_day as date_day,
 sum(total_revenue) as recur_revenue
from {{ref('mart_arc_revenue_recur_actuals_by_day')}}
group by 1 ),

c as (select 
 transaction_date_day as date_day,
 sum(total_revenue) as all_revenue_c
from {{ref('mart_cashflow_actuals_and_budget')}}),

d as (select 
 transaction_date_day as date_day,
 sum(amount) as all_revenue_d
  from  {{ref('mart_arc_revenue_and_donor_count_by_lifetime_gifts')}}
)
    select 
        coalesce(a.date_day, b.date_day, c.date_day, d.date_day) as date_day,
        a.onetime_revenue,
        b.recur_revenue,
        c.all_revenue_c,
        d.all_revenue_d
    from a 
    full join b using (date_day)
    full join c using (date_day)
    where a.onetime_revenue + b.recur_revenue > c.all_revenue_c 
    or a.onetime_revenue + b.recur_revenue > d.all_revenue_d
    or c.all_revenue_c != d.all_revenue_d




