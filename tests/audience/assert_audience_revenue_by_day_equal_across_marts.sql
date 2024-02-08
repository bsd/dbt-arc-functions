with a as (
 {{revenue_CTE(mart_name='mart_arc_revenue_1x_actuals_by_day',
              frequency='onetime')}}
),

b as ({{revenue_CTE(mart_name='mart_arc_revenue_recur_actuals_by_day',
                    frequency='recur')}}),

c as ({{revenue_CTE(mart_name='mart_cashflow_actuals_and_budget',
                    frequency='all')}}),

exceptions as (
    select 
        date_day,
        onetime_revenue,
        recur_revenue,
        all_revenue
    from a 
    full join b using date_day
    full join c using date_day
    where onetime_revenue + recur_revenue > all_revenue
)

select date_day, count(*) from exceptions group by 1



