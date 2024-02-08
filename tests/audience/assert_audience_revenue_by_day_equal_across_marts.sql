{% macro revenue_CTE(revenue_column,
                    mart_name,
                    frequency,
                    day_column='date_day',
                    )%}


select 
 {{day_column}} as date_day,
 sum({{revenue_column}}) as {{frequency}}_revenue
from {{ref('{{mart_name}}')}}
group by 1

{% endmacro %}


with a as (
 {{revenue_CTE(date_column='transaction_date_day',
                        revenue_column='total_revenue',
                        mart_name='mart_arc_revenue_1x_actuals_by_day',
                        frequency='onetime')}}
),

b as ({{revenue_CTE(date_column='transaction_date_day',
                        revenue_column='total_revenue',
                        mart_name='mart_arc_revenue_recur_actuals_by_day',
                        frequency='recur')}}),

c as ({{revenue_CTE(date_column='transaction_date_day',
                        revenue_column='total_revenue',
                        mart_name='mart_cashflow_actuals_and_budget',
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



