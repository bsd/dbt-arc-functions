{% macro revenue_CTE(revenue_column='total_revenue',
                    mart_name,
                    frequency,
                    day_column='transaction_date_day',
                    ) %}


select 
 {{day_column}} as date_day,
 sum({{revenue_column}}) as {{frequency}}_revenue
from {{ref(mart_name)}}
group by 1

{% endmacro %}