{% macro create_stg_audience_first_transaction_this_fy(
    transactions="stg_audience_parameterized_transactions_summary_unioned"
) %}

with 

transaction_fy as (
select
    transaction_date_day,
    person_id,
    case when nth_transaction_this_fiscal_year = 1 
    then True 
    else False 
    end as first_transaction_this_fiscal_year
from {{ ref(transactions) }}
)
    
    
select * from transaction_fy 
where first_transaction_this_fiscal_year = True

{% endmacro %}