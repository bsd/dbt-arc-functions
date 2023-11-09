{% macro create_stg_stitch_sfmc_audience_transaction_with_first_gift_cohort(
    transactions="stg_stitch_sfmc_audience_transactions_enriched_rollup",
    first_gift="stg_stitch_sfmc_audience_transaction_first_gift"
) %}

select
transactions.person_id,
transactions.transaction_date_day,
transactions.recurring,
transactions.amounts,
first_gift.join_month_year_str,
first_gift.join_month_year_date,
first_gift.first_gift_join_source,
first_gift.join_gift_size_string,
first_gift.first_gift_donor_audience,
case when first_gift.first_gift_recur_status = True then 'recur' 
when first_gift.first_gift_recur_status = False then 'one_time'
end as first_gift_recur_status
from {{ ref(transactions)}} transactions
left join {{ ref(first_gift)}} first_gift
on transactions.person_id =  first_gift.person_id


