{% macro create_stg_stitch_sfmc_audience_transaction_with_first_gift_cohort(
    transactions="stg_stitch_sfmc_audience_transactions_enriched_rollup",
    first_gift="stg_stitch_sfmc_parameterized_audience_transaction_first_gift"
) %}

/*

This macro appends first gift related values to the transaction table on person_id, 
so that every transaction has a record of first-gift-related attributes:
- donor audience
- gift size
- join source
- recur or 1x status

*/
select
    transactions.person_id,
    transactions.transaction_date_day,
    lpad(
        cast(
            date_diff(
                date_trunc(transaction_date_day, month), join_month_year_date, month
            ) as string
        ),
        2,
        '0'
    ) as month_diff_str,
    cast(
        date_diff(
            date_trunc(transaction_date_day, month), join_month_year_date, month
        ) as integer
    ) as month_diff_int,
    transactions.recurring,
    transactions.amounts,
    first_gift.join_month_year_str,
    first_gift.join_month_year_date,
    first_gift.first_gift_join_source,
    first_gift.join_gift_size_string,
    first_gift.join_gift_size_string_recur,
    first_gift.first_gift_donor_audience,
    first_gift.first_gift_recur_status,
    row_number() over (
        partition by transactions.person_id order by transactions.transaction_date_day
    ) as txn_rank,
    case
        when first_gift.first_gift_recur_status = true
        then 'recur'
        when first_gift.first_gift_recur_status = false
        then 'one_time'
    end as first_gift_recur_status_string
from {{ ref(transactions) }} transactions
left join
    {{ ref(first_gift) }} first_gift on transactions.person_id = first_gift.person_id

{% endmacro %}
