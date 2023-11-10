{% macro create_stg_stitch_sfmc_audience_transaction_first_gift(
    transactions="stg_stitch_sfmc_parameterized_audience_transactions_enriched",
    audience='stg_stitch_sfmc_arc_audience_union_transaction_joined_enriched'
) %}

{{ config(
    materialized='table',
    partition_by={
      "field": "transaction_date_day",
      "data_type": "date",
      "granularity": "day"
    },
    cluster_by = ["first_gift_recur_status"]
)}}


with first_transactions as (
    select
        person_id,
        min(transaction_date_day) as first_transaction_date
    from {{ ref(transactions) }}
    group by person_id
)

, enriched_first_transactions as  (
    select
        first_transactions.person_id,
        first_transactions.first_transaction_date,
        transactions.inbound_channel as first_gift_join_source,
        safe_cast(transactions.amount as int64) as first_gift_amount_int,
        transactions.recurring as first_gift_recur_status,
        audience.coalesced_audience as first_gift_donor_audience
    from first_transactions 
    join {{ ref(transactions) }} transactions
    on  first_transactions.person_id = transactions.person_id and  first_transactions.first_transaction_date = transactions.transaction_date_day
    join {{ ref(audience)}} audience
    on  first_transactions.person_id = audience.person_id and first_transactions.first_transaction_date = audience.transaction_date_day
)

select 
person_id,
first_transaction_date,
cast(timestamp_trunc(first_transaction_date, day) as date) as join_month_year_date,
format_timestamp('%b %Y', timestamp_trunc(first_transaction_date, month)) as join_month_year_str,
first_gift_join_source,
first_gift_donor_audience,
(
        case
            when first_gift_amount_int between 0 and 25
            then '0-25'
            when first_gift_amount_int between 26 and 100
            then '26-100'
            when first_gift_amount_int between 101 and 250
            then '101-250'
            when first_gift_amount_int between 251 and 500
            then '251-500'
            when first_gift_amount_int between 501 and 1000
            then '501-1000'
            when first_gift_amount_int between 1001 and 10000
            then '1001-10000'
            else '10000+'
        end
    ) as join_gift_size_string,
    (
        case
            when first_gift_amount_int between 0 and 10
            then '0-10'
            when first_gift_amount_int between 11 and 20
            then '11-20'
            when first_gift_amount_int between 21 and 30
            then '21-30'
            when first_gift_amount_int between 31 and 40
            then '31-40'
            when first_gift_amount_int between 41 and 50
            then '41-50'
            when first_gift_amount_int between 51 and 60
            then '51-60'
            when first_gift_amount_int between 61 and 70
            then '61-70'
            when first_gift_amount_int between 71 and 80
            then '71-80'
            when first_gift_amount_int between 81 and 90
            then '81-90'
            when first_gift_amount_int between 91 and 100
            then '91-100'
            when first_gift_amount_int > 100
            then '100+'
        end
    ) as join_gift_size_string_recur,
    first_gift_amount_int
from enriched_first_transactions


{% endmacro %}
