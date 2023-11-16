{% macro create_stg_stitch_sfmc_audience_transaction_first_gift(
    transactions="stg_stitch_sfmc_parameterized_audience_transactions_enriched",
    audience='stg_stitch_sfmc_arc_audience_union_transaction_joined_enriched'
) %}

{{ config(
    materialized='table',
    partition_by={
      "field": "first_transaction_date",
      "data_type": "date",
      "granularity": "day"
    },
    cluster_by = ["first_gift_recur_status"]
)}}


/*

This macro finds the first gift that a person gave, 
and pulls attributes from that first gift. 

*/


with first_transactions as (
    select
        person_id,
        min(transaction_date_day) as first_transaction_date
    from {{ ref(transactions) }}
    group by person_id
)
    select
        first_transactions.person_id,
        first_transactions.first_transaction_date,
        cast(timestamp_trunc(first_transactions.first_transaction_date, day) as date) as join_month_year_date,
        format_timestamp('%b %Y', timestamp_trunc(first_transactions.first_transaction_date, month)) as join_month_year_str,
        transactions.inbound_channel as first_gift_join_source,
(
        case
            when transactions.amount between 0 and 25.99
            then '0-25'
            when transactions.amount between 26 and 100.99
            then '26-100'
            when transactions.amount between 101 and 250.99
            then '101-250'
            when transactions.amount between 251 and 500.99
            then '251-500'
            when transactions.amount between 501 and 1000.99
            then '501-1000'
            when transactions.amount between 1001 and 10000.99
            then '1001-10000'
            when transactions.amount > 10000
            then '10000+'
        end
    ) as join_gift_size_string,
    (
        case
            when transactions.amount between 0 and 10.99
            then '0-10'
            when transactions.amount between 11 and 20.99
            then '11-20'
            when transactions.amount between 21 and 30.99
            then '21-30'
            when transactions.amount between 31 and 40.99
            then '31-40'
            when transactions.amount between 41 and 50.99
            then '41-50'
            when transactions.amount between 51 and 60.99
            then '51-60'
            when transactions.amount between 61 and 70.99
            then '61-70'
            when transactions.amount between 71 and 80.99
            then '71-80'
            when transactions.amount between 81 and 90.99
            then '81-90'
            when transactions.amount between 91 and 100.99
            then '91-100'
            when transactions.amount > 100
            then '100+'
        end
    ) as join_gift_size_string_recur,
        transactions.recurring as first_gift_recur_status,
        audience.coalesced_audience as first_gift_donor_audience,
        transactions.amount
    from first_transactions 
    join {{ ref(transactions) }} transactions
    on  first_transactions.person_id = transactions.person_id and  first_transactions.first_transaction_date = transactions.transaction_date_day
    join {{ ref(audience)}} audience
    on  first_transactions.person_id = audience.person_id and first_transactions.first_transaction_date = audience.transaction_date_day



{% endmacro %}
