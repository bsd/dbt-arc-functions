-- fmt: off
{% macro create_stg_stitch_sfmc_arc_audience_union_transaction_joined_enriched(
    donor_audience_by_day="stg_audience_donor_audience_by_day",
    donor_engagement_by_day="stg_stitch_sfmc_donor_engagement_by_date_day",
    donor_loyalty='stg_stitch_sfmc_donor_loyalty_start_and_end',
    channel="NULL",
    transactions="stg_stitch_sfmc_parameterized_audience_transactions_summary_unioned"
    
) %}

{{ config(
    materialized='table',
    partition_by={
      "field": "transaction_date_day",
      "data_type": "date",
      "granularity": "day"
    },
    cluster_by = ["recurring"]
)}}


 with base as (
    select
        transactions.transaction_id,
        transactions.person_id,
        transactions.transaction_date_day,
        transactions.fiscal_year,
        cast(transactions.amount as float64) as amount,
        transactions.appeal_business_unit,
        {{ channel }} as channel,
        transactions.recurring,
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
        ) as gift_size_string,
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
        ) as gift_size_string_recur,
        donor_audience_by_day.audience_unioned,
        donor_audience_by_day.audience_calculated,
        donor_audience_by_day.coalesced_audience,
        donor_engagement.donor_engagement,
        donor_loyalty.donor_loyalty,
        row_number() over (
            partition by transactions.transaction_id order by transactions.transaction_date_day
        ) as transactions_by_day,
         row_number() over (
                    partition by transactions.person_id order by transactions.transaction_date_day
                ) as gift_count,
        transactions.nth_transaction_this_fiscal_year
    from {{ref(transactions)}} transactions
    left join
        {{ ref(donor_audience_by_day) }} donor_audience_by_day
        on transactions.transaction_date_day = donor_audience_by_day.date_day
        and transactions.person_id = donor_audience_by_day.person_id
    left join
        {{ ref(donor_engagement_by_day) }} donor_engagement
        on transactions.transaction_date_day = donor_engagement.date_day
        and transactions.person_id = donor_engagement.person_id
    left join
        {{ref(donor_loyalty)}} donor_loyalty
        on transactions.person_id = donor_loyalty.person_id
        and transactions.transaction_date_day 
        between donor_loyalty.start_date and donor_loyalty.end_date

        ),

        dedupe as (
            select *
            from base
            where transactions_by_day = 1
        ),


select * from dedupe

{% endmacro %}



   