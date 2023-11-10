{% macro create_stg_stitch_sfmc_audience_transaction_first_gift(
    transactions="stg_stitch_sfmc_parameterized_audience_transactions_enriched",
    audience='stg_stitch_sfmc_arc_audience_union_transaction_joined_enriched'
) %}

with
    renamed as (
        select
            person_id,
            transaction_date_day as first_transaction_date,
            inbound_channel as first_transaction_inbound_channel,
            safe_cast(amount as int64) as first_transaction_amount,
            best_guess_inbound_channel,
            recurring as first_gift_recur_status,
            row_number() over (
                partition by person_id order by transaction_date_day asc
            ) as row_number
        from {{ ref(transactions) }} 
    )

, first_transactions as (
select
    person_id,
    first_transaction_date,
    cast(timestamp_trunc(first_transaction_date, day) as date) as join_month_year_date,
    format_timestamp('%b %Y', timestamp_trunc(first_transaction_date, month)) as join_month_year_str,
    best_guess_inbound_channel as first_gift_join_source,
    first_gift_recur_status,
    first_transaction_amount as first_gift_amount_int,
    (
        case
            when first_transaction_amount between 0 and 25
            then '0-25'
            when first_transaction_amount between 26 and 100
            then '26-100'
            when first_transaction_amount between 101 and 250
            then '101-250'
            when first_transaction_amount between 251 and 500
            then '251-500'
            when first_transaction_amount between 501 and 1000
            then '501-1000'
            when first_transaction_amount between 1001 and 10000
            then '1001-10000'
            else '10000+'
        end
    ) as join_gift_size_string,
    (
        case
            when first_transaction_amount between 0 and 10
            then '0-10'
            when first_transaction_amount between 11 and 20
            then '11-20'
            when first_transaction_amount between 21 and 30
            then '21-30'
            when first_transaction_amount between 31 and 40
            then '31-40'
            when first_transaction_amount between 41 and 50
            then '41-50'
            when first_transaction_amount between 51 and 60
            then '51-60'
            when first_transaction_amount between 61 and 70
            then '61-70'
            when first_transaction_amount between 71 and 80
            then '71-80'
            when first_transaction_amount between 81 and 90
            then '81-90'
            when first_transaction_amount between 91 and 100
            then '91-100'
            when first_transaction_amount > 100
            then '100+'
        end
    ) as join_gift_size_string_recur
from renamed
where row_number = 1
)

select 
first_transactions.*,
audience.coalesced_audience as first_gift_donor_audience
from first_transactions
left join {{ ref(audience) }} audience
on first_transactions.first_transaction_date = audience.transaction_date_day


{% endmacro %}
