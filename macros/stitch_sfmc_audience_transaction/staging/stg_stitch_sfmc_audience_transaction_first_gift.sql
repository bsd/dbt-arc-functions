{% macro create_stg_stitch_sfmc_audience_transaction_first_gift(
    reference_name="stg_stitch_sfmc_parameterized_audience_transactions_enriched"
) %}

    with
        first_transactions as (
            select
                person_id,
                transaction_date_day as firsttransactiondate,
                inbound_channel as firsttransactioninbound_channel,
                safe_cast(amount as int64) as firsttransactionamount,
                best_guess_inbound_channel,
                row_number() over (
                    partition by person_id order by transaction_date_day asc
                ) as row_number
            from {{ ref(reference_name) }} ft
        )
    select
        ft.person_id,
        cast(
            timestamp_trunc(ft.firsttransactiondate, day) as date
        ) as join_month_year_date,
        ft.best_guess_inbound_channel as first_gift_join_source,
        ft.firsttransactionamount as first_gift_amount_int,
        (
            case
                when ft.firsttransactionamount between 0 and 25
                then '0-25'
                when ft.firsttransactionamount between 26 and 100
                then '26-100'
                when ft.firsttransactionamount between 101 and 250
                then '101-250'
                when ft.firsttransactionamount between 251 and 500
                then '251-500'
                when ft.firsttransactionamount between 501 and 1000
                then '501-1000'
                when ft.firsttransactionamount between 1001 and 10000
                then '1001-10000'
                else '10000+'
            end
        ) as join_gift_size_string,
        (
            case
                when ft.firsttransactionamount between 0 and 10
                then '0-10'
                when ft.firsttransactionamount between 11 and 20
                then '11-20'
                when ft.firsttransactionamount between 21 and 30
                then '21-30'
                when ft.firsttransactionamount between 31 and 40
                then '31-40'
                when ft.firsttransactionamount between 41 and 50
                then '41-50'
                when ft.firsttransactionamount between 51 and 60
                then '51-60'
                when ft.firsttransactionamount between 61 and 70
                then '61-70'
                when ft.firsttransactionamount between 71 and 80
                then '71-80'
                when ft.firsttransactionamount between 81 and 90
                then '81-90'
                when ft.firsttransactionamount between 91 and 100
                then '91-100'
                when ft.firsttransactionamount > 100
                then '100+'
            end
        ) as join_gift_size_string_recur
    from first_transactions as ft
    where ft.row_number = 1

{% endmacro %}
