{% macro create_stg_stitch_sfmc_fundraiseup_recent_transactions(
    reference_name="stg_src_stitch_sfmc_fundraiseup_recent_transaction",
    reference_name1="stg_stitch_sfmc_bbcrm_transactions"
) %}
    with bbcrm as (select * from {{ ref(reference_name1) }})
    select
        revenue_id as transaction_id,
        lookup_id as person_id,
        initial_market_source as source_code,
        initial_market_source as transaction_source_code,
        null as channel_from_source_code, -- this can be regex later
        safe_cast('sfmc_fundraiseup' as string) as crm,
        safe_cast(
            regexp_extract(initial_market_source, r"sfmc(\d{6})") as int
        ) as message_id,
        transaction_date,
        timestamp(transaction_date) as transaction_timestamp,
        amount,
        gift_type,
        appeal,
        case
            when lower(gift_type) = 'monthly'
            then safe_cast(1 as boolean)
            else safe_cast(0 as boolean)
        end as recurring,
        null as best_guess_message_id

    from {{ ref(reference_name) }}
    where transaction_date > (select max(transaction_date) from bbcrm)

{% endmacro %}
