{% macro create_stg_stitch_sfmc_fundraiseup_recent_transactions(
    reference_name="stg_src_stitch_sfmc_fundraiseup_recent_transaction",
    reference_name1="stg_stitch_sfmc_bbcrm_transactions"
) %}
    with bbcrm as (select * from {{ ref(reference_name1) }})
    select
        revenue_id as transaction_id,
        revenue_id as transaction_id_in_source_crm,  -- required for transaction rollup
        lookup_id as person_id,
        initial_market_source as source_code,
        initial_market_source as transaction_source_code,  -- required for transaction rollup
        null as channel_from_source_code,  -- this can be regex later
        null as channel,  -- required for transaction rollup
        null as campaign,  -- required for transaction rollup
        null as audience,  -- required for transaction rollup
        safe_cast('sfmc_fundraiseup' as string) as crm,
        safe_cast('sfmc_fundraiseup' as string) as crm_entity,
        null as source_code_entity,  -- required for transaction rollup
        safe_cast(
            regexp_extract(initial_market_source, r"sfmc(\d{6})") as int
        ) as message_id,
        transaction_date,
        timestamp(transaction_date) as transaction_timestamp,
        amount,
        null as new_recurring_revenue,  -- required for transaction rollup
        gift_type,
        appeal,
        case
            when lower(gift_type) = 'monthly'
            then safe_cast(1 as boolean)
            else safe_cast(0 as boolean)
        end as recurring,
        case
            when lower(gift_type) = 'monthly'
            then safe_cast(1 as boolean)
            else safe_cast(0 as boolean)
        end as recurring_revenue,  -- required for transaction rollup
        null as best_guess_message_id  -- required for transaction rollup
    from {{ ref(reference_name) }}
    where transaction_date > (select max(transaction_date) from bbcrm)

{% endmacro %}
