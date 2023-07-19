{% macro create_stg_stitch_sfmc_bbcrm_transactions(
    reference_name="stg_src_stitch_sfmc_bbcrm_transaction"
) %}

    select
        revenue_id as transaction_id,
        revenue_id as transaction_id_in_source_crm, -- required for transaction rollup
        bbcrmlookupid as person_id,
        initial_market_source as source_code,
        initial_market_source as transaction_source_code,  -- required for transaction rollup
        safe_cast('sfmc_bbcrm' as string) as crm,
        safe_cast('sfmc_bbcrm' as string) as crm_entity,  -- required for transaction rollup
        safe_cast(
            regexp_extract(initial_market_source, r"sfmc(\d{6})") as int
        ) as message_id,
        inbound_channel,
        inbound_channel as channel,  -- required field for transaction rollups
        null as channel_from_source_code,  -- this can be regex later
        transaction_date,
        timestamp(transaction_date) as transaction_timestamp,  -- required for transaction rollups
        amount,
        appeal,
        appeal_business_unit,
        null as channel,  -- required for transaction rollup
        null as campaign,  -- required for transaction rollup
        null as audience,  -- required for transaction rollup
        null as source_code_entity,  -- required for transaction rollup
        null as recurring_revenue,  -- required for transaction rollup
        null as new_recurring_revenue,  -- required for transaction rollup
        application,
        case
            when lower(application) = 'recurring gift'
            then safe_cast(1 as boolean)
            else safe_cast(0 as boolean)
        end as recurring,
        null as best_guess_message_id  -- required for transaction rollup
    from {{ ref(reference_name) }}

{% endmacro %}
