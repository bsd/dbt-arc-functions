{% macro create_stg_stitch_sfmc_bbcrm_transactions(
    reference_name="stg_src_stitch_sfmc_bbcrm_transaction"
) %}

    select
        revenue_id as transaction_id,
        bbcrmlookupid as person_id,
        initial_market_source as source_code,
        safe_cast('sfmc_bbcrm' as string) as crm,
        safe_cast(
            regexp_extract(initial_market_source, r"sfmc(\d{6})") as int
        ) as message_id,
        inbound_channel,
        transaction_date,
        amount,
        appeal,
        appeal_business_unit,
        application,
        case
            when lower(application) = 'recurring gift'
            then safe_cast(1 as boolean)
            else safe_cast(0 as boolean)
        end as recurring
    from {{ ref(reference_name) }}

{% endmacro %}
