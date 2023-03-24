{% macro create_stg_stitch_sfmc_bbcrm_transactions(
    reference_name = 'stg_src_stitch_sfmc_bbcrm_transactions'
) %}

Select
        revenue_id as transaction_id,
        bbcrmlookupid as person_id,
        initial_market_source as source_code,
        SAFE_CAST('sfmc_bbcrm' as STRING) as crm,
        SAFE_CAST(REGEXP_EXTRACT(initial_market_source,r"sfmc(\d{6})") AS INT) as message_id,
        transaction_date,
        amount,
        appeal,
        appeal_business_unit,
        application
        from {{ref(reference_name)}}

{% endmacro %}

