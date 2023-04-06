{% macro create_stg_stitch_sfmc_bbcrm_audience_transactions(
    reference_name = 'stg_src_stitch_sfmc_bbcrm_transaction'
) %}

Select
        SAFE_CAST(transaction_date as date) as transaction_date,
        SAFE_CAST(revenue_id as STRING) as transaction_id,
        SAFE_CAST(bbcrmlookupid as STRING) as person_id,
        SAFE_CAST(initial_market_source as STRING) as source_code,
        SAFE_CAST(inbound_channel as STRING) as channel,
        SAFE_CAST(amount as FLOAT64) as amount,
        SAFE_CAST(appeal as STRING) as appeal,
        SAFE_CAST(application as STRING) as application_type,
        CASE WHEN lower(application) = 'recurring gift' 
        THEN SAFE_CAST(1 as BOOLEAN) 
        ELSE SAFE_CAST(0 as BOOLEAN) END as recurring

        from {{ref(reference_name)}}

{% endmacro %}
