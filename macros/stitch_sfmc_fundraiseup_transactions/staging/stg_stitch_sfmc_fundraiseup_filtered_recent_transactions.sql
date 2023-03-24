{% macro create_stg_stitch_sfmc_fundraiseup_recent_transactions(
    reference_name = 'stg_src_stitch_sfmc_fundraiseup_recent_transactions',
    reference_name1= 'stg_stitch_sfmc_bbcrm_transactions'
) %}
with bbcrm as(
SELECT *
FROM {{ref(reference_name1)}}
)
    Select
        revenue_id as transaction_id,
        lookup_id as person_id,
        initial_market_source as source_code,
        SAFE_CAST('sfmc_fundraiseup' as STRING) as crm,
        SAFE_CAST(REGEXP_EXTRACT(initial_market_source,r"sfmc(\d{6})") AS INT) as message_id,
        transaction_date,
        amount,
        gift_type,
        appeal
        from {{ref(reference_name)}}
        where transaction_date > (SELECT MAX(transaction_date) FROM bbcrm)





    {% endmacro %}