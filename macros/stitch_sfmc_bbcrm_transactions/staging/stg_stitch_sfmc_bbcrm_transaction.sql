{% macro create_stg_stitch_sfmc_bbcrm_transaction(
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
    transaction_date,
    amount,
    appeal,
    appeal_business_unit,
    application
from {{ ref(reference_name) }}

{% endmacro %}
