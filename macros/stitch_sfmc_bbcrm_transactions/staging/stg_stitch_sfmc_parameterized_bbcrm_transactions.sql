{% macro create_stg_stitch_sfmc_parameterized_bbcrm_transactions(
    reference_name="stg_src_stitch_sfmc_bbcrm_transaction",
    message_id="NULL",
    recurring="NULL"
) %}

select
    revenue_id as transaction_id,
    bbcrmlookupid as person_id,
    initial_market_source as source_code,
    safe_cast('sfmc_bbcrm' as string) as crm,
    safe_cast('sfmc_bbcrm' as string) as crm_entity,
    safe_cast({{ message_id }} as int) as message_id,
    transaction_date,
    amount,
    appeal,
    appeal_business_unit,
    safe_cast({{ recurring }} as boolean) as recurring_revenue,
    safe_cast({{ recurring }} as boolean) as recurring,
from {{ ref(reference_name) }}

{% endmacro %}
