{% macro create_stg_stitch_sfmc_parameterized_fundraiseup_filtered_recent_transactions(
    reference_name="stg_src_stitch_sfmc_fundraiseup_recent_transaction",
    reference_name1="stg_stitch_sfmc_parameterized_bbcrm_transactions",
    message_id="NULL",
    recurring="NULL"
) %}
with bbcrm as (select * from {{ ref(reference_name1) }})
select
    revenue_id as transaction_id,
    lookup_id as person_id,
    initial_market_source as source_code,
    safe_cast('sfmc_fundraiseup' as string) as crm,
    safe_cast('sfmc_fundraiseup' as string) as crm_entity,
    safe_cast({{ message_id }} as int) as message_id,
    transaction_date,
    amount,
    appeal,
    safe_cast(null as string) as appeal_business_unit,
    cast({{ recurring }} as boolean) as recurring,
    cast({{ recurring }} as boolean) as recurring_revenue,
from {{ ref(reference_name) }}
where transaction_date > (select max(transaction_date) from bbcrm)

{% endmacro %}
