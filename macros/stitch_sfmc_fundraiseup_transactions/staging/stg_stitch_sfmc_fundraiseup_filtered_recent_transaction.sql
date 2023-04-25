{% macro create_stg_stitch_sfmc_fundraiseup_recent_transaction(
    reference_name="stg_src_stitch_sfmc_fundraiseup_recent_transaction",
    reference_name1="stg_stitch_sfmc_bbcrm_transaction"
) %}
with bbcrm as (select * from {{ ref(reference_name1) }})
select
    revenue_id as transaction_id,
    lookup_id as person_id,
    initial_market_source as source_code,
    safe_cast('sfmc_fundraiseup' as string) as crm,
    safe_cast(
        regexp_extract(initial_market_source, r"sfmc(\d{6})") as int
    ) as message_id,
    transaction_date,
    amount,
    gift_type,
    appeal,
    case
        when lower(gift_type) = 'monthly'
        then safe_cast(1 as boolean)
        else safe_cast(0 as boolean)
    end as recurring

from {{ ref(reference_name) }}
where transaction_date > (select max(transaction_date) from bbcrm)

{% endmacro %}
