{% macro create_stg_stitch_sfmc_parameterized_fundraiseup_filtered_recent_transactions(
    reference_name="stg_src_stitch_sfmc_fundraiseup_recent_transaction",
    reference_name1="stg_stitch_sfmc_parameterized_bbcrm_transactions",
    message_id=NULL,
    recurring=NULL,
    recurring_revenue=NULL
) %}
    with bbcrm as (select * from {{ ref(reference_name1) }})
    select
        revenue_id as transaction_id,
        revenue_id as transaction_id_in_source_crm,  -- required for transaction rollup
        lookup_id as person_id,
        initial_market_source as source_code,
        initial_market_source as transaction_source_code,  -- required for transaction rollup
        safe_cast(null as string) as channel_from_source_code,  -- this can be regex later
        safe_cast(null as string) as channel,  -- required for transaction rollup
        safe_cast(null as string) as campaign,  -- required for transaction rollup
        safe_cast(null as string) as audience,  -- required for transaction rollup
        safe_cast('sfmc_fundraiseup' as string) as crm,
        safe_cast('sfmc_fundraiseup' as string) as crm_entity,
        safe_cast(null as string) as source_code_entity,  -- required for transaction rollup
        safe_cast(
            {{message_id}} as int
        ) as message_id,
        transaction_date,
        timestamp(transaction_date) as transaction_timestamp,
        amount,
        safe_cast(null as float64) as new_recurring_revenue,  -- required for transaction rollup
        gift_type,
        appeal,
        cast( {{recurring}} as boolean) as recurring,
        cast( {{recurring_revenue}} as float64) as recurring_revenue,
        safe_cast(null as string) as best_guess_message_id  -- required for transaction rollup
    from {{ ref(reference_name) }}
    where transaction_date > (select max(transaction_date) from bbcrm)

{% endmacro %}
