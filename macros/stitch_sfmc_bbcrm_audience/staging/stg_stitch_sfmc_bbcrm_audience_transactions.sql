{% macro create_stg_stitch_sfmc_bbcrm_audience_transactions(
    reference_name="stg_src_stitch_sfmc_bbcrm_transaction"
) %}

select
    safe_cast(transaction_date as date) as transaction_date,
    safe_cast(revenue_id as string) as transaction_id,
    safe_cast(bbcrmlookupid as string) as person_id,
    safe_cast(initial_market_source as string) as source_code,
    safe_cast(inbound_channel as string) as channel,
    safe_cast(amount as float64) as amount,
    safe_cast(appeal as string) as appeal,
    safe_cast(application as string) as application_type,
    case
        when lower(application) = 'recurring gift'
        then safe_cast(1 as boolean)
        else safe_cast(0 as boolean)
    end as recurring

from {{ ref(reference_name) }}

{% endmacro %}
