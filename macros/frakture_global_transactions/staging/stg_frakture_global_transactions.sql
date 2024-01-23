{% macro create_stg_frakture_global_transactions(
    source_name="frakture_global_transactions",
    source_table_name="transaction_summary"
) %}
select
    regexp_extract(transaction_bot_id, '([A-Za-z_]+)_[a-z0-9]{3}') as source_crm,
    remote_transaction_id as transaction_id_in_source_crm,
    person_id_int as person_id,
    amount - refund_amount as amount,
    safe_cast(
        {{ dbt_date.convert_timezone("cast(ts as TIMESTAMP)") }} as timestamp
    ) as transaction_timestamp,
    recurs != 'Non-recurring' as recurring_revenue,
    recurs_int = 1 and recurs != 'Non-recurring' as new_recurring_revenue,
    transaction_source_code,
    message_id as best_guess_message_id,
    campaign as campaign,
    initcap(channel) as channel,
    audience as audience,
    safe_cast(transaction_bot_id as string) as crm_entity,
    safe_cast(affiliation as string) as source_code_entity,
    source_code_channel as channel_from_source_code
from {{ source(source_name, source_table_name) }}
{% endmacro %}
