{% macro create_stg_frakture_global_transactions(
    source_name='frakture_global_transactions',
    source_table_name='transaction_summary') %}
SELECT
  REGEXP_EXTRACT(transaction_bot_id,'([A-Za-z_]+)_[a-z0-9]{3}') AS source_crm,
  remote_transaction_id AS transaction_id_in_source_crm,
  person_id_int AS person_id,
  amount - refund_amount AS amount,
  SAFE_CAST({{ dbt_date.convert_timezone('cast(ts as TIMESTAMP)') }} as TIMESTAMP) as transaction_timestamp,
  recurs != 'Non-recurring' AS recurring_revenue,
  recurs_int = 1
  AND recurs != 'Non-recurring' AS new_recurring_revenue,
  transaction_source_code,
  message_id AS best_guess_message_id,
  campaign AS campaign,
  channel AS channel,
  audience AS audience,
  safe_cast(transaction_bot_id as STRING) as crm_entity,
  safe_cast(affiliation as STRING) as source_code_entity,
  source_code_channel AS channel_from_source_code
FROM
  {{ source(source_name,source_table_name) }}
{% endmacro %}