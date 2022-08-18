{% macro create_stg_frakture_global_message_email_campaigns(
    reference_name='stg_frakture_global_message_email_summary') %}
SELECT DISTINCT SAFE_CAST(message_id AS STRING) AS message_id
  , SAFE_CAST(bot_nickname as STRING) as crm_entity
  , SAFE_CAST(account_prefix as STRING) as source_code_entity
  ,SAFE_CAST(audience as STRING) as audience
FROM {{ ref(reference_name) }}
{% endmacro %}