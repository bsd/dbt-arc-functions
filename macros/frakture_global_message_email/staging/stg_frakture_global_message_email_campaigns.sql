{% macro create_stg_frakture_global_message_email_campaigns(
    reference_name='stg_frakture_global_message_email_summary') %}
SELECT DISTINCT SAFE_CAST(message_id AS STRING) AS message_id,
  SAFE_CAST(bot_nickname as STRING) as crm_entity,
  SAFE_CAST(account_prefix as STRING) as source_code_entity,
  SAFE_CAST(audience as STRING) as audience,
  SAFE_CAST(recurtype as STRING) as recurtype,
  CASE WHEN variant IS NOT NULL then SAFE_CAST(message_set as STRING) 
  WHEN variant IS NULL then NULL end
  as test_group, 
  SAFE_CAST(message_set as STRING) as campaign_category,
  SAFE_CAST(campaign_name AS STRING) AS crm_campaign,
  SAFE_CAST(coalesce(campaign,campaign_label) AS STRING) AS source_code_campaign
FROM {{ ref(reference_name) }}

{% endmacro %}