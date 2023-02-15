{% macro create_stg_stitch_sfmc_email_campaigns(
    reference_name='stg_stitch_sfmc_email_summary') %}
SELECT DISTINCT SAFE_CAST(message_id AS STRING) AS message_id,
  SAFE_CAST(bot_nickname as STRING) as crm_entity,
  SAFE_CAST(account_prefix as STRING) as source_code_entity,
  SAFE_CAST(audience as STRING) as audience,
  SAFE_CAST(recurtype as STRING) as recurtype,
  SAFE_CAST(message_set as STRING) as campaign_category,
  SAFE_CAST(campaign_name AS STRING) AS crm_campaign,
  SAFE_CAST(coalesce(campaign,campaign_label) AS STRING) AS source_code_campaign
FROM {{ ref(reference_name) }}

{% endmacro %}