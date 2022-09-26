{% macro create_stg_frakture_global_message_email_campaign_dates(
    reference_name='stg_frakture_global_message_email_summary') %}
SELECT 
  SAFE_CAST(publish_date as TIMESTAMP) as campaign_timestamp,
  SAFE_CAST(campaign_name AS STRING) AS crm_campaign,
  SAFE_CAST(coalesce(campaign_name,campaign_label) AS STRING) AS source_code_campaign
FROM {{ ref(reference_name) }}
{% endmacro %}
