{% macro create_stg_frakture_actionkit_email_campaign_dates(
    reference_name='stg_frakture_actionkit_email_summary_unioned') %}
SELECT 
  SAFE_CAST(publish_date as TIMESTAMP) as campaign_timestamp,
  SAFE_CAST(campaign_label AS STRING) AS crm_campaign,
  SAFE_CAST(campaign AS STRING) AS source_code_campaign
FROM {{ ref(reference_name) }}
{% endmacro %}