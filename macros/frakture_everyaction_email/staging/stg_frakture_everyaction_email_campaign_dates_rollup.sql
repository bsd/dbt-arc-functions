{% macro create_stg_frakture_everyaction_email_campaign_dates_rollup(
    reference_name='stg_frakture_everyaction_email_campaign_dates') %}
SELECT 
  COALESCE(crm_campaign,source_code_campaign) AS campaign_name,
  MIN(campaign_timestamp) as campaign_start_timestamp,
  MAX(campaign_timestamp) as campaign_latest_timestamp
FROM {{ ref(reference_name) }}
GROUP BY 1
{% endmacro %}