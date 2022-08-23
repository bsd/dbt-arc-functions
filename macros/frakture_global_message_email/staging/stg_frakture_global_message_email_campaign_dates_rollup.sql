{% macro create_stg_frakture_global_message_email_campaign_dates_rollup(
    reference_name='stg_frakture_global_message_email_campaign_dates') %}
SELECT 
  case when crm_campaign is not null 
                then crm_campaign 
                else source_code_campaign END 
                AS campaign_name,
  MIN(campaign_timestamp) as campaign_start_timestamp,
  MAX(campaign_timestamp) as campaign_latest_timestamp
FROM {{ ref(reference_name) }}
GROUP BY 1
{% endmacro %}