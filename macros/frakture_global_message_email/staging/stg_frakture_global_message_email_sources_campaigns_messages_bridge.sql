{% macro create_stg_frakture_global_message_email_sources_campaigns_messages_bridge(
    reference_name='stg_frakture_global_message_email_summary_by_date') %}
SELECT DISTINCT SAFE_CAST(message_id AS STRING) AS message_id,
SAFE_CAST(publish_date AS TIMESTAMP) AS date_timestamp,
       SAFE_CAST(primary_source_code AS STRING) AS source_code,       
       SAFE_CAST(campaign_id AS STRING) AS campaign_id
FROM  {{ ref(reference_name) }}
{% endmacro %}
