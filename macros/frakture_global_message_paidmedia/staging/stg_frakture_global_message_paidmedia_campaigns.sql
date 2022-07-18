{% macro create_stg_frakture_global_message_paidmedia_campaigns(
    reference_name='stg_frakture_global_message_paidmedia_ad_summary_by_date') %}
WITH BASE AS (SELECT SAFE_CAST(campaign_id AS STRING) AS campaign_id
  , SAFE_CAST(message_id AS STRING) AS message_id
  , SAFE_CAST(channel AS STRING) AS channel
  , SAFE_CAST(type AS STRING) AS channel_type
  , SAFE_CAST(campaign_name AS STRING) AS campaign_name
FROM {{ ref(reference_name) }})

SELECT DISTINCT * FROM BASE
{% endmacro %}