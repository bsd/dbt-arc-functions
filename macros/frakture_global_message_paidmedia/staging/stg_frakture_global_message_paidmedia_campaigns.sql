{% macro create_stg_frakture_global_message_paidmedia_campaigns(
    reference_name='stg_frakture_global_message_paidmedia_ad_summary_by_date') %}
SELECT SELECT DISTINCT SAFE_CAST(campaign_id AS STRING) AS campaign_id,
  SAFE_CAST(message_id AS STRING) AS message_id,
    CASE WHEN REGEXP_CONTAINS(type,'(?i)search')=True THEN 'search'
    WHEN REGEXP_CONTAINS(type,'(?i)ad')=True THEN 'search'
    WHEN REGEXP_CONTAINS(type,'(?i)display')=True THEN 'display'
    WHEN REGEXP_CONTAINS(type,'(?i)video')=True THEN 'display'
    WHEN ad_summary.channel IN ('ad') THEN 'search'
    WHEN ad_summary.channel IN ('facebook_ad', 'promoted_tweet') THEN 'social'
    ELSE CONCAT(ad_summary.channel, ' - ', ad_summary.type) 
  END AS channel_category,
  SAFE_CAST(channel AS STRING) AS channel,
  SAFE_CAST(type AS STRING) AS channel_type,
  SAFE_CAST(campaign_name AS STRING) AS campaign_name,
  SAFE_CAST(bot_nickname as STRING) as crm_entity,
  SAFE_CAST(account_prefix as STRING) as source_code_entity
FROM {{ ref(reference_name) }}
{% endmacro %}