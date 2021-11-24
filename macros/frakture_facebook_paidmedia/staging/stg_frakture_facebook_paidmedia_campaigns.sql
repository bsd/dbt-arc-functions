{% macro create_stg_frakture_facebook_paidmedia_campaigns(
    reference_name='stg_frakture_facebook_paidmedia_messages_unioned') %}
SELECT DISTINCT SAFE_CAST(campaign_id AS STRING) AS campaign_id
  , SAFE_CAST(message_id AS STRING) AS message_id
  , SAFE_CAST(channel AS STRING) AS channel
  , SAFE_CAST(campaign_name AS STRING) AS campaign_name
FROM {{ ref(reference_name) }}
{% endmacro %}