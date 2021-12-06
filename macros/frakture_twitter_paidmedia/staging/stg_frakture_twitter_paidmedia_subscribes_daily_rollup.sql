{% macro create_stg_frakture_twitter_paidmedia_subscribes_daily_rollup(
    reference_name='stg_frakture_twitter_paidmedia_ad_summary_by_date_unioned') %}
SELECT DISTINCT  SAFE_CAST(ad_summary_by_date.message_id AS STRING) AS message_id,
  SAFE_CAST(ad_summary_by_date.date AS TIMESTAMP) AS date_timestamp,
  SAFE_CAST(ad_summary_by_date.conversion_subscribe_total AS INTEGER) AS subscribes
FROM  {{ ref(reference_name) }} ad_summary_by_date
{% endmacro %}