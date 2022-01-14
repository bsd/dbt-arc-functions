{% macro create_stg_frakture_global_message_paidmedia_impressions_daily_rollup(
    reference_name='stg_frakture_global_message_paidmedia_ad_summary_by_date') %}
SELECT DISTINCT SAFE_CAST(ad_summary_by_date.message_id AS STRING) AS message_id,  SAFE_CAST(ad_summary_by_date.date AS TIMESTAMP) AS date_timestamp,
  SAFE_CAST(ad_summary_by_date.impressions AS INT) AS total_impressions,
  SAFE_CAST(ad_summary_by_date.reach AS INT) AS unique_impressions
FROM {{ ref(reference_name) }} ad_summary_by_date
{% endmacro %}