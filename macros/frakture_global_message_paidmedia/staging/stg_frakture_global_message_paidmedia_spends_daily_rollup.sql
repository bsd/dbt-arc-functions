{% macro create_stg_frakture_global_message_paidmedia_spends_daily_rollup(
    reference_name='stg_frakture_global_message_paidmedia_ad_summary_by_date') %}
SELECT SAFE_CAST(ad_summary_by_date.message_id AS STRING) AS message_id,
  MIN(SAFE_CAST(ad_summary_by_date.date AS TIMESTAMP)) AS date_timestamp,
  SUM(SAFE_CAST(ad_summary_by_date.spend AS NUMERIC)) AS spend_amount
FROM  {{ ref(reference_name) }} ad_summary_by_date
GROUP BY 1
{% endmacro %}