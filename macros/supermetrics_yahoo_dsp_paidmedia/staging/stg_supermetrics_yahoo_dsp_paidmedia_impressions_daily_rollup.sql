{% macro create_stg_supermetrics_yahoo_dsp_paidmedia_impressions_daily_roundup(
       source_name='supermetrics_yahoo_dsp_paidmedia',
       source_table_name='alldates_VDSP_AD') 
%}
SELECT  DISTINCT SAFE_CAST(ad_summary_by_date.ad_id AS STRING) AS message_id
       ,SAFE_CAST(ad_summary_by_date.date AS TIMESTAMP)        AS date_timestamp
       ,SAFE_CAST(ad_summary_by_date.impressions AS INT)       AS total_impressions
       ,SAFE_CAST(ad_summary_by_date.impressions AS INT)       AS unique_impressions
FROM {{ source(source_name,source_table_name) }} ad_summary_by_date
{% endmacro %}