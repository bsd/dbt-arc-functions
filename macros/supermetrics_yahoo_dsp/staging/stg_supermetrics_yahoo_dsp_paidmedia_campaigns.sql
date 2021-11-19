{% macro create_stg_supermetrics_yahoo_dsp_paidmedia_campaigns(
       source_name='supermetrics_yahoo_dsp',
       source_table_name='alldates_VDSP_AD') 
%}
SELECT  DISTINCT SAFE_CAST(ad.campaign_id AS STRING) AS campaign_id
       ,SAFE_CAST(ad.ad_id AS STRING)                AS message_id
       ,SAFE_CAST('yahoo_dsp' AS STRING)             AS channel
       ,SAFE_CAST(ad.campaign_name AS STRING)        AS campaign_name
FROM {{ source(source_name,source_table_name) }} ad
{% endmacro %}