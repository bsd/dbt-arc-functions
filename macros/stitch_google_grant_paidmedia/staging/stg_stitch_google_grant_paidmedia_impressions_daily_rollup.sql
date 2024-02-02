{% macro create_stg_stitch_google_grant_paidmedia_impressions_daily_rollup(
    source_name="src_stitch_google_grant_paidmedia",
    source_table="ad_performance_report"
) %}
select
    cast(id as string) as message_id,
    cast(date as timestamp) as date_timestamp,
    sum(impressions) as total_impressions,
    sum(impressions) as unique_impressions
from {{ source(source_name, source_table) }}
where campaign_status = 'ENABLED'
group by 1, 2
{% endmacro %}
