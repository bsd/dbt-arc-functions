{% macro create_stg_stitch_google_paidmedia_clicks_daily_rollup(
    source_name="src_stitch_googleads_paidmedia", source_table="ad_performance_report"
) %}
Select 
id as message_id,
cast(date as timestamp) as date,
sum(clicks) as clicks
from {{ source(source_name,source_table) }}
group by 1,2
{% endmacro %}