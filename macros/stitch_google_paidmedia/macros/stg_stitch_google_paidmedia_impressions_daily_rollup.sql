{% macro create_stg_stitch_google_paidmedia_impressions_daily_rollup(
    source_name="src_stitch_googleads_paidmedia", source_table="google_ad_performance"
) %}
Select 
id as message_id,
date as date,
sum(impressions) as impressions
from {{ source(source_name, source_table ) }}
group by 1,2 
{% endmacro %}