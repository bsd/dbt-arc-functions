{% macro create_stg_stitch_google_paidmedia_subscribes_daily_rollup(
    source_name="src_stitch_googleads_paidmedia", source_table="ad_performance_report"
) %}

Select
cast(id as string) as message_id,
cast(date as timestamp) as date_timestamp,
cast(null as INT64) as subscribes
from {{ source(source_name, source_table ) }}
{% endmacro %}