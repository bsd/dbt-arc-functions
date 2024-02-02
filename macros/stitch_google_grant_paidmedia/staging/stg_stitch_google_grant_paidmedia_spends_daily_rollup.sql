{% macro create_stg_stitch_google_grant_paidmedia_spends_daily_rollup(
    source_name="src_stitch_google_grant_paidmedia",
    source_table="ad_performance_report"
) %}
select
    cast(id as string) as message_id,
    cast(date as timestamp) as date_timestamp,
    (cost_micros / 1000000) as spend_amount
from {{ source(source_name, source_table) }}
where campaign_status = 'ENABLED'
{% endmacro %}
