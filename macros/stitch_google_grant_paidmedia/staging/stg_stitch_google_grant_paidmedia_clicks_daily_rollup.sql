{% macro create_stg_stitch_google_grant_paidmedia_clicks_daily_rollup(
    source_name="src_stitch_google_grant_paidmedia",
    source_table="ad_performance_report"
) %}
    select
        cast(id as string) as message_id,
        cast(date as timestamp) as date_timestamp,
        sum(clicks) as total_clicks,
        sum(clicks) as unique_clicks
    from {{ source(source_name, source_table) }}
    group by 1, 2
{% endmacro %}
