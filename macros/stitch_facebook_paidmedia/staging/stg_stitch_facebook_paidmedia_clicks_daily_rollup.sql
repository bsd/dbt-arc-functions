{% macro create_stg_stitch_facebook_paidmedia_clicks_daily_rollup(
    source_name="src_stitch_facebook_paidmedia", source_table="ads_insights"
) %}
    select
        ad_id as message_id,
        safe_cast(date_start as timestamp) as date_timestamp,
        sum(obc.value.value) as total_clicks,
        null as unique_clicks
    from {{ source(source_name, source_table) }} ads_insights
    cross join unnest(outbound_clicks) obc
    group by 1, 2
{% endmacro %}
