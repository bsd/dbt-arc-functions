{% macro create_stg_stitch_facebook_paidmedia_clicks_daily_rollup(
    source_name="src_stitch_facebook_paidmedia",
    source_table="ads_insights"
) %}
    with
        obc as (
            select
                ad_id as message_id,
                safe_cast(date_start as timestamp) as date_timestamp,
                sum(obc.value.value) as total_clicks,
            from {{ source(source_name, source_table) }} ads_insights
            cross join unnest(outbound_clicks) obc
            group by 1, 2
        ),
        uobc as (
            select
                ad_id as message_id,
                safe_cast(date_start as timestamp) as date_timestamp,
                sum(uobc.value.value) as unique_clicks,
            from {{ source(source_name, source_table) }} ads_insights
            cross join unnest(unique_outbound_clicks) uobc
            group by 1, 2
        )
    select obc.date_timestamp, obc.message_id, obc.total_clicks, uobc.unique_clicks,
    from obc
    join uobc using (date_timestamp, message_id)
{% endmacro %}
