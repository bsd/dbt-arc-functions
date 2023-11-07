{% macro create_stg_frakture_global_message_paidmedia_clicks_daily_rollup(
    source_name="src_stitch_facebook_paidmedia",
    source_table="ads_insights"
) %}
    select
        ad_id as message_id,
        safe_cast(date_start as timestamp) as date_timestamp,
        clicks as total_clicks,
        unique_clicks as unique_clicks
    from {{ source(source_name, source_table) }} ads_insights
{% endmacro %}
