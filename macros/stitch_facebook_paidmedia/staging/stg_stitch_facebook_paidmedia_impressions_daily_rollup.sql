{% macro create_stg_frakture_global_message_paidmedia_impressions_daily_rollup(
    source_name="src_stitch_facebook_paidmedia",
    source_table="ads_insights"
) %}
    select
        ad_id as message_id,
        safe_cast(date_start as timestamp) as date_timestamp,
        impressions as total_impressions,
        reach as unique_impressions
    from {{ source(source_name, source_table) }} ads_insights
{% endmacro %}
