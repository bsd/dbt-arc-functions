{% macro create_stg_stitch_google_paidmedia_clicks_daily_rollup_by_country(
    source_name="src_stitch_googleads_paidmedia",
    source_table="geo_performance_report"
) %}
    select
        cast(ad_group_id as string) as message_id,
        cast(date as timestamp) as date_timestamp,
        geographic_view_country_criterion_id as country,
        sum(clicks) as total_clicks,
        sum(clicks) as unique_clicks
    from {{ source(source_name, source_table) }}
    where campaign_status = 'ENABLED'
    group by 1, 2, 3
{% endmacro %}
