{% macro create_stg_stitch_google_paidmedia_spends_daily_rollup_by_country_by_country(
    source_name="src_stitch_googleads_paidmedia",
    source_table="ad_performance_report"
) %}
    select
        cast(id as string) as message_id,
        cast(date as timestamp) as date_timestamp,
        geographic_view_country_criterion_id as country,
        sum((cost_micros / 1000000)) as spend_amount
    from {{ source(source_name, source_table) }}
    where campaign_status = 'ENABLED'
    group by 1, 2, 3
{% endmacro %}
