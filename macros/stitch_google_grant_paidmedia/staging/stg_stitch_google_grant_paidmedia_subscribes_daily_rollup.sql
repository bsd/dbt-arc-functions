{% macro create_stg_stitch_google_grant_paidmedia_subscribes_daily_rollup(
    source_name="src_stitch_google_grant_paidmedia",
    source_table="ad_performance_report"
) %}

    select distinct
        cast(id as string) as message_id,
        cast(date as timestamp) as date_timestamp,
        cast(null as int64) as subscribes
    from {{ source(source_name, source_table) }}
{% endmacro %}
