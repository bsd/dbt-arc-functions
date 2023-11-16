{% macro create_stg_stitch_facebook_paidmedia_transactions_sourced_rollup(
    source_name="src_stitch_facebook_paidmedia",
    source_table="ads_insights"
) %}
    select
        cast(null AS string) as message_id,
        cast(null as timestamp) as date_timestamp,
        cast(null as int) as total_revenue,
        cast(null as int) as total_gifts,
        cast(null as int) as total_donors,
        cast(null as int) as one_time_revenue,
        cast(null as int) as one_time_gifts,
        cast(null as int) as new_monthly_revenue,
        cast(null as int) as new_monthly_gifts,
        cast(null as int) as total_monthly_revenue,
        cast(null as int) as total_monthly_gifts,
        cast(null as string) as objective,
        cast(null as string) as campaign,
        cast(null as string) as campaign_label,
        cast(null as string) as audience,
        cast(null as string) as appeal,
        cast(null as string) as source_code,
    from {{ source(source_name, source_table) }} ad_summary
{% endmacro %}
